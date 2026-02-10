import grpc
import raft_pb2
import raft_pb2_grpc
import sys
import threading
from concurrent import futures
import configparser
import random
import raft_pb2, raft_pb2_grpc, utils


class KeyValueStoreServicer(raft_pb2_grpc.KeyValueStoreServicer):
    def __init__(self, server_id):
        self.store = {}  # In-memory key-value store
        self.store_lock = threading.RLock()  # Thread safety
        self.server_id = server_id

        self.peer_servers = self._get_peer_servers()
        self.peer_stubs = {} # server_id -> stub, might be in a bad state if associated server is down

        # Raft variables
        self.currentTerm = 0
        self.votedFor = None
        self.role = "follower" # can be leader, follower, candidate
        self.leaderId = None
        self.state_lock = threading.RLock()

        self.election_timer = None
        self._reset_election_timer()

    def Get(self, request, context):
        key = request.arg  # StringArg has .arg field
        with self.store_lock:
            value = self.store.get(key, "")  # Empty string for missing keys
            return raft_pb2.KeyValue(key=key, value=value)

    def Put(self, request, context):
        with self.store_lock:
            self.store[request.key] = request.value
            return raft_pb2.GenericResponse(success=True)

    def ping(self, request, context):
        return raft_pb2.GenericResponse(success=True)

    def GetState(self, request, context):
        with self.state_lock:
            return raft_pb2.State(term=self.currentTerm, isLeader=self.role == "leader")

    def RequestVote(self, request, context):
        cand_term = request.term
        cand_id = request.candidateId
        last_log_index = request.lastLogIndex
        last_log_term = request.lastLogTerm

        with self.state_lock:
            # become_follower
            if cand_term > self.currentTerm:
                self.currentTerm = cand_term
                self.role = "follower"
                self.votedFor = None
            
            if cand_term >= self.currentTerm and (self.votedFor is None or self.votedFor == cand_id):
                self.votedFor = cand_id
                self._reset_election_timer() # reset election timer if vote is granted
                return raft_pb2.RequestVoteReply(term=self.currentTerm, voteGranted=True)
            
            return raft_pb2.RequestVoteReply(term=self.currentTerm, voteGranted=False)

    def AppendEntries(self, request, context):
        leaderTerm = request.term
        leaderId = request.leaderId
        prevLogIndex = request.prevLogIndex
        prevLogTerm = request.prevLogTerm
        entries = request.entries
        leaderCommit = request.leaderCommit

        with self.state_lock:
            if leaderTerm < self.currentTerm:
                return raft_pb2.AppendEntriesReply(term=self.currentTerm, success=False)
            else:
                # valid heartbeat
                self.role = "follower"
                # become_follower
                if leaderTerm > self.currentTerm:
                    self.currentTerm = leaderTerm
                    self.votedFor = None

                self.leaderId = leaderId
                self._reset_election_timer() # reset election timer if heartbeat is valid

                return raft_pb2.AppendEntriesReply(term=self.currentTerm, success=True)

    def _get_peer_servers(self):
        config = configparser.ConfigParser()
        config.read("config.ini")
        active_str = config.get("Servers", "active")  # Gets "0,1,2,3,4"
        active_ids = [int(id.strip()) for id in active_str.split(",") if int(id.strip()) != self.server_id]
        return active_ids

    def _get_server_stub(self, server_id):
        if server_id in self.peer_stubs:
            return self.peer_stubs[server_id]
        port = 9001 + server_id
        channel = grpc.insecure_channel(f"127.0.0.1:{port}")
        self.peer_stubs[server_id] = raft_pb2_grpc.KeyValueStoreStub(channel)
        return self.peer_stubs[server_id]

    def _reset_election_timer(self):
        if self.election_timer is not None:
            self.election_timer.cancel()

        self.election_timer = threading.Timer(random.uniform(0.15, 0.3), self._start_election)
        self.election_timer.start()
        
    def _start_election(self):
        with self.state_lock:
            if self.role == "follower" or self.role == "candidate":
                self.currentTerm += 1
                self.role = "candidate"
                self.votedFor = self.server_id
                self._reset_election_timer() # reset election timer if election is started (will try again if election fails)
        
            term = self.currentTerm
        
        votes_received = set()
        votes_received.add(self.server_id)
        threads = []   
        vote_lock = threading.RLock()
        for peer_id in self.peer_servers:
            t = threading.Thread(target=self._request_vote_from_peer, args=(peer_id, self.currentTerm, votes_received, vote_lock, self.state_lock))
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

        if len(votes_received) > len(self.peer_servers) / 2:
            with self.state_lock:
                if self.role == "candidate" and self.currentTerm == term:
                    self._become_leader()


    def _request_vote_from_peer(self, peer_id, term, votes_received, vote_lock, state_lock):
        stub = self._get_server_stub(peer_id)
        try:
            reply = stub.RequestVote(raft_pb2.RequestVoteArgs(term=term, candidateId=self.server_id, lastLogIndex=0, lastLogTerm=0), timeout=2)
            if reply.voteGranted:
                with vote_lock:
                    votes_received.add(peer_id)
            if reply.term > self.currentTerm:
                with state_lock:
                    self.currentTerm = reply.term
                    self.role = "follower"
                    self.votedFor = None
        except grpc.RpcError:
            return

    def _become_leader(self):
        pass

def serve(server_id):
    if not utils.is_server_id_valid(server_id):
        raise Exception(f"Invalid server ID: {server_id}")

    port = 9001 + server_id
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    raft_pb2_grpc.add_KeyValueStoreServicer_to_server(
        KeyValueStoreServicer(server_id), server
    )
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    server_id = int(sys.argv[1])
    serve(server_id)
