import enum
import grpc
import logging
import queue
import random
import sys
import threading
from concurrent import futures
from typing import Optional, Any

import raft_pb2, raft_pb2_grpc, utils

logger = logging.getLogger(__name__)


class ServerRole(enum.Enum):
    FOLLOWER = 0
    CANDIDATE = 1
    LEADER = 2


class KeyValueStoreServicer(raft_pb2_grpc.KeyValueStoreServicer):
    def __init__(self, server_id):
        self._store = {}  # In-memory key-value store
        self._store_lock = threading.RLock()  # Thread safety
        self._server_id = server_id

        # Raft state variables (Assignment 3)
        self._state_lock = threading.RLock()
        self._current_term: int = 0
        self._voted_for: Optional[int] = None
        self._role: ServerRole = ServerRole.FOLLOWER
        self._leader_id: Optional[int] = None
        self._election_timer = None
        self._reset_election_timer()

        # Channels
        active_servers = utils.get_active_servers()
        self._channels = {
            server_id: grpc.insecure_channel(f"127.0.0.1:{9001 + server_id}")
            for server_id in active_servers
            if server_id != self._server_id
        }

    def _reset_election_timer(self):
        with self._state_lock:
            if self._election_timer is not None:
                self._election_timer.cancel()

            # Election timeout between 150ms and 300ms
            timeout_seconds = random.uniform(0.15, 0.3)
            self._election_timer = threading.Timer(
                timeout_seconds, self._start_election
            )
            self._election_timer.start()

    def Get(self, request, context):
        key = request.arg  # StringArg has .arg field
        with self._store_lock:
            value = self._store.get(key, "")  # Empty string for missing keys
            return raft_pb2.KeyValue(key=key, value=value)

    def Put(self, request, context):
        with self._store_lock:
            self._store[request.key] = request.value
            return raft_pb2.GenericResponse(success=True)

    def ping(self, request, context):
        return raft_pb2.GenericResponse(success=True)

    def GetState(self, request, context):
        with self._state_lock:
            return raft_pb2.State(
                term=self._current_term, isLeader=(self._role == ServerRole.LEADER)
            )

    def _on_higher_term_discovery(self, term: int):
        with self._state_lock:
            assert term > self._current_term, (
                "Should only call on_higher_term_discovery with a higher term"
            )
            # TODO: Verify that the current role doesn't matter here
            self._role = ServerRole.FOLLOWER
            self._current_term = term
            self._voted_for = None
            self._leader_id = None
            # TODO: Verify that _reset_election_timer should be called here
            self._reset_election_timer()

    def RequestVote(self, request, context):
        """A candidate is requesting a vote in this server"""
        logging.debug(
            f"Received RequestVote from candidate {request.candidateId} for term {request.term}"
        )
        with self._state_lock:
            candidate_term = request.term
            candidate_id = request.candidateId

            # Update term if candidate's is higher
            if candidate_term > self._current_term:
                self._on_higher_term_discovery(candidate_term)

            # Note that the previous higher term discovery may have changed self._current_term, so we need to compare with candidate_term again
            if candidate_term == self._current_term:
                # Same term, and not voted or voted for the one who is asking again
                if self._voted_for is None or self._voted_for == candidate_id:
                    self._voted_for = candidate_id
                    self._reset_election_timer()  # Reset election timer on vote grant
                    logger.debug(
                        f"{self._server_id} voting for candidate {candidate_id} in term {candidate_term}"
                    )
                    return raft_pb2.RequestVoteReply(
                        term=self._current_term, voteGranted=True
                    )

            logger.debug(
                f"{self._server_id} rejecting voting for candidate {candidate_id} in term {candidate_term}"
            )
            return raft_pb2.RequestVoteReply(
                term=self._current_term,
                voteGranted=False,
            )

    def AppendEntries(self, request, context):
        with self._state_lock:
            if request.term < self._current_term:
                return raft_pb2.AppendEntriesReply(
                    term=self._current_term, success=False
                )
            elif request.term > self._current_term:
                self._on_higher_term_discovery(request.term)
            # Same term
            assert (
                self._leader_id is None or self._leader_id == request.leaderId
            )  # sanity
            assert self._role != ServerRole.LEADER  # sanity
            self._leader_id = request.leaderId
            self._reset_election_timer()
            return raft_pb2.AppendEntriesReply(term=self._current_term, success=True)

    def _call_server(
        self,
        stub: raft_pb2_grpc.KeyValueStoreStub,
        request_params: raft_pb2.RequestVoteArgs,
        server_id: int,
        queue: Any,
    ):
        try:
            response = stub.RequestVote(request_params)
            queue.put((server_id, response, None))
        except Exception as e:
            queue.put((server_id, None, e))

    def _start_election(self):
        """One election timeout, advance to candidacy, request votes, possibly become leader"""
        logging.debug(f"{self._server_id} starting leader election")
        active_servers = utils.get_active_servers()

        with self._state_lock:
            # Only followers and candidates can start election
            if self._role == ServerRole.LEADER:
                return

            # Advance to candidacy and vote for self
            self._current_term += 1
            self._role = ServerRole.CANDIDATE
            self._voted_for = self._server_id
            election_term = self._current_term
            votes = {self._server_id}

            # Reset election timer so a new election starts if this one fails
            self._reset_election_timer()

        # Launch vote request calls in parallel (outside lock)
        result_queue = queue.Queue()
        for server_id, channel in self._channels.items():
            stub = raft_pb2_grpc.KeyValueStoreStub(channel)
            args = raft_pb2.RequestVoteArgs(
                term=election_term, candidateId=self._server_id
            )
            threading.Thread(
                target=self._call_server, args=(stub, args, server_id, result_queue)
            ).start()
        logger.debug(f"{self._server_id} requested vote from all peers.")

        # Process results as they arrive (outside lock)
        for _ in range(len(self._channels)):
            server_id, response, error = result_queue.get()
            response: raft_pb2.RequestVoteReply = response
            logger.debug(
                f"{self._server_id} received RequestVote reply from server {server_id}"
            )

            if error:
                continue

            with self._state_lock:
                # We do not need to check if we are still a candidate here.
                if response.term > self._current_term:
                    self._on_higher_term_discovery(response.term)
                    return
                elif response.term < self._current_term:
                    continue
                elif not response.voteGranted:
                    logger.debug(
                        f"{self._server_id} denied vote from peer {server_id} for term {self._current_term}, total votes: {len(votes)}"
                    )
                    continue
                else:  # vote granted
                    votes.add(server_id)
                    logger.debug(
                        f"{self._server_id} received vote from peer {server_id} for term {self._current_term}, total votes: {len(votes)}"
                    )
                    if len(votes) > len(active_servers) // 2:
                        self._role = ServerRole.LEADER
                        self._leader_id = self._server_id
                        self._reset_election_timer()
                        threading.Thread(
                            target=self._send_heartbeats, daemon=True
                        ).start()
                        return

    def _send_heartbeats(self):
        """Periodically send AppendEntries to all peers while leader."""
        while True:
            with self._state_lock:
                if self._role != ServerRole.LEADER:
                    return
                term = self._current_term

            # Send AppendEntries in parallel (outside lock)
            result_queue = queue.Queue()
            for server_id, channel in self._channels.items():
                stub = raft_pb2_grpc.KeyValueStoreStub(channel)
                args = raft_pb2.AppendEntriesArgs(
                    term=term,
                    leaderId=self._server_id,
                )
                threading.Thread(
                    target=self._send_append_entry,
                    args=(stub, args, server_id, result_queue),
                ).start()

            for _ in range(len(self._channels)):
                server_id, response, error = result_queue.get()
                if error:
                    continue
                with self._state_lock:
                    if response.term > self._current_term:
                        self._on_higher_term_discovery(response.term)
                        return  # No longer leader

            logger.debug(f"{self._server_id} sleeping now for 50ms.")
            threading.Event().wait(0.05)

    def _send_append_entry(self, stub, args, server_id, q):
        try:
            response = stub.AppendEntries(args)
            q.put((server_id, response, None))
        except Exception as e:
            q.put((server_id, None, e))


def serve(server_id):
    if not utils.is_server_id_valid(server_id):
        raise Exception(f"Invalid server ID: {server_id}")

    logging.basicConfig(
        filename=f"server_{server_id}.log",
        level=logging.ERROR,
        format=f"%(asctime)s [server {server_id}] %(message)s",
    )

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
