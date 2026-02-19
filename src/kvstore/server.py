import enum
import grpc
import logging
import queue
import random
import sys
import threading
import time
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

        self._log = [None]          # Raft log: index 0 unused, entries start at index 1
        self._commit_index = 0         # Commit index: highest index known to be committed
        self._last_applied = 0        # Last applied: highest index applied to state machine

        self._next_index = {}   # server_id -> next log index to send
        self._match_index = {}  # server_id -> highest replicated index

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
        with self._state_lock:
            if self._role != ServerRole.LEADER:
                return raft_pb2.GenericResponse(success=False, error="Not leader")

            # Create and append log entry
            entry = raft_pb2.LogEntry(
                term=self._current_term,
                key=request.key,
                value=request.value,
                clientId=request.clientId,
                requestId=request.requestId,
            )
            self._log.append(entry)
            log_index = len(self._log) - 1

        # Wait for commit (heartbeat loop replicates; poll to avoid blocking it)
        timeout = 10
        deadline = time.time() + timeout
        while time.time() < deadline:
            time.sleep(0.02)
            with self._state_lock:
                if self._role != ServerRole.LEADER:
                    return raft_pb2.GenericResponse(success=False, error="Not leader")
                if self._commit_index >= log_index:
                    return raft_pb2.GenericResponse(success=True)

        return raft_pb2.GenericResponse(success=False, error="Timeout waiting for commit")

    def ping(self, request, context):
        return raft_pb2.GenericResponse(success=True)

    def GetState(self, request, context):
        with self._state_lock:
            return raft_pb2.State(
                term=self._current_term, 
                isLeader=self._role == ServerRole.LEADER,
                commitIndex=self._commit_index,
                lastApplied=self._last_applied,
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
                # Candidate's log must be at least as up-to-date as ours (Raft safety)
                our_last_idx = len(self._log) - 1
                our_last_term = self._log[our_last_idx].term if our_last_idx >= 1 else 0
                candidate_up_to_date = (
                    request.lastLogTerm > our_last_term
                    or (request.lastLogTerm == our_last_term and request.lastLogIndex >= our_last_idx)
                )
                if (self._voted_for is None or self._voted_for == candidate_id) and candidate_up_to_date:
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
            
            # If prevLogIndex > 0, leader says "my log has X at index prevLogIndex with term prevLogTerm"
            # Follower must have the same entry there, or we reject
            if request.prevLogIndex > 0:
                if request.prevLogIndex >= len(self._log):
                    # Follower doesn't have an entry at prevLogIndex
                    return raft_pb2.AppendEntriesReply(term=self._current_term, success=False)
                if self._log[request.prevLogIndex].term != request.prevLogTerm:
                    # Term mismatch at prevLogIndex
                    return raft_pb2.AppendEntriesReply(term=self._current_term, success=False)

            for i, entry in enumerate(request.entries):
                log_index = request.prevLogIndex + i + 1
                if log_index < len(self._log):
                    if self._log[log_index].term != entry.term:
                        # Conflict: truncate from here and replace
                        self._log = self._log[:log_index]
                        self._log.append(entry)
                else:
                    self._log.append(entry)

            if request.leaderCommit > self._commit_index:
                self._commit_index = min(request.leaderCommit, len(self._log) - 1)
                self._apply_committed_entries()        
            
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
            last_log_index = len(self._log) - 1
            last_log_term = self._log[last_log_index].term if last_log_index >= 1 else 0

            # Reset election timer so a new election starts if this one fails
            self._reset_election_timer()

        # Launch vote request calls in parallel (outside lock)
        result_queue = queue.Queue()
        for server_id, channel in self._channels.items():
            stub = raft_pb2_grpc.KeyValueStoreStub(channel)
            args = raft_pb2.RequestVoteArgs(
                term=election_term,
                candidateId=self._server_id,
                lastLogIndex=last_log_index,
                lastLogTerm=last_log_term,
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

                        # Initialize leader-only state
                        last_log_index = len(self._log) - 1
                        self._next_index = {
                            sid: last_log_index + 1
                            for sid in self._channels.keys()
                        }
                        self._match_index = {sid: 0 for sid in self._channels.keys()}

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
                # Build AppendEntries args for each follower
                requests = {}  # server_id -> args
                for server_id in self._channels.keys():
                    prev_idx = self._next_index[server_id] - 1
                    prev_term = self._log[prev_idx].term if prev_idx >= 1 else 0
                    entries = self._log[self._next_index[server_id]:]  # entries from next_index onward
                    requests[server_id] = raft_pb2.AppendEntriesArgs(
                        term=term,
                        leaderId=self._server_id,
                        prevLogIndex=prev_idx,
                        prevLogTerm=prev_term,
                        entries=entries,
                        leaderCommit=self._commit_index,
                    )

            result_queue = queue.Queue()
            for server_id, channel in self._channels.items():
                stub = raft_pb2_grpc.KeyValueStoreStub(channel)
                args = requests[server_id]
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
                        return
                    if self._role != ServerRole.LEADER:
                        return
                    if response.success:
                        # Update nextIndex and matchIndex
                        last_sent = requests[server_id].prevLogIndex + len(requests[server_id].entries)
                        self._next_index[server_id] = last_sent + 1
                        self._match_index[server_id] = last_sent
                    else:
                        # Decrement nextIndex and retry next time
                        self._next_index[server_id] = max(1, self._next_index[server_id] - 1)

            # Advance commitIndex if majority replicated (after processing ALL responses)
            with self._state_lock:
                if self._role != ServerRole.LEADER:
                    return
                n = len(self._log) - 1
                while n > self._commit_index:
                    if self._log[n].term != self._current_term:
                        n -= 1
                        continue
                    count = 1  # leader has it
                    for sid, mi in self._match_index.items():
                        if mi >= n:
                            count += 1
                    if count > len(utils.get_active_servers()) // 2:
                        self._commit_index = n
                        self._apply_committed_entries()
                        break
                    n -= 1

            logger.debug(f"{self._server_id} sleeping now for 50ms.")
            threading.Event().wait(0.05)


    def _send_append_entry(self, stub, args, server_id, q):
        try:
            response = stub.AppendEntries(args)
            q.put((server_id, response, None))
        except Exception as e:
            q.put((server_id, None, e))

    def _apply_committed_entries(self):
        """Apply committed entries (last_applied+1 ... commit_index) to state machine.
        _apply_committed_entries must always be called while holding _state_lock"""
        while self._last_applied < self._commit_index:
            self._last_applied += 1
            entry = self._log[self._last_applied]
            # Apply: state_machine[key] = value
            with self._store_lock:
                self._store[entry.key] = entry.value


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
