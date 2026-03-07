import enum
import grpc
import logging
import queue
import random
import sys
import threading
import os
import json
from cache import create_cache
from concurrent import futures
from typing import List, Optional, Any

import raft_pb2, raft_pb2_grpc, utils

logger = logging.getLogger(__name__)


class ServerRole(enum.Enum):
    FOLLOWER = 0
    CANDIDATE = 1
    LEADER = 2


class KeyValueStoreServicer(raft_pb2_grpc.KeyValueStoreServicer):
    def __init__(self, server_id):
        logger.debug("init start")
        self._server_id = server_id

        # State lock
        self._state_lock = threading.RLock()

        # Raft state variables (Assignment 3)
        self._current_term: int = 0
        self._voted_for: Optional[int] = None
        self._role: ServerRole = ServerRole.FOLLOWER
        self._leader_id: Optional[int] = None
        self._election_timer = None
        self._reset_election_timer()

        # log related variables
        self._log: List[raft_pb2.LogEntry] = [None]  # type: ignore Index 0 unused, log starts at index 1
        self._commit_index = 0
        self._last_applied = 0
        self._state_machine = {}  # In-memory key-value store
        self._next_index = {}  # Leader specific

        # Cache configuration
        cache_config = utils.get_cache_config()
        write_strategy, cache_capacity, cache_policy = (
            cache_config["write_strategy"],
            cache_config["capacity"],
            cache_config["policy"],
        )
        self._cache_write_strategy = write_strategy
        if cache_policy != "none" and cache_capacity > 0:
            self._cache = create_cache(cache_policy, cache_capacity)
        else:
            self._cache = None

        # Persistence configuration
        self._state_dir = utils.get_persistent_state_path()
        self._state_file: Optional[str] = None
        if self._state_dir != "memory":
            os.makedirs(self._state_dir, exist_ok=True)
            self._state_file = os.path.join(
                self._state_dir, f"server_{self._server_id}.json"
            )
            self._load_state()

        # Channels
        self._active_servers = utils.get_active_servers()
        self._channels = {
            server_id: grpc.insecure_channel(f"127.0.0.1:{9001 + server_id}")
            for server_id in self._active_servers
            if server_id != self._server_id
        }
        logger.debug("init finished")

    def ping(self, request, context):
        """Health check endpoint"""
        return raft_pb2.GenericResponse(success=True)

    def GetState(self, request, context):
        """Return current server state including commit index"""
        with self._state_lock:
            return raft_pb2.State(
                term=self._current_term,
                isLeader=(self._role == ServerRole.LEADER),
                commitIndex=self._commit_index,
                lastApplied=self._last_applied,
            )

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

    def _on_higher_term_discovery(self, term: int):
        with self._state_lock:
            logger.debug(
                f"Higher term discovered, current term {self._current_term}, discovered {term}"
            )
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
            self._next_index.clear()
            self._persist_state()

    def RequestVote(self, request, context):
        """A candidate is requesting a vote in this server"""
        # logger.debug(
        #     f"Received RequestVote from candidate {request.candidateId} for term {request.term}"
        # )
        with self._state_lock:
            candidate_term = request.term
            candidate_id = request.candidateId

            # Update term if candidate's is higher
            if candidate_term > self._current_term:
                self._on_higher_term_discovery(candidate_term)

            # Note that the previous higher term discovery may have changed self._current_term
            # so we need to compare with candidate_term again
            if candidate_term == self._current_term:
                # Same term, and not voted or voted for the one who is asking again
                if self._voted_for is None or self._voted_for == candidate_id:
                    # Log up-to-dateness check (Section 5.4.1)
                    my_last_log_index = len(self._log) - 1
                    my_last_log_term = (
                        self._log[-1].term if my_last_log_index > 0 else 0
                    )
                    if request.lastLogTerm > my_last_log_term or (
                        request.lastLogTerm == my_last_log_term
                        and request.lastLogIndex >= my_last_log_index
                    ):
                        self._voted_for = candidate_id
                        self._reset_election_timer()
                        self._persist_state()
                        return raft_pb2.RequestVoteReply(
                            term=self._current_term, voteGranted=True
                        )

            # logger.debug(
            #     f"{self._server_id} rejecting voting for candidate {candidate_id} in term {candidate_term}"
            # )
            return raft_pb2.RequestVoteReply(
                term=self._current_term,
                voteGranted=False,
            )

    def _send_request_vote(
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
            logger.debug(f"Sending {request_params} failed with exception {e}")
            queue.put((server_id, None, e))

    def _start_election(self):
        """One election timeout, advance to candidacy, request votes, possibly become leader"""
        # logger.debug(f"{self._server_id} starting leader election")
        active_servers = utils.get_active_servers()

        with self._state_lock:
            # Only followers and candidates can start election
            if self._role == ServerRole.LEADER:
                return

            # Advance to candidacy and vote for self
            # logger.debug(
            #     f"Server {self._server_id} starting election for term {self._current_term + 1}"
            # )
            self._current_term += 1
            self._role = ServerRole.CANDIDATE
            self._voted_for = self._server_id
            election_term = self._current_term
            votes = {self._server_id}
            last_log_index = len(self._log) - 1
            last_log_term = self._log[-1].term if last_log_index > 0 else 0

            # Reset election timer so a new election starts if this one fails
            self._reset_election_timer()
            self._persist_state()

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
                target=self._send_request_vote,
                args=(stub, args, server_id, result_queue),
            ).start()
        # logger.debug(f"{self._server_id} requested vote from all peers.")

        # Process results as they arrive (outside lock)
        for _ in range(len(self._channels)):
            server_id, response, error = result_queue.get()
            response: raft_pb2.RequestVoteReply = response
            # logger.debug(
            #     f"{self._server_id} received RequestVote reply from server {server_id}"
            # )

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
                    # logger.debug(
                    #     f"{self._server_id} denied vote from peer {server_id} for term {self._current_term}, total votes: {len(votes)}"
                    # )
                    continue
                else:  # vote granted
                    votes.add(server_id)
                    if len(votes) > len(active_servers) // 2:
                        # Become leader for this term
                        self._role = ServerRole.LEADER
                        self._leader_id = self._server_id
                        self._reset_election_timer()

                        # Immediately append a no-op entry (Raft 5.4.3)
                        noop_entry = raft_pb2.LogEntry(
                            term=self._current_term,
                            key="",
                            value="",
                            clientId=0,
                            requestId=0,
                        )
                        self._log.append(noop_entry)
                        self._persist_state()

                        # Bootstrap self.next_indexes (after appending no-op)
                        self._next_index.clear()
                        for server_id in self._active_servers:
                            if server_id == self._server_id:
                                continue
                            self._next_index[server_id] = len(self._log)

                        # Start sending append_entries to followers
                        threading.Thread(
                            target=self._replicate_to_followers, daemon=True
                        ).start()
                        return

    def _apply_committed_entries(self):
        """Apply committed entries to state machine"""
        while self._last_applied < self._commit_index:
            self._last_applied += 1
            entry = self._log[self._last_applied]
            # Skip no-op entries (used for committing prior terms)
            if entry.key != "":
                self._state_machine[entry.key] = entry.value
                if self._cache is not None:
                    if self._cache_write_strategy == "write_through":
                        self._cache.put(entry.key, entry.value)
                    else:
                        self._cache.invalidate(entry.key)
            logger.debug(f"Applied {entry}")

    def AppendEntries(self, request: raft_pb2.AppendEntriesArgs, context):
        logger.debug(f"AppendEntry request received {request}")
        """Handle AppendEntries RPC with log replication"""
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
            # If was candidate and heard from a leader, step down
            if self._role == ServerRole.CANDIDATE:
                self._role = ServerRole.FOLLOWER
            self._leader_id = request.leaderId
            self._reset_election_timer()

            # Check log consistency
            logger.debug("Server checking log consistency")
            if request.prevLogIndex > 0:
                if request.prevLogIndex >= len(self._log):
                    logger.debug(
                        f"Inconsistency found: {request.prevLogIndex} {len(self._log)}"
                    )
                    return raft_pb2.AppendEntriesReply(
                        term=self._current_term, success=False
                    )
                elif self._log[request.prevLogIndex].term != request.prevLogTerm:
                    logger.debug(
                        f"Inconsistency found: {self._log[request.prevLogIndex].term} {request.prevLogTerm}"
                    )
                    return raft_pb2.AppendEntriesReply(
                        term=self._current_term, success=False
                    )

            # Append new entries
            log_changed = False
            for i, entry in enumerate(request.entries):
                logger.debug(f"Need to append log entry {entry}")
                log_index = request.prevLogIndex + i + 1
                if log_index < len(self._log):
                    # Remove conflicting entry and all that follow
                    if self._log[log_index].term != entry.term:
                        self._log = self._log[:log_index]
                        self._log.append(entry)
                        log_changed = True
                    elif (
                        self._log[log_index] != entry
                    ):  # this branch is purely for debugging
                        # logger.debug(
                        #     f"entries with same log_index and term must match, {self._log[log_index]}, {entry}"
                        # )
                        pass
                else:
                    self._log.append(entry)
                    log_changed = True
            # logger.debug(f"append new entries finished")

            # Update commit index
            if request.leaderCommit > self._commit_index:
                self._commit_index = min(request.leaderCommit, len(self._log) - 1)
                logger.debug(f"Need to apply entries till {self._commit_index}")
                self._apply_committed_entries()

            if log_changed:
                self._persist_state()

            return raft_pb2.AppendEntriesReply(term=self._current_term, success=True)

    def _send_append_entry(self, stub, args, server_id, q):
        try:
            response = stub.AppendEntries(args)
            q.put((server_id, response, None, args))  # TODO: Use an object not tuple
        except Exception as e:
            logger.debug(f"Sending {args} failed with exception {e}")
            q.put((server_id, None, e, args))

    def _replicate_to_followers(self):
        """Periodically send AppendEntries to all peers while leader, also serving as heartbeat."""
        while True:
            result_queue = queue.Queue()
            # for each follower prepare and send an AppendEntry request
            for follower_id, channel in self._channels.items():
                with self._state_lock:  # take lock only for preparing args
                    if self._role != ServerRole.LEADER:
                        return
                    next_index = self._next_index[follower_id]
                    prevLogIndex = next_index - 1
                    prevLogTerm = (
                        self._log[prevLogIndex].term if prevLogIndex > 0 else 0
                    )
                    entries = (
                        self._log[next_index:] if next_index < len(self._log) else []
                    )
                    args = raft_pb2.AppendEntriesArgs(
                        term=self._current_term,
                        leaderId=self._server_id,
                        prevLogIndex=prevLogIndex,
                        prevLogTerm=prevLogTerm,
                        entries=entries,
                        leaderCommit=self._commit_index,
                    )
                stub = raft_pb2_grpc.KeyValueStoreStub(channel)
                threading.Thread(
                    target=self._send_append_entry,
                    args=(stub, args, follower_id, result_queue),
                ).start()

            # Work on responses as they come
            for _ in range(len(self._channels)):
                server_id, response, error, args = result_queue.get()
                if error:  # Network error not a follower refusal
                    continue
                with self._state_lock:
                    if self._role != ServerRole.LEADER:
                        return
                    args: raft_pb2.AppendEntriesArgs = args
                    response: raft_pb2.AppendEntriesReply = response

                    # Higher term discovered
                    if response.term > self._current_term:
                        self._on_higher_term_discovery(response.term)
                        return  # No longer leader

                    if response.success:
                        next_index = args.prevLogIndex + len(args.entries) + 1
                        self._next_index[server_id] = next_index
                        if next_index > len(self._log):
                            logger.error(
                                f"nextIndex value {self._next_index[server_id]} greater than log length of leader {len(self._log)} for follower {server_id}"
                            )
                    else:
                        self._next_index[server_id] -= 1
                        if self._next_index[server_id] < 1:
                            logger.error(
                                f"nextIndex value {self._next_index[server_id]} less than 1 for follower {server_id}"
                            )
                    # Attempt to advance commit index following Raft's commit rule:
                    # only commit log entries from the current term directly.
                    # When an entry from current term commits, all prior entries
                    # are committed indirectly (Raft Figure 8).
                    for candidate_index in range(
                        self._commit_index + 1, len(self._log)
                    ):
                        # Only consider entries from the current term for direct commit
                        if self._log[candidate_index].term != self._current_term:
                            continue  # Skip entries from old terms

                        # Check if this entry is replicated on majority
                        count = 0
                        for sid in self._active_servers:
                            if sid == self._server_id:
                                if len(self._log) > candidate_index:
                                    count += 1
                            elif self._next_index.get(sid, 1) > candidate_index:
                                count += 1

                        if count > (len(self._active_servers) // 2):
                            # Commit this entry and all prior entries
                            self._commit_index = candidate_index
                            self._apply_committed_entries()
                        # Don't break - check next entries too

            # logger.debug(f"{self._server_id} sleeping now for 15ms between heartbeats")
            threading.Event().wait(0.015)

    def Put(self, request: raft_pb2.KeyValue, context):
        """Handle client PUT request"""
        # logger.debug(f"Put request received {request}")
        with self._state_lock:
            if self._role != ServerRole.LEADER:
                return raft_pb2.GenericResponse(success=False, error="Not leader")

            # Create log entry with all fields from request
            entry = raft_pb2.LogEntry(
                term=self._current_term,
                key=request.key,
                value=request.value,
                clientId=request.clientId,
                requestId=request.requestId,
            )
            # Append to log
            self._log.append(entry)
            log_index = len(self._log) - 1
            # logger.debug(f"Inserted entry {entry} to log at index {log_index}")
            self._persist_state()

        # Check on commitIndex sleeping in between
        # TODO: Better to check only when leadership status has changed or self._commit_index advances
        while True:
            with self._state_lock:
                if self._role != ServerRole.LEADER:
                    logger.debug(f"Put request rejected as not leader {request}")
                    return raft_pb2.GenericResponse(success=False, error="Not leader")
                if self._commit_index >= log_index:
                    logger.debug(f"Put request success")
                    return raft_pb2.GenericResponse(success=True)

            # logger.debug(f"{self._server_id} sleeping now for 50ms.")
            threading.Event().wait(0.05)

    def Get(self, request, context):
        """Handle client GET request - all servers can serve reads"""
        with self._state_lock:
            key = request.arg  # StringArg has .arg field

            # check if in cache first
            if self._cache is not None:
                cached = self._cache.get(key)
                if cached is not None:
                    return raft_pb2.KeyValue(key=key, value=cached)
                
            # either cache miss or no cache, read from state machine
            value = self._state_machine.get(key, "")  # Empty string for missing keys
            if self._cache is not None and key in self._state_machine:
                self._cache.put(key, value)
            return raft_pb2.KeyValue(key=key, value=value)

    def _persist_state(self) -> None:
        """Persist currentTerm, votedFor, and log[] to disk atomically."""
        if self._state_dir == "memory":
            return

        assert self._state_file is not None, (
            f"State file is a must if state_dir is {self._state_dir}"
        )
        # Serialize log entries (skip index 0 sentinel)
        log_data = []
        for entry in self._log[1:]:
            log_data.append(
                {
                    "term": entry.term,
                    "key": entry.key,
                    "value": entry.value,
                    "clientId": entry.clientId,
                    "requestId": entry.requestId,
                }
            )

        state = {
            "currentTerm": self._current_term,
            "votedFor": self._voted_for,
            "log": log_data,
        }

        temp_file = f"{self._state_file}.tmp"
        try:
            with open(temp_file, "w") as f:
                json.dump(state, f)
                f.flush()
                os.fsync(f.fileno())
            os.replace(temp_file, self._state_file)
        except Exception as e:
            logger.error(f"Failed to persist state to {self._state_file}: {e}")

    def _load_state(self) -> None:
        """Load persistent state from disk if available."""
        if self._state_dir == "memory":
            return

        assert self._state_file is not None, (
            f"State file is a must if state_dir is {self._state_dir}"
        )
        if not os.path.exists(self._state_file):
            return

        try:
            with open(self._state_file, "r") as f:
                state = json.load(f)

            self._current_term = int(state.get("currentTerm", 0))
            voted_for = state.get("votedFor", None)
            self._voted_for = int(voted_for) if voted_for is not None else None

            # Rebuild log with sentinel at index 0
            self._log = [None]  # type: ignore
            for entry_dict in state.get("log", []):
                self._log.append(
                    raft_pb2.LogEntry(
                        term=int(entry_dict.get("term")),
                        key=entry_dict.get("key"),
                        value=entry_dict.get("value"),
                        clientId=int(entry_dict.get("clientId")),
                        requestId=int(entry_dict.get("requestId")),
                    )
                )
        except Exception as e:
            logger.error(f"Failed to load state from {self._state_file}: {e}")


def serve(server_id):
    if not utils.is_server_id_valid(server_id):
        raise Exception(f"Invalid server ID: {server_id}")

    def thread_excepthook(args):
        logger.exception(
            f"Unhandled exception in thread '{args.thread.name}'",
            exc_info=(args.exc_type, args.exc_value, args.exc_traceback),
        )

    threading.excepthook = thread_excepthook

    logging.basicConfig(
        filename=f"server_{server_id}.log",
        level=logging.ERROR,
        format=f"%(asctime)s [server {server_id}] %(message)s",
    )

    port = 9001 + server_id
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10),
    )
    raft_pb2_grpc.add_KeyValueStoreServicer_to_server(
        KeyValueStoreServicer(server_id), server
    )
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    server_id = int(sys.argv[1])
    serve(server_id)
