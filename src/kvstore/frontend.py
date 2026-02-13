import configparser
import grpc
import os
import subprocess
from concurrent import futures

import raft_pb2, raft_pb2_grpc, utils


class FrontEndServicer(raft_pb2_grpc.FrontEndServicer):
    def __init__(self):
        self._server_processes = {}  # server_id -> process handle
        self._kill_timeout_seconds = 5
        self._rpc_timeout_seconds = 2

    def _start_server(self, server_id: int):
        """Start a single server process"""
        try:
            process = subprocess.Popen(
                ["python", "server.py", str(server_id)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                preexec_fn=os.setsid,  # Create new process group to able to cleanly kill it
            )
            self._server_processes[server_id] = process
            return True
        except Exception as e:
            raise Exception(f"Failed to start server {server_id}: {e}")

    def _kill_server(self, server_id: int):
        """Kill a single server process by ID"""
        try:
            if not server_id in self._server_processes:
                return

            process = self._server_processes[server_id]
            del self._server_processes[server_id]
            if process and process.poll() is None:
                try:
                    process.terminate()
                    process.wait(timeout=self._kill_timeout_seconds)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
        except Exception as e:
            raise Exception(f"Failed to kill server {server_id}: {e}")

    def _kill_all_servers(self):
        try:
            for server_id in list(self._server_processes.keys()):
                self._kill_server(server_id)

            # Also try to kill any orphaned server processes
            subprocess.run(
                ["pkill", "-f", "server.py"],
                capture_output=True,
                timeout=self._kill_timeout_seconds,
            )
        except Exception as e:
            raise Exception(f"Failed to kill all servers: {e}")

    def StartRaft(self, request, context):
        """Start N servers (1-5) with IDs 0 to N-1"""
        num_servers = request.arg

        # Input validation
        if not utils.is_num_servers_valid(num_servers):
            return raft_pb2.Reply(
                wrongLeader=False, error=f"Invalid number of servers: {num_servers}"
            )

        try:
            # Kill any existing servers first
            self._kill_all_servers()
            # Start all the servers
            for server_id in range(num_servers):
                self._start_server(server_id)
        except Exception as e:
            return raft_pb2.Reply(wrongLeader=False, error=f"Failed to start Raft: {e}")

        # success response
        return raft_pb2.Reply(wrongLeader=False)

    def StartServer(self, request, context):
        """Start individual server by ID (for restarting crashed servers)"""
        server_id = request.arg

        # Input validation
        if not utils.is_server_id_valid(server_id):
            return raft_pb2.Reply(
                wrongLeader=False, error=f"Invalid server ID: {server_id}"
            )

        # Restart the server
        try:
            self._kill_server(server_id)
            self._start_server(server_id)
        except Exception as e:
            return raft_pb2.Reply(
                wrongLeader=False, error=f"Failed to start server {server_id}: {e}"
            )

        # success response
        return raft_pb2.Reply(wrongLeader=False)

    def _ping_server(self, server_id):
        """Returns True if server responds to ping, False otherwise"""
        port = 9001 + server_id
        channel = grpc.insecure_channel(f"127.0.0.1:{port}")
        try:
            stub = raft_pb2_grpc.KeyValueStoreStub(channel)
            stub.ping(raft_pb2.Empty(), timeout=self._rpc_timeout_seconds)
            return True
        except grpc.RpcError:
            return False
        finally:
            channel.close()

    def _get_server_state(self, server_id):
        """Returns (success, term, is_leader) for the given server_id"""
        port = 9001 + server_id
        channel = grpc.insecure_channel(f"127.0.0.1:{port}")
        try:
            stub = raft_pb2_grpc.KeyValueStoreStub(channel)
            response = stub.GetState(
                raft_pb2.Empty(), timeout=self._rpc_timeout_seconds
            )
            if response.success:
                return True, response.term, response.isLeader
            else:
                return False, None, None
        except grpc.RpcError:
            return False, None, None
        finally:
            channel.close()

    def _find_available_server(self):
        """Returns server_id of first available server, or None"""
        for server_id in utils.get_active_servers():
            if self._ping_server(server_id):
                return server_id
        return None

    def _find_leader_server(self):
        """Find current leader by checking GetState on all servers"""
        # TODO: Make GetState calls to server parallel
        active_servers = utils.get_active_servers()
        for server_id in active_servers:
            try:
                success, _, is_leader = self._get_server_state(server_id)
                if success and is_leader:
                    return server_id
            except:
                continue

        # No leader found, return any available server
        return self._find_available_server()

    def Get(self, request, context):
        """Forward Get request to an available server"""
        server_id = self._find_leader_server()
        if server_id is None:
            return raft_pb2.Reply(wrongLeader=True, error="No servers available")

        port = 9001 + server_id
        channel = grpc.insecure_channel(f"127.0.0.1:{port}")
        try:
            stub = raft_pb2_grpc.KeyValueStoreStub(channel)
            key = raft_pb2.StringArg(arg=request.key)
            response = stub.Get(key, timeout=self._rpc_timeout_seconds)
            return raft_pb2.Reply(wrongLeader=False, value=response.value)
        except grpc.RpcError as e:
            return raft_pb2.Reply(wrongLeader=True, error=str(e))
        finally:
            channel.close()

    def Put(self, request, context):
        """Forward Put request to an available server"""
        server_id = self._find_leader_server()
        if server_id is None:
            return raft_pb2.Reply(wrongLeader=True, error="No servers available")

        port = 9001 + server_id
        channel = grpc.insecure_channel(f"127.0.0.1:{port}")
        try:
            stub = raft_pb2_grpc.KeyValueStoreStub(channel)
            key_value = raft_pb2.KeyValue(key=request.key, value=request.value)
            response = stub.Put(key_value, timeout=self._rpc_timeout_seconds)
            if response.success:
                return raft_pb2.Reply(wrongLeader=False)
            else:
                return raft_pb2.Reply(
                    wrongLeader=True, error="Failed to put key-value pair"
                )
        except grpc.RpcError as e:
            return raft_pb2.Reply(
                wrongLeader=True, error=f"Failed to put key-value pair due to {str(e)}"
            )
        finally:
            channel.close()


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    raft_pb2_grpc.add_FrontEndServicer_to_server(FrontEndServicer(), server)
    server.add_insecure_port("[::]:8001")
    server.start()
    print("Frontend started on port 8001")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
