import grpc
from concurrent import futures
import subprocess
import os
import signal
import raft_pb2
import raft_pb2_grpc

class FrontEndServicer(raft_pb2_grpc.FrontEndServicer):
    def __init__(self):
        self.server_processes = {}  # server_id -> subprocess.Popen
    
    def _kill_all_servers(self):
        """Kill all running server processes"""
        for server_id, process in list(self.server_processes.items()):
            if process and process.poll() is None:
                try:
                    process.terminate()
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
                except Exception:
                    pass
        self.server_processes.clear()
        
        # Also try to kill any orphaned server processes
        try:
            subprocess.run(['pkill', '-f', 'server.py'], capture_output=True, timeout=5)
        except Exception:
            pass
    
    def _start_server(self, server_id):
        """Start a single server process"""
        try:
            process = subprocess.Popen(
                ['python3', 'server.py', str(server_id)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                preexec_fn=os.setsid  # Create new process group
            )
            self.server_processes[server_id] = process
            return True
        except Exception as e:
            print(f"Failed to start server {server_id}: {e}")
            return False
    
    def StartRaft(self, request, context):
        """Start N servers (1-5) with IDs 0 to N-1"""
        n = request.arg
        
        if n < 1 or n > 5:
            return raft_pb2.Reply(wrongLeader=False, error=f"Invalid number of servers: {n}")
        
        # Kill any existing servers first
        self._kill_all_servers()
        
        # Start N servers
        for server_id in range(n):
            if not self._start_server(server_id):
                return raft_pb2.Reply(wrongLeader=False, error=f"Failed to start server {server_id}")
        
        return raft_pb2.Reply(wrongLeader=False, error="")
    
    def StartServer(self, request, context):
        """Start individual server by ID (for restarting crashed servers)"""
        server_id = request.arg
        
        if server_id < 0 or server_id > 4:
            return raft_pb2.Reply(wrongLeader=False, error=f"Invalid server ID: {server_id}")
        
        # Kill existing process for this server if running
        if server_id in self.server_processes:
            process = self.server_processes[server_id]
            if process and process.poll() is None:
                try:
                    process.terminate()
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
                except Exception:
                    pass
        
        # Start the server
        if not self._start_server(server_id):
            return raft_pb2.Reply(wrongLeader=False, error=f"Failed to start server {server_id}")
        
        return raft_pb2.Reply(wrongLeader=False, error="")
    
    def Get(self, request, context):
        """Return 'Not implemented' for Assignment 1"""
        return raft_pb2.Reply(wrongLeader=True, error="Not implemented")
    
    def Put(self, request, context):
        """Return 'Not implemented' for Assignment 1"""
        return raft_pb2.Reply(wrongLeader=True, error="Not implemented")

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    raft_pb2_grpc.add_FrontEndServicer_to_server(FrontEndServicer(), server)
    server.add_insecure_port('[::]:8001')
    server.start()
    print("Frontend started on port 8001")
    server.wait_for_termination()

if __name__ == '__main__':
    serve()