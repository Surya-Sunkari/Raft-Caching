import sys
import grpc
from concurrent import futures
import raft_pb2
import raft_pb2_grpc

class KeyValueStoreServicer(raft_pb2_grpc.KeyValueStoreServicer):
    def __init__(self, server_id):
        self.server_id = server_id
    
    def ping(self, request, context):
        """Health check - return success=True"""
        return raft_pb2.GenericResponse(success=True)
    
    def GetState(self, request, context):
        """Return server state - for Assignment 1, just return term=0, isLeader=False"""
        return raft_pb2.State(term=0, isLeader=False)
    
    def Get(self, request, context):
        """Not implemented for Assignment 1"""
        return raft_pb2.Reply(wrongLeader=True, error="Not implemented")
    
    def Put(self, request, context):
        """Not implemented for Assignment 1"""
        return raft_pb2.Reply(wrongLeader=True, error="Not implemented")
    
    def AppendEntries(self, request, context):
        """Not implemented for Assignment 1"""
        return raft_pb2.AppendEntriesReply(term=0, success=False)
    
    def RequestVote(self, request, context):
        """Not implemented for Assignment 1"""
        return raft_pb2.RequestVoteReply(term=0, voteGranted=False)

def serve(server_id):
    port = 9001 + server_id
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    raft_pb2_grpc.add_KeyValueStoreServicer_to_server(
        KeyValueStoreServicer(server_id), server
    )
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    print(f"Server {server_id} started on port {port}")
    server.wait_for_termination()

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python server.py <server_id>")
        sys.exit(1)
    
    server_id = int(sys.argv[1])
    serve(server_id)