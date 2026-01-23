import grpc
import raft_pb2
import raft_pb2_grpc
import sys
from concurrent import futures

import raft_pb2, raft_pb2_grpc, utils


class KeyValueStoreServicer(raft_pb2_grpc.KeyValueStoreServicer):
    def ping(self, request, context):
        return raft_pb2.GenericResponse(success=True)

    def GetState(self, request, context):
        return raft_pb2.State(term=0, isLeader=False)


def serve(server_id):
    if not utils.is_server_id_valid(server_id):
        raise Exception(f"Invalid server ID: {server_id}")

    port = 9001 + server_id
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    raft_pb2_grpc.add_KeyValueStoreServicer_to_server(KeyValueStoreServicer(), server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    server_id = int(sys.argv[1])
    serve(server_id)
