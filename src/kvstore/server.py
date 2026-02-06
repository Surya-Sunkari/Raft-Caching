import grpc
import raft_pb2
import raft_pb2_grpc
import sys
import threading
from concurrent import futures

import raft_pb2, raft_pb2_grpc, utils


class KeyValueStoreServicer(raft_pb2_grpc.KeyValueStoreServicer):
    def __init__(self, server_id):
        self.store = {}  # In-memory key-value store
        self.store_lock = threading.RLock()  # Thread safety
        self.server_id = server_id

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
        return raft_pb2.State(term=0, isLeader=False)


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
