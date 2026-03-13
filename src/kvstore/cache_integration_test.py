#!/usr/bin/env python3
"""Integration test for cache + Raft server.

Starts a 5-server Raft cluster, exercises Get/Put through the frontend,
and verifies cache behavior via the GetCacheStats RPC.

Requires config.ini to have cache enabled (policy != none, capacity > 0).
"""

import subprocess
import time
import grpc
import sys
import os
import signal
import atexit
import json

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

import raft_pb2
import raft_pb2_grpc
import utils

NUM_SERVERS = 5

# Global process handles
frontend_process = None


def load_test_config():
    config_file = "test_config.json"
    default = {
        "frontend": {
            "command": [sys.executable, os.path.join(PROJECT_ROOT, "frontend.py")],
            "working_dir": PROJECT_ROOT,
            "startup_timeout": 30,
        },
        "server": {
            "command_template": [
                sys.executable,
                os.path.join(PROJECT_ROOT, "server.py"),
                "{server_id}",
            ],
            "working_dir": PROJECT_ROOT,
        },
        "ports": {"frontend_port": 8001, "base_server_port": 9001},
        "timeouts": {
            "rpc_timeout": 5,
            "startup_wait": 4,
            "leader_election_timeout": 25,
        },
    }
    if os.path.exists(config_file):
        with open(config_file) as f:
            user = json.load(f)
        for k, v in user.items():
            if k in default and isinstance(default[k], dict) and isinstance(v, dict):
                default[k].update(v)
            else:
                default[k] = v
    return default


CFG = load_test_config()


def cleanup():
    global frontend_process
    if frontend_process and frontend_process.poll() is None:
        frontend_process.terminate()
        try:
            frontend_process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            frontend_process.kill()
            frontend_process.wait()
    for pattern in ["frontend.py", "server.py"]:
        subprocess.run(["pkill", "-f", pattern], capture_output=True)
    time.sleep(2)


def start_cluster():
    global frontend_process
    cleanup()

    frontend_process = subprocess.Popen(
        CFG["frontend"]["command"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd=CFG["frontend"]["working_dir"],
        env={**os.environ, "PYTHONUNBUFFERED": "1"},
        preexec_fn=os.setsid,
    )

    port = CFG["ports"]["frontend_port"]
    for _ in range(CFG["frontend"]["startup_timeout"]):
        try:
            channel = grpc.insecure_channel(f"127.0.0.1:{port}")
            stub = raft_pb2_grpc.FrontEndStub(channel)
            stub.Get(raft_pb2.GetKey(key="ping", clientId=0, requestId=0), timeout=2)
            channel.close()
            print("Frontend ready.")
            break
        except Exception:
            time.sleep(1)
    else:
        raise RuntimeError("Frontend failed to start")

    # Start 5-server raft cluster
    channel = grpc.insecure_channel(f"127.0.0.1:{port}")
    stub = raft_pb2_grpc.FrontEndStub(channel)
    resp = stub.StartRaft(raft_pb2.IntegerArg(arg=NUM_SERVERS), timeout=30)
    channel.close()
    if resp.error:
        raise RuntimeError(f"StartRaft failed: {resp.error}")

    time.sleep(CFG["timeouts"]["startup_wait"])

    # Wait for leader election
    wait_for_leader()


def wait_for_leader():
    """Wait until exactly one leader is elected among all servers."""
    base_port = CFG["ports"]["base_server_port"]
    timeout = CFG["timeouts"]["leader_election_timeout"]
    print(f"Waiting up to {timeout}s for leader election...")

    start_time = time.time()
    while time.time() - start_time < timeout:
        leaders = []
        for sid in range(NUM_SERVERS):
            try:
                channel = grpc.insecure_channel(f"127.0.0.1:{base_port + sid}")
                stub = raft_pb2_grpc.KeyValueStoreStub(channel)
                state = stub.GetState(raft_pb2.Empty(), timeout=2)
                channel.close()
                if state.isLeader:
                    leaders.append(sid)
            except Exception:
                pass
        if len(leaders) == 1:
            print(f"Leader elected: server {leaders[0]}")
            return leaders[0]
        time.sleep(1)

    raise RuntimeError("No leader elected within timeout")


def get_cache_stats(server_id=0):
    port = CFG["ports"]["base_server_port"] + server_id
    channel = grpc.insecure_channel(f"127.0.0.1:{port}")
    stub = raft_pb2_grpc.KeyValueStoreStub(channel)
    stats = stub.GetCacheStats(raft_pb2.Empty(), timeout=CFG["timeouts"]["rpc_timeout"])
    channel.close()
    return stats


def frontend_put(key, value):
    port = CFG["ports"]["frontend_port"]
    channel = grpc.insecure_channel(f"127.0.0.1:{port}")
    stub = raft_pb2_grpc.FrontEndStub(channel)
    resp = stub.Put(
        raft_pb2.KeyValue(key=key, value=value, clientId=1, requestId=1),
        timeout=CFG["timeouts"]["rpc_timeout"],
    )
    channel.close()
    if resp.wrongLeader:
        print(f"  WARNING: PUT({key}) failed: {resp.error}")
    return resp


def frontend_get(key):
    port = CFG["ports"]["frontend_port"]
    channel = grpc.insecure_channel(f"127.0.0.1:{port}")
    stub = raft_pb2_grpc.FrontEndStub(channel)
    resp = stub.Get(
        raft_pb2.GetKey(key=key, clientId=1, requestId=1),
        timeout=CFG["timeouts"]["rpc_timeout"],
    )
    channel.close()
    return resp


def assert_eq(actual, expected, msg):
    if actual != expected:
        print(f"  FAIL: {msg}: expected {expected!r}, got {actual!r}")
        return False
    return True


def assert_ge(actual, threshold, msg):
    if actual < threshold:
        print(f"  FAIL: {msg}: expected >= {threshold}, got {actual}")
        return False
    return True


def run_tests():
    cache_config = utils.get_cache_config()
    if cache_config["policy"] == "none" or cache_config["capacity"] <= 0:
        print("ERROR: Cache is disabled in config.ini. Enable it to run this test.")
        print(f"  Current config: policy={cache_config['policy']}, capacity={cache_config['capacity']}")
        sys.exit(1)

    print(f"Cache config: policy={cache_config['policy']}, capacity={cache_config['capacity']}, "
          f"write_strategy={cache_config['write_strategy']}")
    print()

    # Find the leader — we'll query cache stats from it since the frontend
    # routes Get to any server but Put goes to the leader
    leader_id = wait_for_leader()

    passed = 0
    failed = 0

    # Test 1: Stats endpoint works and starts at zero
    print("Test 1: GetCacheStats returns zeroes on fresh server")
    stats = get_cache_stats(leader_id)
    ok = (assert_eq(stats.hits, 0, "initial hits")
          and assert_eq(stats.misses, 0, "initial misses")
          and assert_eq(stats.evictions, 0, "initial evictions")
          and assert_eq(stats.capacity, cache_config["capacity"], "capacity"))
    if ok:
        print("  PASS")
        passed += 1
    else:
        failed += 1

    # For remaining tests, query stats and do Gets on the same specific server
    # to avoid ambiguity about which server the frontend routes to.
    # We'll use direct server Get instead of frontend Get for cache verification.
    def server_get(key, server_id=leader_id):
        port = CFG["ports"]["base_server_port"] + server_id
        channel = grpc.insecure_channel(f"127.0.0.1:{port}")
        stub = raft_pb2_grpc.KeyValueStoreStub(channel)
        resp = stub.Get(raft_pb2.StringArg(arg=key), timeout=CFG["timeouts"]["rpc_timeout"])
        channel.close()
        return resp

    # Test 2: Get on missing key = cache miss, value not cached
    print("Test 2: GET missing key registers miss but does not cache")
    server_get("nonexistent")
    stats = get_cache_stats(leader_id)
    ok = (assert_eq(stats.misses, 1, "misses after get missing")
          and assert_eq(stats.current_size, 0, "cache size after get missing"))
    if ok:
        print("  PASS")
        passed += 1
    else:
        failed += 1

    # Test 3: Put then Get = cache hit (write-through) or miss then hit
    print("Test 3: PUT then GET, GET again (hit)")
    frontend_put("key1", "value1")
    time.sleep(1)  # wait for commit + replication + apply

    resp = server_get("key1")
    ok = assert_eq(resp.value, "value1", "first GET value")
    stats_after_first = get_cache_stats(leader_id)

    resp = server_get("key1")
    ok = ok and assert_eq(resp.value, "value1", "second GET value")
    stats_after_second = get_cache_stats(leader_id)

    if cache_config["write_strategy"] == "write_through":
        # Write-through: apply populates cache, so both GETs are hits
        ok = ok and assert_eq(stats_after_second.hits - stats_after_first.hits, 1, "second GET added a hit")
    else:
        # Write-invalidate: first GET is a miss (populates), second is a hit
        ok = ok and assert_ge(stats_after_second.hits, 1, "at least one hit after two GETs")

    if ok:
        print("  PASS")
        passed += 1
    else:
        failed += 1

    # Test 4: Update value consistency — GET after PUT update returns new value
    print("Test 4: PUT update, then GET returns new value")
    frontend_put("key1", "updated1")
    time.sleep(1)
    resp = server_get("key1")
    ok = assert_eq(resp.value, "updated1", "GET after update")
    if ok:
        print("  PASS")
        passed += 1
    else:
        failed += 1

    # Test 5: Fill cache to capacity and verify evictions
    print(f"Test 5: Fill cache beyond capacity ({cache_config['capacity']}) triggers evictions")
    capacity = cache_config["capacity"]
    for i in range(capacity + 2):
        frontend_put(f"fill_{i}", f"val_{i}")
    time.sleep(2)  # wait for all commits
    # Read all keys to populate cache (some will cause evictions)
    for i in range(capacity + 2):
        server_get(f"fill_{i}")
    stats = get_cache_stats(leader_id)
    ok = assert_ge(stats.evictions, 1, "evictions after exceeding capacity")
    ok = ok and assert_eq(stats.current_size, capacity, "cache size at capacity")
    if ok:
        print("  PASS")
        passed += 1
    else:
        failed += 1

    # Test 6: Multiple GETs on same key increment hits
    print("Test 6: Repeated GETs increment hit counter")
    frontend_put("repeat", "val")
    time.sleep(1)
    server_get("repeat")  # populate cache (or hit if write-through)
    stats_before = get_cache_stats(leader_id)
    for _ in range(5):
        server_get("repeat")
    stats_after = get_cache_stats(leader_id)
    ok = assert_eq(stats_after.hits - stats_before.hits, 5, "5 additional hits")
    if ok:
        print("  PASS")
        passed += 1
    else:
        failed += 1

    # Print final cache stats
    final_stats = get_cache_stats(leader_id)
    print(f"\nCache stats: hits={final_stats.hits}, misses={final_stats.misses}, "
          f"evictions={final_stats.evictions}, hit_rate={final_stats.hit_rate:.2%}")

    print(f"\n{'='*40}")
    print(f"Results: {passed} passed, {failed} failed out of {passed + failed}")
    return failed == 0


if __name__ == "__main__":
    atexit.register(cleanup)
    signal.signal(signal.SIGINT, lambda *_: (cleanup(), sys.exit(1)))
    signal.signal(signal.SIGTERM, lambda *_: (cleanup(), sys.exit(1)))

    try:
        print("Starting cluster...")
        start_cluster()
        print("Cluster ready.\n")
        success = run_tests()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"\nFATAL: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        cleanup()
