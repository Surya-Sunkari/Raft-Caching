# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Setup

- Python 3.12 managed with `uv`
- Install dependencies: `uv sync`
- Activate venv: `source .venv/bin/activate`
- All source code lives in `src/kvstore/`; tests and servers must be run from that directory

## Protobuf Generation

Generated files (`raft_pb2.py`, `raft_pb2_grpc.py`, `raft_pb2.pyi`) are gitignored. Regenerate after any `.proto` changes:

```bash
cd src/kvstore
python -m grpc_tools.protoc --proto_path=. --python_out=. --grpc_python_out=. --pyi_out=. raft.proto
```

## Running Tests

All test commands must be run from `src/kvstore/`:

```bash
cd src/kvstore
python a1_tests.py   # Infrastructure (start/stop cluster)
python a2_tests.py   # Key-value store operations
python a3_tests.py   # Leader election
python a4_tests.py   # Log replication
python a5_tests.py   # Fault tolerance
python a6_tests.py   # State persistence
python -m unittest cache_policy_tests -v  # Cache eviction policy unit tests
python -m unittest workload_tests -v      # Workload generator unit tests
python cache_integration_test.py          # Cache + Raft integration test (requires cache enabled)
```

Integration tests (a1-a6) spawn real processes via `frontend.py` and use gRPC. They read `test_config.json` for timeouts and commands. `cache_policy_tests` and `workload_tests` are standalone with no network dependencies. The cache integration test starts a 5-server cluster and verifies cache behavior via `GetCacheStats` RPC — requires `config.ini` to have cache enabled (`policy != none`, `capacity > 0`).

## Benchmarking

`benchmark.py` has two modes, both driven from `src/kvstore/`:

```bash
cd src/kvstore
python benchmark.py                       # Unit mode: drives cache.py directly (fast, 7 policies x 6 workloads x 3 capacities)
python benchmark.py --mode integration    # Integration mode: drives a real 5-server Raft cluster via gRPC (slow)
```

Flags: `--policies`, `--workloads`, `--capacities`, `--num-keys`, `--num-ops`, `--seed`, `--write-strategy`, `--output`. Unit mode writes `benchmark_results.csv`; integration mode writes `benchmark_integration_results.csv`. Integration mode temporarily rewrites `config.ini`'s `[Cache]` section per (policy, capacity) combination and restores it on exit; it calls `StartRaft` between every workload so each run sees a fresh cache.

## Architecture

**Raft consensus KV store with pluggable cache layer.**

### Core Components

- **`frontend.py`** — gRPC gateway on port 8001. Manages server process lifecycles (subprocess spawn/kill). Routes `Get` to any available server, `Put` to the leader.
- **`server.py`** — Raft node implementing `KeyValueStoreServicer`. Each server runs on port `9001 + server_id` (IDs 0-4). Implements leader election, log replication, and state machine application. Uses `threading.RLock` for concurrency and `threading.Timer` for election timeouts (150-300ms).
- **`cache.py`** — Pluggable cache eviction policies (random, fifo, lru, lfu, lfu_decay, slru, sieve). `lfu` is classic LFU (no decay); `lfu_decay` is the same `LFUEviction` class with periodic frequency halving enabled. Abstract base `CachePolicy` with `get/put/invalidate/clear` interface. Factory: `create_cache(policy_name, capacity)`.
- **`workloads.py`** — Workload generators (`UniformWorkload`, `ZipfianWorkload`, `HotKeyWorkload`, `ScanWorkload`, `TemporalLocalityWorkload`, `WriteHeavyWorkload`) that yield seeded `(op, key, value)` operations for the benchmark harness. Factory: `create_workload(name, num_keys, **kwargs)`.
- **`benchmark.py`** — Benchmark harness with `unit` and `integration` modes. Unit mode exercises `cache.py` directly; integration mode rewrites `config.ini` and drives a live Raft cluster via frontend gRPC, aggregating `GetCacheStats` across nodes.
- **`raft.proto`** — Defines two gRPC services: `FrontEnd` (client-facing) and `KeyValueStore` (inter-node Raft RPCs: `AppendEntries`, `RequestVote`, `GetCacheStats`).
- **`utils.py`** — Config helpers reading from `config.ini` (active servers, persistence path, ports, cache config via `get_cache_config()`).
- **`config.ini`** — Runtime config: active server IDs, ports, persistence path, cache settings (`[Cache]` section: `policy`, `capacity`, `write_strategy`). Setting `persistent_state_path = memory` disables disk persistence. Setting `policy = none` or `capacity = 0` disables caching.

### Data Flow

1. Client → Frontend `Put(k,v)` → Frontend finds leader → Leader appends to log
2. Leader replicates via `AppendEntries` → majority ack → leader advances `commit_index`
3. All servers apply committed entries to `_state_machine` dict (and update/invalidate cache per `write_strategy`)
4. Client → Frontend `Get(k)` → any server checks cache first, falls back to `_state_machine` on miss (populates cache for existing keys)

### Key Implementation Details

- Log is 1-indexed (`_log[0]` is a sentinel `None`)
- Leader appends a no-op entry on election (Raft §5.4.3)
- Persistence: JSON files in `raft_state/server_{id}.json` (currentTerm, votedFor, log)
- `StartRaft` clears all persistent state for a fresh cluster; `StartServer` preserves it for crash recovery
