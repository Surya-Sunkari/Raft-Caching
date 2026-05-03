# Raft Key-Value Store

A distributed key-value store implementation using the Raft consensus protocol.

## Setup Instructions

1. Install uv using https://docs.astral.sh/uv/getting-started/installation/
2. Setup the virtual environment and install dependencies using `uv sync`
3. Activate the virtual environment using `source .venv/bin/activate`
4. Change working directory `cd src/kvstore`
5. Generate protocol buffer files:
   ```bash
   python -m grpc_tools.protoc \
   --proto_path=. \
   --python_out=. \
   --grpc_python_out=. \
   --pyi_out=. \
   raft.proto
   ```

## Running Tests

```bash
source .venv/bin/activate
cd src/kvstore
python a1_tests.py  # Infrastructure tests
python a2_tests.py  # Key-value store tests
python a3_tests.py  # Leader election tests
python a4_tests.py  # Log replication tests
python a5_tests.py  # Fault-tolerance tests
python a6_tests.py  # State persistence tests
python -m unittest cache_policy_tests -v  # Cache eviction policy unit tests
python -m unittest workload_tests -v      # Workload generator unit tests
python cache_integration_test.py          # Cache + Raft integration test (requires cache enabled in config.ini)
```

## Cache Layer

The project includes a pluggable cache layer integrated into the Raft KV store. Each server optionally maintains a local cache in front of its state machine.

### Configuration

Cache settings live in `src/kvstore/config.ini` under `[Cache]`:

```ini
[Cache]
policy = none       # none, random, fifo, lru, lfu, slru, sieve
capacity = 0        # max entries (0 = disabled)
write_strategy = write_through  # write_through or write_invalidate
```

Set `policy` to a policy name and `capacity > 0` to enable caching.

### Eviction Policies

| Policy | Description |
|--------|-------------|
| `random` | Evicts a random key (O(1) via swap-with-last) |
| `fifo` | Evicts the oldest inserted key (no reordering on access) |
| `lru` | Evicts the least recently used key |
| `lfu` | Evicts the least frequently used key (LRU tiebreak) |
| `slru` | Segmented LRU with probation and protected segments |
| `sieve` | Circular hand sweeps a visited bit; first unvisited key is evicted |

### Write Strategies

- **`write_through`** — on log apply, the new value is pushed into the cache immediately
- **`write_invalidate`** — on log apply, the stale cache entry is removed; next Get repopulates it

### Cache Stats

Each server exposes a `GetCacheStats` gRPC endpoint returning hits, misses, evictions, invalidations, current size, capacity, and hit rate.

### Usage (standalone)

```python
from cache import create_cache

cache = create_cache("lru", capacity=100)
cache.put("key", "value")
cache.get("key")        # returns "value"
cache.invalidate("key") # returns True
print(cache.stats)      # CacheStats(hits=1, misses=0, ...)
```

## Benchmarking

`benchmark.py` compares eviction policies under different workloads. Both modes are run from `src/kvstore/`:

```bash
# Unit mode: drives cache.py directly (no network, fast)
python benchmark.py

# Integration mode: drives a real 5-server Raft cluster via gRPC; defaults to a subset of policies for speed
python benchmark.py --mode integration

# Run integration test for all policies and all workloads
python benchmark.py --mode integration --policies random,fifo,lru,lfu,slru,sieve --workloads uniform,zipfian,hotkey,scan,temporal,writeheavy
```

### Workloads

| Workload | Access pattern |
|----------|----------------|
| `uniform` | Keys drawn uniformly over `[0, num_keys)` |
| `zipfian` | Power-law skew; `alpha` controls skew (default 1.1) |
| `hotkey` | Small hot set receives `hot_fraction` of traffic |
| `scan` | Sequential sweep through `[0, num_keys)`, wrapping forever |
| `temporal` | Recent keys reused with `reaccess_prob`; otherwise uniform fallback |
| `writeheavy` | Uniform key access with a 50/50 read/write mix |

### Flags

`--policies`, `--workloads`, `--capacities`, `--num-keys`, `--num-ops`, `--seed`, `--write-strategy`, `--output`. Unit mode writes `benchmark_results.csv`; integration mode writes `benchmark_integration_results.csv` and prints a hit-rate pivot table.

Integration mode temporarily rewrites `config.ini`'s `[Cache]` section per (policy, capacity) combination and restores it on exit. It calls `StartRaft` between every workload so each run sees a fresh cache. Rule of thumb: make `num_keys` at least 3-4× the largest capacity so evictions actually happen.
