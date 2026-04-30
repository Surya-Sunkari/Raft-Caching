# Implementation Guide: Pluggable Cache Eviction for Raft KV Store

Each step below is self-contained and testable. Complete them in order.

---

## Step 1: Cache Interface & 5 Core Eviction Policies

**Create `src/kvstore/cache.py`**

Define the abstract base class and core policies:

| Class | Data Structures | Eviction Logic |
|-------|----------------|----------------|
| `CachePolicy` (ABC) | `CacheStats` dataclass | Abstract: `get`, `put`, `invalidate`, `clear` |
| `RandomEviction` | `dict` + `list` of keys | `random.choice` from key list, swap-with-last for O(1) removal |
| `FIFOEviction` | `dict` + `collections.deque` | `deque.popleft()` — insertion order, no reordering on access |
| `LRUEviction` | `OrderedDict` | `move_to_end(key)` on access, `popitem(last=False)` to evict |
| `LFUEviction` | `dict` + freq counters + `dict[int, OrderedDict]` freq buckets + `min_freq` | Pop LRU key from `freq_buckets[min_freq]` |
| `SLRUEviction` | Two `OrderedDict`s (probation + protected) | New keys enter probation. On re-access, promote to protected. Evict from probation front. Protected overflow demotes to probation. Default `protected_ratio=0.8` hardcoded in constructor. |

SIEVE and MAT policies are added in Steps 9 and 10 respectively.

Key interface:
- `get(key) -> Optional[str]` — `None` = miss, `""` = valid cached empty string
- `put(key, value) -> Optional[str]` — returns evicted key or `None`
- `invalidate(key) -> bool` — returns whether key existed
- `clear()` — remove all entries
- `stats` — `CacheStats` with hits, misses, evictions, invalidations, current_size
- `create_cache(policy_name, capacity, **kwargs)` — factory function

**Done when:** You can instantiate all 5 policies, do puts up to capacity, verify eviction happens, and `stats` are correct.

---

## Step 2: Configuration Helpers

**Modify `src/kvstore/config.ini`** — add:
```ini
[Cache]
policy = none
capacity = 0
write_strategy = write_through
```
Default: disabled (`policy = none`, capacity 0). Policy-specific parameters (e.g., SLRU's `protected_ratio`) are hardcoded defaults in the policy constructors, not exposed in config.

**Modify `src/kvstore/utils.py`** — add:
- `get_cache_policy() -> str`
- `get_cache_capacity() -> int`
- `get_cache_write_strategy() -> str`

**Done when:** Calling these functions returns the config values. `policy = none` or `capacity = 0` means "no cache."

---

## Step 3: Server Integration

**Modify `src/kvstore/server.py`** at 3 points:

### 3a. Constructor (after line 44: `self._state_machine = {}`)
```python
cache_policy = utils.get_cache_policy()
cache_capacity = utils.get_cache_capacity()
self._cache_write_strategy = utils.get_cache_write_strategy()
if cache_policy != "none" and cache_capacity > 0:
    from cache import create_cache
    self._cache = create_cache(cache_policy, cache_capacity)
else:
    self._cache = None
```

### 3b. Get RPC (lines 479-484)
Before reading from `_state_machine`, check cache:
```python
if self._cache is not None:
    cached = self._cache.get(key)
    if cached is not None:
        return raft_pb2.KeyValue(key=key, value=cached)
# Cache miss — read from state machine
value = self._state_machine.get(key, "")
if self._cache is not None and key in self._state_machine:
    self._cache.put(key, value)
```
Note: only cache keys that actually exist in `_state_machine` to avoid filling the cache with empty-string entries for non-existent keys.

### 3c. _apply_committed_entries (after line 270: `self._state_machine[entry.key] = entry.value`)
```python
if self._cache is not None:
    if self._cache_write_strategy == "write_through":
        self._cache.put(entry.key, entry.value)
    else:
        self._cache.invalidate(entry.key)
```

**Done when:** All existing tests (a1-a6) pass with cache disabled AND with cache enabled (set policy=lru, capacity=50 in config.ini).

---

## Step 4: GetCacheStats gRPC Endpoint

**Modify `src/kvstore/raft.proto`** — add:
```protobuf
message CacheStatsResponse {
    int64 hits = 1;
    int64 misses = 2;
    int64 evictions = 3;
    int64 invalidations = 4;
    int64 current_size = 5;
    int64 capacity = 6;
    double hit_rate = 7;
}
```
Add to `KeyValueStore` service: `rpc GetCacheStats(Empty) returns (CacheStatsResponse);`

**Regenerate proto files:**
```bash
python -m grpc_tools.protoc --proto_path=. --python_out=. --grpc_python_out=. --pyi_out=. raft.proto
```

**Implement in `server.py`:**
```python
def GetCacheStats(self, request, context):
    if self._cache:
        s = self._cache.stats
        return raft_pb2.CacheStatsResponse(
            hits=s.hits, misses=s.misses, evictions=s.evictions,
            invalidations=s.invalidations, current_size=s.current_size,
            capacity=s.capacity, hit_rate=s.hit_rate)
    return raft_pb2.CacheStatsResponse()
```

**Done when:** You can call GetCacheStats on a running server and get correct metrics.

---

## Step 5: Cache Unit Tests

**Create `src/kvstore/cache_tests.py`**

Test each policy for:
- Basic get/put (returns correct values)
- Eviction at capacity (correct victim chosen)
- Invalidate removes entry, next get returns None
- Clear empties cache
- Stats tracking (hit_rate, eviction count)
- Policy-specific: LRU evicts least recent, LFU evicts least frequent, SLRU promotes on re-access
- Edge cases: capacity=1, empty string values, overwrite existing key

**Done when:** All tests pass for all 5 core policies. (SIEVE and MAT tests added in Steps 9–10.)

---

## Step 6: Integration Verification

Run existing test suite with cache enabled:
```bash
# In config.ini, set policy=lru, capacity=50
python a6_tests.py
```

Test cache consistency:
- PUT a key, GET it — should return correct value (from cache)
- PUT same key with new value, GET it — should return new value
- Verify GetCacheStats shows hits/misses incrementing correctly

**Done when:**a6 tests pass with each policy. No regressions.

---

## Step 7: Workload Generators

**Create `src/kvstore/workloads.py`**

| Workload | Description | Key Parameter |
|----------|-------------|---------------|
| `UniformWorkload` | Keys drawn uniformly from [0, N) | `num_keys` |
| `ZipfianWorkload` | Power-law skewed access | `alpha` (default 1.1) |
| `HotKeyWorkload` | Small set gets 90% of traffic | `hot_keys`, `hot_fraction` |
| `ScanWorkload` | Sequential sweep through all keys | `num_keys` |
| `TemporalLocalityWorkload` | Recent keys re-accessed with high probability | `window_size`, `reaccess_prob` |
| `WriteHeavyWorkload` | 50% PUT / 50% GET | `read_ratio=0.5` |

Interface: `generate(num_ops) -> Iterator[(op, key, value)]` where op is "get" or "put".

**Done when:** Each workload produces the expected key distribution (verify with a histogram).

---

## Step 8: Benchmark Harness

**Create `src/kvstore/benchmark.py`**

Two modes:
1. **Unit-level**: Instantiate policy directly, feed workload, measure hit rate / evictions. Fast, no network.
2. **Integration-level**: Start Raft cluster, drive traffic via frontend gRPC, query GetCacheStats per node.

Output:
- `BenchmarkResult` dataclass: policy, workload, capacity, hit_rate, evictions, avg_latency, p99_latency, throughput
- CSV export for all results
- Matplotlib comparison charts (hit rate by policy, latency CDF, throughput bars)

Run matrix: 5 policies x 6 workloads x 3 capacities (50, 100, 500). SIEVE and MAT are added to the matrix after Steps 9–10.

**Done when:** `python benchmark.py` produces CSV + charts comparing all 5 core policies.

---

## Step 9: SIEVE Eviction Policy

**Status: implemented.** `SIEVEEviction` is registered as `"sieve"`, included in unit and integration benchmark policy lists, and covered by common + SIEVE-specific tests in `src/kvstore/cache_policy_tests.py`.

**Add `SIEVEEviction` to `src/kvstore/cache.py`**

| Class | Data Structures | Eviction Logic |
|-------|----------------|----------------|
| `SIEVEEviction` | Doubly-linked list (`_Node` with prev/next/key/value/visited) + `dict` + hand pointer | Advance hand; skip visited nodes (clear their bit). Evict first unvisited. |

- Register as `"sieve"` in `create_cache` factory
- Add SIEVE-specific tests to `cache_policy_tests.py` (hand advancement, visited-bit clearing)

**Done when:** SIEVE passes all common + policy-specific tests and works in the benchmark harness.

---

## Step 10: MAT Policy (ML-based)

**Create `src/kvstore/mat_cache.py`**

- Add `numpy` and `scikit-learn` to `pyproject.toml`
- Implement a small model (logistic regression) that predicts which candidate to evict
- Model is applied only to a small candidate set (e.g., 10 entries near tail) at eviction time
- Features: access count, time since last access, time since insertion
- Training: online learning from observed access patterns
- Register in `cache.py` factory function
- Add MAT-specific tests to `cache_tests.py`

**Done when:** MAT policy works in unit tests and benchmark harness. Compare against other policies.

---

## Step 11: Failure Case Analysis & Mitigation

Run benchmarks and document failures:

| Policy | Expected Failure | Workload to Trigger |
|--------|-----------------|-------------------|
| LRU | Scan pollution — sequential scan evicts all hot keys | ScanWorkload after warming with ZipfianWorkload |
| LFU | Stale counts — old hot keys never evicted | Phase change: Zipfian with different hot sets |
| FIFO | No recency/frequency awareness | Any skewed workload |
| SLRU | Segment ratio sensitivity | Vary protected_ratio, measure hit rate |
| SIEVE | Full sweep under uniform high frequency | UniformWorkload with high access rate |
| Random | No pattern awareness (baseline) | All workloads |

**Implement at least one mitigation:**
- LFU aging: periodically halve all frequency counts
- OR LRU admission filter: don't cache on first access

Re-run benchmarks to show improvement.

**Done when:** Written report with charts showing failure case + mitigation effect.
