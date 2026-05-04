# Distributed KV Store with Pluggable Cache Eviction

**Om Agrawal, Surya Sunkari, Ayuj Verma**

---

## Idea Behind the Project

Raft is a consensus protocol that allows a cluster of servers to maintain a replicated state machine, ensuring all nodes agree on the same sequence of operations even in the presence of failures. While Raft provides strong consistency, every read must access the committed state machine and every write must travel through log replication — appending to the leader's log, replicating to a majority of followers, and applying the committed entry. This adds latency to every operation depending on the state machine implementation.

This project adds a pluggable cache layer to a Raft KV store. Each server maintains its own local cache sitting in front of its state machine. On a read, the server checks the cache first and returns immediately on a hit, bypassing the state machine lookup. On a write, committed entries update the cache under one of two strategies: write-through (update the cache with the new value) or write-invalidate (evict the stale entry).

The cache uses finite space, so when it is full and a new entry arrives, some existing entry must be removed. Rather than hardcoding a single eviction strategy, the system supports six pluggable policies: Random, FIFO, LRU, LFU, SLRU, and SIEVE. Each policy uses different data structures and heuristics to decide which entry to evict. This lets us directly compare how different strategies perform under various distributed load patterns.

---

## Motivation

In a single-server system, cache eviction policy is a pure performance question. In a Raft cluster, it becomes more interesting. All writes flow through the leader and propagate to every follower, so writes keep all server caches in sync. But reads are local — any server can serve a read directly from its cache, meaning each server's cache evolves independently based on the reads it happens to receive. Two servers holding identical state machines can end up with entirely different cache contents, and different eviction policies will interact with this divergence in different ways depending on how read traffic is distributed across the cluster.

This raises questions that don't exist in single-server systems: Does a frequency-based policy (LFU) diverge more or less than a recency-based one (LRU) when read traffic is skewed? Does SIEVE's visited-bit mechanism hold up under distributed access? Does write strategy (write-through vs. write-invalidate) interact with eviction policy under write-heavy loads?

To answer these questions concretely, we built a benchmark harness that evaluates all six policies across six workload types in both a direct (unit) mode and a live five-node Raft cluster (integration) mode.

---

## Design

The system runs as a set of separate processes communicating over gRPC. A front-end process accepts `Get` and `Put` requests from clients and forwards them to a server node. Each server node runs as its own process and maintains three things: its own copy of the Raft log, its own committed state machine (a key-value dictionary), and its own local cache. Nodes talk to each other using gRPC calls defined in a shared protobuf schema covering both client-facing operations (`Get`, `Put`) and internal Raft messaging (`AppendEntries`, `RequestVote`). The frontend also exposes operations for starting and stopping individual server processes, which is how tests simulate node failures and recoveries.

Within each server, the cache sits between the RPC handler and the state machine. When a `Get` arrives, the handler checks the cache first and returns on a hit. On a miss, it reads from the state machine, stores the result in the cache, and returns. `Put` requests skip the cache at the RPC layer. Instead, a write travels through Raft log replication, and once the entry is applied to the state machine the cache is updated: either the new value is written in (write-through) or the stale entry is removed (write-invalidate). Each server exposes a `GetCacheStats` gRPC endpoint returning hits, misses, evictions, invalidations, current size, capacity, and hit rate.

The cache is implemented behind an interface with four operations: `get`, `put`, `invalidate`, and `clear`. A factory function maps a policy name to the corresponding implementation, making the policy selectable through `config.ini` with no changes to server code. Setting `policy = none` or `capacity = 0` disables caching entirely. The config also sets cache capacity and write strategy. This design keeps the Raft server code policy-agnostic.

---

## Implementation

### Cache Eviction Policies

Six policies are implemented, all inheriting from an abstract `CachePolicy` base class:

- **Random**: When the cache is full, pick an existing key at random and evict it. Uses a swap-with-last technique for O(1) removal. No tracking of access patterns; every key has equal eviction probability.

- **FIFO** (First In, First Out): Keys are evicted in insertion order. A `deque` tracks arrival order; the front (oldest) key is evicted when space is needed. Access frequency has no effect.

- **LRU** (Least Recently Used): Every access moves a key to the "most recently used" end of an `OrderedDict`. On eviction, the front entry (untouched longest) is removed. The bet: if you haven't needed something recently, you probably won't soon.

- **LFU** (Least Frequently Used): Each key carries an access count. Eviction picks the lowest-count key, with LRU as a tiebreaker. The bet: a key accessed once is less likely to be needed than one accessed a hundred times.

- **SLRU** (Segmented LRU): The cache is split into a small probation zone and a larger protected zone (default 80% protected). New entries enter probation. Re-access promotes an entry to protected. Eviction pulls from probation first. A single access could be a fluke; a second access earns the safe zone.

- **SIEVE** (from NSDI '24): Keys are kept in a linked list with a moving hand. Each key has a visited bit. Hits set `visited = True`. When evicting, the hand advances and clears visited bits until it finds an unvisited entry to evict. New entries are inserted at the head. This gives LRU-like behavior with less bookkeeping — recently used entries survive because the hand passes over them.

### Workload Generators

Six workloads model different access patterns, all seeded for reproducibility:

- **Uniform**: Keys drawn uniformly at random from `[0, num_keys)`. Baseline; no locality.
- **Zipfian**: Power-law distribution (α = 1.1). A small fraction of keys receive the majority of accesses, modeling real-world web and database traffic.
- **HotKey**: 10 hot keys receive 90% of accesses; the remaining 10% are uniform over cold keys. Extreme skew; the hot set fits in any reasonable cache.
- **Scan**: Sequential scan through `[0, num_keys)` wrapping around. Simulates full-table scans; any finite cache is overwhelmed.
- **TemporalLocality**: With 70% probability, re-accesses a key from the last 10 operations; otherwise uniform random. Models bursty access patterns.
- **WriteHeavy**: Uniform keys with a 50/50 read/write split (default is 90% reads). Stresses write strategies and invalidation paths.

### Benchmark Harness

`benchmark.py` has two modes. **Unit mode** drives `cache.py` directly with no network or Raft involvement: 6 policies × 6 workloads × 3 capacities (50, 100, 500) × 50,000 operations = 108 runs. This isolates policy behavior from distributed overhead. **Integration mode** rewrites `config.ini`'s `[Cache]` section per `(policy, capacity)` combination, starts a fresh five-node cluster via `StartRaft`, and drives real gRPC calls through the frontend: 6 policies × 6 workloads × 300 operations = 36 runs. Both modes collect hits, misses, evictions, invalidations, hit rate, average latency, p50/p99 latency, and throughput.

---

## Evaluation

We measure four classes of metrics across all benchmarks:

- **Hit rate** — fraction of reads served from the cache (primary effectiveness metric)
- **Eviction count** — how many entries were displaced; high eviction count relative to hits signals the policy is churning the cache rather than protecting useful data
- **Throughput** — operations per second; captures the per-operation overhead of each policy's data structures
- **Latency** — average and p99 per-operation time; in unit mode this reflects policy overhead, in integration mode it reflects Raft consensus cost plus policy overhead

All unit benchmarks use 1,000 keys and 50,000 operations. Integration benchmarks use a live five-node Raft cluster, 200 keys, and 300 operations at capacity = 50.

---

### Unit Benchmarks

**Hit rate at capacity = 50:**

| Workload | random | fifo | lru | lfu | slru | sieve |
|---|---|---|---|---|---|---|
| hotkey | 0.896 | 0.897 | 0.907 | 0.906 | 0.906 | 0.906 |
| zipfian | 0.511 | 0.509 | 0.567 | 0.652 | 0.651 | **0.664** |
| temporal | 0.712 | 0.718 | 0.718 | 0.377 | 0.718 | 0.716 |
| uniform | 0.050 | 0.050 | 0.050 | 0.050 | 0.049 | 0.049 |
| writeheavy | 0.049 | 0.050 | 0.050 | 0.050 | 0.050 | 0.049 |
| scan | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 |

**Hit rate at capacity = 500:**

| Workload | random | fifo | lru | lfu | slru | sieve |
|---|---|---|---|---|---|---|
| hotkey | 0.947 | 0.947 | 0.947 | 0.947 | 0.948 | 0.947 |
| zipfian | 0.876 | 0.876 | 0.903 | **0.914** | 0.913 | 0.912 |
| temporal | 0.847 | 0.850 | 0.849 | 0.748 | 0.849 | 0.849 |
| uniform | 0.493 | 0.500 | 0.500 | 0.497 | 0.497 | 0.499 |
| writeheavy | 0.494 | 0.502 | 0.504 | 0.499 | 0.498 | 0.500 |
| scan | **0.197** | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 |

**Eviction count at capacity = 50 (out of 50,000 ops):**

| Workload | random | fifo | lru | lfu | slru | sieve |
|---|---|---|---|---|---|---|
| hotkey | 5,158 | 5,123 | 4,631 | 4,664 | 4,654 | 4,655 |
| zipfian | 24,406 | 24,508 | 21,595 | **17,339** | 17,381 | **16,702** |
| temporal | 14,288 | 13,984 | 13,974 | 31,098 | 13,991 | 14,071 |
| uniform | 47,476 | 47,456 | 47,448 | 47,438 | 47,492 | 47,510 |
| writeheavy | 47,476 | 47,456 | 47,448 | 47,438 | 47,434 | 47,510 |
| scan | 49,950 | 49,950 | 49,950 | 49,950 | 49,950 | 49,950 |

**Throughput at capacity = 50 (ops/sec, ×1,000):**

| Workload | random | fifo | lru | lfu | slru | sieve |
|---|---|---|---|---|---|---|
| hotkey | 3,044 | 2,865 | **3,153** | 1,574 | 2,867 | 2,812 |
| zipfian | 1,566 | **2,534** | 2,260 | 1,441 | 2,194 | 1,776 |
| temporal | 2,122 | **2,850** | 2,564 | 1,326 | 2,053 | 1,874 |
| uniform | 1,167 | **1,424** | 1,368 | 1,292 | 1,572 | 974 |
| writeheavy | 1,145 | **2,000** | 1,759 | 1,251 | 1,566 | 950 |
| scan | 1,108 | **1,985** | 1,721 | 1,498 | 1,600 | 933 |

**Average latency at capacity = 50 (µs):**

| Workload | random | fifo | lru | lfu | slru | sieve |
|---|---|---|---|---|---|---|
| hotkey | 0.251 | 0.253 | **0.239** | 0.548 | 0.269 | 0.278 |
| zipfian | 0.548 | **0.310** | 0.354 | 0.610 | 0.368 | 0.479 |
| temporal | 0.386 | **0.267** | 0.308 | 0.664 | 0.397 | 0.449 |
| uniform | 0.760 | 0.589 | 0.621 | 0.690 | **0.556** | 0.943 |
| writeheavy | 0.782 | **0.412** | 0.477 | 0.706 | 0.546 | 0.959 |
| scan | 0.809 | **0.418** | 0.486 | 0.581 | 0.531 | 0.971 |

**Findings:**

**Workload shape dominates policy choice.** HotKey is easy for every policy (hit rates cluster at 0.90); Scan defeats every policy at small capacity (0.000 for all). The interesting workloads — Zipfian and TemporalLocality — are where policy selection produces 15–40 percentage point swings in hit rate.

**Zipfian rewards frequency awareness, and the eviction count shows why.** LFU and SIEVE achieve the best hit rates (0.652 and 0.664 at capacity = 50) because they identify and retain the small set of keys that dominate Zipfian traffic. The eviction table makes the mechanism visible: LFU evicts 17,339 times and SIEVE only 16,702, versus 24,406–24,508 for Random and FIFO. Frequency-aware policies make smarter choices, so they turn over the cache less aggressively. At capacity = 500, LFU continues to lead (0.914) as more of the frequency distribution fits in cache.

**LFU fails on TemporalLocality — and the eviction count reveals the cause.** At capacity = 50, LFU's hit rate collapses to 0.377 while all other policies hit 0.712–0.718. Worse, LFU evicts 31,098 times, more than twice as often as every other policy (~14,000). The TemporalLocality workload accesses keys in recency bursts: the valuable entries are recently active, not historically frequent. LFU's stale counters keep old high-frequency keys and continuously evict the currently popular ones, triggering a self-defeating cycle of high evictions and low hits. This failure mode persists at capacity = 500 (LFU: 0.748 vs. others: 0.848–0.850).

**Random uniquely survives Scan at large capacity.** At capacity = 500 with 1,000 keys, random eviction achieves 0.197 hit rate on Scan while all deterministic policies get 0.000. Deterministic policies always evict the entry that is "next up" in the scan, guaranteeing it will be needed soon. Random occasionally preserves a key that the scan will re-access on its next pass, turning chance into a small but real hit rate.

**FIFO and LRU lead on throughput; LFU and SIEVE trail.** FIFO consistently achieves the highest throughput (1.4M–3.2M ops/sec) because eviction requires only a dequeue from the front — O(1) with minimal overhead. LRU is similarly fast via Python's `OrderedDict.move_to_end`. LFU maxes out at 1.6M ops/sec due to its frequency-bucket management. SIEVE is the slowest overall (933K–2.8M ops/sec) because hand-walking under eviction pressure is expensive in Python's linked-list implementation. All policies operate below 1 µs average latency with p99 under 2.3 µs, so the throughput gap is real but all remain fast in absolute terms.

**Uniform and WriteHeavy are capacity-bound, not policy-bound.** With uniform access over 1,000 keys, no policy can identify a hot set to protect, so hit rate tracks capacity/num_keys (~5% at 50, ~50% at 500) regardless of policy. Eviction counts confirm this: all policies evict ~47,400–47,500 times at capacity = 50, indicating constant churn with no policy gaining an advantage.

---

### Integration Benchmarks

**Hit rate (integration, capacity = 50):**

| Workload | random | fifo | lru | lfu | slru | sieve |
|---|---|---|---|---|---|---|
| hotkey | 0.842 | 0.849 | 0.853 | 0.849 | 0.845 | **0.860** |
| zipfian | 0.554 | 0.625 | 0.622 | 0.629 | **0.652** | 0.622 |
| temporal | 0.708 | **0.731** | 0.705 | 0.727 | 0.689 | 0.678 |
| uniform | 0.217 | 0.250 | 0.239 | 0.250 | 0.243 | 0.257 |
| writeheavy | 0.244 | 0.244 | 0.237 | 0.244 | **0.282** | 0.205 |
| scan | 0.034 | 0.026 | 0.011 | 0.004 | 0.026 | 0.011 |

**Throughput (integration, ops/sec):**

| Workload | random | fifo | lru | lfu | slru | sieve |
|---|---|---|---|---|---|---|
| hotkey | 111.6 | **114.3** | 112.8 | 73.3 | 79.1 | 88.5 |
| zipfian | 118.3 | **113.4** | 103.0 | 64.8 | 76.2 | 70.8 |
| temporal | 101.3 | **112.2** | 79.5 | 76.9 | 75.2 | 70.4 |
| uniform | **129.3** | 127.5 | 83.5 | 79.0 | 81.5 | 106.8 |
| writeheavy | 34.3 | **36.1** | 28.5 | 31.1 | 34.7 | 34.5 |
| scan | 117.3 | **120.6** | 99.0 | 78.2 | 49.4 | 103.5 |

**Median (p50) and p99 latency (integration, ms):**

| Workload | fifo p50 | fifo p99 | lfu p50 | lfu p99 | lru p50 | lru p99 | sieve p50 | sieve p99 |
|---|---|---|---|---|---|---|---|---|
| hotkey | 2.6 | 55.2 | 7.7 | 60.7 | 2.6 | 54.9 | 5.3 | 58.0 |
| zipfian | 3.0 | 55.5 | 9.0 | 65.1 | 4.1 | 56.5 | 7.9 | 61.1 |
| uniform | 2.9 | 54.6 | 7.7 | 64.2 | 6.3 | 62.2 | 4.6 | 56.0 |
| writeheavy | 7.6 | 61.2 | 11.5 | 62.7 | 19.1 | 76.8 | 6.0 | 58.0 |
| scan | 2.5 | 54.9 | 7.1 | 59.5 | 4.5 | 56.5 | 4.1 | 55.8 |

**Findings:**

**Hit rates are directionally consistent with unit mode but compressed.** With only 300 operations, caches have less warmup time, so policy differences are smaller in magnitude. The relative ordering mostly holds: SIEVE leads on HotKey (0.860), SLRU leads on Zipfian (0.652), FIFO leads on TemporalLocality (0.731).

**Throughput collapses under Raft consensus — policy overhead becomes visible again.** Read-heavy workloads achieve 70–130 ops/sec and write-heavy workloads drop to 29–36 ops/sec, versus millions of ops/sec in unit mode. The Raft round-trip dominates. Yet the policy ordering from unit mode re-emerges: FIFO and Random lead, LFU consistently trails at 64–79 ops/sec. The frequency-bucket overhead that costs LFU 2× throughput in unit mode also costs it ~2× throughput in integration.

**Median latency (p50) exposes policy cost; p99 is Raft-dominated.** FIFO achieves 2.5–3.0ms p50 on read-heavy workloads; LFU is 3× slower at 7–9ms p50. The p99 column tells a different story: all policies land between 54–77ms with no meaningful ordering. At the tail, Raft network jitter and leader election timers dominate — policy bookkeeping is irrelevant. This means p50 is the right metric for comparing policy efficiency in a distributed system; p99 is a floor set by the consensus protocol.

**WriteHeavy exposes write-path cost.** All policies drop to ~30 ops/sec because every `Put` requires log replication to a majority of 5 nodes before the state machine (and cache) is updated. The consensus path, not eviction policy, is the bottleneck. Within WriteHeavy, FIFO's 7.6ms p50 vs. LRU's 19.1ms p50 is notable: with 50% of operations being writes, LRU's `OrderedDict.move_to_end` call on every cache update accumulates under the Raft server's reentrant lock, inflating median latency.

---

### Failure-Case Analysis

**Scan workload.** All policies fail at small-to-medium capacity because sequential access evicts entries before they can be reused. Eviction counts hit 49,950 out of 50,000 ops — nearly every put displaces something. Mitigation: detect sequential access patterns and bypass the cache (don't populate it for scan reads), or use a large enough capacity to hold the full scanned range.

**LFU under temporal workloads.** Stale frequency counters keep historically popular keys and discard currently popular ones, producing 2× the evictions of other policies and roughly half the hit rate at small capacity. Mitigation: decay frequency counts over time (windowed LFU) or use SLRU, which achieves similar frequency awareness through its probation/protected split without accumulating stale state.

**WriteHeavy with write-invalidate.** Under write-invalidate strategy, every committed write evicts its entry from the cache, requiring a miss on the next read. With 50% writes, this would eliminate nearly all cache benefit. Write-through is the correct strategy for write-heavy workloads: it keeps the cache populated with current values at the cost of updating on every write, which under Raft is a committed-entry update already paid for by consensus.

---

## Conclusion

We built a five-node Raft KV store with a pluggable cache layer, six eviction policies, six benchmark workloads, and a harness that evaluates all combinations in both isolated and distributed settings.

The central finding is that **workload shape dominates policy choice**. HotKey is easy for every policy; Scan defeats every policy; Zipfian and TemporalLocality are where policy selection actually matters. No single policy wins everywhere, but SIEVE is the most consistent performer: it matches or beats LRU on most workloads, handles Zipfian nearly as well as LFU, and avoids LFU's failure on temporal workloads. Its simpler bookkeeping (visited bits vs. frequency buckets) also translates to better throughput in the distributed setting.

The integration results confirm that the cache layer is effective: reducing hits to the Raft state machine meaningfully reduces per-operation latency on read-heavy workloads. The Raft consensus path dominates write latency regardless of cache policy, so write-heavy optimization should target batching or leader locality rather than eviction strategy.

Future directions include MAT (a small ML model scoring eviction candidates from recent access features), per-server policy heterogeneity allowing different nodes to run different policies in the same cluster, and adaptive policy switching that detects workload shifts at runtime.
