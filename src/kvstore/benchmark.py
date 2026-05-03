#!/usr/bin/env python3
"""Cache benchmark harness with unit-level and integration-level modes.

Unit mode: drives the pluggable cache layer in `cache.py` directly against
a simulated backing store. Fast, no network, fully deterministic per-seed
for all counter fields.

Integration mode: spawns a real frontend + 5-server Raft cluster and
drives traffic through the frontend gRPC service. Cache stats are pulled
from each node via `GetCacheStats` and summed across the cluster. Timing
metrics reflect end-to-end client-visible latency (Raft consensus for
writes, single-hop reads). Counter fields are not bit-exact across runs
because the frontend routes Gets to whichever server is first to respond
to a state query, but hit rate and eviction totals should be stable.

Both modes emit the same `BenchmarkResult` schema with a `mode` field
distinguishing them. CSV output paths default to
`benchmark_results.csv` (unit) and `benchmark_integration_results.csv`
(integration).

Simulated flow (unit mode)
--------------------------
Matches `server.py` Get/Put behavior with `write_through`:
- GET: `cache.get(key)`. On miss, if the key exists in the backing store,
  populate the cache via `cache.put(key, backing[key])`.
- PUT: write to backing store; then either `cache.put` (write_through)
  or `cache.invalidate` (write_invalidate).

The backing store is pre-populated with every key in `[0, num_keys)` so
benchmarks reflect steady-state cache performance, not cold-start misses.

Integration mode
----------------
For each (policy, capacity) combination the harness rewrites
`config.ini`'s `[Cache]` section, calls `StartRaft` to respawn all
servers with the new config, waits for leader election, and drives the
workload through the frontend. Between combinations the cluster is
restarted so every run sees a fresh cache. The original `config.ini` is
restored on exit.
"""

import argparse
import atexit
import configparser
import csv
import os
import random
import signal
import statistics
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Optional

import grpc

import raft_pb2
import raft_pb2_grpc
from cache import create_cache
from workloads import create_workload


# non integration mode defaults, doesn't run through raft.
POLICIES = ["random", "fifo", "lru", "lfu", "slru", "sieve"]
WORKLOADS = ["uniform", "zipfian", "hotkey", "scan", "temporal", "writeheavy"]
CAPACITIES = [50, 100, 500]
DEFAULT_NUM_KEYS = 1000
DEFAULT_NUM_OPS = 50_000
DEFAULT_SEED = 42

# Integration-mode defaults — far smaller than unit since every op
# crosses the network and every PUT runs a Raft consensus round.
# num_keys is deliberately ~4x the max capacity so evictions happen and
# hit rate is informative. If num_keys <= capacity, the pre-populate
# phase fills the cache with every key and every GET becomes a hit.
INTEGRATION_POLICIES = ["random", "fifo", "lru", "sieve"]
INTEGRATION_WORKLOADS = ["uniform", "zipfian", "hotkey", "scan", "temporal", "writeheavy"]
INTEGRATION_CAPACITIES = [50]
DEFAULT_INTEGRATION_NUM_KEYS = 200
DEFAULT_INTEGRATION_NUM_OPS = 300
DEFAULT_NUM_SERVERS = 5
DEFAULT_FRONTEND_PORT = 8001
DEFAULT_BASE_SERVER_PORT = 9001

WORKLOAD_KWARGS: dict[str, dict] = {
    "uniform": {},
    "zipfian": {"alpha": 1.1},
    "hotkey": {"hot_keys": 5, "hot_fraction": 0.9},
    "scan": {},
    "temporal": {"window_size": 5, "reaccess_prob": 0.7},
    "writeheavy": {},
}


@dataclass(frozen=True)
class BenchmarkResult:
    mode: str
    policy: str
    workload: str
    workload_params: str
    num_keys: int
    capacity: int
    num_ops: int
    seed: int
    write_strategy: str
    hits: int
    misses: int
    evictions: int
    invalidations: int
    hit_rate: float
    avg_latency_us: float
    p50_latency_us: float
    p99_latency_us: float
    throughput_ops_per_sec: float


def _percentile(sorted_values: list[float], p: float) -> float:
    if not sorted_values:
        return 0.0
    k = (len(sorted_values) - 1) * p / 100.0
    lo = int(k)
    hi = min(lo + 1, len(sorted_values) - 1)
    return sorted_values[lo] + (sorted_values[hi] - sorted_values[lo]) * (k - lo)


def _format_params(kwargs: dict) -> str:
    if not kwargs:
        return ""
    return ",".join(f"{k}={v}" for k, v in sorted(kwargs.items()))


def run_benchmark(
    policy: str,
    workload_name: str,
    num_keys: int,
    capacity: int,
    num_ops: int,
    seed: int = DEFAULT_SEED,
    workload_kwargs: Optional[dict] = None,
    write_strategy: str = "write_through",
) -> BenchmarkResult:
    """Run one (policy, workload, capacity) combination.

    Deterministic in hit_rate/evictions/invalidations for a fixed seed.
    """
    if write_strategy not in ("write_through", "write_invalidate"):
        raise ValueError(f"Unknown write_strategy: {write_strategy}")

    workload_kwargs = workload_kwargs or {}
    workload = create_workload(
        workload_name, num_keys=num_keys, seed=seed, **workload_kwargs
    )
    # Seed the global `random` module so RandomEviction is deterministic.
    random.seed(seed)
    cache = create_cache(policy, capacity)

    backing: dict[str, str] = {f"key_{i}": f"val_{i}" for i in range(num_keys)}
    ops = list(workload.generate(num_ops))
    latencies_ns: list[int] = []

    wall_start = time.perf_counter()
    for op in ops:
        t0 = time.perf_counter_ns()
        if op.op == "get":
            cached = cache.get(op.key)
            if cached is None and op.key in backing:
                cache.put(op.key, backing[op.key])
        else:
            backing[op.key] = op.value
            if write_strategy == "write_through":
                cache.put(op.key, op.value)
            else:
                cache.invalidate(op.key)
        latencies_ns.append(time.perf_counter_ns() - t0)
    wall_elapsed = time.perf_counter() - wall_start

    latencies_us = sorted(ns / 1000.0 for ns in latencies_ns)
    s = cache.stats
    return BenchmarkResult(
        mode="unit",
        policy=policy,
        workload=workload_name,
        workload_params=_format_params(workload_kwargs),
        num_keys=num_keys,
        capacity=capacity,
        num_ops=num_ops,
        seed=seed,
        write_strategy=write_strategy,
        hits=s.hits,
        misses=s.misses,
        evictions=s.evictions,
        invalidations=s.invalidations,
        hit_rate=s.hit_rate,
        avg_latency_us=statistics.fmean(latencies_us) if latencies_us else 0.0,
        p50_latency_us=_percentile(latencies_us, 50),
        p99_latency_us=_percentile(latencies_us, 99),
        throughput_ops_per_sec=num_ops / wall_elapsed if wall_elapsed > 0 else 0.0,
    )


def run_matrix(
    policies: list[str] = POLICIES,
    workloads: list[str] = WORKLOADS,
    capacities: list[int] = CAPACITIES,
    num_keys: int = DEFAULT_NUM_KEYS,
    num_ops: int = DEFAULT_NUM_OPS,
    seed: int = DEFAULT_SEED,
    workload_kwargs: Optional[dict[str, dict]] = None,
) -> list[BenchmarkResult]:
    kwargs_by_workload = workload_kwargs if workload_kwargs is not None else WORKLOAD_KWARGS
    results: list[BenchmarkResult] = []
    total = len(policies) * len(workloads) * len(capacities)
    done = 0
    for workload in workloads:
        for capacity in capacities:
            for policy in policies:
                done += 1
                r = run_benchmark(
                    policy=policy,
                    workload_name=workload,
                    num_keys=num_keys,
                    capacity=capacity,
                    num_ops=num_ops,
                    seed=seed,
                    workload_kwargs=kwargs_by_workload.get(workload, {}),
                )
                results.append(r)
                print(
                    f"[{done:>2}/{total}] {workload:<10} cap={capacity:<4} "
                    f"{policy:<7} hit_rate={r.hit_rate:.4f} "
                    f"evictions={r.evictions:>6} "
                    f"avg_lat={r.avg_latency_us:>7.2f}us "
                    f"p99={r.p99_latency_us:>7.2f}us "
                    f"throughput={r.throughput_ops_per_sec:>10,.0f} ops/s"
                )
    return results


def write_csv(results: list[BenchmarkResult], path: Path) -> None:
    if not results:
        return
    fieldnames = list(asdict(results[0]).keys())
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in results:
            writer.writerow(asdict(r))


def print_hit_rate_table(results: list[BenchmarkResult]) -> None:
    """Pivot: rows=(workload, capacity), cols=policy, cells=hit_rate."""
    workloads = sorted({r.workload for r in results})
    capacities = sorted({r.capacity for r in results})
    policies = sorted({r.policy for r in results})

    header = f"{'workload':<10} {'cap':>5}  " + "  ".join(f"{p:>8}" for p in policies)
    print("\nHit rate by (workload, capacity) x policy:")
    print(header)
    print("-" * len(header))
    for w in workloads:
        for c in capacities:
            row = {r.policy: r for r in results if r.workload == w and r.capacity == c}
            cells = "  ".join(
                f"{row[p].hit_rate:>8.4f}" if p in row else f"{'-':>8}"
                for p in policies
            )
            print(f"{w:<10} {c:>5}  {cells}")


def print_latency_table(results: list[BenchmarkResult], metric: str = "p99_latency_us") -> None:
    """Pivot: rows=(workload, capacity), cols=policy, cells=latency in microseconds."""
    workloads = sorted({r.workload for r in results})
    capacities = sorted({r.capacity for r in results})
    policies = sorted({r.policy for r in results})

    label = {
        "avg_latency_us": "Avg latency (us)",
        "p50_latency_us": "p50 latency (us)",
        "p99_latency_us": "p99 latency (us)",
    }.get(metric, metric)
    header = f"{'workload':<10} {'cap':>5}  " + "  ".join(f"{p:>10}" for p in policies)
    print(f"\n{label} by (workload, capacity) x policy:")
    print(header)
    print("-" * len(header))
    for w in workloads:
        for c in capacities:
            row = {r.policy: r for r in results if r.workload == w and r.capacity == c}
            cells = "  ".join(
                f"{getattr(row[p], metric):>10.2f}" if p in row else f"{'-':>10}"
                for p in policies
            )
            print(f"{w:<10} {c:>5}  {cells}")


CONFIG_PATH = Path(__file__).parent / "config.ini"


def _set_cache_config(policy: str, capacity: int, write_strategy: str) -> None:
    """Rewrite [Cache] section in config.ini; preserves other sections."""
    cfg = configparser.ConfigParser()
    cfg.read(CONFIG_PATH)
    if "Cache" not in cfg:
        cfg["Cache"] = {}
    cfg["Cache"]["policy"] = policy
    cfg["Cache"]["capacity"] = str(capacity)
    cfg["Cache"]["write_strategy"] = write_strategy
    with CONFIG_PATH.open("w") as f:
        cfg.write(f)


def _backup_config() -> str:
    return CONFIG_PATH.read_text()


def _restore_config(contents: str) -> None:
    CONFIG_PATH.write_text(contents)


class IntegrationCluster:
    """Manages a frontend + Raft server cluster for benchmarking.

    Call `start()` once to spawn the frontend. Use `start_raft()` to
    (re)spawn all N servers with whatever config.ini currently says —
    call this after rewriting the `[Cache]` section. `stop()` terminates
    everything.
    """

    def __init__(
        self,
        num_servers: int = DEFAULT_NUM_SERVERS,
        frontend_port: int = DEFAULT_FRONTEND_PORT,
        base_server_port: int = DEFAULT_BASE_SERVER_PORT,
        rpc_timeout: float = 5.0,
        frontend_startup_timeout: float = 30.0,
        leader_election_timeout: float = 30.0,
        startup_wait: float = 4.0,
    ):
        self.num_servers = num_servers
        self.frontend_port = frontend_port
        self.base_server_port = base_server_port
        self.rpc_timeout = rpc_timeout
        self.frontend_startup_timeout = frontend_startup_timeout
        self.leader_election_timeout = leader_election_timeout
        self.startup_wait = startup_wait
        self._project_root = os.path.dirname(os.path.abspath(__file__))
        self._frontend_process: Optional[subprocess.Popen] = None
        self._frontend_channel: Optional[grpc.Channel] = None
        self._frontend_stub: Optional[raft_pb2_grpc.FrontEndStub] = None

    @staticmethod
    def _kill_stray_processes() -> None:
        for pattern in ("frontend.py", "server.py"):
            subprocess.run(["pkill", "-f", pattern], capture_output=True)
        time.sleep(1)

    def start(self) -> None:
        """Spawn the frontend and wait until it accepts RPCs."""
        self._kill_stray_processes()
        self._frontend_process = subprocess.Popen(
            [sys.executable, os.path.join(self._project_root, "frontend.py")],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            cwd=self._project_root,
            env={**os.environ, "PYTHONUNBUFFERED": "1"},
            preexec_fn=os.setsid,
        )
        deadline = time.time() + self.frontend_startup_timeout
        last_err: Optional[str] = None
        while time.time() < deadline:
            try:
                ch = grpc.insecure_channel(f"127.0.0.1:{self.frontend_port}")
                stub = raft_pb2_grpc.FrontEndStub(ch)
                stub.Get(
                    raft_pb2.GetKey(key="_ping", clientId=0, requestId=0),
                    timeout=2,
                )
                ch.close()
                self._frontend_channel = grpc.insecure_channel(
                    f"127.0.0.1:{self.frontend_port}"
                )
                self._frontend_stub = raft_pb2_grpc.FrontEndStub(self._frontend_channel)
                return
            except Exception as e:
                last_err = str(e)
                time.sleep(1)
        raise RuntimeError(f"Frontend failed to start: {last_err}")

    def start_raft(self) -> int:
        """(Re)start all N servers and wait for leader election."""
        assert self._frontend_stub is not None, "call start() first"
        resp = self._frontend_stub.StartRaft(
            raft_pb2.IntegerArg(arg=self.num_servers), timeout=30
        )
        if resp.error:
            raise RuntimeError(f"StartRaft failed: {resp.error}")
        time.sleep(self.startup_wait)
        return self.wait_for_leader()

    def wait_for_leader(self) -> int:
        deadline = time.time() + self.leader_election_timeout
        while time.time() < deadline:
            for sid in range(self.num_servers):
                try:
                    ch = grpc.insecure_channel(
                        f"127.0.0.1:{self.base_server_port + sid}"
                    )
                    stub = raft_pb2_grpc.KeyValueStoreStub(ch)
                    state = stub.GetState(raft_pb2.Empty(), timeout=2)
                    ch.close()
                    if state.isLeader:
                        return sid
                except Exception:
                    pass
            time.sleep(0.5)
        raise RuntimeError("No leader elected within timeout")

    def put(self, key: str, value: str, retries: int = 5) -> None:
        """Send PUT through frontend; retry on wrongLeader / transient errors."""
        assert self._frontend_stub is not None
        last_err: Optional[str] = None
        for _ in range(retries):
            try:
                resp = self._frontend_stub.Put(
                    raft_pb2.KeyValue(
                        key=key, value=value, clientId=1, requestId=1
                    ),
                    timeout=self.rpc_timeout,
                )
                if not resp.wrongLeader:
                    return
                last_err = resp.error or "wrongLeader"
            except grpc.RpcError as e:
                last_err = str(e)
            time.sleep(0.3)
        raise RuntimeError(f"PUT({key!r}) failed after {retries} retries: {last_err}")

    def get(self, key: str) -> str:
        assert self._frontend_stub is not None
        resp = self._frontend_stub.Get(
            raft_pb2.GetKey(key=key, clientId=1, requestId=1),
            timeout=self.rpc_timeout,
        )
        return resp.value

    def get_cache_stats_all(self) -> list:
        """Per-server CacheStatsResponse (or None for servers we couldn't reach)."""
        stats = []
        for sid in range(self.num_servers):
            try:
                ch = grpc.insecure_channel(
                    f"127.0.0.1:{self.base_server_port + sid}"
                )
                stub = raft_pb2_grpc.KeyValueStoreStub(ch)
                s = stub.GetCacheStats(raft_pb2.Empty(), timeout=self.rpc_timeout)
                ch.close()
                stats.append(s)
            except Exception:
                stats.append(None)
        return stats

    def stop(self) -> None:
        if self._frontend_channel is not None:
            try:
                self._frontend_channel.close()
            except Exception:
                pass
            self._frontend_channel = None
            self._frontend_stub = None
        proc = self._frontend_process
        if proc is not None and proc.poll() is None:
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
                proc.wait(timeout=5)
            except (subprocess.TimeoutExpired, ProcessLookupError, OSError):
                try:
                    os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                    proc.wait(timeout=2)
                except (subprocess.TimeoutExpired, ProcessLookupError, OSError):
                    pass
        self._frontend_process = None
        self._kill_stray_processes()


def run_integration_benchmark(
    cluster: IntegrationCluster,
    policy: str,
    workload_name: str,
    num_keys: int,
    capacity: int,
    num_ops: int,
    seed: int = DEFAULT_SEED,
    workload_kwargs: Optional[dict] = None,
    write_strategy: str = "write_through",
    settle_seconds: float = 1.0,
) -> BenchmarkResult:
    """Run one (policy, workload, capacity) against a live cluster.

    Caller is responsible for rewriting config.ini and calling
    `cluster.start_raft()` before this function so the cluster is fresh.
    Pre-populates `num_keys` backing entries via frontend PUT so GETs
    don't all miss on a cold state machine. Pre-populate PUTs do not
    register hits/misses against the cache, only evictions, so the
    reported hit_rate reflects the workload phase.
    """
    workload_kwargs = workload_kwargs or {}
    workload = create_workload(
        workload_name, num_keys=num_keys, seed=seed, **workload_kwargs
    )

    if num_keys <= capacity:
        print(
            f"    WARNING: num_keys ({num_keys}) <= capacity ({capacity}); "
            "every key fits in cache, hit_rate will be ~1.0"
        )
    print(f"    pre-populating {num_keys} keys...")
    for i in range(num_keys):
        cluster.put(f"key_{i}", f"val_{i}")

    ops = list(workload.generate(num_ops))
    latencies_ns: list[int] = []

    print(f"    running {num_ops} ops...")
    wall_start = time.perf_counter()
    for op in ops:
        t0 = time.perf_counter_ns()
        if op.op == "get":
            cluster.get(op.key)
        else:
            cluster.put(op.key, op.value)
        latencies_ns.append(time.perf_counter_ns() - t0)
    wall_elapsed = time.perf_counter() - wall_start

    time.sleep(settle_seconds)  # let any in-flight replication apply
    stats_all = cluster.get_cache_stats_all()
    hits = sum(s.hits for s in stats_all if s is not None)
    misses = sum(s.misses for s in stats_all if s is not None)
    evictions = sum(s.evictions for s in stats_all if s is not None)
    invalidations = sum(s.invalidations for s in stats_all if s is not None)
    total = hits + misses
    hit_rate = hits / total if total > 0 else 0.0

    latencies_us = sorted(ns / 1000.0 for ns in latencies_ns)
    return BenchmarkResult(
        mode="integration",
        policy=policy,
        workload=workload_name,
        workload_params=_format_params(workload_kwargs),
        num_keys=num_keys,
        capacity=capacity,
        num_ops=num_ops,
        seed=seed,
        write_strategy=write_strategy,
        hits=hits,
        misses=misses,
        evictions=evictions,
        invalidations=invalidations,
        hit_rate=hit_rate,
        avg_latency_us=statistics.fmean(latencies_us) if latencies_us else 0.0,
        p50_latency_us=_percentile(latencies_us, 50),
        p99_latency_us=_percentile(latencies_us, 99),
        throughput_ops_per_sec=num_ops / wall_elapsed if wall_elapsed > 0 else 0.0,
    )


def run_integration_matrix(
    policies: list[str] = INTEGRATION_POLICIES,
    workloads: list[str] = INTEGRATION_WORKLOADS,
    capacities: list[int] = INTEGRATION_CAPACITIES,
    num_keys: int = DEFAULT_INTEGRATION_NUM_KEYS,
    num_ops: int = DEFAULT_INTEGRATION_NUM_OPS,
    seed: int = DEFAULT_SEED,
    workload_kwargs: Optional[dict[str, dict]] = None,
    write_strategy: str = "write_through",
    num_servers: int = DEFAULT_NUM_SERVERS,
    frontend_port: int = DEFAULT_FRONTEND_PORT,
    base_server_port: int = DEFAULT_BASE_SERVER_PORT,
) -> list[BenchmarkResult]:
    """Run the integration-mode matrix.

    Rewrites config.ini's [Cache] section for each (policy, capacity)
    combination and calls StartRaft between every run so each workload
    sees a fresh cache. Restores the original config.ini on completion
    (or on crash/signal).
    """
    kwargs_by_workload = (
        workload_kwargs if workload_kwargs is not None else WORKLOAD_KWARGS
    )

    original_config = _backup_config()
    cluster = IntegrationCluster(
        num_servers=num_servers,
        frontend_port=frontend_port,
        base_server_port=base_server_port,
    )

    def _cleanup():
        try:
            cluster.stop()
        finally:
            _restore_config(original_config)

    atexit.register(_cleanup)
    prev_sigint = signal.getsignal(signal.SIGINT)
    prev_sigterm = signal.getsignal(signal.SIGTERM)

    def _sig_handler(signum, frame):
        _cleanup()
        sys.exit(1)

    signal.signal(signal.SIGINT, _sig_handler)
    signal.signal(signal.SIGTERM, _sig_handler)

    results: list[BenchmarkResult] = []
    try:
        print("Starting frontend...")
        cluster.start()
        print("Frontend ready.")

        total = len(policies) * len(capacities) * len(workloads)
        done = 0
        for policy in policies:
            for capacity in capacities:
                _set_cache_config(policy, capacity, write_strategy)
                for workload in workloads:
                    done += 1
                    print(
                        f"\n[{done}/{total}] policy={policy} capacity={capacity} "
                        f"workload={workload}"
                    )
                    # Fresh cluster per workload so caches start empty.
                    leader = cluster.start_raft()
                    print(f"    leader elected: server {leader}")
                    r = run_integration_benchmark(
                        cluster=cluster,
                        policy=policy,
                        workload_name=workload,
                        num_keys=num_keys,
                        capacity=capacity,
                        num_ops=num_ops,
                        seed=seed,
                        workload_kwargs=kwargs_by_workload.get(workload, {}),
                        write_strategy=write_strategy,
                    )
                    results.append(r)
                    print(
                        f"    hit_rate={r.hit_rate:.4f} evictions={r.evictions} "
                        f"avg_lat={r.avg_latency_us:,.1f}us "
                        f"p99={r.p99_latency_us:,.1f}us "
                        f"tput={r.throughput_ops_per_sec:,.0f} ops/s"
                    )
    finally:
        _cleanup()
        atexit.unregister(_cleanup)
        signal.signal(signal.SIGINT, prev_sigint)
        signal.signal(signal.SIGTERM, prev_sigterm)

    return results


def _parse_csv_list(s: Optional[str], cast=str) -> Optional[list]:
    if s is None:
        return None
    return [cast(x.strip()) for x in s.split(",") if x.strip()]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--mode", choices=["unit", "integration"], default="unit",
        help="unit: in-process cache only. integration: drive real Raft cluster via gRPC.",
    )
    parser.add_argument("--policies", type=str, default=None,
                        help="comma-separated, e.g. lru,lfu,fifo")
    parser.add_argument("--workloads", type=str, default=None,
                        help="comma-separated, e.g. uniform,zipfian")
    parser.add_argument("--capacities", type=str, default=None,
                        help="comma-separated ints, e.g. 50,100")
    parser.add_argument("--num-keys", type=int, default=None)
    parser.add_argument("--num-ops", type=int, default=None)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--write-strategy", type=str, default="write_through",
                        choices=["write_through", "write_invalidate"])
    parser.add_argument("--output", type=str, default=None)
    parser.add_argument("--num-servers", type=int, default=DEFAULT_NUM_SERVERS,
                        help="integration only")
    parser.add_argument("--frontend-port", type=int, default=DEFAULT_FRONTEND_PORT,
                        help="integration only")
    parser.add_argument("--base-server-port", type=int, default=DEFAULT_BASE_SERVER_PORT,
                        help="integration only")
    args = parser.parse_args()

    policies = _parse_csv_list(args.policies, str)
    workloads = _parse_csv_list(args.workloads, str)
    capacities = _parse_csv_list(args.capacities, int)

    if args.mode == "unit":
        pol = policies or POLICIES
        wl = workloads or WORKLOADS
        caps = capacities or CAPACITIES
        nk = args.num_keys if args.num_keys is not None else DEFAULT_NUM_KEYS
        nops = args.num_ops if args.num_ops is not None else DEFAULT_NUM_OPS
        out_path = Path(args.output) if args.output else (
            Path(__file__).parent / "benchmark_results.csv"
        )

        print(
            f"Running UNIT benchmark matrix: {len(pol)} policies x "
            f"{len(wl)} workloads x {len(caps)} capacities\n"
            f"  num_keys={nk}, num_ops={nops:,}, seed={args.seed}"
        )
        for w, kw in WORKLOAD_KWARGS.items():
            if w in wl and kw:
                print(f"  {w}: {_format_params(kw)}")
        print()

        results = run_matrix(
            policies=pol, workloads=wl, capacities=caps,
            num_keys=nk, num_ops=nops, seed=args.seed,
        )
    else:
        pol = policies or INTEGRATION_POLICIES
        wl = workloads or INTEGRATION_WORKLOADS
        caps = capacities or INTEGRATION_CAPACITIES
        nk = args.num_keys if args.num_keys is not None else DEFAULT_INTEGRATION_NUM_KEYS
        nops = args.num_ops if args.num_ops is not None else DEFAULT_INTEGRATION_NUM_OPS
        out_path = Path(args.output) if args.output else (
            Path(__file__).parent / "benchmark_integration_results.csv"
        )

        print(
            f"Running INTEGRATION benchmark matrix: {len(pol)} policies x "
            f"{len(wl)} workloads x {len(caps)} capacities\n"
            f"  num_keys={nk}, num_ops={nops}, seed={args.seed}, "
            f"write_strategy={args.write_strategy}\n"
            f"  cluster: {args.num_servers} servers, frontend :{args.frontend_port}"
        )
        print()

        results = run_integration_matrix(
            policies=pol, workloads=wl, capacities=caps,
            num_keys=nk, num_ops=nops, seed=args.seed,
            write_strategy=args.write_strategy,
            num_servers=args.num_servers,
            frontend_port=args.frontend_port,
            base_server_port=args.base_server_port,
        )

    write_csv(results, out_path)
    print(f"\nWrote {len(results)} rows to {out_path}")
    print_hit_rate_table(results)
    print_latency_table(results, metric="avg_latency_us")
    print_latency_table(results, metric="p99_latency_us")


if __name__ == "__main__":
    main()
