import bisect
import random
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Iterator, Optional


@dataclass
class Operation:
    op: str  # "get" or "put"
    key: str
    value: Optional[str] = None


class Workload(ABC):
    """Generates a sequence of cache operations for benchmarking.

    Subclasses implement `_next_key` to define the access distribution.
    The base class handles the get/put split, value generation, and seeding.
    """

    def __init__(self, num_keys: int, read_ratio: float = 0.9, seed: Optional[int] = None):
        if num_keys <= 0:
            raise ValueError("num_keys must be positive")
        if not 0.0 <= read_ratio <= 1.0:
            raise ValueError("read_ratio must be in [0, 1]")
        self._num_keys = num_keys
        self._read_ratio = read_ratio
        self._rng = random.Random(seed)

    def generate(self, num_ops: int) -> Iterator[Operation]:
        for _ in range(num_ops):
            key = self._next_key()
            if self._rng.random() < self._read_ratio:
                yield Operation("get", key)
            else:
                yield Operation("put", key, self._make_value(key))

    @abstractmethod
    def _next_key(self) -> str:
        """Return the next key per this workload's access distribution."""
        ...

    def _make_value(self, key: str) -> str:
        return f"val_{key}"

    @staticmethod
    def _format_key(i: int) -> str:
        return f"key_{i}"


class UniformWorkload(Workload):
    """Keys drawn uniformly from [0, num_keys)."""

    def _next_key(self) -> str:
        return self._format_key(self._rng.randrange(self._num_keys))


class ZipfianWorkload(Workload):
    """Keys drawn from a Zipfian distribution: rank k has weight 1/(k+1)^alpha.
    Higher alpha => more skew toward a few hot keys.
    """

    def __init__(self, num_keys: int, alpha: float = 1.1, read_ratio: float = 0.9, seed: Optional[int] = None):
        super().__init__(num_keys, read_ratio, seed)
        if alpha <= 0:
            raise ValueError("alpha must be positive")
        self._alpha = alpha
        self._cdf = self._build_cdf(num_keys, alpha)

    @staticmethod
    def _build_cdf(num_keys: int, alpha: float) -> list[float]:
        cdf: list[float] = []
        running = 0.0
        for k in range(num_keys):
            running += 1.0 / ((k + 1) ** alpha)
            cdf.append(running)
        total = cdf[-1]
        return [c / total for c in cdf]

    def _next_key(self) -> str:
        rank = bisect.bisect_left(self._cdf, self._rng.random())
        if rank >= self._num_keys:
            rank = self._num_keys - 1
        return self._format_key(rank)


class HotKeyWorkload(Workload):
    """A small hot set receives `hot_fraction` of accesses; the rest go to cold keys."""

    def __init__(
        self,
        num_keys: int,
        hot_keys: int = 10,
        hot_fraction: float = 0.9,
        read_ratio: float = 0.9,
        seed: Optional[int] = None,
    ):
        super().__init__(num_keys, read_ratio, seed)
        if not 0 < hot_keys < num_keys:
            raise ValueError("hot_keys must be in (0, num_keys)")
        if not 0.0 <= hot_fraction <= 1.0:
            raise ValueError("hot_fraction must be in [0, 1]")
        self._hot_keys = hot_keys
        self._hot_fraction = hot_fraction

    def _next_key(self) -> str:
        if self._rng.random() < self._hot_fraction:
            idx = self._rng.randrange(self._hot_keys)
        else:
            idx = self._rng.randrange(self._hot_keys, self._num_keys)
        return self._format_key(idx)


def create_workload(name: str, num_keys: int, **kwargs) -> Workload:
    workloads = {
        "uniform": UniformWorkload,
        "zipfian": ZipfianWorkload,
        "hotkey": HotKeyWorkload,
    }
    workload_cls = workloads.get(name.lower())
    if workload_cls is None:
        raise ValueError(f"Unknown workload: {name}. Available: {list(workloads.keys())}")
    return workload_cls(num_keys, **kwargs)
