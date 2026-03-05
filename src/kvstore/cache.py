import random
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class CacheStats:
    hits: int = 0
    misses: int = 0
    evictions: int = 0
    invalidations: int = 0
    current_size: int = 0
    capacity: int = 0

    @property
    def hit_rate(self) -> float:
        total = self.hits + self.misses
        return self.hits / total if total > 0 else 0.0


class CachePolicy(ABC):
    def __init__(self, capacity: int):
        if capacity <= 0:
            raise ValueError("Cache capacity must be positive")
        self._stats = CacheStats(capacity=capacity)

    @property
    def stats(self) -> CacheStats:
        return self._stats

    @abstractmethod
    def get(self, key: str) -> Optional[str]:
        """Return cached value or None on miss."""
        ...

    @abstractmethod
    def put(self, key: str, value: str) -> Optional[str]:
        """Insert key-value pair. Returns evicted key or None."""
        ...

    @abstractmethod
    def invalidate(self, key: str) -> bool:
        """Remove key from cache. Returns whether key existed."""
        ...

    @abstractmethod
    def clear(self) -> None:
        """Remove all entries."""
        ...


class RandomEviction(CachePolicy):
    def __init__(self, capacity: int):
        super().__init__(capacity)
        self._data: dict[str, str] = {}
        self._keys: list[str] = []
        self._key_index: dict[str, int] = {}  # key -> index in _keys list

    def get(self, key: str) -> Optional[str]:
        value = self._data.get(key)
        if value is not None:
            self._stats.hits += 1
        else:
            self._stats.misses += 1
        return value

    def put(self, key: str, value: str) -> Optional[str]:
        evicted_key = None

        if key in self._data:
            self._data[key] = value
            return None

        if len(self._data) >= self._stats.capacity:
            evicted_key = self._evict()

        self._data[key] = value
        idx = len(self._keys)
        self._keys.append(key)
        self._key_index[key] = idx
        self._stats.current_size = len(self._data)
        return evicted_key

    def invalidate(self, key: str) -> bool:
        if key not in self._data:
            return False
        self._remove_key(key)
        self._stats.invalidations += 1
        self._stats.current_size = len(self._data)
        return True

    def clear(self) -> None:
        self._data.clear()
        self._keys.clear()
        self._key_index.clear()
        self._stats.current_size = 0

    def _evict(self) -> str:
        idx = random.randrange(len(self._keys))
        key = self._keys[idx]
        self._remove_key(key)
        self._stats.evictions += 1
        self._stats.current_size = len(self._data)
        return key

    def _remove_key(self, key: str) -> None:
        """O(1) removal using swap-with-last."""
        idx = self._key_index[key]
        last_key = self._keys[-1]
        self._keys[idx] = last_key
        self._key_index[last_key] = idx
        self._keys.pop()
        del self._key_index[key]
        del self._data[key]


def create_cache(policy_name: str, capacity: int, **kwargs) -> CachePolicy:
    policies = {
        "random": RandomEviction,
    }
    policy_cls = policies.get(policy_name.lower())
    if policy_cls is None:
        raise ValueError(f"Unknown cache policy: {policy_name}. Available: {list(policies.keys())}")
    return policy_cls(capacity, **kwargs)
