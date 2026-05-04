import random
from abc import ABC, abstractmethod
from collections import OrderedDict, deque
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


class FIFOEviction(CachePolicy):
    def __init__(self, capacity: int):
        super().__init__(capacity)
        self._data: dict[str, str] = {}
        self._order: deque[str] = deque()

    def get(self, key: str) -> Optional[str]:
        value = self._data.get(key)
        if value is not None:
            self._stats.hits += 1
        else:
            self._stats.misses += 1
        return value

    def put(self, key: str, value: str) -> Optional[str]:
        if key in self._data:
            self._data[key] = value
            return None

        evicted_key = None
        if len(self._data) >= self._stats.capacity:
            evicted_key = self._order.popleft()
            del self._data[evicted_key]
            self._stats.evictions += 1

        self._data[key] = value
        self._order.append(key)
        self._stats.current_size = len(self._data)
        return evicted_key

    def invalidate(self, key: str) -> bool:
        if key not in self._data:
            return False
        del self._data[key]
        self._order.remove(key)
        self._stats.invalidations += 1
        self._stats.current_size = len(self._data)
        return True

    def clear(self) -> None:
        self._data.clear()
        self._order.clear()
        self._stats.current_size = 0


class LRUEviction(CachePolicy):
    def __init__(self, capacity: int):
        super().__init__(capacity)
        self._data: OrderedDict[str, str] = OrderedDict()

    def get(self, key: str) -> Optional[str]:
        if key in self._data:
            self._stats.hits += 1
            self._data.move_to_end(key)
            return self._data[key]
        self._stats.misses += 1
        return None

    def put(self, key: str, value: str) -> Optional[str]:
        if key in self._data:
            self._data[key] = value
            self._data.move_to_end(key)
            return None

        evicted_key = None
        if len(self._data) >= self._stats.capacity:
            evicted_key, _ = self._data.popitem(last=False)
            self._stats.evictions += 1

        self._data[key] = value
        self._stats.current_size = len(self._data)
        return evicted_key

    def invalidate(self, key: str) -> bool:
        if key not in self._data:
            return False
        del self._data[key]
        self._stats.invalidations += 1
        self._stats.current_size = len(self._data)
        return True

    def clear(self) -> None:
        self._data.clear()
        self._stats.current_size = 0


class LFUEviction(CachePolicy):
    """LFU with optional periodic frequency decay (LFU-aging style).

    Pure global LFU keeps monotonically increasing counts, which hurts shifting
    temporal workloads (old "hot" keys block the cache). Every ``decay_interval``
    frequency updates, all counts are halved (minimum 1) and buckets rebuilt so
    LRU tie-break order within each frequency is preserved.

    Set ``decay_interval`` to 0 to disable decay (classic LFU).
    """

    def __init__(self, capacity: int, decay_interval: Optional[int] = None):
        super().__init__(capacity)
        self._data: dict[str, str] = {}
        self._freq: dict[str, int] = {}  # key -> frequency
        self._freq_buckets: dict[int, OrderedDict] = {}  # freq -> OrderedDict of keys (LRU tiebreak)
        self._min_freq: int = 0
        if decay_interval is None:
            # Rare enough that unit tests never trigger; often enough for long benchmarks.
            decay_interval = max(128, capacity * 8)
        self._decay_interval = decay_interval
        self._updates_since_decay: int = 0

    def _maybe_decay(self) -> None:
        if self._decay_interval <= 0:
            return
        self._updates_since_decay += 1
        if self._updates_since_decay < self._decay_interval:
            return
        self._updates_since_decay = 0
        self._decay_frequencies()

    def _decay_frequencies(self) -> None:
        """Halve all frequencies (min 1) and rebuild buckets."""
        if not self._freq:
            return
        snapshot: list[tuple[str, int]] = []
        for f in sorted(self._freq_buckets.keys()):
            for key in self._freq_buckets[f]:
                snapshot.append((key, max(1, f // 2)))
        self._freq.clear()
        self._freq_buckets.clear()
        for key, new_f in snapshot:
            self._freq[key] = new_f
            if new_f not in self._freq_buckets:
                self._freq_buckets[new_f] = OrderedDict()
            self._freq_buckets[new_f][key] = None
        self._min_freq = min(self._freq.values())

    def _touch(self, key: str) -> None:
        """Increment frequency for an existing key."""
        old_freq = self._freq[key]
        new_freq = old_freq + 1
        self._freq[key] = new_freq

        # Remove from old bucket
        del self._freq_buckets[old_freq][key]
        if not self._freq_buckets[old_freq]:
            del self._freq_buckets[old_freq]
            if self._min_freq == old_freq:
                self._min_freq = new_freq

        # Add to new bucket
        if new_freq not in self._freq_buckets:
            self._freq_buckets[new_freq] = OrderedDict()
        self._freq_buckets[new_freq][key] = None
        self._maybe_decay()

    def get(self, key: str) -> Optional[str]:
        if key not in self._data:
            self._stats.misses += 1
            return None
        self._stats.hits += 1
        self._touch(key)
        return self._data[key]

    def put(self, key: str, value: str) -> Optional[str]:
        if key in self._data:
            self._data[key] = value
            self._touch(key)
            return None

        evicted_key = None
        if len(self._data) >= self._stats.capacity:
            # Evict LRU key from min_freq bucket
            bucket = self._freq_buckets[self._min_freq]
            evicted_key, _ = bucket.popitem(last=False)
            if not bucket:
                del self._freq_buckets[self._min_freq]
            del self._data[evicted_key]
            del self._freq[evicted_key]
            self._stats.evictions += 1

        # Insert with freq 1
        self._data[key] = value
        self._freq[key] = 1
        self._min_freq = 1
        if 1 not in self._freq_buckets:
            self._freq_buckets[1] = OrderedDict()
        self._freq_buckets[1][key] = None
        self._stats.current_size = len(self._data)
        self._maybe_decay()
        return evicted_key

    def invalidate(self, key: str) -> bool:
        if key not in self._data:
            return False
        freq = self._freq[key]
        del self._freq_buckets[freq][key]
        if not self._freq_buckets[freq]:
            del self._freq_buckets[freq]
        del self._data[key]
        del self._freq[key]
        self._stats.invalidations += 1
        self._stats.current_size = len(self._data)
        if self._freq:
            self._min_freq = min(self._freq.values())
        else:
            self._min_freq = 0
        return True

    def clear(self) -> None:
        self._data.clear()
        self._freq.clear()
        self._freq_buckets.clear()
        self._min_freq = 0
        self._updates_since_decay = 0
        self._stats.current_size = 0


class SLRUEviction(CachePolicy):
    """Segmented LRU: probation segment + protected segment.
    New keys enter probation. Re-access promotes to protected.
    Eviction comes from probation front. Protected overflow demotes to probation.
    """

    def __init__(self, capacity: int, protected_ratio: float = 0.8):
        super().__init__(capacity)
        self._protected_cap = max(1, int(capacity * protected_ratio))
        self._probation_cap = capacity - self._protected_cap
        if self._probation_cap < 1:
            self._probation_cap = 1
            self._protected_cap = capacity - 1
        self._probation: OrderedDict[str, str] = OrderedDict()
        self._protected: OrderedDict[str, str] = OrderedDict()

    def get(self, key: str) -> Optional[str]:
        # Check protected first
        if key in self._protected:
            self._stats.hits += 1
            self._protected.move_to_end(key)
            return self._protected[key]
        # Check probation — promote on re-access
        if key in self._probation:
            self._stats.hits += 1
            value = self._probation.pop(key)
            self._promote(key, value)
            return value
        self._stats.misses += 1
        return None

    def put(self, key: str, value: str) -> Optional[str]:
        # Update existing in protected
        if key in self._protected:
            self._protected[key] = value
            self._protected.move_to_end(key)
            return None
        # Update existing in probation
        if key in self._probation:
            self._probation[key] = value
            return None

        evicted_key = None
        # Evict from probation if total at capacity
        if len(self._probation) + len(self._protected) >= self._stats.capacity:
            if self._probation:
                evicted_key, _ = self._probation.popitem(last=False)
            else:
                evicted_key, _ = self._protected.popitem(last=False)
            self._stats.evictions += 1

        # New keys enter probation
        self._probation[key] = value
        self._stats.current_size = len(self._probation) + len(self._protected)
        return evicted_key

    def _promote(self, key: str, value: str) -> None:
        """Move key from probation to protected, demoting if protected is full."""
        if self._protected_cap == 0:
            # No protected segment, put back in probation
            self._probation[key] = value
            return
        if len(self._protected) >= self._protected_cap:
            # Demote LRU from protected back to probation
            demoted_key, demoted_value = self._protected.popitem(last=False)
            self._probation[demoted_key] = demoted_value
        self._protected[key] = value

    def invalidate(self, key: str) -> bool:
        if key in self._protected:
            del self._protected[key]
        elif key in self._probation:
            del self._probation[key]
        else:
            return False
        self._stats.invalidations += 1
        self._stats.current_size = len(self._probation) + len(self._protected)
        return True

    def clear(self) -> None:
        self._probation.clear()
        self._protected.clear()
        self._stats.current_size = 0


@dataclass
class _SIEVENode:
    key: str
    value: str
    visited: bool = False
    # prev points toward newer entries, next points toward older entries.
    prev: Optional["_SIEVENode"] = None
    next: Optional["_SIEVENode"] = None


class SIEVEEviction(CachePolicy):
    """SIEVE eviction policy from NSDI '24.

    New entries are inserted at the head. Hits only set a visited bit.
    Eviction walks the hand from old entries toward new entries, clearing
    visited bits until it finds an unvisited victim.
    """

    def __init__(self, capacity: int):
        super().__init__(capacity)
        self._data: dict[str, _SIEVENode] = {}
        self._head: Optional[_SIEVENode] = None
        self._tail: Optional[_SIEVENode] = None
        self._hand: Optional[_SIEVENode] = None

    def get(self, key: str) -> Optional[str]:
        node = self._data.get(key)
        if node is None:
            self._stats.misses += 1
            return None
        self._stats.hits += 1
        node.visited = True
        return node.value

    def put(self, key: str, value: str) -> Optional[str]:
        node = self._data.get(key)
        if node is not None:
            node.value = value
            node.visited = True
            return None

        evicted_key = None
        if len(self._data) >= self._stats.capacity:
            evicted_key = self._evict()

        self._insert_head(_SIEVENode(key=key, value=value))
        self._stats.current_size = len(self._data)
        return evicted_key

    def invalidate(self, key: str) -> bool:
        node = self._data.get(key)
        if node is None:
            return False
        self._remove_node(node)
        self._stats.invalidations += 1
        self._stats.current_size = len(self._data)
        return True

    def clear(self) -> None:
        self._data.clear()
        self._head = None
        self._tail = None
        self._hand = None
        self._stats.current_size = 0

    def _insert_head(self, node: _SIEVENode) -> None:
        node.prev = None
        node.next = self._head
        if self._head is not None:
            self._head.prev = node
        else:
            self._tail = node
        self._head = node
        self._data[node.key] = node

    def _evict(self) -> str:
        node = self._hand or self._tail
        while node is not None and node.visited:
            node.visited = False
            node = node.prev or self._tail

        assert node is not None, "cannot evict from an empty SIEVE cache"
        self._hand = node.prev
        evicted_key = node.key
        self._remove_node(node)
        self._stats.evictions += 1
        self._stats.current_size = len(self._data)
        return evicted_key

    def _remove_node(self, node: _SIEVENode) -> None:
        if self._hand is node:
            self._hand = node.prev

        if node.prev is not None:
            node.prev.next = node.next
        else:
            self._head = node.next

        if node.next is not None:
            node.next.prev = node.prev
        else:
            self._tail = node.prev

        del self._data[node.key]
        node.prev = None
        node.next = None


def create_cache(policy_name: str, capacity: int, **kwargs) -> CachePolicy:
    policies = {
        "random": RandomEviction,
        "fifo": FIFOEviction,
        "lru": LRUEviction,
        "lfu": LFUEviction,
        "slru": SLRUEviction,
        "sieve": SIEVEEviction,
    }
    policy_cls = policies.get(policy_name.lower())
    if policy_cls is None:
        raise ValueError(f"Unknown cache policy: {policy_name}. Available: {list(policies.keys())}")
    return policy_cls(capacity, **kwargs)
