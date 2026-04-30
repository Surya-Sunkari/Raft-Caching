#!/usr/bin/env python3
"""Unit tests for cache eviction policies."""

import unittest
from cache import (
    create_cache,
    CachePolicy,
    RandomEviction,
    FIFOEviction,
    LRUEviction,
    LFUEviction,
    SLRUEviction,
    SIEVEEviction,
)

ALL_POLICIES = ["random", "fifo", "lru", "lfu", "slru", "sieve"]


class TestCacheFactory(unittest.TestCase):
    def test_create_all_policies(self):
        for name in ALL_POLICIES:
            cache = create_cache(name, 10)
            self.assertIsInstance(cache, CachePolicy)

    def test_unknown_policy_raises(self):
        with self.assertRaises(ValueError):
            create_cache("nonexistent", 10)

    def test_zero_capacity_raises(self):
        with self.assertRaises(ValueError):
            create_cache("lru", 0)

    def test_negative_capacity_raises(self):
        with self.assertRaises(ValueError):
            create_cache("lru", -5)


class TestCommonBehavior(unittest.TestCase):
    """Tests that apply to every policy."""

    def _run_for_all(self, test_fn):
        for name in ALL_POLICIES:
            with self.subTest(policy=name):
                test_fn(name)

    def test_basic_put_get(self):
        def check(name):
            c = create_cache(name, 10)
            c.put("a", "1")
            c.put("b", "2")
            self.assertEqual(c.get("a"), "1")
            self.assertEqual(c.get("b"), "2")
        self._run_for_all(check)

    def test_get_miss_returns_none(self):
        def check(name):
            c = create_cache(name, 10)
            self.assertIsNone(c.get("missing"))
        self._run_for_all(check)

    def test_overwrite_existing_key(self):
        def check(name):
            c = create_cache(name, 10)
            c.put("a", "old")
            c.put("a", "new")
            self.assertEqual(c.get("a"), "new")
        self._run_for_all(check)

    def test_overwrite_does_not_evict(self):
        def check(name):
            c = create_cache(name, 2)
            c.put("a", "1")
            c.put("b", "2")
            evicted = c.put("a", "updated")
            self.assertIsNone(evicted)
            self.assertEqual(c.stats.evictions, 0)
            self.assertEqual(c.stats.current_size, 2)
        self._run_for_all(check)

    def test_eviction_at_capacity(self):
        def check(name):
            c = create_cache(name, 3)
            c.put("a", "1")
            c.put("b", "2")
            c.put("c", "3")
            evicted = c.put("d", "4")
            self.assertIsNotNone(evicted)
            self.assertIn(evicted, ["a", "b", "c"])
            self.assertEqual(c.stats.evictions, 1)
            self.assertEqual(c.stats.current_size, 3)
        self._run_for_all(check)

    def test_invalidate_existing(self):
        def check(name):
            c = create_cache(name, 10)
            c.put("a", "1")
            result = c.invalidate("a")
            self.assertTrue(result)
            self.assertIsNone(c.get("a"))
            self.assertEqual(c.stats.invalidations, 1)
        self._run_for_all(check)

    def test_invalidate_missing(self):
        def check(name):
            c = create_cache(name, 10)
            result = c.invalidate("missing")
            self.assertFalse(result)
            self.assertEqual(c.stats.invalidations, 0)
        self._run_for_all(check)

    def test_clear(self):
        def check(name):
            c = create_cache(name, 10)
            c.put("a", "1")
            c.put("b", "2")
            c.clear()
            self.assertIsNone(c.get("a"))
            self.assertIsNone(c.get("b"))
            self.assertEqual(c.stats.current_size, 0)
        self._run_for_all(check)

    def test_empty_string_value(self):
        def check(name):
            c = create_cache(name, 10)
            c.put("a", "")
            self.assertEqual(c.get("a"), "")
            self.assertEqual(c.stats.hits, 1)
        self._run_for_all(check)

    def test_capacity_one(self):
        def check(name):
            c = create_cache(name, 1)
            c.put("a", "1")
            evicted = c.put("b", "2")
            self.assertEqual(evicted, "a")
            self.assertIsNone(c.get("a"))
            self.assertEqual(c.get("b"), "2")
        self._run_for_all(check)

    def test_stats_hits_misses(self):
        def check(name):
            c = create_cache(name, 10)
            c.put("a", "1")
            c.get("a")  # hit
            c.get("a")  # hit
            c.get("b")  # miss
            self.assertEqual(c.stats.hits, 2)
            self.assertEqual(c.stats.misses, 1)
            self.assertAlmostEqual(c.stats.hit_rate, 2 / 3)
        self._run_for_all(check)

    def test_put_after_invalidate(self):
        def check(name):
            c = create_cache(name, 3)
            c.put("a", "1")
            c.put("b", "2")
            c.put("c", "3")
            c.invalidate("b")
            evicted = c.put("d", "4")
            self.assertIsNone(evicted)
            self.assertEqual(c.stats.current_size, 3)
            self.assertEqual(c.stats.evictions, 0)
        self._run_for_all(check)

    def test_reuse_after_clear(self):
        def check(name):
            c = create_cache(name, 3)
            c.put("a", "1")
            c.put("b", "2")
            c.put("c", "3")
            c.clear()
            c.put("x", "10")
            c.put("y", "20")
            c.put("z", "30")
            self.assertEqual(c.get("x"), "10")
            self.assertEqual(c.stats.current_size, 3)
            evicted = c.put("w", "40")
            self.assertIsNotNone(evicted)
            self.assertIn(evicted, ["x", "y", "z"])
            self.assertEqual(c.stats.evictions, 1)
        self._run_for_all(check)


class TestRandomEviction(unittest.TestCase):
    def test_evicted_key_was_in_cache(self):
        c = create_cache("random", 3)
        c.put("a", "1")
        c.put("b", "2")
        c.put("c", "3")
        evicted = c.put("d", "4")
        self.assertIn(evicted, ["a", "b", "c"])
        self.assertIsNone(c.get(evicted))
        self.assertEqual(c.get("d"), "4")

    def test_multiple_evictions(self):
        c = create_cache("random", 2)
        c.put("a", "1")
        c.put("b", "2")
        for i in range(20):
            evicted = c.put(f"key_{i}", f"val_{i}")
            self.assertIsNotNone(evicted)
            self.assertIsNone(c.get(evicted))
        self.assertEqual(c.stats.current_size, 2)
        self.assertEqual(c.stats.evictions, 20)


class TestFIFOEviction(unittest.TestCase):
    def test_evicts_oldest(self):
        c = create_cache("fifo", 3)
        c.put("a", "1")
        c.put("b", "2")
        c.put("c", "3")
        evicted = c.put("d", "4")
        self.assertEqual(evicted, "a")

    def test_access_does_not_change_order(self):
        c = create_cache("fifo", 3)
        c.put("a", "1")
        c.put("b", "2")
        c.put("c", "3")
        c.get("a")  # accessing "a" should NOT save it
        evicted = c.put("d", "4")
        self.assertEqual(evicted, "a")

    def test_sequential_evictions(self):
        c = create_cache("fifo", 2)
        c.put("a", "1")
        c.put("b", "2")
        self.assertEqual(c.put("c", "3"), "a")
        self.assertEqual(c.put("d", "4"), "b")
        self.assertEqual(c.put("e", "5"), "c")


class TestLRUEviction(unittest.TestCase):
    def test_evicts_least_recently_used(self):
        c = create_cache("lru", 3)
        c.put("a", "1")
        c.put("b", "2")
        c.put("c", "3")
        evicted = c.put("d", "4")
        self.assertEqual(evicted, "a")

    def test_access_moves_to_back(self):
        c = create_cache("lru", 3)
        c.put("a", "1")
        c.put("b", "2")
        c.put("c", "3")
        c.get("a")  # "a" is now most recently used
        evicted = c.put("d", "4")
        self.assertEqual(evicted, "b")

    def test_put_update_moves_to_back(self):
        c = create_cache("lru", 3)
        c.put("a", "1")
        c.put("b", "2")
        c.put("c", "3")
        c.put("a", "updated")
        evicted = c.put("d", "4")
        self.assertEqual(evicted, "b")

    def test_sequential_evictions_with_interleaved_access(self):
        c = create_cache("lru", 3)
        c.put("a", "1")
        c.put("b", "2")
        c.put("c", "3")
        c.get("a")  # order: b, c, a
        c.get("b")  # order: c, a, b
        self.assertEqual(c.put("d", "4"), "c")  # order: a, b, d
        c.get("a")  # order: b, d, a
        self.assertEqual(c.put("e", "5"), "b")  # order: d, a, e


class TestLFUEviction(unittest.TestCase):
    def test_evicts_least_frequent(self):
        c = create_cache("lfu", 3)
        c.put("a", "1")
        c.put("b", "2")
        c.put("c", "3")
        c.get("b")  # freq: a=1, b=2, c=1
        c.get("c")  # freq: a=1, b=2, c=2
        evicted = c.put("d", "4")
        self.assertEqual(evicted, "a")  # "a" has lowest freq

    def test_lru_tiebreak(self):
        c = create_cache("lfu", 3)
        c.put("a", "1")
        c.put("b", "2")
        c.put("c", "3")
        # all have freq=1, "a" was inserted first (LRU among tied)
        evicted = c.put("d", "4")
        self.assertEqual(evicted, "a")

    def test_frequency_increases_on_put_update(self):
        c = create_cache("lfu", 3)
        c.put("a", "1")
        c.put("b", "2")
        c.put("c", "3")
        c.put("a", "updated")  # "a" freq bumps to 2
        evicted = c.put("d", "4")
        self.assertEqual(evicted, "b")  # "b" now has lowest freq

    def test_min_freq_correct_after_invalidate(self):
        c = create_cache("lfu", 3)
        c.put("a", "1")  # freq 1
        c.put("b", "2")  # freq 1
        c.put("c", "3")  # freq 1
        c.get("b")  # freq: a=1, b=2, c=1
        c.get("c")  # freq: a=1, b=2, c=2
        c.invalidate("a")  # remove only key at min_freq=1
        # Now insert "d" — should not crash and should evict correctly
        evicted = c.put("d", "4")  # d has freq=1, new min_freq=1
        self.assertIsNone(evicted)  # was below capacity after invalidate
        # Fill to capacity and evict
        evicted = c.put("e", "5")
        self.assertEqual(evicted, "d")  # d has lowest freq (1)

    def test_sequential_evictions(self):
        c = create_cache("lfu", 3)
        c.put("a", "1")
        c.put("b", "2")
        c.put("c", "3")
        c.get("c")  # freq: a=1, b=1, c=2
        # Evict "a" (freq=1, inserted first)
        self.assertEqual(c.put("d", "4"), "a")
        # freq: b=1, c=2, d=1. Evict "b" (freq=1, inserted before d)
        self.assertEqual(c.put("e", "5"), "b")


class TestSLRUEviction(unittest.TestCase):
    def test_new_keys_enter_probation(self):
        c = create_cache("slru", 5)
        c.put("a", "1")
        self.assertIn("a", c._probation)
        self.assertNotIn("a", c._protected)

    def test_reaccess_promotes_to_protected(self):
        c = create_cache("slru", 5)
        c.put("a", "1")
        c.get("a")  # promote
        self.assertIn("a", c._protected)
        self.assertNotIn("a", c._probation)

    def test_evicts_from_probation(self):
        c = create_cache("slru", 3)
        c.put("a", "1")
        c.put("b", "2")
        c.put("c", "3")
        # promote "b" and "c" to protected
        c.get("b")
        c.get("c")
        # "a" is still in probation, should be evicted
        evicted = c.put("d", "4")
        self.assertEqual(evicted, "a")

    def test_protected_overflow_demotes(self):
        # capacity=3, protected_cap=2 (0.8*3=2), probation_cap=1
        c = create_cache("slru", 3)
        c.put("a", "1")
        c.put("b", "2")
        c.put("c", "3")
        c.get("a")  # promote a to protected
        c.get("b")  # promote b to protected (protected now full: a, b)
        c.get("c")  # promote c to protected -> demotes "a" back to probation
        self.assertIn("a", c._probation)
        self.assertIn("c", c._protected)

    def test_put_update_in_protected(self):
        c = create_cache("slru", 5)
        c.put("a", "1")
        c.get("a")  # promote to protected
        self.assertIn("a", c._protected)
        c.put("a", "updated")
        self.assertEqual(c.get("a"), "updated")
        self.assertIn("a", c._protected)

    def test_invalidate_eviction_interaction(self):
        c = create_cache("slru", 3)
        c.put("a", "1")
        c.put("b", "2")
        c.put("c", "3")
        c.get("b")  # promote b to protected
        c.invalidate("a")  # remove from probation
        # Should not evict since we have room
        evicted = c.put("d", "4")
        self.assertIsNone(evicted)
        self.assertEqual(c.stats.current_size, 3)
        # Now at capacity, should evict from probation
        evicted = c.put("e", "5")
        self.assertIn(evicted, ["c", "d"])


class TestSIEVEEviction(unittest.TestCase):
    def test_factory_creates_sieve(self):
        c = create_cache("sieve", 3)
        self.assertIsInstance(c, SIEVEEviction)

    def test_new_keys_insert_at_head(self):
        c = create_cache("sieve", 3)
        c.put("a", "1")
        c.put("b", "2")
        c.put("c", "3")
        self.assertEqual(c._head.key, "c")
        self.assertEqual(c._tail.key, "a")

    def test_hit_sets_visited_without_reordering(self):
        c = create_cache("sieve", 3)
        c.put("a", "1")
        c.put("b", "2")
        c.put("c", "3")
        self.assertEqual(c.get("a"), "1")
        self.assertTrue(c._data["a"].visited)
        self.assertEqual(c._head.key, "c")
        self.assertEqual(c._tail.key, "a")

    def test_eviction_skips_visited_and_clears_bit(self):
        c = create_cache("sieve", 3)
        c.put("a", "1")
        c.put("b", "2")
        c.put("c", "3")
        c.get("a")

        evicted = c.put("d", "4")

        self.assertEqual(evicted, "b")
        self.assertIn("a", c._data)
        self.assertFalse(c._data["a"].visited)
        self.assertEqual(c._hand.key, "c")

    def test_eviction_wraps_when_all_entries_were_visited(self):
        c = create_cache("sieve", 2)
        c.put("a", "1")
        c.put("b", "2")
        c.get("a")
        c.get("b")

        evicted = c.put("c", "3")

        self.assertEqual(evicted, "a")
        self.assertIn("b", c._data)
        self.assertFalse(c._data["b"].visited)
        self.assertEqual(c._hand.key, "b")

    def test_invalidate_removes_hand_target(self):
        c = create_cache("sieve", 3)
        c.put("a", "1")
        c.put("b", "2")
        c.put("c", "3")
        c.get("a")
        c.put("d", "4")  # evicts b, leaves hand on c

        self.assertEqual(c._hand.key, "c")
        self.assertTrue(c.invalidate("c"))
        self.assertEqual(c._hand.key, "d")
        self.assertEqual(c.stats.current_size, 2)


if __name__ == "__main__":
    unittest.main()
