#!/usr/bin/env python3
"""Unit tests for workload generators."""

import unittest
from collections import Counter

from workloads import (
    Operation,
    Workload,
    UniformWorkload,
    ZipfianWorkload,
    HotKeyWorkload,
    create_workload,
)

ALL_WORKLOADS = ["uniform", "zipfian", "hotkey"]


def _parse_key_id(key: str) -> int:
    prefix = "key_"
    assert key.startswith(prefix), f"unexpected key format: {key!r}"
    return int(key[len(prefix):])


class TestWorkloadFactory(unittest.TestCase):
    def test_create_all_workloads(self):
        for name in ALL_WORKLOADS:
            with self.subTest(workload=name):
                w = create_workload(name, num_keys=100)
                self.assertIsInstance(w, Workload)

    def test_unknown_workload_raises(self):
        with self.assertRaises(ValueError):
            create_workload("nonexistent", num_keys=10)

    def test_zero_num_keys_raises(self):
        for name in ALL_WORKLOADS:
            with self.subTest(workload=name):
                with self.assertRaises(ValueError):
                    create_workload(name, num_keys=0)

    def test_negative_num_keys_raises(self):
        for name in ALL_WORKLOADS:
            with self.subTest(workload=name):
                with self.assertRaises(ValueError):
                    create_workload(name, num_keys=-5)

    def test_invalid_read_ratio_raises(self):
        for name in ALL_WORKLOADS:
            for bad in (-0.1, 1.1):
                with self.subTest(workload=name, read_ratio=bad):
                    with self.assertRaises(ValueError):
                        create_workload(name, num_keys=10, read_ratio=bad)

    def test_case_insensitive(self):
        for name in ALL_WORKLOADS:
            with self.subTest(workload=name):
                w = create_workload(name.upper(), num_keys=100)
                self.assertIsInstance(w, Workload)


class TestCommonBehavior(unittest.TestCase):
    """Properties that hold across every workload."""

    N_SAMPLE = 20000

    def _make(self, name, **kwargs):
        defaults = {"num_keys": 50, "seed": 42}
        defaults.update(kwargs)
        return create_workload(name, **defaults)

    def _run_for_all(self, fn):
        for name in ALL_WORKLOADS:
            with self.subTest(workload=name):
                fn(name)

    def test_generate_yields_exact_count(self):
        def check(name):
            w = self._make(name)
            ops = list(w.generate(100))
            self.assertEqual(len(ops), 100)
        self._run_for_all(check)

    def test_generate_zero_yields_nothing(self):
        def check(name):
            w = self._make(name)
            self.assertEqual(list(w.generate(0)), [])
        self._run_for_all(check)

    def test_yields_operation_instances(self):
        def check(name):
            w = self._make(name)
            for op in w.generate(10):
                self.assertIsInstance(op, Operation)
        self._run_for_all(check)

    def test_keys_in_valid_range(self):
        def check(name):
            w = self._make(name, num_keys=50)
            for op in w.generate(5000):
                idx = _parse_key_id(op.key)
                self.assertGreaterEqual(idx, 0)
                self.assertLess(idx, 50)
        self._run_for_all(check)

    def test_reproducibility_same_seed(self):
        def check(name):
            w1 = self._make(name, seed=123)
            w2 = self._make(name, seed=123)
            ops1 = [(o.op, o.key, o.value) for o in w1.generate(500)]
            ops2 = [(o.op, o.key, o.value) for o in w2.generate(500)]
            self.assertEqual(ops1, ops2)
        self._run_for_all(check)

    def test_different_seeds_differ(self):
        def check(name):
            w1 = self._make(name, seed=1)
            w2 = self._make(name, seed=2)
            ops1 = [(o.op, o.key) for o in w1.generate(500)]
            ops2 = [(o.op, o.key) for o in w2.generate(500)]
            self.assertNotEqual(ops1, ops2)
        self._run_for_all(check)

    def test_multiple_generate_calls_advance_rng(self):
        def check(name):
            w = self._make(name, seed=7)
            ops1 = [(o.op, o.key) for o in w.generate(200)]
            ops2 = [(o.op, o.key) for o in w.generate(200)]
            self.assertNotEqual(ops1, ops2)
        self._run_for_all(check)

    def test_op_is_get_or_put(self):
        def check(name):
            w = self._make(name)
            for op in w.generate(1000):
                self.assertIn(op.op, ("get", "put"))
        self._run_for_all(check)

    def test_put_has_value_get_does_not(self):
        def check(name):
            w = self._make(name)
            for op in w.generate(1000):
                if op.op == "put":
                    self.assertIsNotNone(op.value)
                    self.assertIsInstance(op.value, str)
                else:
                    self.assertIsNone(op.value)
        self._run_for_all(check)

    def test_read_ratio_honored(self):
        def check(name):
            for r in (0.1, 0.5, 0.9):
                w = self._make(name, read_ratio=r, seed=99)
                ops = list(w.generate(self.N_SAMPLE))
                observed = sum(1 for o in ops if o.op == "get") / self.N_SAMPLE
                self.assertAlmostEqual(observed, r, delta=0.02)
        self._run_for_all(check)

    def test_read_ratio_zero_all_puts(self):
        def check(name):
            w = self._make(name, read_ratio=0.0)
            for op in w.generate(500):
                self.assertEqual(op.op, "put")
        self._run_for_all(check)

    def test_read_ratio_one_all_gets(self):
        def check(name):
            w = self._make(name, read_ratio=1.0)
            for op in w.generate(500):
                self.assertEqual(op.op, "get")
        self._run_for_all(check)

    def test_single_key_universe(self):
        # HotKey requires hot_keys < num_keys, so can't run with num_keys=1.
        for name in ("uniform", "zipfian"):
            with self.subTest(workload=name):
                w = create_workload(name, num_keys=1, seed=7)
                for op in w.generate(100):
                    self.assertEqual(op.key, "key_0")


class TestUniformWorkload(unittest.TestCase):
    def test_distribution_is_uniform(self):
        num_keys = 20
        N = 40000
        w = create_workload("uniform", num_keys=num_keys, read_ratio=1.0, seed=42)
        counts = Counter(op.key for op in w.generate(N))
        self.assertEqual(len(counts), num_keys)
        expected = N / num_keys
        for key, count in counts.items():
            with self.subTest(key=key):
                self.assertAlmostEqual(count, expected, delta=expected * 0.10)

    def test_large_key_space(self):
        # With 1000 keys and 10k ops, some keys may not appear — that's fine.
        w = create_workload("uniform", num_keys=1000, read_ratio=1.0, seed=1)
        ops = list(w.generate(10000))
        self.assertEqual(len(ops), 10000)
        ids = {_parse_key_id(o.key) for o in ops}
        self.assertTrue(all(0 <= i < 1000 for i in ids))


class TestZipfianWorkload(unittest.TestCase):
    def test_invalid_alpha_raises(self):
        for bad in (0, -0.5, -1):
            with self.subTest(alpha=bad):
                with self.assertRaises(ValueError):
                    create_workload("zipfian", num_keys=10, alpha=bad)

    def test_cdf_is_monotonic_and_normalized(self):
        w = ZipfianWorkload(num_keys=50, alpha=1.1, seed=0)
        cdf = w._cdf
        self.assertEqual(len(cdf), 50)
        for i in range(1, len(cdf)):
            self.assertGreaterEqual(cdf[i], cdf[i - 1])
        self.assertAlmostEqual(cdf[-1], 1.0, places=10)

    def test_rank_0_is_most_frequent(self):
        N = 30000
        w = create_workload("zipfian", num_keys=50, alpha=1.1, read_ratio=1.0, seed=42)
        counts = Counter(op.key for op in w.generate(N))
        most_frequent_key = counts.most_common(1)[0][0]
        self.assertEqual(most_frequent_key, "key_0")
        # Rank 0 should dominate rank (N-1) by a wide margin with alpha=1.1.
        self.assertGreater(counts["key_0"], counts["key_49"] * 10)

    def test_higher_alpha_more_skewed(self):
        N = 30000
        nk = 50
        w_low = create_workload("zipfian", num_keys=nk, alpha=0.8, read_ratio=1.0, seed=42)
        w_high = create_workload("zipfian", num_keys=nk, alpha=1.5, read_ratio=1.0, seed=42)
        low = Counter(op.key for op in w_low.generate(N))
        high = Counter(op.key for op in w_high.generate(N))
        self.assertGreater(high["key_0"] / N, low["key_0"] / N)

    def test_all_ranks_reachable(self):
        N = 100000
        nk = 20
        w = create_workload("zipfian", num_keys=nk, alpha=1.1, read_ratio=1.0, seed=42)
        seen = {op.key for op in w.generate(N)}
        self.assertEqual(len(seen), nk)


class TestHotKeyWorkload(unittest.TestCase):
    def test_invalid_hot_keys_raises(self):
        # Valid range: (0, num_keys). num_keys=10 => bad values: 0, -1, 10, 11.
        for bad in (0, -1, 10, 11):
            with self.subTest(hot_keys=bad):
                with self.assertRaises(ValueError):
                    create_workload("hotkey", num_keys=10, hot_keys=bad)

    def test_invalid_hot_fraction_raises(self):
        for bad in (-0.1, 1.1):
            with self.subTest(hot_fraction=bad):
                with self.assertRaises(ValueError):
                    create_workload(
                        "hotkey", num_keys=100, hot_keys=10, hot_fraction=bad
                    )

    def test_hot_fraction_honored(self):
        N = 30000
        hk = 10
        w = create_workload(
            "hotkey", num_keys=100, hot_keys=hk, hot_fraction=0.8,
            read_ratio=1.0, seed=42,
        )
        hot = sum(1 for op in w.generate(N) if _parse_key_id(op.key) < hk)
        self.assertAlmostEqual(hot / N, 0.8, delta=0.01)

    def test_hot_fraction_zero_never_hot(self):
        w = create_workload(
            "hotkey", num_keys=100, hot_keys=10, hot_fraction=0.0,
            read_ratio=1.0, seed=42,
        )
        for op in w.generate(2000):
            self.assertGreaterEqual(_parse_key_id(op.key), 10)

    def test_hot_fraction_one_never_cold(self):
        w = create_workload(
            "hotkey", num_keys=100, hot_keys=10, hot_fraction=1.0,
            read_ratio=1.0, seed=42,
        )
        for op in w.generate(2000):
            self.assertLess(_parse_key_id(op.key), 10)

    def test_within_hot_set_uniform(self):
        N = 30000
        hk = 10
        w = create_workload(
            "hotkey", num_keys=100, hot_keys=hk, hot_fraction=1.0,
            read_ratio=1.0, seed=42,
        )
        counts = Counter(op.key for op in w.generate(N))
        self.assertEqual(len(counts), hk)
        expected = N / hk
        for key, count in counts.items():
            with self.subTest(key=key):
                self.assertAlmostEqual(count, expected, delta=expected * 0.10)

    def test_boundary_hot_keys_values(self):
        for hk in (1, 99):
            with self.subTest(hot_keys=hk):
                w = create_workload(
                    "hotkey", num_keys=100, hot_keys=hk, hot_fraction=0.9,
                    read_ratio=1.0, seed=42,
                )
                ops = list(w.generate(1000))
                self.assertEqual(len(ops), 1000)
                for op in ops:
                    idx = _parse_key_id(op.key)
                    self.assertGreaterEqual(idx, 0)
                    self.assertLess(idx, 100)


if __name__ == "__main__":
    unittest.main()
