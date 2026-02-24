#!/usr/bin/env python3
"""
Test suite for the Elastic Hash Table (C + Python ctypes).

Run:  python test_elastic.py
"""
import time
import sys

from elastic_hash_table import ElasticHashTable


def test_basic_insert_get():
    t = ElasticHashTable(256)
    t.insert("hello", 1)
    t.insert("world", 2)
    assert t.get("hello") == 1
    assert t.get("world") == 2
    assert t.get("missing") is None
    assert t.get("missing", -1) == -1
    assert len(t) == 2
    print("[PASS] Basic insert / get")


def test_update_existing():
    t = ElasticHashTable(256)
    t["key"] = 10
    t["key"] = 99
    assert t["key"] == 99
    assert len(t) == 1
    print("[PASS] Update existing key (no duplicates)")


def test_dict_interface():
    t = ElasticHashTable(256)
    t["x"] = 42
    assert t["x"] == 42
    assert "x" in t
    assert "nope" not in t
    del t["x"]
    assert "x" not in t
    assert len(t) == 0

    raised = False
    try:
        _ = t["missing"]
    except KeyError:
        raised = True
    assert raised

    raised = False
    try:
        del t["missing"]
    except KeyError:
        raised = True
    assert raised

    print("[PASS] Dict interface (__getitem__ / __setitem__ / __delitem__ / "
          "__contains__ / KeyError)")


def test_complex_values():
    t = ElasticHashTable(256)
    t["dict"]  = {"nested": [1, 2, 3], "flag": True}
    t["list"]  = [None, 3.14, "hello"]
    t["tuple"] = (1, 2, (3, 4))
    t["set"]   = {10, 20, 30}
    t["int"]   = 42
    t["float"] = 2.718
    t["none"]  = None

    assert t["dict"]  == {"nested": [1, 2, 3], "flag": True}
    assert t["list"]  == [None, 3.14, "hello"]
    assert t["tuple"] == (1, 2, (3, 4))
    assert t["set"]   == {10, 20, 30}
    assert t["int"]   == 42
    assert t["float"] == 2.718
    assert t["none"]  is None

    print("[PASS] Complex picklable values (dict, list, tuple, set, None)")


def test_deletion_and_tombstones():
    t = ElasticHashTable(512)
    for i in range(200):
        t[f"k{i}"] = i
    assert len(t) == 200

    for i in range(0, 200, 2):
        assert t.delete(f"k{i}")
    assert len(t) == 100

    # Odd keys survive
    for i in range(1, 200, 2):
        assert t.get(f"k{i}") == i, f"Lost key k{i}"
    # Even keys gone
    for i in range(0, 200, 2):
        assert t.get(f"k{i}") is None

    print("[PASS] Deletion with tombstones")


def test_reinsert_after_delete():
    t = ElasticHashTable(256)
    t["a"] = 1
    del t["a"]
    t["a"] = 2
    assert t["a"] == 2
    assert len(t) == 1
    print("[PASS] Re-insert after delete (reclaims tombstone)")


def test_iteration():
    t = ElasticHashTable(256)
    expected = {f"key_{i}": i * 10 for i in range(50)}
    for k, v in expected.items():
        t[k] = v

    found = dict(t.items())
    assert found == expected
    assert set(t.keys()) == set(expected.keys())
    assert sorted(t.values()) == sorted(expected.values())

    # __iter__ == keys
    assert set(t) == set(expected.keys())

    print("[PASS] Iteration (keys / values / items / __iter__)")


def test_bool_repr():
    empty = ElasticHashTable(128)
    assert not empty

    t = ElasticHashTable(128)
    t["x"] = 1
    assert t
    assert "ElasticHashTable" in repr(t)
    print("[PASS] __bool__ / __repr__")


def test_high_load_stress():
    cap = 10_000
    t = ElasticHashTable(cap)
    n_items = int(cap * 0.90)

    t0 = time.perf_counter()
    for i in range(n_items):
        t[f"user_{i}"] = i
    insert_ms = (time.perf_counter() - t0) * 1000

    t0 = time.perf_counter()
    for i in range(n_items):
        assert t[f"user_{i}"] == i
    lookup_ms = (time.perf_counter() - t0) * 1000

    print(f"[PASS] Stress: {n_items:,} inserts in {insert_ms:.0f} ms, "
          f"{n_items:,} lookups in {lookup_ms:.0f} ms")
    return t


def test_geometric_distribution(t):
    stats = t.level_stats()
    loads = [s["load"] for s in stats]
    assert loads[0] >= loads[-1], \
        f"Level 0 ({loads[0]:.1%}) should be >= last level ({loads[-1]:.1%})"
    t.print_stats()
    print("[PASS] Geometric load distribution (level 0 densest)")


def test_auto_resize():
    t = ElasticHashTable(64)
    for i in range(300):
        t[str(i)] = i * 7
    assert len(t) == 300
    for i in range(300):
        assert t[str(i)] == i * 7
    assert t.capacity > 64
    print(f"[PASS] Auto-resize: 64 → {t.capacity} to hold 300 items")


def test_numeric_string_keys():
    """Keys are stringified — verify int keys round-trip via str()."""
    t = ElasticHashTable(256)
    t[42]  = "from_int"
    t[3.14] = "from_float"
    t[(1, 2)] = "from_tuple"

    assert t[42]    == "from_int"
    assert t[3.14]  == "from_float"
    assert t[(1, 2)] == "from_tuple"
    print("[PASS] Numeric / tuple keys (stringified)")


def test_large_values():
    t = ElasticHashTable(256)
    big = list(range(10_000))
    t["big"] = big
    assert t["big"] == big
    print("[PASS] Large value (10 k element list)")


def main():
    print("=" * 64)
    print("Elastic Hash Table — Python + C Test Suite")
    print("=" * 64)

    test_basic_insert_get()
    test_update_existing()
    test_dict_interface()
    test_complex_values()
    test_deletion_and_tombstones()
    test_reinsert_after_delete()
    test_iteration()
    test_bool_repr()
    t = test_high_load_stress()
    test_geometric_distribution(t)
    test_auto_resize()
    test_numeric_string_keys()
    test_large_values()

    print()
    print("=" * 64)
    print(f"All 13 tests passed.")
    print("=" * 64)


if __name__ == "__main__":
    main()
