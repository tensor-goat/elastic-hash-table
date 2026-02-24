# Elastic Hash Table (C + Python)

A C implementation of the geometric sub-array hash table from Farach-Colton et al. (2025), with Python `ctypes` bindings that give you a drop-in `dict`-like interface backed by C speed.

## Build

```bash
# Linux
gcc -O2 -shared -fPIC -o libelastic_hash_table.so elastic_hash_table.c -lm

# macOS
gcc -O2 -shared -fPIC -o libelastic_hash_table.dylib elastic_hash_table.c -lm

# Windows (MSVC)
cl /O2 /LD elastic_hash_table.c /Fe:libelastic_hash_table.dll
```

Place the shared library in the same directory as `elastic_hash_table.py`.

## Usage

```python
from elastic_hash_table import ElasticHashTable

t = ElasticHashTable(10_000)

# Dict-style interface — values can be any picklable Python object
t["user:1"] = {"name": "Alice", "scores": [98, 87, 95]}
t["user:2"] = {"name": "Bob",   "scores": [72, 88, 91]}

print(t["user:1"])          # {'name': 'Alice', 'scores': [98, 87, 95]}
print("user:2" in t)        # True
print(len(t))               # 2

del t["user:2"]

for key, value in t.items():
    print(key, value)

# Diagnostics
t.print_stats()
```

## Test

```bash
python test_elastic.py
```

```bash
Elastic Hash Table — Python + C Test Suite
================================================================
[PASS] Basic insert / get
[PASS] Update existing key (no duplicates)
[PASS] Dict interface (__getitem__ / __setitem__ / __delitem__ / __contains__ / KeyError)
[PASS] Complex picklable values (dict, list, tuple, set, None)
[PASS] Deletion with tombstones
[PASS] Re-insert after delete (reclaims tombstone)
[PASS] Iteration (keys / values / items / __iter__)
[PASS] __bool__ / __repr__
[PASS] Stress: 9,000 inserts in 529 ms, 9,000 lookups in 361 ms
================================================================
ElasticHashTable  —  9,000 / 10,000  (90.0% load)
────────────────────────────────────────────────────────────────
  Level  0:    5,000 slots |    5,000 live (100.0%) |     0 tombstones
  Level  1:    2,500 slots |    2,500 live (100.0%) |     0 tombstones
  Level  2:    1,250 slots |    1,250 live (100.0%) |     0 tombstones
  Level  3:      625 slots |      249 live (39.8%) |     0 tombstones
  Level  4:      312 slots |        1 live ( 0.3%) |     0 tombstones
  Level  5:      156 slots |        0 live ( 0.0%) |     0 tombstones
  Level  6:       78 slots |        0 live ( 0.0%) |     0 tombstones
  Level  7:       39 slots |        0 live ( 0.0%) |     0 tombstones
  Level  8:       20 slots |        0 live ( 0.0%) |     0 tombstones
  Level  9:       20 slots |        0 live ( 0.0%) |     0 tombstones
================================================================
[PASS] Geometric load distribution (level 0 densest)
[PASS] Auto-resize: 64 → 512 to hold 300 items
[PASS] Numeric / tuple keys (stringified)
[PASS] Large value (10 k element list)

================================================================
All 13 tests passed.
================================================================
```

## Files

| File | Description |
|---|---|
| `elastic_hash_table.h` | C public API |
| `elastic_hash_table.c` | C implementation (~340 lines) |
| `elastic_hash_table.py` | Python ctypes wrapper with dict interface |
| `test_elastic.py` | 13-test Python suite |
| `libelastic_hash_table.so` | Pre-built shared library (Linux x86-64) |


## References
> **Optimal Bounds for Open Addressing Without Reordering**
> Martín Farach‐Colton, Andrew Krapivin, William Kuszmaul
> (https://arxiv.org/pdf/2501.02305)
