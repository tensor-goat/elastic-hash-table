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
