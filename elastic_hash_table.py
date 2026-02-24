"""
elastic_hash_table — Python bindings for the Elastic Hash Table C library.

Usage
-----
>>> from elastic_hash_table import ElasticHashTable
>>> t = ElasticHashTable(4096)
>>> t["hello"] = {"nested": [1, 2, 3]}
>>> t["hello"]
{'nested': [1, 2, 3]}
>>> del t["hello"]
>>> len(t)
0

Any picklable Python key (converted to str) and picklable value works.
"""

from __future__ import annotations

import ctypes
import ctypes.util
import os
import pickle
import platform
import struct
import sys
from pathlib import Path
from typing import Any, Iterator, Optional, Tuple


# -------------------------------------------------------------------
# Locate and load the shared library
# -------------------------------------------------------------------

def _find_library() -> ctypes.CDLL:
    """Find and load libelastic_hash_table.so next to this file."""
    here = Path(__file__).resolve().parent

    system = platform.system()
    if system == "Darwin":
        suffixes = (".dylib", ".so")
    elif system == "Windows":
        suffixes = (".dll", ".pyd")
    else:
        suffixes = (".so",)

    for sfx in suffixes:
        path = here / f"libelastic_hash_table{sfx}"
        if path.exists():
            return ctypes.CDLL(str(path))

    raise OSError(
        f"Cannot find libelastic_hash_table shared library in {here}.\n"
        f"Build it first:  gcc -O2 -shared -fPIC -lm "
        f"-o libelastic_hash_table.so elastic_hash_table.c"
    )


_lib = _find_library()

# -------------------------------------------------------------------
# C type declarations
# -------------------------------------------------------------------

class _EHTLevelInfo(ctypes.Structure):
    _fields_ = [
        ("level",      ctypes.c_int),
        ("capacity",   ctypes.c_size_t),
        ("count",      ctypes.c_size_t),
        ("tombstones", ctypes.c_size_t),
    ]


# -- Lifecycle --
_lib.eht_create.argtypes  = [ctypes.c_size_t]
_lib.eht_create.restype   = ctypes.c_void_p

_lib.eht_destroy.argtypes = [ctypes.c_void_p]
_lib.eht_destroy.restype  = None

# -- Core ops --
_lib.eht_insert.argtypes  = [ctypes.c_void_p, ctypes.c_char_p,
                              ctypes.c_void_p, ctypes.c_size_t]
_lib.eht_insert.restype   = ctypes.c_int

_lib.eht_get.argtypes     = [ctypes.c_void_p, ctypes.c_char_p,
                              ctypes.POINTER(ctypes.c_void_p),
                              ctypes.POINTER(ctypes.c_size_t)]
_lib.eht_get.restype      = ctypes.c_int

_lib.eht_delete.argtypes  = [ctypes.c_void_p, ctypes.c_char_p]
_lib.eht_delete.restype   = ctypes.c_int

_lib.eht_contains.argtypes = [ctypes.c_void_p, ctypes.c_char_p]
_lib.eht_contains.restype  = ctypes.c_int

# -- Metadata --
_lib.eht_len.argtypes        = [ctypes.c_void_p]
_lib.eht_len.restype         = ctypes.c_size_t

_lib.eht_capacity.argtypes   = [ctypes.c_void_p]
_lib.eht_capacity.restype    = ctypes.c_size_t

_lib.eht_num_levels.argtypes = [ctypes.c_void_p]
_lib.eht_num_levels.restype  = ctypes.c_size_t

_lib.eht_level_stats.argtypes = [ctypes.c_void_p,
                                  ctypes.POINTER(_EHTLevelInfo),
                                  ctypes.c_size_t]
_lib.eht_level_stats.restype  = None

# -- Iteration --
_lib.eht_iter_create.argtypes  = [ctypes.c_void_p]
_lib.eht_iter_create.restype   = ctypes.c_void_p

_lib.eht_iter_next.argtypes    = [ctypes.c_void_p,
                                   ctypes.POINTER(ctypes.c_char_p),
                                   ctypes.POINTER(ctypes.c_void_p),
                                   ctypes.POINTER(ctypes.c_size_t)]
_lib.eht_iter_next.restype     = ctypes.c_int

_lib.eht_iter_destroy.argtypes = [ctypes.c_void_p]
_lib.eht_iter_destroy.restype  = None


# -------------------------------------------------------------------
# Serialisation helpers
# -------------------------------------------------------------------

def _key_to_bytes(key: Any) -> bytes:
    """Convert an arbitrary Python key to a UTF-8 C string."""
    if isinstance(key, bytes):
        return key
    return str(key).encode("utf-8")


def _ser_value(value: Any) -> bytes:
    return pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)


def _de_value(buf: bytes) -> Any:
    return pickle.loads(buf)


# -------------------------------------------------------------------
# Public API
# -------------------------------------------------------------------

class ElasticHashTable:
    """
    A Python dict-like wrapper around the C Elastic Hash Table.

    Keys are converted to strings via ``str(key)``.  Values can be any
    picklable Python object.

    Parameters
    ----------
    capacity : int
        Initial total slot count across all geometric levels.
    """

    __slots__ = ("_handle",)

    def __init__(self, capacity: int = 1024) -> None:
        self._handle = _lib.eht_create(max(capacity, 64))
        if not self._handle:
            raise MemoryError("Failed to allocate ElasticHashTable")

    def __del__(self) -> None:
        if getattr(self, "_handle", None):
            _lib.eht_destroy(self._handle)
            self._handle = None

    # ---- Core operations ---------------------------------------------

    def insert(self, key: Any, value: Any) -> None:
        """Insert or update *key* → *value*."""
        kb = _key_to_bytes(key)
        vb = _ser_value(value)
        rc = _lib.eht_insert(self._handle, kb, vb, len(vb))
        if rc < 0:
            raise MemoryError("eht_insert failed (allocation error)")

    def get(self, key: Any, default: Any = None) -> Any:
        """Return the value for *key*, or *default*."""
        kb = _key_to_bytes(key)
        val_ptr = ctypes.c_void_p()
        val_len = ctypes.c_size_t()
        found = _lib.eht_get(self._handle, kb,
                              ctypes.byref(val_ptr),
                              ctypes.byref(val_len))
        if not found:
            return default
        buf = (ctypes.c_char * val_len.value).from_address(val_ptr.value)
        return _de_value(bytes(buf))

    def delete(self, key: Any) -> bool:
        """Remove *key*.  Returns True if it was present."""
        kb = _key_to_bytes(key)
        return bool(_lib.eht_delete(self._handle, kb))

    # ---- Dict interface ----------------------------------------------

    def __setitem__(self, key: Any, value: Any) -> None:
        self.insert(key, value)

    def __getitem__(self, key: Any) -> Any:
        kb = _key_to_bytes(key)
        val_ptr = ctypes.c_void_p()
        val_len = ctypes.c_size_t()
        found = _lib.eht_get(self._handle, kb,
                              ctypes.byref(val_ptr),
                              ctypes.byref(val_len))
        if not found:
            raise KeyError(key)
        buf = (ctypes.c_char * val_len.value).from_address(val_ptr.value)
        return _de_value(bytes(buf))

    def __delitem__(self, key: Any) -> None:
        if not self.delete(key):
            raise KeyError(key)

    def __contains__(self, key: Any) -> bool:
        kb = _key_to_bytes(key)
        return bool(_lib.eht_contains(self._handle, kb))

    def __len__(self) -> int:
        return _lib.eht_len(self._handle)

    def __bool__(self) -> bool:
        return len(self) > 0

    def __repr__(self) -> str:
        return (f"ElasticHashTable(count={len(self)}, "
                f"capacity={self.capacity}, "
                f"levels={self.num_levels})")

    # ---- Iteration ---------------------------------------------------

    def keys(self) -> Iterator[str]:
        """Iterate over all keys (as strings)."""
        it = _lib.eht_iter_create(self._handle)
        if not it:
            raise MemoryError("Failed to create iterator")
        try:
            k_ptr = ctypes.c_char_p()
            v_ptr = ctypes.c_void_p()
            v_len = ctypes.c_size_t()
            while _lib.eht_iter_next(it,
                                      ctypes.byref(k_ptr),
                                      ctypes.byref(v_ptr),
                                      ctypes.byref(v_len)):
                yield k_ptr.value.decode("utf-8")
        finally:
            _lib.eht_iter_destroy(it)

    def values(self) -> Iterator[Any]:
        """Iterate over all values."""
        it = _lib.eht_iter_create(self._handle)
        if not it:
            raise MemoryError("Failed to create iterator")
        try:
            k_ptr = ctypes.c_char_p()
            v_ptr = ctypes.c_void_p()
            v_len = ctypes.c_size_t()
            while _lib.eht_iter_next(it,
                                      ctypes.byref(k_ptr),
                                      ctypes.byref(v_ptr),
                                      ctypes.byref(v_len)):
                buf = (ctypes.c_char * v_len.value).from_address(v_ptr.value)
                yield _de_value(bytes(buf))
        finally:
            _lib.eht_iter_destroy(it)

    def items(self) -> Iterator[Tuple[str, Any]]:
        """Iterate over all (key, value) pairs."""
        it = _lib.eht_iter_create(self._handle)
        if not it:
            raise MemoryError("Failed to create iterator")
        try:
            k_ptr = ctypes.c_char_p()
            v_ptr = ctypes.c_void_p()
            v_len = ctypes.c_size_t()
            while _lib.eht_iter_next(it,
                                      ctypes.byref(k_ptr),
                                      ctypes.byref(v_ptr),
                                      ctypes.byref(v_len)):
                key = k_ptr.value.decode("utf-8")
                buf = (ctypes.c_char * v_len.value).from_address(v_ptr.value)
                yield key, _de_value(bytes(buf))
        finally:
            _lib.eht_iter_destroy(it)

    def __iter__(self) -> Iterator[str]:
        return self.keys()

    # ---- Metadata / diagnostics --------------------------------------

    @property
    def capacity(self) -> int:
        return _lib.eht_capacity(self._handle)

    @property
    def num_levels(self) -> int:
        return _lib.eht_num_levels(self._handle)

    @property
    def load_factor(self) -> float:
        cap = self.capacity
        return len(self) / cap if cap else 0.0

    def level_stats(self) -> list[dict]:
        """Return per-level diagnostics as a list of dicts."""
        n = self.num_levels
        arr = (_EHTLevelInfo * n)()
        _lib.eht_level_stats(self._handle, arr, n)
        return [
            {
                "level":      arr[i].level,
                "capacity":   arr[i].capacity,
                "count":      arr[i].count,
                "tombstones": arr[i].tombstones,
                "load":       arr[i].count / arr[i].capacity
                              if arr[i].capacity else 0.0,
            }
            for i in range(n)
        ]

    def print_stats(self) -> None:
        """Print a full diagnostic summary."""
        count = len(self)
        cap   = self.capacity
        load  = self.load_factor
        print(f"{'=' * 64}")
        print(f"ElasticHashTable  —  {count:,} / {cap:,}  ({load:.1%} load)")
        print(f"{'─' * 64}")
        for s in self.level_stats():
            tomb = s["tombstones"]
            print(f"  Level {s['level']:>2}: {s['capacity']:>8,} slots | "
                  f"{s['count']:>8,} live ({s['load']:5.1%}) | "
                  f"{tomb:>5,} tombstones")
        print(f"{'=' * 64}")
