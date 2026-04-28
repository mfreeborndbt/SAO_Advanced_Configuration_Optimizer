"""DuckDB-backed persistent cache.

Provides a key-value store that survives app restarts.  All values are
stored as JSON text so any JSON-serialisable Python object can be cached.
Thread-safe via a single lock around all DuckDB operations.
"""

import json
import os
import threading
import time

import duckdb

DB_PATH = os.path.join(os.path.dirname(__file__), ".cache", "sao_cache.duckdb")
_lock = threading.Lock()
_conn = None


def _get_conn():
    """Return (and lazily create) the module-level DuckDB connection."""
    global _conn
    if _conn is None:
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        _conn = duckdb.connect(DB_PATH)
        _conn.execute(
            """
            CREATE TABLE IF NOT EXISTS kv_cache (
                key   VARCHAR PRIMARY KEY,
                data  VARCHAR NOT NULL,
                ts    DOUBLE  NOT NULL
            )
            """
        )
    return _conn


# ------------------------------------------------------------------
# Public API
# ------------------------------------------------------------------

def cache_get(key, ttl=None):
    """Return cached value or *None* if missing / expired.

    *ttl* in seconds.  Pass ``None`` to accept any age.
    """
    with _lock:
        conn = _get_conn()
        row = conn.execute(
            "SELECT data, ts FROM kv_cache WHERE key = ?", [key]
        ).fetchone()
    if row is None:
        return None
    data_str, ts = row
    if ttl is not None and (time.time() - ts) > ttl:
        return None
    return json.loads(data_str)


def cache_set(key, data):
    """Store *data* under *key*, replacing any previous value."""
    data_str = json.dumps(data, default=_json_default)
    with _lock:
        conn = _get_conn()
        conn.execute(
            "INSERT OR REPLACE INTO kv_cache (key, data, ts) VALUES (?, ?, ?)",
            [key, data_str, time.time()],
        )


def cache_exists(key, ttl=None):
    """Return True if *key* is present (and younger than *ttl* seconds)."""
    with _lock:
        conn = _get_conn()
        row = conn.execute(
            "SELECT ts FROM kv_cache WHERE key = ?", [key]
        ).fetchone()
    if row is None:
        return False
    if ttl is not None and (time.time() - row[0]) > ttl:
        return False
    return True


def cache_clear():
    """Delete every entry in the cache."""
    with _lock:
        conn = _get_conn()
        conn.execute("DELETE FROM kv_cache")


def close():
    """Close the DuckDB connection (call before deleting the DB file)."""
    global _conn
    with _lock:
        if _conn is not None:
            _conn.close()
            _conn = None


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _json_default(obj):
    """Allow sets to be serialised as lists."""
    if isinstance(obj, set):
        return list(obj)
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
