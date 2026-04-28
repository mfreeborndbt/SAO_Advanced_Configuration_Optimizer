"""DuckDB-backed persistent cache.

Provides a key-value store that survives app restarts.  All values are
stored as JSON text so any JSON-serialisable Python object can be cached.
Thread-safe via a single lock around all DuckDB operations.

Falls back to in-memory-only mode if DuckDB cannot open the database
(e.g. another process holds the lock).
"""

import json
import os
import threading
import time

DB_PATH = os.path.join(os.path.dirname(__file__), ".cache", "sao_cache.duckdb")
_lock = threading.Lock()
_conn = None
_fallback = False          # True when DuckDB is unavailable
_mem_cache = {}            # in-memory fallback store


def _get_conn():
    """Return (and lazily create) the module-level DuckDB connection."""
    global _conn, _fallback
    if _fallback:
        return None
    if _conn is None:
        try:
            import duckdb
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
        except Exception as e:
            print(f"Warning: DuckDB unavailable ({e}), using in-memory cache only")
            _fallback = True
            return None
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
        if conn is None:
            entry = _mem_cache.get(key)
            if entry is None:
                return None
            data, ts = entry
            if ttl is not None and (time.time() - ts) > ttl:
                return None
            return data
        try:
            row = conn.execute(
                "SELECT data, ts FROM kv_cache WHERE key = ?", [key]
            ).fetchone()
        except Exception:
            return None
    if row is None:
        return None
    data_str, ts = row
    if ttl is not None and (time.time() - ts) > ttl:
        return None
    return json.loads(data_str)


def cache_set(key, data):
    """Store *data* under *key*, replacing any previous value."""
    with _lock:
        conn = _get_conn()
        if conn is None:
            _mem_cache[key] = (data, time.time())
            return
        data_str = json.dumps(data, default=_json_default)
        try:
            conn.execute(
                "INSERT OR REPLACE INTO kv_cache (key, data, ts) VALUES (?, ?, ?)",
                [key, data_str, time.time()],
            )
        except Exception:
            _mem_cache[key] = (data, time.time())


def cache_exists(key, ttl=None):
    """Return True if *key* is present (and younger than *ttl* seconds)."""
    with _lock:
        conn = _get_conn()
        if conn is None:
            entry = _mem_cache.get(key)
            if entry is None:
                return False
            if ttl is not None and (time.time() - entry[1]) > ttl:
                return False
            return True
        try:
            row = conn.execute(
                "SELECT ts FROM kv_cache WHERE key = ?", [key]
            ).fetchone()
        except Exception:
            return False
    if row is None:
        return False
    if ttl is not None and (time.time() - row[0]) > ttl:
        return False
    return True


def cache_clear():
    """Delete every entry in the cache."""
    with _lock:
        _mem_cache.clear()
        conn = _get_conn()
        if conn is None:
            return
        try:
            conn.execute("DELETE FROM kv_cache")
        except Exception:
            pass


def close():
    """Close the DuckDB connection (call before deleting the DB file)."""
    global _conn, _fallback
    with _lock:
        if _conn is not None:
            try:
                _conn.close()
            except Exception:
                pass
            _conn = None
        _fallback = False
        _mem_cache.clear()


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _json_default(obj):
    """Allow sets to be serialised as lists."""
    if isinstance(obj, set):
        return list(obj)
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
