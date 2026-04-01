"""
Supabase L2 cache layer for Charged Alpha.

Two-tier caching:
  L1 = in-memory TTLCache (fast, per-process)
  L2 = Supabase table (shared across restarts/workers)

Set SUPABASE_URL and SUPABASE_KEY env vars to enable.
Without them, all L2 operations silently no-op and the app
uses L1-only caching (same as before).
"""

import json
import os
import time

_SUPABASE_URL = os.environ.get("SUPABASE_URL")
_SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
_TABLE = "cache"

_client = None


def _get_client():
    global _client
    if _client is not None:
        return _client
    if not _SUPABASE_URL or not _SUPABASE_KEY:
        return None
    try:
        from supabase import create_client
        _client = create_client(_SUPABASE_URL, _SUPABASE_KEY)
        return _client
    except Exception as e:
        print(f"[supabase_cache] Failed to init client: {e}", flush=True)
        return None


def get(key):
    """Fetch a cached value from Supabase. Returns None on miss or error."""
    client = _get_client()
    if client is None:
        return None
    try:
        resp = (client.table(_TABLE)
                .select("value, expires_at")
                .eq("key", key)
                .limit(1)
                .execute())
        rows = resp.data
        if not rows:
            return None
        row = rows[0]
        if row["expires_at"] and row["expires_at"] < time.time():
            # Expired — delete async, return miss
            try:
                client.table(_TABLE).delete().eq("key", key).execute()
            except Exception:
                pass
            return None
        return json.loads(row["value"])
    except Exception as e:
        print(f"[supabase_cache] get({key}): {e}", flush=True)
        return None


def set(key, value, ttl=300):
    """Store a value in Supabase cache with TTL in seconds."""
    client = _get_client()
    if client is None:
        return
    try:
        expires_at = time.time() + ttl
        payload = {
            "key": key,
            "value": json.dumps(value),
            "expires_at": expires_at,
        }
        client.table(_TABLE).upsert(payload, on_conflict="key").execute()
    except Exception as e:
        print(f"[supabase_cache] set({key}): {e}", flush=True)


def delete(key):
    """Remove a key from Supabase cache."""
    client = _get_client()
    if client is None:
        return
    try:
        client.table(_TABLE).delete().eq("key", key).execute()
    except Exception as e:
        print(f"[supabase_cache] delete({key}): {e}", flush=True)


# ── SQL schema (run once in Supabase SQL editor) ──
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS cache (
    key TEXT PRIMARY KEY,
    value JSONB NOT NULL,
    expires_at DOUBLE PRECISION,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_cache_expires ON cache (expires_at);
"""
