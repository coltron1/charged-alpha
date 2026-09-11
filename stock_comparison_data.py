"""Read-only listing lookup and bounded custom-comparison quote hydration."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from threading import Lock, BoundedSemaphore
import re

import yfinance as yf
from stock_research import normalize_profile, age_days
from yf_utils import TTLCache, fetch_ticker_info

_pool = ThreadPoolExecutor(max_workers=2)
_lock = Lock()
_pending = set()
_profiles = TTLCache(default_ttl=900, max_size=128)
_failures = TTLCache(default_ttl=60, max_size=128)
_search_cache = TTLCache(default_ttl=300, max_size=128)
_search_slots = BoundedSemaphore(2)
SYMBOL = re.compile(r"^[A-Z0-9][A-Z0-9.-]{0,19}$")


def search_stocks(query, profiles, remote=False):
    query = query.strip()[:60]
    upper = query.upper()
    matches = [{"ticker": ticker, "company": profile["company"], "exchange": profile.get("exchange", "")}
               for ticker, profile in profiles.items()
               if upper in ticker or query.lower() in profile["company"].lower()]
    matches.sort(key=lambda p: (p["ticker"] != upper, not p["ticker"].startswith(upper), p["ticker"]))
    if not remote:
        return matches[:12]
    cached = _search_cache.get(upper)
    if cached is None:
        if not _search_slots.acquire(blocking=False):
            raise ValueError("Listing search is busy. Please try again.")
        try:
            quotes = yf.Search(query, max_results=12, news_count=0, lists_count=0, recommended=0, timeout=8).quotes
            cached = [{"ticker": q["symbol"], "company": q.get("longname") or q.get("shortname") or q["symbol"],
                       "exchange": q.get("exchDisp") or q.get("exchange") or ""}
                      for q in quotes if q.get("quoteType") == "EQUITY" and SYMBOL.fullmatch(q.get("symbol", ""))]
            _search_cache.set(upper, cached)
        finally:
            _search_slots.release()
    seen = {p["ticker"] for p in matches}
    return (matches + [p for p in cached if p["ticker"] not in seen])[:20]


def custom_profile_status(symbol, registry):
    profile = registry.get("profiles", {}).get(symbol)
    if profile and age_days(profile.get("observed_at")) <= 14:
        return profile, "ready"
    cached = _profiles.get(symbol)
    if cached:
        return cached, "ready"
    if _failures.get(symbol):
        return None, "unavailable"
    with _lock:
        if symbol in _pending:
            return None, "loading"
        if len(_pending) >= 8:
            return None, "busy"
        _pending.add(symbol)
    def fetch():
        try:
            _, info = fetch_ticker_info(symbol, max_retries=1)
            if not info or info.get("quoteType") != "EQUITY":
                raise ValueError("Not an available stock listing")
            # Live custom selections use only known-current USD conversion.
            # Other currencies retain native ratios, without stale FX comparisons.
            data = normalize_profile(symbol, info, datetime.now(timezone.utc).isoformat(), {"USD": 1})
            _profiles.set(symbol, data)
        except Exception:
            _failures.set(symbol, True)
        finally:
            with _lock:
                _pending.discard(symbol)
    _pool.submit(fetch)
    return None, "loading"
