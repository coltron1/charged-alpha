"""
Shared yfinance utilities — caching, chart fetching, common helpers.
Used by all screener/data modules to avoid duplication and share caches.
"""

import time
import uuid
import threading
from collections import OrderedDict
import yfinance as yf
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── Thread-safe cache with TTL ─────────────────────────────────────────────

class TTLCache:
    """Simple thread-safe dict cache with per-key TTL and max size."""

    def __init__(self, default_ttl=300, max_size=2000):
        self._data = OrderedDict()
        self._lock = threading.Lock()
        self._ttl = default_ttl
        self._max_size = max_size

    def get(self, key, ttl=None):
        ttl = ttl or self._ttl
        with self._lock:
            entry = self._data.get(key)
            if entry and (time.time() - entry[0]) < ttl:
                self._data.move_to_end(key)
                return entry[1]
            if entry:
                del self._data[key]
            return None

    def set(self, key, value):
        with self._lock:
            if key in self._data:
                del self._data[key]
            elif len(self._data) >= self._max_size:
                self._data.popitem(last=False)
            self._data[key] = (time.time(), value)

    def clear(self):
        with self._lock:
            self._data.clear()


# ── Global shared caches ───────────────────────────────────────────────────

ticker_info_cache = TTLCache(default_ttl=300, max_size=1000)
chart_cache = TTLCache(default_ttl=300, max_size=500)


# ── Ticker info fetcher (shared across stock, ETF, REIT screeners) ─────────

def fetch_ticker_info(symbol, max_retries=2):
    """Fetch yfinance Ticker and .info with caching and rate-limit retry."""
    cached = ticker_info_cache.get(symbol)
    if cached:
        return cached

    for attempt in range(max_retries):
        try:
            t = yf.Ticker(symbol)
            info = t.info
            if not info:
                return None, None
            result = (t, info)
            ticker_info_cache.set(symbol, result)
            return result
        except Exception as e:
            err = str(e)
            if "Too Many Requests" in err or "Rate" in err or "429" in err:
                time.sleep(5 * (attempt + 1))
            else:
                return None, None
    return None, None


# ── Safe float extractor ───────────────────────────────────────────────────

def safe_float(info, key, scale=1.0):
    """Extract a float from info dict, return None on failure."""
    v = info.get(key)
    if v is None:
        return None
    try:
        return round(float(v) * scale, 4)
    except (TypeError, ValueError):
        return None


# ── Dividend yield normalizer ──────────────────────────────────────────────

def normalize_div_yield(raw):
    """Normalize yfinance dividendYield — returns percentage or None."""
    if not raw:
        return None
    raw = float(raw)
    if raw < 1:
        return round(raw * 100, 2)
    return round(raw, 2)


# ── Chart fetcher (shared across all tools) ────────────────────────────────

DEFAULT_CHART_PARAMS = {
    "1d":  dict(period="1d",  interval="5m"),
    "1w":  dict(period="5d",  interval="30m"),
    "1m":  dict(period="1mo", interval="1d"),
    "3m":  dict(period="3mo", interval="1d"),
    "6m":  dict(period="6mo", interval="1d"),
    "1y":  dict(period="1y",  interval="1d"),
    "5y":  dict(period="5y",  interval="1wk"),
    "10y": dict(period="10y", interval="1mo"),
}

def fetch_chart(ticker, range_key="1y", params_map=None, decimals=2):
    """Fetch price chart data with caching. Returns {labels, prices} or None."""
    cache_key = f"{ticker}_{range_key}"
    ttl = 60 if range_key in ("1d", "1w") else 300
    cached = chart_cache.get(cache_key, ttl=ttl)
    if cached:
        return cached

    p = (params_map or DEFAULT_CHART_PARAMS).get(range_key,
          DEFAULT_CHART_PARAMS.get("1y"))
    try:
        t = yf.Ticker(ticker)
        hist = t.history(period=p["period"], interval=p["interval"])
        if hist.empty:
            return None
        if hist.index.tz is not None:
            hist.index = hist.index.tz_localize(None)
        fmt = "%Y-%m-%d %H:%M" if range_key in ("1d", "1w") else "%Y-%m-%d"
        labels = hist.index.strftime(fmt).tolist()
        prices = [round(float(v), decimals) if pd.notna(v) else None
                  for v in hist["Close"]]
        data = {"labels": labels, "prices": prices}
        chart_cache.set(cache_key, data)
        return data
    except Exception:
        return None


# ── Bulk ticker download (for banner, batch operations) ────────────────────

def fetch_banner_tickers(symbols, cache_obj=None, cache_key="banner",
                         cache_ttl=120):
    """Fetch 1d 5m data for many tickers in a single yf.download() call."""
    if cache_obj:
        cached = cache_obj.get(cache_key, ttl=cache_ttl)
        if cached:
            return cached

    try:
        df = yf.download(symbols, period="1d", interval="5m",
                         group_by="ticker", threads=True, progress=False)
    except Exception:
        return []

    results = []
    for sym in symbols:
        try:
            if len(symbols) == 1:
                closes_series = df["Close"]
            else:
                closes_series = df[sym]["Close"]
            closes = closes_series.dropna().tolist()
            if len(closes) < 2:
                continue
            closes = [round(float(c), 2) for c in closes]
            current = closes[-1]
            open_price = closes[0]
            change_pct = round((current - open_price) / open_price * 100, 2) \
                if open_price else 0
            step = max(1, len(closes) // 20)
            spark = closes[::step]
            if spark[-1] != closes[-1]:
                spark.append(closes[-1])
            results.append({
                "symbol": sym, "price": current,
                "change_pct": change_pct, "spark": spark,
            })
        except Exception:
            continue

    if cache_obj:
        cache_obj.set(cache_key, results)
    return results


# ── Job store with automatic cleanup ──────────────────────────────────────

class JobStore:
    """Thread-safe job store with automatic TTL cleanup."""

    def __init__(self, ttl=600):
        self._jobs = {}
        self._lock = threading.Lock()
        self._ttl = ttl
        self._start_reaper()

    def create(self):
        job_id = str(uuid.uuid4())
        with self._lock:
            self._jobs[job_id] = {
                "status": "running", "processed": 0, "total": 0,
                "matches": [], "error": None, "_created": time.time(),
            }
        return job_id

    def get(self, job_id):
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return None
            # Return copy without internal fields
            return {k: v for k, v in job.items() if not k.startswith("_")}

    def update(self, job_id, **kwargs):
        with self._lock:
            job = self._jobs.get(job_id)
            if job:
                job.update(kwargs)

    def append_match(self, job_id, match):
        with self._lock:
            job = self._jobs.get(job_id)
            if job:
                job["matches"].append(match)

    def set_progress(self, job_id, processed, total):
        with self._lock:
            job = self._jobs.get(job_id)
            if job:
                job["processed"] = processed
                job["total"] = total

    def _start_reaper(self):
        def reap():
            while True:
                time.sleep(60)
                now = time.time()
                with self._lock:
                    expired = [k for k, v in self._jobs.items()
                               if now - v.get("_created", 0) > self._ttl]
                    for k in expired:
                        del self._jobs[k]
        t = threading.Thread(target=reap, daemon=True)
        t.start()
