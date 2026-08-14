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
quote_snapshot_cache = TTLCache(default_ttl=120, max_size=500)


# ── Ticker info fetcher (shared across stock, ETF, REIT screeners) ─────────

def _has_usable_ticker_info(info):
    """Reject Yahoo's occasional shell payloads that contain no stock data."""
    if not isinstance(info, dict):
        return False
    return any(
        info.get(key) not in (None, "")
        for key in (
            "longName",
            "shortName",
            "currentPrice",
            "regularMarketPrice",
            "previousClose",
            "marketCap",
            "fiftyTwoWeekHigh",
            "sector",
            "industry",
            "website",
            "longBusinessSummary",
        )
    )

def fetch_ticker_info(symbol, max_retries=2):
    """Fetch yfinance Ticker and info dict with caching and rate-limit retry.

    Prefer `get_info()` because `.info` has recently become much less reliable for
    quote/valuation fields in this environment.
    """
    cached = ticker_info_cache.get(symbol)
    if cached:
        return cached

    attempts = max(1, int(max_retries or 1))
    for attempt in range(attempts):
        retry_delay = 0.5 * (attempt + 1)
        try:
            t = yf.Ticker(symbol)
            info = None
            try:
                info = t.get_info()
            except Exception:
                info = None
            if not info:
                try:
                    info = t.info
                except Exception:
                    info = None
            if _has_usable_ticker_info(info):
                result = (t, info)
                ticker_info_cache.set(symbol, result)
                return result
        except Exception as e:
            err = str(e)
            if "Too Many Requests" in err or "Rate" in err or "429" in err:
                retry_delay = 5 * (attempt + 1)

        # Yahoo occasionally returns an empty payload without raising. Treat it
        # as a transient failure instead of poisoning downstream pages with an
        # immediate empty result.
        if attempt < attempts - 1:
            time.sleep(retry_delay)
    return None, None


def fetch_quote_snapshot(symbol):
    """Return resilient quote-level data from Yahoo's chart endpoint.

    ``Ticker.get_info`` is richer but can fail independently of historical
    prices. This deliberately small fallback lets detail pages retain a current
    price, range, volume, and issuer name when fundamentals are temporarily
    unavailable. It is not used as a substitute for financial-statement data.
    """
    sym = (symbol or "").upper()
    if not sym:
        return {}
    cached = quote_snapshot_cache.get(sym)
    if cached:
        return cached

    try:
        ticker = yf.Ticker(sym)
        history = ticker.history(period="5d", interval="1h")
        try:
            metadata = ticker.history_metadata or {}
        except Exception:
            metadata = {}
        try:
            fast = ticker.fast_info or {}
        except Exception:
            fast = {}

        def _number(value):
            try:
                if pd.isna(value):
                    return None
            except Exception:
                pass
            try:
                return float(value)
            except (TypeError, ValueError):
                return None

        def _pick(*values):
            for value in values:
                number = _number(value)
                if number is not None:
                    return number
            return None

        closes = []
        try:
            closes = [
                float(value)
                for value in history.get("Close", pd.Series(dtype=float)).dropna().tolist()
            ]
        except Exception:
            closes = []

        def _fast_get(key):
            try:
                return fast.get(key)
            except Exception:
                return None

        price = _pick(metadata.get("regularMarketPrice"), _fast_get("lastPrice"), closes[-1] if closes else None)
        previous_close = _pick(
            metadata.get("previousClose"),
            _fast_get("previousClose"),
            closes[-2] if len(closes) >= 2 else None,
        )
        market_cap = _pick(_fast_get("marketCap"))
        shares = _pick(_fast_get("shares"))
        if market_cap is None and price is not None and shares is not None:
            market_cap = price * shares

        snapshot = {
            "symbol": sym,
            "name": metadata.get("longName") or metadata.get("shortName") or "",
            "price": round(price, 2) if price is not None else None,
            "previous_close": round(previous_close, 2) if previous_close is not None else None,
            "market_cap": round(market_cap) if market_cap is not None else None,
            "volume": round(_pick(metadata.get("regularMarketVolume"), _fast_get("lastVolume")) or 0) or None,
            "week_52_high": _pick(metadata.get("fiftyTwoWeekHigh"), _fast_get("yearHigh")),
            "week_52_low": _pick(metadata.get("fiftyTwoWeekLow"), _fast_get("yearLow")),
        }
        if snapshot["price"] is not None and snapshot["previous_close"] not in (None, 0):
            snapshot["change"] = round(snapshot["price"] - snapshot["previous_close"], 2)
            snapshot["change_pct"] = round(
                (snapshot["price"] - snapshot["previous_close"])
                / snapshot["previous_close"]
                * 100,
                2,
            )
        else:
            snapshot["change"] = None
            snapshot["change_pct"] = None

        if any(snapshot.get(key) is not None for key in ("price", "market_cap", "volume")):
            quote_snapshot_cache.set(sym, snapshot)
            return snapshot
    except Exception:
        pass
    return {}


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
    """Normalize yfinance dividendYield — returns percentage or None.
    yfinance inconsistently returns dividendYield as a percentage (2.5 = 2.5%)
    or decimal (0.025 = 2.5%). Use trailingAnnualDividendYield (always decimal)
    when available, or detect format from dividendYield."""
    if not raw:
        return None
    raw = float(raw)
    # Values > 1 are already percentages (e.g., 2.5 = 2.5%)
    # Values < 0.2 are decimals (e.g., 0.025 = 2.5%) — multiply by 100
    # Threshold of 0.2 handles even 20% yields correctly
    if raw < 0.2:
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

    def set_progress(self, job_id, processed, total, **kwargs):
        with self._lock:
            job = self._jobs.get(job_id)
            if job:
                job["processed"] = processed
                job["total"] = total
                for k, v in kwargs.items():
                    job[k] = v

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
