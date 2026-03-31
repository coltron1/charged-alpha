import time
import yfinance as yf
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

_cache = {}
_CACHE_TTL = 120

CURRENCIES = ["USD", "EUR", "GBP", "JPY", "CHF", "AUD", "CAD", "NZD"]

PAIRS = [
    "EURUSD", "GBPUSD", "USDJPY", "USDCHF", "AUDUSD", "USDCAD", "NZDUSD",
    "EURGBP", "EURJPY", "EURCHF", "EURAUD", "EURCAD", "EURNZD",
    "GBPJPY", "GBPCHF", "GBPAUD", "GBPCAD", "GBPNZD",
    "AUDJPY", "AUDCHF", "AUDCAD", "AUDNZD",
    "CADJPY", "CADCHF",
    "NZDJPY", "NZDCHF", "NZDCAD",
    "CHFJPY",
]

def _yf_ticker(pair):
    return f"{pair[:3]}{pair[3:]}=X"

def _get_pair_data(pair, period="5d", interval="1d"):
    ticker = _yf_ticker(pair)
    try:
        t = yf.Ticker(ticker)
        hist = t.history(period=period, interval=interval)
        if hist.empty:
            return None
        return hist
    except Exception:
        return None

def get_all_pairs(timeframe="1d"):
    cache_key = f"pairs_{timeframe}"
    cached = _cache.get(cache_key)
    if cached and (time.time() - cached[0]) < _CACHE_TTL:
        return cached[1]

    period_map = {
        "1d": "2d", "1w": "7d", "1m": "1mo", "3m": "3mo", "ytd": "ytd", "1y": "1y",
    }
    period = period_map.get(timeframe, "2d")

    results = []

    def fetch(pair):
        hist = _get_pair_data(pair, period=period)
        if hist is None or len(hist) < 2:
            return None
        current = float(hist["Close"].iloc[-1])
        prev = float(hist["Close"].iloc[0])
        change_pct = round((current - prev) / prev * 100, 3)
        return {
            "pair": pair,
            "display": f"{pair[:3]}/{pair[3:]}",
            "rate": round(current, 5),
            "change_pct": change_pct,
            "prev": round(prev, 5),
        }

    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = {pool.submit(fetch, p): p for p in PAIRS}
        for f in as_completed(futures):
            r = f.result()
            if r:
                results.append(r)

    results.sort(key=lambda x: PAIRS.index(x["pair"]))
    _cache[cache_key] = (time.time(), results)
    return results


def get_currency_strength(timeframe="1d"):
    pairs_data = get_all_pairs(timeframe)
    strength = {c: [] for c in CURRENCIES}

    for p in pairs_data:
        pair = p["pair"]
        base = pair[:3]
        quote = pair[3:]
        change = p["change_pct"]
        if base in strength:
            strength[base].append(change)
        if quote in strength:
            strength[quote].append(-change)

    result = []
    for currency, changes in strength.items():
        avg = round(sum(changes) / len(changes), 3) if changes else 0
        result.append({"currency": currency, "strength": avg, "pairs_count": len(changes)})

    result.sort(key=lambda x: x["strength"], reverse=True)
    return result


def get_pair_chart(pair, range_key="1y"):
    cache_key = f"chart_{pair}_{range_key}"
    cached = _cache.get(cache_key)
    if cached and (time.time() - cached[0]) < _CACHE_TTL:
        return cached[1]

    params = {
        "1d": dict(period="1d", interval="5m"),
        "1w": dict(period="5d", interval="30m"),
        "1m": dict(period="1mo", interval="1d"),
        "3m": dict(period="3mo", interval="1d"),
        "1y": dict(period="1y", interval="1d"),
        "5y": dict(period="5y", interval="1wk"),
    }
    p = params.get(range_key, params["1y"])

    try:
        t = yf.Ticker(_yf_ticker(pair))
        hist = t.history(**p)
        if hist.empty:
            return None
        if hist.index.tz is not None:
            hist.index = hist.index.tz_localize(None)
        fmt = "%Y-%m-%d %H:%M" if range_key in ("1d", "1w") else "%Y-%m-%d"
        labels = hist.index.strftime(fmt).tolist()
        prices = [round(float(v), 5) if pd.notna(v) else None for v in hist["Close"]]
        data = {"labels": labels, "prices": prices}
        _cache[cache_key] = (time.time(), data)
        return data
    except Exception:
        return None
