import time
import yfinance as yf
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from yf_utils import TTLCache, fetch_chart, normalize_div_yield

_cache = TTLCache(default_ttl=300, max_size=50)

YIELD_TICKERS = {
    "^IRX": {"name": "13-Week T-Bill", "maturity": 0.25},
    "^FVX": {"name": "5-Year Treasury", "maturity": 5},
    "^TNX": {"name": "10-Year Treasury", "maturity": 10},
    "^TYX": {"name": "30-Year Treasury", "maturity": 30},
}

BOND_ETFS = ["TLT", "IEF", "SHY", "BND", "TIP", "HYG", "LQD"]

def get_yields():
    cached = _cache.get("yields")
    if cached:
        return cached

    def fetch_yield(item):
        ticker, meta = item
        try:
            t = yf.Ticker(ticker)
            hist = t.history(period="5d")
            if hist.empty:
                return None
            current = float(hist["Close"].iloc[-1])
            prev = float(hist["Close"].iloc[-2]) if len(hist) > 1 else current
            return {
                "ticker": ticker,
                "name": meta["name"],
                "maturity": meta["maturity"],
                "yield_pct": round(current, 3),
                "change": round(current - prev, 3),
            }
        except Exception:
            return None

    results = []
    with ThreadPoolExecutor(max_workers=4) as pool:
        for r in pool.map(fetch_yield, YIELD_TICKERS.items()):
            if r:
                results.append(r)

    yields_map = {r["ticker"]: r["yield_pct"] for r in results}
    ten_yr = yields_map.get("^TNX")
    three_mo = yields_map.get("^IRX")
    thirty_yr = yields_map.get("^TYX")

    spreads = {}
    if ten_yr is not None and three_mo is not None:
        spreads["10Y_3M"] = round(ten_yr - three_mo, 3)
    if thirty_yr is not None and ten_yr is not None:
        spreads["30Y_10Y"] = round(thirty_yr - ten_yr, 3)

    data = {"yields": results, "spreads": spreads}
    _cache.set("yields", data)
    return data


def get_yield_history(ticker, range_key):
    result = fetch_chart(ticker, range_key, params_map={
        "1m": dict(period="1mo", interval="1d"),
        "3m": dict(period="3mo", interval="1d"),
        "1y": dict(period="1y", interval="1d"),
        "5y": dict(period="5y", interval="1wk"),
    }, decimals=3)
    if not result:
        return {"labels": [], "values": []}
    # fetch_chart returns {labels, prices} but bond template expects {labels, values}
    return {"labels": result["labels"], "values": result.get("values", result.get("prices", []))}


def get_bond_etfs():
    cached = _cache.get("etfs")
    if cached:
        return cached

    def fetch_etf(sym):
        try:
            t = yf.Ticker(sym)
            info = t.info
            hist = t.history(period="ytd")
            price = info.get("regularMarketPrice") or info.get("previousClose") or 0
            prev_close = info.get("regularMarketPreviousClose") or info.get("previousClose") or 0
            day_change = round((price - prev_close) / prev_close * 100, 2) if prev_close else 0

            ytd_return = None
            if not hist.empty and len(hist) > 1:
                first = float(hist["Close"].iloc[0])
                last = float(hist["Close"].iloc[-1])
                ytd_return = round((last - first) / first * 100, 2) if first else None

            return {
                "symbol": sym,
                "name": info.get("shortName", sym),
                "price": round(price, 2),
                "day_change": day_change,
                "ytd_return": ytd_return,
                "div_yield": normalize_div_yield(info.get("dividendYield")),
                "expense_ratio": info.get("annualReportExpenseRatio"),
            }
        except Exception:
            return None

    results = []
    with ThreadPoolExecutor(max_workers=7) as pool:
        for r in pool.map(fetch_etf, BOND_ETFS):
            if r:
                results.append(r)

    _cache.set("etfs", results)
    return results
