import time
import yfinance as yf
import pandas as pd

_cache = {}
_CACHE_TTL = 300

YIELD_TICKERS = {
    "^IRX": {"name": "13-Week T-Bill", "maturity": 0.25},
    "^FVX": {"name": "5-Year Treasury", "maturity": 5},
    "^TNX": {"name": "10-Year Treasury", "maturity": 10},
    "^TYX": {"name": "30-Year Treasury", "maturity": 30},
}

BOND_ETFS = ["TLT", "IEF", "SHY", "BND", "TIP", "HYG", "LQD"]

def get_yields():
    cached = _cache.get("yields")
    if cached and (time.time() - cached[0]) < _CACHE_TTL:
        return cached[1]

    results = []
    for ticker, meta in YIELD_TICKERS.items():
        try:
            t = yf.Ticker(ticker)
            hist = t.history(period="5d")
            if hist.empty:
                continue
            current = float(hist["Close"].iloc[-1])
            prev = float(hist["Close"].iloc[-2]) if len(hist) > 1 else current
            change = round(current - prev, 3)
            results.append({
                "ticker": ticker,
                "name": meta["name"],
                "maturity": meta["maturity"],
                "yield_pct": round(current, 3),
                "change": change,
            })
        except Exception:
            pass

    # Calculate spreads
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
    _cache["yields"] = (time.time(), data)
    return data


def get_yield_history(ticker, range_key):
    cache_key = f"hist_{ticker}_{range_key}"
    cached = _cache.get(cache_key)
    if cached and (time.time() - cached[0]) < _CACHE_TTL:
        return cached[1]

    params = {
        "1m": dict(period="1mo", interval="1d"),
        "3m": dict(period="3mo", interval="1d"),
        "1y": dict(period="1y", interval="1d"),
        "5y": dict(period="5y", interval="1wk"),
    }
    p = params.get(range_key, params["1y"])

    try:
        t = yf.Ticker(ticker)
        hist = t.history(**p)
        if hist.empty:
            return {"labels": [], "values": []}
        if hist.index.tz is not None:
            hist.index = hist.index.tz_localize(None)
        labels = hist.index.strftime("%Y-%m-%d").tolist()
        values = [round(float(v), 3) if pd.notna(v) else None for v in hist["Close"]]
        data = {"labels": labels, "values": values}
        _cache[cache_key] = (time.time(), data)
        return data
    except Exception:
        return {"labels": [], "values": []}


def get_bond_etfs():
    cached = _cache.get("etfs")
    if cached and (time.time() - cached[0]) < _CACHE_TTL:
        return cached[1]

    results = []
    for sym in BOND_ETFS:
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

            div_yield = info.get("dividendYield")
            if div_yield:
                # yfinance returns this as percentage already (e.g. 4.28 = 4.28%)
                # but sometimes as decimal (0.0428) — normalize
                if div_yield < 1:
                    div_yield = round(div_yield * 100, 2)
                else:
                    div_yield = round(div_yield, 2)

            results.append({
                "symbol": sym,
                "name": info.get("shortName", sym),
                "price": round(price, 2),
                "day_change": day_change,
                "ytd_return": ytd_return,
                "div_yield": div_yield,
                "expense_ratio": info.get("annualReportExpenseRatio"),
            })
        except Exception:
            pass

    _cache["etfs"] = (time.time(), results)
    return results
