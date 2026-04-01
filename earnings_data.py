import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from yf_utils import TTLCache

_cache = TTLCache(default_ttl=3600, max_size=50)

TOP_STOCKS = [
    "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "GOOG", "TSLA", "BRK-B", "JPM",
    "V", "UNH", "XOM", "JNJ", "WMT", "PG", "MA", "HD", "CVX", "MRK",
    "ABBV", "PEP", "KO", "COST", "BAC", "AVGO", "TMO", "MCD", "CSCO", "ACN",
    "LIN", "ABT", "DHR", "NKE", "ORCL", "TXN", "CRM", "PM", "NEE", "UPS",
    "MS", "RTX", "HON", "AMGN", "UNP", "IBM", "LOW", "GE", "CAT", "INTC",
    "QCOM", "AMAT", "SBUX", "DE", "ADP", "MDLZ", "PLD", "GS", "BLK", "ISRG",
    "GILD", "ADI", "VRTX", "SYK", "T", "MMC", "BKNG", "LMT", "ZTS", "PYPL",
    "AXP", "SCHW", "CI", "CB", "CME", "TMUS", "MO", "SO", "DUK", "CL",
    "FIS", "USB", "PNC", "TGT", "NSC", "BDX", "SHW", "ITW", "APD", "MMM",
    "SPGI", "FDX", "AON", "COF", "WM", "CCI", "GM", "F", "DAL", "BA",
]

def get_earnings_week(week_str=None, sector=None):
    if week_str:
        try:
            start = datetime.strptime(week_str, "%Y-%m-%d").date()
        except ValueError:
            start = _get_monday()
    else:
        start = _get_monday()

    cache_key = f"week_{start.isoformat()}"
    cached = _cache.get(cache_key)
    if cached:
        return cached

    end = start + timedelta(days=4)
    results = []

    def fetch(sym):
        try:
            t = yf.Ticker(sym)
            cal = t.calendar
            if cal is None:
                return None

            # yfinance returns calendar as dict or DataFrame
            earnings_date = None
            eps_est = None
            rev_est = None

            if isinstance(cal, dict):
                ed = cal.get("Earnings Date")
                if ed:
                    if isinstance(ed, list) and len(ed) > 0:
                        earnings_date = pd.Timestamp(ed[0]).date()
                    elif hasattr(ed, 'date'):
                        earnings_date = ed.date()
                eps_est = cal.get("EPS Estimate")
                rev_est = cal.get("Revenue Estimate")
            elif isinstance(cal, pd.DataFrame) and not cal.empty:
                if "Earnings Date" in cal.index:
                    ed = cal.loc["Earnings Date"]
                    if hasattr(ed, 'iloc'):
                        ed = ed.iloc[0]
                    if hasattr(ed, 'date'):
                        earnings_date = ed.date()
                if "EPS Estimate" in cal.index:
                    eps_est = cal.loc["EPS Estimate"]
                    if hasattr(eps_est, 'iloc'):
                        eps_est = eps_est.iloc[0]
                if "Revenue Estimate" in cal.index:
                    rev_est = cal.loc["Revenue Estimate"]
                    if hasattr(rev_est, 'iloc'):
                        rev_est = rev_est.iloc[0]

            if earnings_date is None:
                return None

            if not (start <= earnings_date <= end):
                return None

            info = t.info
            return {
                "symbol": sym,
                "name": info.get("shortName", sym),
                "earnings_date": earnings_date.isoformat(),
                "day_of_week": earnings_date.strftime("%A"),
                "eps_estimate": float(eps_est) if eps_est and pd.notna(eps_est) else None,
                "revenue_estimate": float(rev_est) if rev_est and pd.notna(rev_est) else None,
                "market_cap": info.get("marketCap"),
                "sector": info.get("sector"),
                "price": info.get("regularMarketPrice") or info.get("previousClose"),
            }
        except Exception:
            return None

    with ThreadPoolExecutor(max_workers=20) as pool:
        futures = {pool.submit(fetch, s): s for s in TOP_STOCKS}
        for f in as_completed(futures):
            r = f.result()
            if r:
                results.append(r)

    results.sort(key=lambda x: (x["earnings_date"], -(x.get("market_cap") or 0)))

    data = {
        "week_start": start.isoformat(),
        "week_end": end.isoformat(),
        "earnings": results,
        "total": len(results),
    }
    _cache.set(cache_key, data)
    return data


def get_stock_earnings_history(symbol):
    cache_key = f"hist_{symbol}"
    cached = _cache.get(cache_key)
    if cached:
        return cached

    try:
        t = yf.Ticker(symbol)
        info = t.info

        # Get earnings history
        earnings = t.earnings_history
        history = []
        if earnings is not None and not earnings.empty:
            for _, row in earnings.iterrows():
                history.append({
                    "date": str(row.name.date()) if hasattr(row.name, 'date') else str(row.name),
                    "eps_estimate": float(row.get("epsEstimate")) if pd.notna(row.get("epsEstimate")) else None,
                    "eps_actual": float(row.get("epsActual")) if pd.notna(row.get("epsActual")) else None,
                    "surprise_pct": float(row.get("surprisePercent")) if pd.notna(row.get("surprisePercent")) else None,
                })

        # Get price chart (3 months)
        hist = t.history(period="3mo", interval="1d")
        labels = []
        prices = []
        if not hist.empty:
            if hist.index.tz is not None:
                hist.index = hist.index.tz_localize(None)
            labels = hist.index.strftime("%Y-%m-%d").tolist()
            prices = [round(float(v), 2) if pd.notna(v) else None for v in hist["Close"]]

        data = {
            "symbol": symbol,
            "name": info.get("shortName", symbol),
            "sector": info.get("sector"),
            "market_cap": info.get("marketCap"),
            "earnings_history": history[-8:],  # last 8 quarters
            "chart": {"labels": labels, "prices": prices},
        }
        _cache.set(cache_key, data)
        return data
    except Exception:
        return None


def _get_monday():
    today = datetime.now().date()
    return today - timedelta(days=today.weekday())
