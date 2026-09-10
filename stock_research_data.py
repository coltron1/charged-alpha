"""Bounded, single-flight background financial statement requests."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from threading import Lock
import time

import yfinance as yf
from stock_research import divide, number
from yf_utils import TTLCache

_pool = ThreadPoolExecutor(max_workers=3)
_lock = Lock()
_pending = {}
_cache = TTLCache(default_ttl=3600, max_size=128)
_failed = TTLCache(default_ttl=60, max_size=128)


def statement_rows(income, cashflow, balance, limit):
    def value(frame, date, names):
        for name in names:
            if name in frame.index and date in frame.columns:
                return number(frame.loc[name, date])
        return None
    rows = []
    dates = sorted(set(income.columns) | set(cashflow.columns) | set(balance.columns), reverse=True)[:limit]
    for date in reversed(dates):
        revenue = value(income, date, ["Total Revenue"])
        earnings = value(income, date, ["Net Income Common Stockholders", "Net Income"])
        operating = value(income, date, ["Operating Income"])
        fcf = value(cashflow, date, ["Free Cash Flow"])
        if fcf is None:
            ocf = value(cashflow, date, ["Operating Cash Flow"])
            capex = value(cashflow, date, ["Capital Expenditure"])
            if ocf is not None and capex is not None:
                fcf = ocf - abs(capex)
        rows.append({"date": date.strftime("%Y-%m-%d"), "revenue": revenue, "net_income": earnings,
                     "eps": value(income, date, ["Diluted EPS"]), "free_cashflow": fcf,
                     "operating_margin": divide(operating, revenue, 100),
                     "debt": value(balance, date, ["Total Debt"]),
                     "cash": value(balance, date, ["Cash And Cash Equivalents"]),
                     "shares": value(income, date, ["Diluted Average Shares"])})
    return [row for row in rows if any(value is not None for key, value in row.items() if key != "date")]


def fetch_financials(symbol):
    ticker = yf.Ticker(symbol)
    quarterly = statement_rows(ticker.quarterly_income_stmt, ticker.quarterly_cashflow, ticker.quarterly_balance_sheet, 8)
    annual = statement_rows(ticker.income_stmt, ticker.cashflow, ticker.balance_sheet, 5)
    if not any(r.get("revenue") is not None or r.get("net_income") is not None for r in quarterly + annual):
        raise ValueError("Financial statements unavailable")
    return {"quarterly": quarterly, "annual": annual, "fetched_at": datetime.now(timezone.utc).isoformat()}


def financial_status(symbol):
    cached = _cache.get(symbol)
    if cached is not None:
        return {"status": "ready", **cached}, 200
    if _failed.get(symbol):
        return {"status": "unavailable", "message": "Statements are temporarily unavailable. Try again in a minute."}, 503
    with _lock:
        if symbol in _pending:
            if time.monotonic() - _pending[symbol] > 35:
                return {"status": "unavailable", "message": "The data provider is taking longer than expected. Try again later."}, 503
            return {"status": "loading"}, 202
        if len(_pending) >= 12:
            return {"status": "busy", "message": "Financial data is busy. Try again shortly."}, 503
        _pending[symbol] = time.monotonic()
    def work():
        try:
            _cache.set(symbol, fetch_financials(symbol))
        except Exception:
            _failed.set(symbol, True)
        finally:
            with _lock:
                _pending.pop(symbol, None)
    _pool.submit(work)
    return {"status": "loading"}, 202
