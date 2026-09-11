"""Bounded, single-flight background financial statement requests."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from threading import Lock
import time

import yfinance as yf
from stock_research import divide, number
from yf_utils import TTLCache
from sec_financials import fetch_sec_financials

_pool = ThreadPoolExecutor(max_workers=3)
_lock = Lock()
_pending = {}
_cache = TTLCache(default_ttl=3600, max_size=128)
_failed = TTLCache(default_ttl=60, max_size=128)


def derived_metrics(row):
    def difference(a, b):
        return row[a] - row[b] if row.get(a) is not None and row.get(b) is not None else None
    row.update({
        "gross_margin": divide(row.get("gross_profit"), row.get("revenue"), 100),
        "operating_margin": divide(row.get("operating_income"), row.get("revenue"), 100),
        "net_margin": divide(row.get("net_income"), row.get("revenue"), 100),
        "fcf_margin": divide(row.get("free_cashflow"), row.get("revenue"), 100),
        "net_debt": difference("debt", "cash"),
        "working_capital": difference("current_assets", "current_liabilities"),
        "current_ratio": divide(row.get("current_assets"), row.get("current_liabilities")),
        "debt_to_equity": divide(row.get("debt"), row.get("equity")),
    })
    return row


def statement_rows(income, cashflow, balance, limit):
    def value(frame, date, names):
        for name in names:
            if name in frame.index and date in frame.columns:
                result = number(frame.loc[name, date])
                if result is not None:
                    return result
        return None
    rows = []
    dates = sorted(set(income.columns) | set(cashflow.columns) | set(balance.columns), reverse=True)[:limit]
    for date in reversed(dates):
        revenue = value(income, date, ["Total Revenue"])
        earnings = value(income, date, ["Net Income Common Stockholders", "Net Income"])
        operating = value(income, date, ["Operating Income"])
        ocf = value(cashflow, date, ["Operating Cash Flow"])
        capex = value(cashflow, date, ["Capital Expenditure"])
        fcf = value(cashflow, date, ["Free Cash Flow"])
        if fcf is None:
            if ocf is not None and capex is not None:
                fcf = ocf - abs(capex)
        row = {"date": date.strftime("%Y-%m-%d"), "revenue": revenue, "net_income": earnings,
                     "eps": value(income, date, ["Diluted EPS"]), "free_cashflow": fcf,
                     "operating_income": operating, "gross_profit": value(income, date, ["Gross Profit"]),
                     "operating_cashflow": ocf, "capex": abs(capex) if capex is not None else None,
                     "debt": value(balance, date, ["Total Debt"]),
                     "cash": value(balance, date, ["Cash And Cash Equivalents"]),
                     "assets": value(balance, date, ["Total Assets"]),
                     "liabilities": value(balance, date, ["Total Liabilities Net Minority Interest"]),
                     "equity": value(balance, date, ["Stockholders Equity"]),
                     "current_assets": value(balance, date, ["Current Assets"]),
                     "current_liabilities": value(balance, date, ["Current Liabilities"]),
                     "long_term_debt": value(balance, date, ["Long Term Debt"]),
                     "shares": value(income, date, ["Diluted Average Shares"]),
                     "basic_shares": value(income, date, ["Basic Average Shares"]),
                     "shares_outstanding": value(balance, date, ["Ordinary Shares Number"]),
                     "stock_compensation": value(cashflow, date, ["Stock Based Compensation"])}
        for key, names in [("buybacks", ["Repurchase Of Capital Stock"]), ("dividends", ["Common Stock Dividend Paid", "Cash Dividends Paid"])]:
            amount = value(cashflow, date, names)
            row[key] = abs(amount) if amount is not None else None
        rows.append(derived_metrics(row))
    return [row for row in rows if any(value is not None for key, value in row.items() if key != "date")]


def fetch_financials(symbol, reporting_currency=None):
    from stock_research import read_registry
    currency = reporting_currency or read_registry().get("profiles", {}).get(symbol, {}).get("financial_currency")
    # Keep sources separate: SEC periods are never spliced into Yahoo series.
    try:
        sec = fetch_sec_financials(symbol, currency)
        if sec:
            for period in ("annual", "quarterly"):
                sec[period] = [derived_metrics(row) for row in sec[period]]
            return {**sec, "fetched_at": datetime.now(timezone.utc).isoformat()}
    except Exception:
        pass
    ticker = yf.Ticker(symbol)
    quarterly = statement_rows(ticker.quarterly_income_stmt, ticker.quarterly_cashflow, ticker.quarterly_balance_sheet, 20)
    annual = statement_rows(ticker.income_stmt, ticker.cashflow, ticker.balance_sheet, 5)
    if not any(r.get("revenue") is not None or r.get("net_income") is not None for r in quarterly + annual):
        raise ValueError("Financial statements unavailable")
    return {"quarterly": quarterly, "annual": annual, "source": "Yahoo Finance", "currency": currency,
            "source_url": "https://finance.yahoo.com/quote/" + symbol + "/financials/",
            "note": "Provider history is typically limited to four annual periods and five quarters. Missing periods are not estimated.",
            "fetched_at": datetime.now(timezone.utc).isoformat()}


def financial_status(symbol, reporting_currency=None):
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
            _cache.set(symbol, fetch_financials(symbol, reporting_currency))
        except Exception:
            _failed.set(symbol, True)
        finally:
            with _lock:
                _pending.pop(symbol, None)
    _pool.submit(work)
    return {"status": "loading"}, 202
