"""Five-year, directly reported US-GAAP series from SEC Company Facts.

Annual and single-quarter durations stay separate. Cumulative cash flows and
annual EPS/share averages are never treated as a quarter or subtracted to invent Q4.
"""
from datetime import date, timedelta
from threading import Lock
import os
import time

import requests
from stock_research import number
from yf_utils import TTLCache

_index = TTLCache(default_ttl=86400, max_size=1)
_request_lock = Lock()
_last_request = 0.0

# Aliases are ordered by semantic specificity; no custom or segment tags.
FLOW_TAGS = {
    "revenue": ["RevenueFromContractWithCustomerExcludingAssessedTax", "RevenueFromContractWithCustomerIncludingAssessedTax", "Revenues", "SalesRevenueNet"],
    "net_income": ["NetIncomeLossAvailableToCommonStockholdersBasic", "NetIncomeLoss"],
    "gross_profit": ["GrossProfit"],
    "operating_income": ["OperatingIncomeLoss"],
    "operating_cashflow": ["NetCashProvidedByUsedInOperatingActivities"],
    "capex": ["PaymentsToAcquireProductiveAssets", "PaymentsToAcquirePropertyPlantAndEquipment"],
    "stock_compensation": ["ShareBasedCompensation"],
    "buybacks": ["PaymentsForRepurchaseOfCommonStock"],
    "dividends": ["PaymentsOfDividendsCommonStock"],
    "eps": ["EarningsPerShareDiluted"],
    "shares": ["WeightedAverageNumberOfDilutedSharesOutstanding", "WeightedAverageNumberOfShareOutstandingBasicAndDiluted"],
    "basic_shares": ["WeightedAverageNumberOfSharesOutstandingBasic", "WeightedAverageNumberOfShareOutstandingBasicAndDiluted"],
}
INSTANT_TAGS = {
    "assets": ["Assets"], "liabilities": ["Liabilities"],
    "equity": ["StockholdersEquity"],
    "cash": ["CashAndCashEquivalentsAtCarryingValue"],
    "current_assets": ["AssetsCurrent"], "current_liabilities": ["LiabilitiesCurrent"],
    "debt": [],
    "current_debt": ["DebtCurrent"],
    "long_term_debt": ["LongTermDebtNoncurrent"],
    "lease_liabilities": ["OperatingLeaseLiability"],
    "shares_outstanding": ["CommonStockSharesOutstanding"],
}


def _json(url):
    global _last_request
    # At most one SEC request/second per worker process, with a declared contact.
    with _request_lock:
        time.sleep(max(0, 1 - (time.monotonic() - _last_request)))
        _last_request = time.monotonic()
        response = requests.get(url, headers={
            "User-Agent": os.environ.get("SEC_USER_AGENT", "ChargedAlpha Research hello@chargedalpha.com"),
            "Accept": "application/json",
        }, timeout=(3, 8))
        response.raise_for_status()
        return response.json()


def _iso(value):
    try:
        return date.fromisoformat(value)
    except (ValueError, TypeError):
        return None


def parse_company_facts(payload, currency="USD", today=None):
    today = today or date.today()
    cutoff = today - timedelta(days=365 * 7)
    facts = payload.get("facts", {}).get("us-gaap", {})
    periods = {"annual": {}, "quarterly": {}}
    instants = {}

    def series(key, aliases, instant=False):
        unit = "shares" if "shares" in key else currency + "/shares" if key == "eps" else currency
        candidates = {}
        for priority, tag in enumerate(aliases):
            for fact in facts.get(tag, {}).get("units", {}).get(unit, []):
                end, start, filed = _iso(fact.get("end")), _iso(fact.get("start")), _iso(fact.get("filed"))
                value = number(fact.get("val"))
                if value is None or not end or not filed or not cutoff <= end <= today or filed > today:
                    continue
                if fact.get("form") not in {"10-K", "10-Q", "10-K/A", "10-Q/A"}:
                    continue
                if instant:
                    if start:
                        continue
                    period = "instant"
                elif start:
                    days = (end - start).days + 1
                    period = "annual" if 330 <= days <= 400 else "quarterly" if 75 <= days <= 105 else None
                    if not period:
                        continue
                else:
                    continue
                identity = (period, end.isoformat())
                # Prefer an exact concept, then its latest filed/restated observation.
                rank = (-priority, filed, fact.get("accn", ""))
                if identity not in candidates or rank > candidates[identity][0]:
                    candidates[identity] = (rank, value, start.isoformat() if start else None)
        return candidates

    for key, aliases in FLOW_TAGS.items():
        for (period, end), (_, value, start) in series(key, aliases).items():
            row = periods[period].setdefault(end, {"date": end, "_starts": {}})
            row[key] = abs(value) if key in {"capex", "buybacks", "dividends"} else value
            row["_starts"][key] = start
    for key, aliases in INSTANT_TAGS.items():
        for (_, end), (_, value, _) in series(key, aliases, instant=True).items():
            instants.setdefault(end, {})[key] = value

    annual_dates = set(periods["annual"])
    # Annual closing balance sheets are also valid quarter-end stock measures.
    for end in annual_dates:
        if end in instants:
            periods["quarterly"].setdefault(end, {"date": end, "_starts": {}})
    result = {}
    for period, rows in periods.items():
        for end, row in rows.items():
            row.update(instants.get(end, {}))
            starts = row.pop("_starts")
            start = starts.get("revenue") or starts.get("net_income")
            # Do not combine different duration windows that happen to end together.
            if start:
                for key, metric_start in starts.items():
                    if metric_start != start:
                        row[key] = None
            if row.get("debt") is None and row.get("current_debt") is not None and row.get("long_term_debt") is not None:
                row["debt"] = row["current_debt"] + row["long_term_debt"]
            ocf, capex = row.get("operating_cashflow"), row.get("capex")
            if ocf is not None and capex is not None and starts.get("operating_cashflow") == starts.get("capex"):
                row["free_cashflow"] = ocf - capex
        result[period] = [rows[key] for key in sorted(rows)][-(5 if period == "annual" else 20):]
    if not any(row.get("revenue") is not None or row.get("net_income") is not None for row in result["annual"]):
        return None
    cik = int(payload["cik"])
    return {**result, "currency": currency, "source": "SEC company filings",
            "source_url": f"https://www.sec.gov/edgar/browse/?CIK={cik}&owner=exclude",
            "note": "US-GAAP, latest filed values. Directly reported periods only; cumulative cash flows and annual EPS/share averages are not substituted for a quarter. Q4 and other gaps remain blank. Standard tags may omit company-specific disclosures. Debt definitions vary; operating leases are shown separately when available."}


def fetch_sec_financials(symbol, currency):
    if currency != "USD":
        return None
    companies = _index.get("tickers")
    if companies is None:
        companies = {row["ticker"]: int(row["cik_str"]) for row in _json("https://www.sec.gov/files/company_tickers.json").values()}
        _index.set("tickers", companies)
    cik = companies.get(symbol.replace(".", "-"))
    if not cik:
        return None
    return parse_company_facts(_json(f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik:010d}.json"), currency)
