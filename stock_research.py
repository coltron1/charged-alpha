"""Dated research snapshots, comparable businesses, and consistent metric units."""

from collections import OrderedDict
from datetime import datetime, timezone
from functools import lru_cache
import json
import math
from pathlib import Path
import re
from statistics import median


ROOT = Path(__file__).resolve().parent
SNAPSHOT_PATH = ROOT / "data/stock_research_snapshots.json"
MAX_PROFILE_AGE_DAYS = 14

# Narrow operating groups override broad provider classifications. Sources and
# maintenance rules are documented in docs/stock-research.md.
BUSINESS_GROUPS = {
    "Convenience stores & fuel retail": ["CASY", "MUSA", "ATD.TO"],
    "Home improvement retail": ["HD", "LOW"],
    "Off-price apparel retail": ["TJX", "ROST", "BURL"],
    "Warehouse clubs": ["COST", "BJ"],
    "General merchandise retail": ["WMT", "TGT"],
    "Supermarkets": ["KR", "ACI", "SFM", "IMKTA", "GO"],
    "Automobile manufacturers": ["F", "GM", "TM", "HMC", "STLA", "TSLA", "RIVN", "LCID", "NIO", "XPEV", "LI"],
    "Integrated oil & gas": ["XOM", "CVX", "SHEL", "BP", "TTE", "EQNR"],
    "Farm equipment": ["DE", "AGCO", "CNH"],
    "Construction & mining equipment": ["CAT", "KMTUY", "TEX"],
    "Aircraft engines & aerospace systems": ["GE", "RTX", "SAFRY"],
    "AI compute & networking chips": ["NVDA", "AMD", "AVGO", "MRVL"],
    "Analog & embedded chips": ["TXN", "ADI", "NXPI", "MCHP", "ON", "STM"],
    "Semiconductor foundries": ["TSM", "GFS", "UMC"],
    "Memory chips": ["MU", "SNDK"],
    "Hard disk drives": ["WDC", "STX"],
    "Semiconductor manufacturing equipment": ["AMAT", "LRCX", "KLAC", "ASML", "ONTO", "ACLS"],
    "Consumer devices": ["AAPL", "SONO", "LOGI", "GPRO"],
    "Search & social advertising": ["GOOGL", "GOOG", "META", "PINS", "SNAP", "RDDT"],
    "Diversified cloud platforms": ["MSFT", "AMZN", "GOOGL", "ORCL"],
    "Streaming entertainment": ["NFLX", "DIS", "WBD", "ROKU"],
    "Payment networks": ["V", "MA"],
    "Card lending": ["AXP", "COF", "SYF"],
    "Diversified banks": ["JPM", "BAC", "C", "WFC", "USB", "PNC", "TFC"],
    "Capital markets & wealth management": ["GS", "MS", "SCHW", "RJF", "LPLA", "IBKR"],
    "Independent power generation": ["VST", "CEG", "NRG", "TLN"],
    "Power generation equipment": ["GEV", "SMEGF"],
    "Cybersecurity software": ["PANW", "CRWD", "ZS", "FTNT", "S", "OKTA", "RBRK", "NTSK"],
    "Software development platforms": ["GTLB", "FROG"],
    "Observability software": ["DDOG", "DT", "ESTC"],
    "Enterprise workflow software": ["CRM", "NOW", "WDAY", "HUBS", "PCTY", "PAYC"],
    "Beverages": ["KO", "PEP", "KDP", "MNST", "FIZZ", "CELH"],
    "Household & personal care": ["PG", "CL", "KMB", "CHD", "CLX", "UL"],
    "Pharmaceuticals": ["MRK", "ABBV", "PFE", "BMY", "LLY", "NVO", "AZN", "NVS", "SNY"],
    "Industrial gases": ["LIN", "APD", "AIQUY"],
    "Pizza restaurants": ["DPZ", "PZZA"],
    "Firearms manufacturers": ["SWBI", "RGR"],
    "Discount variety stores": ["DG", "DLTR", "FIVE", "OLLI"],
}
EXCLUDED_GENERIC_INDUSTRIES = {
    "specialty-retail", "software-application", "software-infrastructure",
    "semiconductors", "conglomerates", "specialty-business-services",
    "internet-content-information", "electronic-components", "other-industrial-metals-mining",
    "capital-markets", "medical-devices", "biotechnology", "information-technology-services",
    "specialty-industrial-machinery", "aerospace-defense", "electrical-equipment-parts",
}
ISSUER_ALIASES = {"GOOG": "GOOGL", "BRK-B": "BRK-A", "HEI-A": "HEI", "FOX": "FOXA", "NWS": "NWSA"}
BUSINESS_NOTES = {
    "CASY": "Convenience stores, prepared food and retail fuel; food mix differs across operators.",
    "MUSA": "Retail fuel and convenience stores; fuel exposure is higher than Casey's prepared-food mix.",
    "ATD.TO": "Global convenience and fuel retail; broader international exposure than Casey's.",
    "AVGO": "AI networking and custom silicon, alongside a substantial infrastructure software business.",
    "AMZN": "Cloud infrastructure is one segment; retail materially changes the consolidated economics.",
    "GOOGL": "Advertising is the largest business; cloud is a separate segment.",
    "DIS": "Streaming is one segment alongside parks, studios and linear television.",
    "TSLA": "Automotive revenue alongside energy storage and other businesses.",
}


def number(value):
    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (TypeError, ValueError):
        return None


def divide(a, b, scale=1):
    a, b = number(a), number(b)
    return a / b * scale if a is not None and b is not None and b > 0 else None


def iso_time(value):
    try:
        return datetime.fromtimestamp(float(value), timezone.utc).isoformat()
    except (TypeError, ValueError, OverflowError, OSError):
        return ""


def age_days(value, now=None):
    try:
        date = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if date.tzinfo is None:
            date = date.replace(tzinfo=timezone.utc)
        return ((now or datetime.now(timezone.utc)) - date).total_seconds() / 86400
    except (AttributeError, ValueError):
        return float("inf")


@lru_cache(maxsize=4)
def _read_json(path, stamp):
    return json.loads(Path(path).read_text())


def read_registry(path=SNAPSHOT_PATH):
    path = Path(path)
    return _read_json(str(path), path.stat().st_mtime_ns) if path.exists() else {"profiles": {}}


def groups_for(ticker, industry_key):
    groups = [name for name, members in BUSINESS_GROUPS.items() if ticker in members]
    if groups:
        return groups
    if industry_key and industry_key not in EXCLUDED_GENERIC_INDUSTRIES:
        return ["industry:" + industry_key]
    return []


def normalize_profile(ticker, info, observed_at, fx=None):
    """All percentage metrics use percentage points; D/E alone is a multiple."""
    currency = info.get("currency") or ""
    financial_currency = info.get("financialCurrency") or ""
    # Quote and statement currencies differ for many ADRs. Currency-dependent
    # valuation calculations must not mix these currencies.
    fx = fx or {"USD": 1}
    cap = number(info.get("marketCap"))
    price = number(info.get("currentPrice")) or number(info.get("regularMarketPrice"))
    previous = number(info.get("regularMarketPreviousClose")) or number(info.get("previousClose"))
    change = price - previous if price is not None and previous is not None else None
    equity = number(info.get("bookValue"))
    eps = number(info.get("trailingEps"))
    forward_eps = number(info.get("forwardEps"))
    debt, cash = number(info.get("totalDebt")), number(info.get("totalCash"))
    revenue, fcf = number(info.get("totalRevenue")), number(info.get("freeCashflow"))
    dividend = number(info.get("dividendRate"))
    groups = groups_for(ticker, info.get("industryKey") or "")
    dates = sorted({date for key in ("earningsTimestamp", "earningsTimestampStart", "earningsTimestampEnd")
                    if (date := iso_time(info.get(key))) and age_days(date) < 0})
    result = {
        "ticker": ticker, "issuer": ISSUER_ALIASES.get(ticker, ticker),
        "company": info.get("longName") or info.get("shortName") or ticker,
        "industry": info.get("industry") or "", "industry_key": info.get("industryKey") or "",
        "sector": info.get("sector") or "", "business_groups": groups,
        "business_note": BUSINESS_NOTES.get(ticker, ""), "country": info.get("country") or "",
        "summary": info.get("longBusinessSummary") or "", "website": info.get("website") or "",
        "exchange": info.get("fullExchangeName") or info.get("exchange") or "",
        "currency": currency, "financial_currency": financial_currency,
        "observed_at": observed_at, "quote_at": iso_time(info.get("regularMarketTime")),
        "period_end": iso_time(info.get("mostRecentQuarter"))[:10],
        "source_url": f"https://finance.yahoo.com/quote/{ticker}/", "source": "Yahoo Finance",
        "price": price, "previous_close": previous, "change": change,
        "change_pct": divide(change, previous, 100), "market_cap": cap,
        "market_cap_usd": cap * fx[currency] if cap is not None and currency in fx else None,
        "revenue_usd": revenue * fx[financial_currency] if revenue is not None and financial_currency in fx else None,
        "total_revenue": revenue, "free_cashflow": fcf, "cash": cash, "debt": debt,
        "net_debt": debt - cash if debt is not None and cash is not None else None,
        "eps": eps, "forward_eps": forward_eps,
        "next_earnings": dates[0][:10] if dates else "", "next_earnings_end": dates[-1][:10] if dates else "",
        "earnings_estimated": True,  # Provider calendars are estimates until issuer-confirmed.
        "trailing_pe": number(info.get("trailingPE")) if eps is not None and eps > 0 else None,
        "forward_pe": number(info.get("forwardPE")) if forward_eps is not None and forward_eps > 0 else None,
        "price_to_book": number(info.get("priceToBook")) if equity is not None and equity > 0 else None,
        "debt_to_equity": number(info.get("debtToEquity")) / 100 if number(info.get("debtToEquity")) is not None and equity is not None and equity > 0 else None,
        "return_on_equity": number(info.get("returnOnEquity")) * 100 if number(info.get("returnOnEquity")) is not None and equity is not None and equity > 0 else None,
        "dividend_yield": divide(dividend, price, 100),
        "fcf_yield": divide(fcf, cap, 100) if currency == financial_currency and currency else None,
        "fcf_margin": divide(fcf, revenue, 100),
        "net_debt_ebitda": divide(debt - cash, info.get("ebitda")) if debt is not None and cash is not None else None,
        "ev_ebitda": number(info.get("enterpriseToEbitda")) if currency == financial_currency else None,
        "ev_sales": number(info.get("enterpriseToRevenue")) if currency == financial_currency else None,
        "current_ratio": number(info.get("currentRatio")), "beta": number(info.get("beta")),
        "analyst_target": number(info.get("targetMeanPrice")), "analyst_count": number(info.get("numberOfAnalystOpinions")),
        "week_52_low": number(info.get("fiftyTwoWeekLow")), "week_52_high": number(info.get("fiftyTwoWeekHigh")),
        "employees": number(info.get("fullTimeEmployees")),
    }
    for key, source in {"revenue_growth": "revenueGrowth", "earnings_growth": "earningsGrowth", "operating_margin": "operatingMargins", "gross_margin": "grossMargins", "profit_margin": "profitMargins", "payout_ratio": "payoutRatio"}.items():
        value = number(info.get(source))
        result[key] = value * 100 if value is not None else None
    for key in ("trailing_pe", "forward_pe", "ev_ebitda", "ev_sales", "price_to_book"):
        if result.get(key) is not None and result[key] <= 0:
            result[key] = None
    if "bank" in result["industry_key"] or "insurance" in result["industry_key"]:
        for key in ("gross_margin", "operating_margin", "fcf_yield", "fcf_margin", "net_debt_ebitda", "current_ratio", "debt_to_equity", "ev_ebitda", "ev_sales"):
            result[key] = None
    return result


def select_peers(ticker, profiles, now=None):
    primary = profiles.get(ticker, {})
    cap = number(primary.get("market_cap_usd"))
    if not cap or cap <= 0 or not primary.get("business_groups") or not 0 <= age_days(primary.get("observed_at"), now) <= MAX_PROFILE_AGE_DAYS or not 0 <= age_days(primary.get("quote_at"), now) <= MAX_PROFILE_AGE_DAYS:
        return [], []
    ranked = []
    for symbol, peer in profiles.items():
        if peer.get("issuer", symbol) == primary.get("issuer", ticker):
            continue
        overlap = set(primary["business_groups"]) & set(peer.get("business_groups", []))
        peer_cap = number(peer.get("market_cap_usd"))
        age = age_days(peer.get("observed_at"), now)
        date_gap = abs(age - age_days(primary.get("observed_at"), now))
        if not overlap or not peer_cap or peer_cap <= 0 or not 0 <= age <= MAX_PROFILE_AGE_DAYS or date_gap > 7 or not 0 <= age_days(peer.get("quote_at"), now) <= MAX_PROFILE_AGE_DAYS:
            continue
        ratio = peer_cap / cap
        size_band = 0 if .5 <= ratio <= 2 else 1 if 1 / 3 <= ratio <= 3 else 2
        revenue_ratio = divide(peer.get("revenue_usd"), primary.get("revenue_usd"))
        distance = abs(math.log(ratio)) + (.25 * abs(math.log(revenue_ratio)) if revenue_ratio and revenue_ratio > 0 else 0)
        group = sorted(overlap)[0]
        label = primary.get("industry") if group.startswith("industry:") else group
        ranked.append((size_band, distance, peer.get("country") != primary.get("country"), symbol, {
            **peer, "size_ratio": ratio, "role": "peer" if size_band < 2 else "benchmark",
            "reason": label, "size_band": "similar size" if size_band == 0 else "extended size range" if size_band == 1 else "larger industry benchmark" if ratio > 1 else "smaller industry benchmark",
        }))
    ranked.sort(key=lambda item: item[:4])
    seen = set()
    peers, benchmarks = [], []
    for _, _, _, symbol, peer in ranked:
        issuer = peer.get("issuer", symbol)
        if issuer in seen:
            continue
        seen.add(issuer)
        if peer["role"] == "peer" and len(peers) < 3:
            peers.append(peer)
        elif peer["role"] == "benchmark" and len(benchmarks) < 2:
            benchmarks.append(peer)
    return peers, benchmarks


# Label, unit, reporting basis, group. No universal investment winner colors.
METRICS = [
    ("market_cap_usd", "Market cap", "usd", "Market value, converted to USD", "overview"),
    ("forward_pe", "Forward P/E", "multiple", "Provider forward earnings estimate", "overview"),
    ("ev_ebitda", "EV / EBITDA", "multiple", "Enterprise value / trailing EBITDA", "overview"),
    ("revenue_growth", "Revenue growth", "percent", "Latest quarter vs prior-year quarter", "overview"),
    ("operating_margin", "Operating margin", "percent", "Latest reported quarter", "overview"),
    ("fcf_yield", "Free cash flow yield", "percent", "Trailing cash flow / market cap", "overview"),
    ("net_debt_ebitda", "Net debt / EBITDA", "multiple", "Net debt / trailing EBITDA", "overview"),
    ("debt_to_equity", "Debt / equity", "multiple", "Total debt / positive book equity", "overview"),
    ("trailing_pe", "Trailing P/E", "multiple", "Trailing twelve-month earnings", "valuation"),
    ("price_to_book", "Price / book", "multiple", "Price / positive book equity per share", "valuation"),
    ("ev_sales", "EV / sales", "multiple", "Enterprise value / trailing revenue", "valuation"),
    ("earnings_growth", "Earnings growth", "percent", "Latest quarter vs prior-year quarter", "quality"),
    ("gross_margin", "Gross margin", "percent", "Trailing twelve months", "quality"),
    ("profit_margin", "Net margin", "percent", "Trailing twelve months", "quality"),
    ("return_on_equity", "Return on equity", "percent", "Provider trailing return; positive equity only", "quality"),
    ("fcf_margin", "Free cash flow margin", "percent", "Trailing free cash flow / revenue", "quality"),
    ("dividend_yield", "Dividend yield", "percent", "Indicated annual dividend / price", "risk"),
    ("payout_ratio", "Earnings payout", "percent", "Provider trailing dividend payout", "risk"),
    ("current_ratio", "Current ratio", "multiple", "Current assets / current liabilities", "risk"),
    ("beta", "Beta", "number", "Provider historical market sensitivity", "risk"),
]


def format_value(value, kind="number", currency="USD"):
    n = number(value)
    if n is None:
        return "\u2014"
    if kind in ("usd", "money", "count"):
        prefix = "US$" if kind == "usd" else (currency + " ") if kind == "money" else ""
        for limit, suffix in ((1e12, "T"), (1e9, "B"), (1e6, "M"), (1e3, "K")):
            if abs(n) >= limit:
                return f"{prefix}{n / limit:,.2f}{suffix}"
        return f"{prefix}{n:,.2f}"
    if kind == "percent":
        return f"{n:,.1f}%"
    if kind == "multiple":
        return f"{n:,.2f}x"
    return f"{n:,.2f}"


def comparison(ticker, profiles, custom=None, now=None):
    primary = profiles.get(ticker, {"ticker": ticker, "company": ticker})
    peers, benchmarks = select_peers(ticker, profiles, now)
    if custom is not None:
        chosen = []
        seen = {primary.get("issuer", ticker)}
        for symbol in custom[:4]:
            profile = profiles.get(symbol)
            if not profile or profile.get("issuer", symbol) in seen:
                continue
            seen.add(profile.get("issuer", symbol))
            chosen.append({**profile, "role": "custom", "reason": "Selected by you", "size_band": "custom comparison", "size_ratio": divide(profile.get("market_cap_usd"), primary.get("market_cap_usd"))})
        columns = [{**primary, "role": "subject"}] + chosen
    else:
        columns = [{**primary, "role": "subject"}] + peers + benchmarks[:max(0, 3 - len(peers))]
    rows = []
    banking = "bank" in primary.get("industry_key", "") or "insurance" in primary.get("industry_key", "")
    for key, label, kind, basis, category in METRICS:
        values = [number(peer.get(key)) for peer in peers]
        values = [value for value in values if value is not None]
        peer_median = median(values) if len(values) >= 2 and custom is None else None
        if banking and key in {"price_to_book", "return_on_equity", "profit_margin", "trailing_pe"}:
            category = "overview"
        if banking and key in {"ev_ebitda", "operating_margin", "fcf_yield", "net_debt_ebitda", "debt_to_equity"}:
            continue
        rows.append({"key": key, "label": label, "kind": kind, "basis": basis, "group": category,
                     "values": [format_value(column.get(key), kind) for column in columns],
                     "median": format_value(peer_median, kind), "median_value": peer_median,
                     "median_count": len(values) if custom is None else 0})
    return {"columns": columns, "rows": rows, "peer_count": len(peers), "custom": custom is not None,
            "stale": age_days(primary.get("observed_at"), now) > MAX_PROFILE_AGE_DAYS,
            "has_median": any(row["median_value"] is not None for row in rows)}


def group_episode_archive(episodes):
    groups = OrderedDict()
    for episode in episodes:
        period = episode.get("quarter") or "Other research"
        # Undated/general reports must not all become one artificial earnings report.
        key = period if re.fullmatch(r"(?:Q[1-4]|H[12])\s+(?:FY)?\d{4}|FY\s?\d{4}", period) else episode.get("youtube_url") or episode.get("title")
        group = groups.setdefault(key, {"quarter": period, "title": episode.get("title"), "published_at": episode.get("published_at"), "editions": [], "shorts": [], "podcasts": [], "packets": []})
        if episode.get("youtube_url") and not any(e["url"] == episode["youtube_url"] for e in group["editions"]):
            group["editions"].append({"url": episode["youtube_url"], "title": episode.get("title"), "label": "Studio edition" if episode.get("studio_primary_youtube_url") or "studio edition" in (episode.get("title") or "").lower() else "Presentation", "published_at": episode.get("published_at")})
        for clip in episode.get("youtube_shorts", []):
            if not any(s["youtube_url"] == clip["youtube_url"] for s in group["shorts"]):
                group["shorts"].append(clip)
        for field, label in (("spotify_url", "Spotify"), ("podbean_url", "Podbean"), ("apple_url", "Apple Podcasts"), ("amazon_url", "Amazon Music"), ("iheart_url", "iHeartRadio"), ("google_url", "YouTube Music")):
            if episode.get(field) and not any(p["url"] == episode[field] for p in group["podcasts"]):
                group["podcasts"].append({"label": label, "url": episode[field]})
        packet = episode.get("research_packet")
        if packet and not any(p["slug"] == packet["slug"] for p in group["packets"]):
            group["packets"].append(packet)
    return list(groups.values())
