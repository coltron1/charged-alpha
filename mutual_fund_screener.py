import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from yf_utils import (
    fetch_ticker_info as _fetch_ticker_info,
    normalize_div_yield,
    safe_float as _safe_float,
)


# ── Curated mutual fund universe ───────────────────────────────────────────
# Broadened to cover the styles investors commonly screen for:
# low-cost core, active growth, value/dividend, balanced allocation,
# taxable/defensive bonds, and international diversification.
FUND_CATALOG = {
    # US equity — core index
    "VFIAX": {"category": "US Equity", "strategy_focus": "Large Blend", "region_focus": "US", "management_style": "Index"},
    "FXAIX": {"category": "US Equity", "strategy_focus": "Large Blend", "region_focus": "US", "management_style": "Index"},
    "SWPPX": {"category": "US Equity", "strategy_focus": "Large Blend", "region_focus": "US", "management_style": "Index"},
    "VTSAX": {"category": "US Equity", "strategy_focus": "Total Market", "region_focus": "US", "management_style": "Index"},
    "FSKAX": {"category": "US Equity", "strategy_focus": "Total Market", "region_focus": "US", "management_style": "Index"},
    "SWTSX": {"category": "US Equity", "strategy_focus": "Total Market", "region_focus": "US", "management_style": "Index"},
    "VIGAX": {"category": "US Equity", "strategy_focus": "Growth", "region_focus": "US", "management_style": "Index"},
    "VVIAX": {"category": "US Equity", "strategy_focus": "Value", "region_focus": "US", "management_style": "Index"},
    "VIMAX": {"category": "US Equity", "strategy_focus": "Mid Cap", "region_focus": "US", "management_style": "Index"},
    "VMGMX": {"category": "US Equity", "strategy_focus": "Mid-Cap Growth", "region_focus": "US", "management_style": "Index"},
    "VSIAX": {"category": "US Equity", "strategy_focus": "Small-Cap Value", "region_focus": "US", "management_style": "Index"},
    "VDIGX": {"category": "US Equity", "strategy_focus": "Dividend Growth", "region_focus": "US", "management_style": "Active"},
    "VEIPX": {"category": "US Equity", "strategy_focus": "Income", "region_focus": "US", "management_style": "Active"},
    "VHYAX": {"category": "US Equity", "strategy_focus": "High Dividend", "region_focus": "US", "management_style": "Index"},
    # US equity — active
    "TRBCX": {"category": "US Equity", "strategy_focus": "Large Growth", "region_focus": "US", "management_style": "Active"},
    "FCNTX": {"category": "US Equity", "strategy_focus": "Large Blend", "region_focus": "US", "management_style": "Active"},
    "PRGFX": {"category": "US Equity", "strategy_focus": "Growth", "region_focus": "US", "management_style": "Active"},
    "AMCPX": {"category": "US Equity", "strategy_focus": "Growth", "region_focus": "US", "management_style": "Active"},
    "ANCFX": {"category": "US Equity", "strategy_focus": "Large Blend", "region_focus": "US", "management_style": "Active"},
    "DODGX": {"category": "US Equity", "strategy_focus": "Value", "region_focus": "US", "management_style": "Active"},
    # Allocation / balanced
    "VWELX": {"category": "Allocation", "strategy_focus": "Balanced", "region_focus": "US", "management_style": "Active"},
    "VWINX": {"category": "Allocation", "strategy_focus": "Conservative Allocation", "region_focus": "US", "management_style": "Active"},
    "FBALX": {"category": "Allocation", "strategy_focus": "Balanced", "region_focus": "US", "management_style": "Active"},
    "PRWCX": {"category": "Allocation", "strategy_focus": "Moderate Allocation", "region_focus": "US", "management_style": "Active"},
    "VASGX": {"category": "Allocation", "strategy_focus": "LifeStrategy Growth", "region_focus": "Global", "management_style": "Index"},
    "VSMGX": {"category": "Allocation", "strategy_focus": "LifeStrategy Moderate", "region_focus": "Global", "management_style": "Index"},
    "VSCGX": {"category": "Allocation", "strategy_focus": "LifeStrategy Conservative", "region_focus": "Global", "management_style": "Index"},
    "TRRIX": {"category": "Allocation", "strategy_focus": "Retirement Income", "region_focus": "Global", "management_style": "Active"},
    # Bonds
    "VBTLX": {"category": "Bond", "strategy_focus": "Core Bond", "region_focus": "US", "management_style": "Index"},
    "FXNAX": {"category": "Bond", "strategy_focus": "Core Bond", "region_focus": "US", "management_style": "Index"},
    "SWAGX": {"category": "Bond", "strategy_focus": "Core Bond", "region_focus": "US", "management_style": "Index"},
    "VBIRX": {"category": "Bond", "strategy_focus": "Short-Term Bond", "region_focus": "US", "management_style": "Index"},
    "VFSTX": {"category": "Bond", "strategy_focus": "Short-Term Investment-Grade", "region_focus": "US", "management_style": "Active"},
    "VWEAX": {"category": "Bond", "strategy_focus": "High Yield Bond", "region_focus": "US", "management_style": "Active"},
    "DODIX": {"category": "Bond", "strategy_focus": "Intermediate Bond", "region_focus": "US", "management_style": "Active"},
    "PTTRX": {"category": "Bond", "strategy_focus": "Total Return Bond", "region_focus": "US", "management_style": "Active"},
    "VWIUX": {"category": "Bond", "strategy_focus": "Intermediate Tax-Exempt", "region_focus": "US", "management_style": "Active"},
    # International equity — broad / developed / emerging
    "VTIAX": {"category": "International Equity", "strategy_focus": "Total International", "region_focus": "International", "management_style": "Index"},
    "FTIHX": {"category": "International Equity", "strategy_focus": "Total International", "region_focus": "International", "management_style": "Index"},
    "FSPSX": {"category": "International Equity", "strategy_focus": "Developed Markets", "region_focus": "International", "management_style": "Index"},
    "FSGGX": {"category": "International Equity", "strategy_focus": "Global ex US", "region_focus": "International", "management_style": "Index"},
    "SWISX": {"category": "International Equity", "strategy_focus": "International Developed", "region_focus": "International", "management_style": "Index"},
    "VTMGX": {"category": "International Equity", "strategy_focus": "Developed Markets", "region_focus": "International", "management_style": "Index"},
    "VEMAX": {"category": "International Equity", "strategy_focus": "Emerging Markets", "region_focus": "International", "management_style": "Index"},
    "FPADX": {"category": "International Equity", "strategy_focus": "Emerging Markets", "region_focus": "International", "management_style": "Index"},
    # International equity — active
    "VWIGX": {"category": "International Equity", "strategy_focus": "International Growth", "region_focus": "International", "management_style": "Active"},
    "VTRIX": {"category": "International Equity", "strategy_focus": "International Value", "region_focus": "International", "management_style": "Active"},
    "RERGX": {"category": "International Equity", "strategy_focus": "Global Real Estate", "region_focus": "Global", "management_style": "Active"},
    "DODFX": {"category": "International Equity", "strategy_focus": "International Value", "region_focus": "International", "management_style": "Active"},
    "NEWFX": {"category": "International Equity", "strategy_focus": "World Allocation", "region_focus": "Global", "management_style": "Active"},
    # International bonds
    "VTABX": {"category": "International Bond", "strategy_focus": "International Core Bond", "region_focus": "International", "management_style": "Index"},
    "FBIIX": {"category": "International Bond", "strategy_focus": "International Bond", "region_focus": "International", "management_style": "Active"},
}

MUTUAL_FUND_UNIVERSE = list(FUND_CATALOG.keys())
_CATEGORY_MAP = {symbol: meta["category"] for symbol, meta in FUND_CATALOG.items()}

_ASSET_CLASS_MAP = {}
for symbol, category in _CATEGORY_MAP.items():
    if category in {"Bond", "International Bond"}:
        _ASSET_CLASS_MAP[symbol] = "Bonds"
    elif category == "Allocation":
        _ASSET_CLASS_MAP[symbol] = "Mixed"
    else:
        _ASSET_CLASS_MAP[symbol] = "Stocks"


# ── Helpers ────────────────────────────────────────────────────────────────
def get_mutual_fund_category(symbol):
    return _CATEGORY_MAP.get(symbol, "US Equity")


def get_mutual_fund_asset_class(symbol):
    return _ASSET_CLASS_MAP.get(symbol, "Stocks")


def get_mutual_fund_strategy_focus(symbol):
    return FUND_CATALOG.get(symbol, {}).get("strategy_focus", get_mutual_fund_category(symbol))


def get_mutual_fund_region_focus(symbol):
    return FUND_CATALOG.get(symbol, {}).get("region_focus", "US")


def get_mutual_fund_catalog_rows():
    rows = []
    for symbol in MUTUAL_FUND_UNIVERSE:
        name = symbol
        try:
            _, info = _fetch_ticker_info(symbol)
            if info:
                name = info.get("longName") or info.get("shortName") or symbol
        except Exception:
            name = symbol
        rows.append(
            {
                "symbol": symbol,
                "name": name,
                "category": get_mutual_fund_category(symbol),
                "asset_class": get_mutual_fund_asset_class(symbol),
                "strategy_focus": get_mutual_fund_strategy_focus(symbol),
                "region_focus": get_mutual_fund_region_focus(symbol),
                "management_style": FUND_CATALOG.get(symbol, {}).get("management_style"),
            }
        )
    return rows


def _normalize_query(value):
    return " ".join((value or "").lower().split())


def _query_matches(symbol, name, query):
    q = _normalize_query(query)
    if not q:
        return True
    haystacks = [
        _normalize_query(symbol),
        _normalize_query(name),
        _normalize_query(f"{symbol} {name or ''}"),
    ]
    return any(q in hay for hay in haystacks)


def _clean_description(text):
    if not text:
        return None
    cleaned = " ".join(str(text).split())
    return cleaned or None


def _build_operation_highlights(info, description, asset_mix):
    highlights = []
    strategy_focus = get_mutual_fund_strategy_focus(info.get("symbol"))
    morningstar_category = info.get("morningstar_category")
    region_focus = get_mutual_fund_region_focus(info.get("symbol"))
    management_style = info.get("management_style")

    if strategy_focus or morningstar_category:
        if morningstar_category and strategy_focus and morningstar_category != strategy_focus:
            highlights.append(
                f"Coverage: focuses on {strategy_focus.lower()} exposure and currently sits in the {morningstar_category} Morningstar category."
            )
        else:
            highlights.append(
                f"Coverage: focused on {((morningstar_category or strategy_focus) or 'its stated target market').lower()} exposure."
            )

    if management_style == "Index":
        highlights.append(
            f"Approach: uses an index-based process aimed at broad {region_focus.lower()} market exposure rather than manager stock picking."
        )
    elif management_style == "Active":
        highlights.append(
            "Approach: actively managed, so portfolio construction depends more on manager decisions, security selection, and trading discipline."
        )

    stock_position = asset_mix.get("stock_position")
    bond_position = asset_mix.get("bond_position")
    cash_position = asset_mix.get("cash_position")
    asset_parts = []
    if stock_position is not None and stock_position > 0:
        asset_parts.append(f"{stock_position:.1f}% stocks")
    if bond_position is not None and bond_position > 0:
        asset_parts.append(f"{bond_position:.1f}% bonds")
    if cash_position is not None and cash_position > 0:
        asset_parts.append(f"{cash_position:.1f}% cash")
    if asset_parts:
        highlights.append(f"Current mix: {' / '.join(asset_parts)}.")

    turnover = info.get("turnover_pct")
    if turnover is not None:
        if turnover <= 25:
            turnover_note = "fairly low turnover, which usually suggests a steadier long-term process"
        elif turnover <= 75:
            turnover_note = "moderate turnover, which suggests some active repositioning but not constant reshuffling"
        else:
            turnover_note = "high turnover, which suggests a more active trading approach and potentially higher hidden trading costs"
        highlights.append(f"Trading style: {turnover:.0f}% annual turnover, implying {turnover_note}.")

    if description and "index" in description.lower() and management_style != "Index":
        highlights.append("Fund text references index tracking language, even though the broader metadata did not cleanly classify it as passive.")

    return highlights


def _extract_morningstar_rating(info):
    for key in ("morningStarOverallRating", "fundFamilyRating"):
        value = info.get(key)
        if value is not None:
            try:
                return int(value)
            except (TypeError, ValueError):
                pass
    return None


def _extract_morningstar_risk(info):
    value = info.get("morningStarRiskRating")
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _infer_management_style(symbol, name=None, description=None):
    explicit = FUND_CATALOG.get(symbol, {}).get("management_style")
    if explicit:
        return explicit
    text = f"{name or ''} {description or ''}".lower()
    if any(
        phrase in text
        for phrase in (
            "index",
            "indexing investment approach",
            "track the performance",
            "tracks the performance",
            "replicate the target index",
            "target index",
        )
    ):
        return "Index"
    return "Active"


def _extract_inception(info):
    raw = info.get("fundInceptionDate")
    if not raw:
        return None, None
    try:
        dt = datetime.fromtimestamp(int(raw))
        years = round((datetime.now() - dt).days / 365.25, 1)
        return dt.strftime("%Y-%m-%d"), years
    except (TypeError, ValueError, OSError):
        return None, None


def _extract_asset_mix(funds_data):
    try:
        data = getattr(funds_data, "asset_classes", None) or {}
    except Exception:
        data = {}

    def _pct(key):
        value = data.get(key)
        if value is None:
            return None
        try:
            return round(float(value) * 100, 2)
        except (TypeError, ValueError):
            return None

    return {
        "stock_position": _pct("stockPosition"),
        "bond_position": _pct("bondPosition"),
        "cash_position": _pct("cashPosition"),
        "preferred_position": _pct("preferredPosition"),
        "convertible_position": _pct("convertiblePosition"),
        "other_position": _pct("otherPosition"),
    }


def _to_float(value, scale=1, decimals=2):
    if value is None:
        return None
    try:
        return round(float(value) * scale, decimals)
    except (TypeError, ValueError):
        return None


def _extract_table_rows(df, labels, percent_labels=None):
    if percent_labels is None:
        percent_labels = set()
    try:
        if df is None or df.empty:
            return []
    except Exception:
        return []

    rows = []
    columns = list(df.columns)
    fund_column = columns[0] if columns else None
    avg_column = columns[1] if len(columns) > 1 else None

    for label in labels:
        try:
            if label not in df.index:
                continue
            row = df.loc[label]
            fund_value = row[fund_column] if fund_column is not None else None
            avg_value = row[avg_column] if avg_column is not None else None
            rows.append(
                {
                    "label": label,
                    "fund": _to_float(fund_value, 100 if label in percent_labels else 1, 2),
                    "category_avg": _to_float(avg_value, 100 if label in percent_labels else 1, 2),
                    "is_percent": label in percent_labels,
                }
            )
        except Exception:
            continue
    return rows


def _extract_bond_ratings(funds_data):
    try:
        ratings = getattr(funds_data, "bond_ratings", None) or {}
    except Exception:
        ratings = {}

    items = []
    for label, value in ratings.items():
        if value is None:
            continue
        try:
            pct = round(float(value) * 100, 2)
        except (TypeError, ValueError):
            continue
        if pct <= 0:
            continue
        nice_label = label.replace("_", " ").upper()
        items.append({"label": nice_label, "weight": pct})

    items.sort(key=lambda item: item["weight"], reverse=True)
    return items


def _extract_sector_weights(funds_data):
    sectors = []
    try:
        weights = getattr(funds_data, "sector_weightings", None)
        if isinstance(weights, dict):
            weights = [weights]
        if weights:
            for item in weights:
                if isinstance(item, dict):
                    for sector_name, weight in item.items():
                        sectors.append(
                            {
                                "sector": sector_name.replace("_", " ").title(),
                                "weight": round(float(weight) * 100, 2),
                            }
                        )
    except Exception:
        return []
    return sorted(sectors, key=lambda item: item["weight"], reverse=True)


def _extract_holdings(funds_data, limit=10):
    holdings = []
    try:
        top = getattr(funds_data, "top_holdings", None)
        if top is not None and not top.empty:
            for idx, row in top.head(limit).iterrows():
                name = row.get("Name") if hasattr(row, "get") else None
                if not name:
                    name = str(idx)
                weight = row.get("Holding Percent") if hasattr(row, "get") else None
                if weight is None and len(row) > 0:
                    weight = row.iloc[-1]
                try:
                    holdings.append({"name": str(name), "weight": round(float(weight) * 100, 2)})
                except (TypeError, ValueError):
                    continue
    except Exception:
        return []
    return holdings


def _get_funds_data(ticker):
    try:
        return ticker.funds_data
    except Exception:
        return None


def get_mutual_fund_data(symbol, include_portfolio=False):
    """Fetch mutual fund data for screener rows."""
    ticker, info = _fetch_ticker_info(symbol)
    if ticker is None or info is None:
        return None

    try:
        current_price = (
            info.get("currentPrice")
            or info.get("regularMarketPrice")
            or info.get("previousClose")
            or info.get("navPrice")
        )
        if current_price is None:
            return None
        current_price = float(current_price)

        expense_ratio_raw = (
            info.get("netExpenseRatio")
            or info.get("annualReportExpenseRatio")
            or info.get("expenseRatio")
        )
        expense_ratio = None
        if expense_ratio_raw is not None:
            try:
                value = float(expense_ratio_raw)
                expense_ratio = round(value, 4) if info.get("netExpenseRatio") is not None else round(value * 100, 4)
            except (TypeError, ValueError):
                pass

        total_assets = info.get("totalAssets")
        if total_assets is not None:
            try:
                total_assets = int(total_assets)
            except (TypeError, ValueError):
                total_assets = None

        raw_trailing = info.get("trailingAnnualDividendYield")
        if raw_trailing:
            dividend_yield = round(float(raw_trailing) * 100, 2)
        else:
            dividend_yield = normalize_div_yield(info.get("yield") or info.get("dividendYield"))

        w52_high = _safe_float(info, "fiftyTwoWeekHigh")
        w52_low = _safe_float(info, "fiftyTwoWeekLow")
        w52_perf = None
        w52_dist_high = None
        if w52_high and w52_low and current_price:
            if w52_low > 0:
                w52_perf = round((current_price - w52_low) / w52_low * 100, 1)
            if w52_high > 0:
                w52_dist_high = round((w52_high - current_price) / w52_high * 100, 1)

        change_pct = info.get("regularMarketChangePercent")
        if change_pct is None:
            previous_close = info.get("previousClose")
            if previous_close and float(previous_close) > 0:
                change_pct = round((current_price - float(previous_close)) / float(previous_close) * 100, 2)
            else:
                change_pct = 0
        else:
            change_pct = round(float(change_pct), 2)

        inception_date, fund_age_years = _extract_inception(info)
        turnover_pct = _safe_float(info, "annualHoldingsTurnover", 100)

        data = {
            "symbol": symbol,
            "name": info.get("shortName") or info.get("longName") or symbol,
            "price": round(current_price, 2),
            "change_pct": change_pct,
            "expense_ratio": expense_ratio,
            "total_assets": total_assets,
            "dividend_yield": dividend_yield,
            "ytd_return": _safe_float(info, "ytdReturn", 100),
            "one_year_return": w52_perf,
            "three_year_return": _safe_float(info, "threeYearAverageReturn", 100),
            "five_year_return": _safe_float(info, "fiveYearAverageReturn", 100),
            "category": get_mutual_fund_category(symbol),
            "asset_class": get_mutual_fund_asset_class(symbol),
            "strategy_focus": get_mutual_fund_strategy_focus(symbol),
            "region_focus": get_mutual_fund_region_focus(symbol),
            "management_style": _infer_management_style(symbol, info.get("longName") or info.get("shortName")),
            "volume": info.get("volume") or info.get("regularMarketVolume"),
            "avg_volume": info.get("averageVolume") or info.get("averageDailyVolume10Day"),
            "w52_high": w52_high,
            "w52_low": w52_low,
            "w52_perf": w52_perf,
            "w52_dist_high": w52_dist_high,
            "beta": _safe_float(info, "beta3Year") or _safe_float(info, "beta"),
            "morningstar_rating": _extract_morningstar_rating(info),
            "morningstar_risk": _extract_morningstar_risk(info),
            "turnover_pct": turnover_pct,
            "fund_age_years": fund_age_years,
            "inception_date": inception_date,
            "fund_family": info.get("fundFamily"),
        }

        if include_portfolio:
            asset_mix = _extract_asset_mix(_get_funds_data(ticker))
            data.update(asset_mix)

        return data
    except Exception as exc:
        print(f"Error processing mutual fund {symbol}: {exc}")
        return None


def _passes_criteria(fund, criteria):
    categories = criteria.get("categories")
    if categories and fund.get("category") not in categories:
        return False

    asset_classes = criteria.get("asset_classes")
    if asset_classes and fund.get("asset_class") not in asset_classes:
        return False

    management_styles = criteria.get("management_styles")
    if management_styles and fund.get("management_style") not in management_styles:
        return False

    max_expense = criteria.get("max_expense_ratio")
    if max_expense is not None:
        value = fund.get("expense_ratio")
        if value is None or value > max_expense:
            return False

    min_aum = criteria.get("min_aum")
    if min_aum is not None:
        value = fund.get("total_assets")
        if value is None or value < min_aum:
            return False

    min_div = criteria.get("min_div_yield")
    if min_div is not None:
        value = fund.get("dividend_yield")
        if value is None or value < min_div:
            return False

    max_div = criteria.get("max_div_yield")
    if max_div is not None:
        value = fund.get("dividend_yield")
        if value is not None and value > max_div:
            return False

    min_ytd = criteria.get("min_ytd_return")
    if min_ytd is not None:
        value = fund.get("ytd_return")
        if value is None or value < min_ytd:
            return False

    min_1y = criteria.get("min_1y_return")
    if min_1y is not None:
        value = fund.get("one_year_return")
        if value is None or value < min_1y:
            return False

    min_3y = criteria.get("min_3y_return")
    if min_3y is not None:
        value = fund.get("three_year_return")
        if value is None or value < min_3y:
            return False

    min_vol = criteria.get("min_avg_volume")
    if min_vol is not None:
        value = fund.get("avg_volume")
        if value is None or value < min_vol:
            return False

    min_w52 = criteria.get("min_w52_perf")
    if min_w52 is not None:
        value = fund.get("w52_perf")
        if value is None or value < min_w52:
            return False

    max_w52 = criteria.get("max_w52_perf")
    if max_w52 is not None:
        value = fund.get("w52_perf")
        if value is not None and value > max_w52:
            return False

    max_dist_high = criteria.get("max_w52_dist_high")
    if max_dist_high is not None:
        value = fund.get("w52_dist_high")
        if value is None or value > max_dist_high:
            return False

    min_rating = criteria.get("min_morningstar_rating")
    if min_rating is not None:
        value = fund.get("morningstar_rating")
        if value is None or value < min_rating:
            return False

    max_risk = criteria.get("max_morningstar_risk")
    if max_risk is not None:
        value = fund.get("morningstar_risk")
        if value is None or value > max_risk:
            return False

    max_beta = criteria.get("max_beta")
    if max_beta is not None:
        value = fund.get("beta")
        if value is None or value > max_beta:
            return False

    max_turnover = criteria.get("max_turnover_pct")
    if max_turnover is not None:
        value = fund.get("turnover_pct")
        if value is None or value > max_turnover:
            return False

    min_years = criteria.get("min_years_history")
    if min_years is not None:
        value = fund.get("fund_age_years")
        if value is None or value < min_years:
            return False

    min_stock = criteria.get("min_stock_position")
    if min_stock is not None:
        value = fund.get("stock_position")
        if value is None or value < min_stock:
            return False

    min_bond = criteria.get("min_bond_position")
    if min_bond is not None:
        value = fund.get("bond_position")
        if value is None or value < min_bond:
            return False

    max_cash = criteria.get("max_cash_position")
    if max_cash is not None:
        value = fund.get("cash_position")
        if value is None or value > max_cash:
            return False

    return True


def screen_mutual_funds(criteria, on_progress=None, on_match=None):
    tickers = list(MUTUAL_FUND_UNIVERSE)

    query = criteria.get("query")

    categories = criteria.get("categories")
    if categories:
        tickers = [symbol for symbol in tickers if _CATEGORY_MAP.get(symbol) in categories]

    asset_classes = criteria.get("asset_classes")
    if asset_classes:
        tickers = [symbol for symbol in tickers if _ASSET_CLASS_MAP.get(symbol) in asset_classes]

    management_styles = criteria.get("management_styles")
    if management_styles:
        tickers = [
            symbol
            for symbol in tickers
            if FUND_CATALOG.get(symbol, {}).get("management_style") in management_styles
        ]

    needs_portfolio = any(
        criteria.get(key) is not None
        for key in ("min_stock_position", "min_bond_position", "max_cash_position")
    )

    total = len(tickers)
    processed = 0
    lock = threading.Lock()
    matches = []

    def process(symbol):
        nonlocal processed
        data = get_mutual_fund_data(symbol, include_portfolio=needs_portfolio)
        with lock:
            processed += 1
            if on_progress:
                on_progress(processed, total)
            if data and _query_matches(symbol, data.get("name"), query) and _passes_criteria(data, criteria):
                matches.append(data)
                if on_match:
                    on_match(data)

    with ThreadPoolExecutor(max_workers=12) as executor:
        list(executor.map(process, tickers))

    return matches


def get_mutual_fund_detail(symbol):
    ticker, info = _fetch_ticker_info(symbol)
    if ticker is None or info is None:
        return None

    try:
        def _safe(key, scale=1, decimals=2):
            value = info.get(key)
            if value is None:
                return None
            try:
                return round(float(value) * scale, decimals)
            except (TypeError, ValueError):
                return None

        current_price = (
            info.get("currentPrice")
            or info.get("regularMarketPrice")
            or info.get("previousClose")
            or info.get("navPrice")
        )

        expense_ratio_raw = (
            info.get("netExpenseRatio")
            or info.get("annualReportExpenseRatio")
            or info.get("expenseRatio")
        )
        expense_ratio = None
        if expense_ratio_raw is not None:
            try:
                raw_value = float(expense_ratio_raw)
                expense_ratio = round(raw_value, 4) if info.get("netExpenseRatio") is not None else round(raw_value * 100, 4)
            except (TypeError, ValueError):
                pass

        raw_trailing = info.get("trailingAnnualDividendYield")
        if raw_trailing:
            dividend_yield = round(float(raw_trailing) * 100, 2)
        else:
            dividend_yield = normalize_div_yield(info.get("yield") or info.get("dividendYield"))

        funds_data = _get_funds_data(ticker)
        overview = {}
        description = None
        if funds_data is not None:
            try:
                overview = getattr(funds_data, "fund_overview", None) or {}
            except Exception:
                overview = {}
            try:
                description = _clean_description(getattr(funds_data, "description", None))
            except Exception:
                description = None

        inception_date, fund_age_years = _extract_inception(info)
        asset_mix = _extract_asset_mix(funds_data)

        fund_info = {
            "symbol": symbol,
            "name": info.get("longName") or info.get("shortName") or symbol,
            "price": _safe("currentPrice") or _safe("regularMarketPrice") or _safe("previousClose") or _safe("navPrice"),
            "previous_close": _safe("previousClose"),
            "open": _safe("regularMarketOpen"),
            "day_high": _safe("dayHigh") or _safe("regularMarketDayHigh"),
            "day_low": _safe("dayLow") or _safe("regularMarketDayLow"),
            "change": _safe("regularMarketChange"),
            "change_pct": _safe("regularMarketChangePercent"),
            "week_52_high": _safe("fiftyTwoWeekHigh"),
            "week_52_low": _safe("fiftyTwoWeekLow"),
            "expense_ratio": expense_ratio,
            "total_assets": info.get("totalAssets"),
            "dividend_yield": dividend_yield,
            "ytd_return": _safe("ytdReturn", 100),
            "three_year_return": _safe("threeYearAverageReturn", 100),
            "five_year_return": _safe("fiveYearAverageReturn", 100),
            "beta": _safe("beta3Year") or _safe("beta"),
            "volume": info.get("volume") or info.get("regularMarketVolume"),
            "avg_volume": info.get("averageVolume") or info.get("averageDailyVolume10Day"),
            "category": get_mutual_fund_category(symbol),
            "asset_class": get_mutual_fund_asset_class(symbol),
            "strategy_focus": get_mutual_fund_strategy_focus(symbol),
            "region_focus": get_mutual_fund_region_focus(symbol),
            "fund_family": overview.get("family") or info.get("fundFamily"),
            "morningstar_rating": _extract_morningstar_rating(info),
            "morningstar_risk": _extract_morningstar_risk(info),
            "morningstar_category": overview.get("categoryName"),
            "legal_type": overview.get("legalType"),
            "management_style": _infer_management_style(symbol, info.get("longName") or info.get("shortName"), description),
            "inception_date": inception_date,
            "fund_age_years": fund_age_years,
            "nav_price": _safe("navPrice"),
            "turnover_pct": _safe("annualHoldingsTurnover", 100),
            "last_cap_gain": _safe("lastCapGain"),
            **asset_mix,
        }

        w52_low = _safe("fiftyTwoWeekLow")
        if w52_low and current_price and w52_low > 0:
            fund_info["one_year_return"] = round((float(current_price) - w52_low) / w52_low * 100, 1)
        else:
            fund_info["one_year_return"] = None

        bond_stats = []
        if funds_data is not None:
            try:
                bond_stats = _extract_table_rows(
                    getattr(funds_data, "bond_holdings", None),
                    ["Duration", "Maturity", "Credit Quality"],
                )
            except Exception:
                bond_stats = []

        operation_highlights = _build_operation_highlights(fund_info, description, asset_mix)

        return {
            "info": fund_info,
            "description": description,
            "description_source": "Yahoo Finance fund profile / fund-provided text when available",
            "operation_highlights": operation_highlights,
            "holdings": _extract_holdings(funds_data, limit=10),
            "sector_weights": _extract_sector_weights(funds_data),
            "bond_stats": bond_stats,
            "bond_ratings": _extract_bond_ratings(funds_data),
            "asset_mix": [
                {"label": "Stocks", "weight": fund_info.get("stock_position")},
                {"label": "Bonds", "weight": fund_info.get("bond_position")},
                {"label": "Cash", "weight": fund_info.get("cash_position")},
                {"label": "Preferred", "weight": fund_info.get("preferred_position")},
                {"label": "Other", "weight": fund_info.get("other_position")},
            ],
        }
    except Exception as exc:
        print(f"Error in get_mutual_fund_detail for {symbol}: {exc}")
        return None
