import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from yf_utils import (
    fetch_ticker_info as _fetch_ticker_info,
    normalize_div_yield,
    safe_float as _safe_float,
)


# ── Mutual Fund Universe ───────────────────────────────────────────────────
# Curated v1 universe focused on large, liquid, widely used retail mutual funds.
# Includes US core funds plus international mutual funds as a first-class slice.
MUTUAL_FUND_UNIVERSE = [
    # US Equity — core index / broad market
    "VFIAX", "FXAIX", "SWPPX", "VTSAX", "FSKAX", "SWTSX",
    # US Equity — active / growth / dividend
    "TRBCX", "FCNTX", "VIGAX", "VHYAX",
    # Allocation / balanced
    "VWELX", "VWINX", "FBALX", "PRWCX",
    # Bond
    "VBTLX", "FXNAX", "SWAGX", "VWEAX",
    # International Equity — broad / developed / emerging
    "VTIAX", "FTIHX", "FSPSX", "FSGGX", "SWISX", "VTMGX", "VEMAX", "FPADX",
    # International Equity — active
    "VWIGX", "VTRIX", "RERGX",
    # International Bond
    "VTABX", "FBIIX",
]
MUTUAL_FUND_UNIVERSE = list(dict.fromkeys(MUTUAL_FUND_UNIVERSE))


# ── Category mapping ───────────────────────────────────────────────────────
_CATEGORY_MAP = {
    # US Equity
    "VFIAX": "US Equity",
    "FXAIX": "US Equity",
    "SWPPX": "US Equity",
    "VTSAX": "US Equity",
    "FSKAX": "US Equity",
    "SWTSX": "US Equity",
    "TRBCX": "US Equity",
    "FCNTX": "US Equity",
    "VIGAX": "US Equity",
    "VHYAX": "US Equity",
    # Allocation
    "VWELX": "Allocation",
    "VWINX": "Allocation",
    "FBALX": "Allocation",
    "PRWCX": "Allocation",
    # Bond
    "VBTLX": "Bond",
    "FXNAX": "Bond",
    "SWAGX": "Bond",
    "VWEAX": "Bond",
    # International equity
    "VTIAX": "International Equity",
    "FTIHX": "International Equity",
    "FSPSX": "International Equity",
    "FSGGX": "International Equity",
    "SWISX": "International Equity",
    "VTMGX": "International Equity",
    "VEMAX": "International Equity",
    "FPADX": "International Equity",
    "VWIGX": "International Equity",
    "VTRIX": "International Equity",
    "RERGX": "International Equity",
    # International bond
    "VTABX": "International Bond",
    "FBIIX": "International Bond",
}

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


def _extract_morningstar_rating(info):
    for key in (
        "morningStarOverallRating",
        "morningStarRiskRating",
        "fundFamilyRating",
    ):
        value = info.get(key)
        if value is not None:
            try:
                return int(value)
            except (TypeError, ValueError):
                pass
    return None


def get_mutual_fund_data(symbol):
    """Fetch mutual fund data from yfinance .info."""
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

        ytd_return = _safe_float(info, "ytdReturn", 100)
        one_year_return = w52_perf
        three_year_return = _safe_float(info, "threeYearAverageReturn", 100)
        beta = _safe_float(info, "beta3Year") or _safe_float(info, "beta")
        morningstar_rating = _extract_morningstar_rating(info)

        change_pct = info.get("regularMarketChangePercent")
        if change_pct is None:
            previous_close = info.get("previousClose")
            if previous_close and float(previous_close) > 0:
                change_pct = round((current_price - float(previous_close)) / float(previous_close) * 100, 2)
            else:
                change_pct = 0
        else:
            change_pct = round(float(change_pct), 2)

        return {
            "symbol": symbol,
            "name": info.get("shortName") or info.get("longName") or symbol,
            "price": round(current_price, 2),
            "change_pct": change_pct,
            "expense_ratio": expense_ratio,
            "total_assets": total_assets,
            "dividend_yield": dividend_yield,
            "ytd_return": ytd_return,
            "one_year_return": one_year_return,
            "three_year_return": three_year_return,
            "category": get_mutual_fund_category(symbol),
            "asset_class": get_mutual_fund_asset_class(symbol),
            "volume": info.get("volume") or info.get("regularMarketVolume"),
            "avg_volume": info.get("averageVolume") or info.get("averageDailyVolume10Day"),
            "w52_high": w52_high,
            "w52_low": w52_low,
            "w52_perf": w52_perf,
            "w52_dist_high": w52_dist_high,
            "beta": beta,
            "morningstar_rating": morningstar_rating,
        }
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

    return True


def screen_mutual_funds(criteria, on_progress=None, on_match=None):
    tickers = list(MUTUAL_FUND_UNIVERSE)

    categories = criteria.get("categories")
    if categories:
        tickers = [symbol for symbol in tickers if _CATEGORY_MAP.get(symbol) in categories]

    asset_classes = criteria.get("asset_classes")
    if asset_classes:
        tickers = [symbol for symbol in tickers if _ASSET_CLASS_MAP.get(symbol) in asset_classes]

    total = len(tickers)
    processed = 0
    lock = threading.Lock()
    matches = []

    def process(symbol):
        nonlocal processed
        data = get_mutual_fund_data(symbol)
        with lock:
            processed += 1
            if on_progress:
                on_progress(processed, total)
            if data and _passes_criteria(data, criteria):
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
            "fund_family": info.get("fundFamily"),
            "morningstar_rating": _extract_morningstar_rating(info),
            "inception_date": None,
            "nav_price": _safe("navPrice"),
        }

        inception_raw = info.get("fundInceptionDate")
        if inception_raw:
            try:
                fund_info["inception_date"] = datetime.fromtimestamp(int(inception_raw)).strftime("%Y-%m-%d")
            except (TypeError, ValueError, OSError):
                pass

        w52_low = _safe("fiftyTwoWeekLow")
        if w52_low and current_price and w52_low > 0:
            fund_info["one_year_return"] = round((float(current_price) - w52_low) / w52_low * 100, 1)
        else:
            fund_info["one_year_return"] = None

        holdings = []
        try:
            fund_holdings = ticker.funds_data
            if fund_holdings:
                try:
                    top = fund_holdings.top_holdings
                    if top is not None and not top.empty:
                        for idx, row in top.head(10).iterrows():
                            name = str(idx)
                            weight = float(row.iloc[0]) * 100 if len(row) > 0 else 0
                            holdings.append({"name": name, "weight": round(weight, 2)})
                except Exception:
                    pass
        except Exception:
            pass

        sector_weights = []
        try:
            fund_holdings = ticker.funds_data
            if fund_holdings:
                try:
                    weights = fund_holdings.sector_weightings
                    if weights:
                        for item in weights:
                            if isinstance(item, dict):
                                for sector_name, weight in item.items():
                                    sector_weights.append({
                                        "sector": sector_name.replace("_", " ").title(),
                                        "weight": round(float(weight) * 100, 2),
                                    })
                except Exception:
                    pass
        except Exception:
            pass

        return {
            "info": fund_info,
            "holdings": holdings,
            "sector_weights": sector_weights,
        }
    except Exception as exc:
        print(f"Error in get_mutual_fund_detail for {symbol}: {exc}")
        return None
