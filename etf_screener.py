import time
import yfinance as yf
import pandas as pd
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta


# ── ETF Universe ────────────────────────────────────────────────────────────
# ~200 of the most traded US ETFs across all major categories
ETF_UNIVERSE = [
    # Broad Market
    "SPY", "QQQ", "VTI", "VOO", "IVV", "DIA", "RSP", "SPLG", "SCHB", "ITOT",
    "VV", "MGC", "SPTM", "SCHX", "IWB",
    # Nasdaq / Growth
    "QQQM", "VUG", "IWF", "SPYG", "SCHG", "MGK", "RPG", "VONG", "IWY",
    # S&P 500 Value
    "SPYV", "VOOV", "IVE", "SCHV", "VTV", "IUSV", "RPV", "VONV",
    # Small Cap
    "IWM", "VB", "IJR", "SCHA", "VBR", "VBK", "IWO", "IWN", "SLYV", "SLYG",
    # Mid Cap
    "MDY", "IJH", "VO", "SCHM", "IWR", "IWS", "IWP", "IVOO",
    # Sector - Technology
    "XLK", "VGT", "FTEC", "IGV", "SMH", "SOXX", "QQQ",
    # Sector - Financials
    "XLF", "VFH", "KRE", "KBE", "IAI",
    # Sector - Health Care
    "XLV", "VHT", "IBB", "XBI", "IHI",
    # Sector - Energy
    "XLE", "VDE", "OIH", "XOP", "AMLP",
    # Sector - Consumer Discretionary
    "XLY", "VCR", "FDIS",
    # Sector - Consumer Staples
    "XLP", "VDC", "FSTA",
    # Sector - Industrials
    "XLI", "VIS", "ITA",
    # Sector - Materials
    "XLB", "VAW",
    # Sector - Utilities
    "XLU", "VPU",
    # Sector - Real Estate
    "VNQ", "XLRE", "IYR", "SCHH", "RWR",
    # Sector - Communication Services
    "XLC", "VOX",
    # Bonds - Total Market
    "AGG", "BND", "SCHZ", "FBND",
    # Bonds - Treasury
    "TLT", "IEF", "SHY", "SHV", "GOVT", "VGSH", "VGIT", "VGLT", "TIP", "STIP",
    "BIL", "SGOV",
    # Bonds - Corporate
    "LQD", "VCIT", "VCSH", "IGSB", "IGIB",
    # Bonds - High Yield
    "HYG", "JNK", "SHYG", "USHY",
    # Bonds - Municipal
    "MUB", "VTEB", "TFI",
    # Bonds - International
    "BNDX", "IAGG", "EMB",
    # International - Developed
    "EFA", "VEA", "IEFA", "SCHF", "SPDW", "VGK", "EWJ", "EWG", "EWU",
    # International - Emerging
    "VWO", "EEM", "IEMG", "SCHE",
    # International - Total
    "VXUS", "IXUS", "ACWI", "VT",
    # International - Regional
    "FXI", "MCHI", "EWZ", "INDA", "EWT", "EWY",
    # Commodities
    "GLD", "IAU", "SLV", "GDX", "GDXJ", "USO", "DBC", "PDBC", "GSG",
    # Thematic / Innovation
    "ARKK", "ARKW", "ARKG", "ARKF", "HACK", "CIBR", "BOTZ", "ROBO",
    "TAN", "ICLN", "QCLN", "LIT", "DRIV",
    # Dividend / Income
    "VYM", "SCHD", "DVY", "HDV", "DGRO", "VIG", "NOBL", "SDY", "SPHD", "SPYD",
    "DIVO", "JEPI", "JEPQ",
    # Covered Call / Income Strategy
    "XYLD", "QYLD", "RYLD",
    # Multi-Asset / Balanced
    "AOR", "AOA", "AOM", "AOK",
    # Volatility / Hedging
    "VIXY",
    # Leveraged (popular ones)
    "TQQQ", "SQQQ", "SPXL", "SPXS", "UPRO",
    # Crypto-related
    "BITO",
    # ESG
    "ESGU", "ESGV", "SUSA",
]

# Deduplicate
ETF_UNIVERSE = list(dict.fromkeys(ETF_UNIVERSE))

# ── Category mapping ────────────────────────────────────────────────────────
# Used for fast pre-filtering before API calls
_CATEGORY_MAP = {}
_EQUITY_ETFS = {
    "SPY", "QQQ", "VTI", "VOO", "IVV", "DIA", "RSP", "SPLG", "SCHB", "ITOT",
    "VV", "MGC", "SPTM", "SCHX", "IWB",
    "QQQM", "VUG", "IWF", "SPYG", "SCHG", "MGK", "RPG", "VONG", "IWY",
    "SPYV", "VOOV", "IVE", "SCHV", "VTV", "IUSV", "RPV", "VONV",
    "IWM", "VB", "IJR", "SCHA", "VBR", "VBK", "IWO", "IWN", "SLYV", "SLYG",
    "MDY", "IJH", "VO", "SCHM", "IWR", "IWS", "IWP", "IVOO",
    "VYM", "SCHD", "DVY", "HDV", "DGRO", "VIG", "NOBL", "SDY", "SPHD", "SPYD",
    "DIVO", "JEPI", "JEPQ", "XYLD", "QYLD", "RYLD",
    "TQQQ", "SQQQ", "SPXL", "SPXS", "UPRO",
    "ESGU", "ESGV", "SUSA",
    "ACWI", "VT",
}
_SECTOR_ETFS = {
    "XLK", "VGT", "FTEC", "IGV", "SMH", "SOXX",
    "XLF", "VFH", "KRE", "KBE", "IAI",
    "XLV", "VHT", "IBB", "XBI", "IHI",
    "XLE", "VDE", "OIH", "XOP", "AMLP",
    "XLY", "VCR", "FDIS",
    "XLP", "VDC", "FSTA",
    "XLI", "VIS", "ITA",
    "XLB", "VAW",
    "XLU", "VPU",
    "VNQ", "XLRE", "IYR", "SCHH", "RWR",
    "XLC", "VOX",
}
_BOND_ETFS = {
    "AGG", "BND", "SCHZ", "FBND",
    "TLT", "IEF", "SHY", "SHV", "GOVT", "VGSH", "VGIT", "VGLT", "TIP", "STIP",
    "BIL", "SGOV",
    "LQD", "VCIT", "VCSH", "IGSB", "IGIB",
    "HYG", "JNK", "SHYG", "USHY",
    "MUB", "VTEB", "TFI",
    "BNDX", "IAGG", "EMB",
}
_INTERNATIONAL_ETFS = {
    "EFA", "VEA", "IEFA", "SCHF", "SPDW", "VGK", "EWJ", "EWG", "EWU",
    "VWO", "EEM", "IEMG", "SCHE",
    "VXUS", "IXUS",
    "FXI", "MCHI", "EWZ", "INDA", "EWT", "EWY",
}
_COMMODITY_ETFS = {
    "GLD", "IAU", "SLV", "GDX", "GDXJ", "USO", "DBC", "PDBC", "GSG",
}
_THEMATIC_ETFS = {
    "ARKK", "ARKW", "ARKG", "ARKF", "HACK", "CIBR", "BOTZ", "ROBO",
    "TAN", "ICLN", "QCLN", "LIT", "DRIV", "BITO", "VIXY",
}

for s in ETF_UNIVERSE:
    if s in _BOND_ETFS:
        _CATEGORY_MAP[s] = "Bond"
    elif s in _COMMODITY_ETFS:
        _CATEGORY_MAP[s] = "Commodity"
    elif s in _INTERNATIONAL_ETFS:
        _CATEGORY_MAP[s] = "International"
    elif s in _SECTOR_ETFS:
        _CATEGORY_MAP[s] = "Sector"
    elif s in _THEMATIC_ETFS:
        _CATEGORY_MAP[s] = "Thematic"
    elif s in _EQUITY_ETFS:
        _CATEGORY_MAP[s] = "Equity"
    else:
        _CATEGORY_MAP[s] = "Equity"

# Asset class mapping
_ASSET_CLASS_MAP = {}
for s in ETF_UNIVERSE:
    if s in _BOND_ETFS:
        _ASSET_CLASS_MAP[s] = "Bonds"
    elif s in _COMMODITY_ETFS:
        _ASSET_CLASS_MAP[s] = "Commodities"
    elif s in _BOND_ETFS | _EQUITY_ETFS:
        _ASSET_CLASS_MAP[s] = "Stocks"
    else:
        _ASSET_CLASS_MAP[s] = "Stocks"
for s in {"AOR", "AOA", "AOM", "AOK"}:
    _ASSET_CLASS_MAP[s] = "Mixed"


def get_etf_category(symbol):
    return _CATEGORY_MAP.get(symbol, "Equity")


def get_etf_asset_class(symbol):
    return _ASSET_CLASS_MAP.get(symbol, "Stocks")


# ── ETF info cache ──────────────────────────────────────────────────────────
from yf_utils import fetch_ticker_info as _fetch_ticker_info, safe_float as _safe_float


def get_etf_data(symbol):
    """Fetch ETF data from yfinance .info — single API call per ticker."""
    t, info = _fetch_ticker_info(symbol)
    if t is None or info is None:
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

        # Expense ratio — yfinance stores as decimal (0.0003 = 0.03%)
        expense_ratio_raw = info.get("annualReportExpenseRatio") or info.get("expenseRatio")
        expense_ratio = None
        if expense_ratio_raw is not None:
            try:
                expense_ratio = round(float(expense_ratio_raw) * 100, 4)
            except (TypeError, ValueError):
                pass

        # Total assets (AUM)
        total_assets = info.get("totalAssets")
        if total_assets is not None:
            try:
                total_assets = int(total_assets)
            except (TypeError, ValueError):
                total_assets = None

        # Dividend yield — yfinance may return as percentage (1.06=1.06%) or decimal (0.0106)
        raw_dy = info.get("yield") or info.get("dividendYield")
        if raw_dy:
            raw_dy = float(raw_dy)
            dividend_yield = round(raw_dy, 2) if raw_dy >= 1 else round(raw_dy * 100, 2)
        else:
            dividend_yield = None

        # 52-week calculations
        w52_high = _safe_float(info, "fiftyTwoWeekHigh")
        w52_low = _safe_float(info, "fiftyTwoWeekLow")
        w52_perf = None
        w52_dist_high = None
        if w52_high and w52_low and current_price:
            if w52_low > 0:
                w52_perf = round((current_price - w52_low) / w52_low * 100, 1)
            if w52_high > 0:
                w52_dist_high = round((w52_high - current_price) / w52_high * 100, 1)

        # YTD return — try info field first, else compute from price history
        ytd_return = _safe_float(info, "ytdReturn", 100)

        # 1-year return
        one_year_return = None
        if w52_low and w52_high and current_price:
            # Approximate: use 52-week performance from low
            one_year_return = w52_perf

        # 3-year return — from info
        three_year_return = _safe_float(info, "threeYearAverageReturn", 100)

        # Volume
        volume = info.get("volume") or info.get("regularMarketVolume")
        avg_volume = info.get("averageVolume") or info.get("averageDailyVolume10Day")

        # Beta
        beta = _safe_float(info, "beta3Year") or _safe_float(info, "beta")

        # Category & asset class from our maps
        category = get_etf_category(symbol)
        asset_class = get_etf_asset_class(symbol)

        # Day change
        change_pct = info.get("regularMarketChangePercent")
        if change_pct is None:
            prev = info.get("previousClose")
            if prev and float(prev) > 0:
                change_pct = round((current_price - float(prev)) / float(prev) * 100, 2)
            else:
                change_pct = 0
        else:
            change_pct = round(float(change_pct), 2)

        result = {
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
            "category": category,
            "asset_class": asset_class,
            "volume": volume,
            "avg_volume": avg_volume,
            "w52_high": w52_high,
            "w52_low": w52_low,
            "w52_perf": w52_perf,
            "w52_dist_high": w52_dist_high,
            "beta": beta,
        }

        return result
    except Exception as e:
        print(f"Error processing {symbol}: {e}")
        return None


def _passes_criteria(etf, criteria):
    """Check all filter criteria against an ETF data dict."""

    # Category filter
    categories = criteria.get("categories")
    if categories:
        if etf.get("category") not in categories:
            return False

    # Asset class filter
    asset_classes = criteria.get("asset_classes")
    if asset_classes:
        if etf.get("asset_class") not in asset_classes:
            return False

    # Expense ratio max
    max_expense = criteria.get("max_expense_ratio")
    if max_expense is not None:
        er = etf.get("expense_ratio")
        if er is None or er > max_expense:
            return False

    # Total assets min (AUM)
    min_aum = criteria.get("min_aum")
    if min_aum is not None:
        ta = etf.get("total_assets")
        if ta is None or ta < min_aum:
            return False

    # Dividend yield range
    min_div = criteria.get("min_div_yield")
    max_div = criteria.get("max_div_yield")
    if min_div is not None:
        dy = etf.get("dividend_yield")
        if dy is None or dy < min_div:
            return False
    if max_div is not None:
        dy = etf.get("dividend_yield")
        if dy is not None and dy > max_div:
            return False

    # YTD return min
    min_ytd = criteria.get("min_ytd_return")
    if min_ytd is not None:
        yr = etf.get("ytd_return")
        if yr is None or yr < min_ytd:
            return False

    # 1-year return min
    min_1y = criteria.get("min_1y_return")
    if min_1y is not None:
        r1 = etf.get("one_year_return")
        if r1 is None or r1 < min_1y:
            return False

    # 3-year return min
    min_3y = criteria.get("min_3y_return")
    if min_3y is not None:
        r3 = etf.get("three_year_return")
        if r3 is None or r3 < min_3y:
            return False

    # Average daily volume min
    min_vol = criteria.get("min_avg_volume")
    if min_vol is not None:
        av = etf.get("avg_volume")
        if av is None or av < min_vol:
            return False

    # 52-week performance range
    min_w52 = criteria.get("min_w52_perf")
    if min_w52 is not None:
        wp = etf.get("w52_perf")
        if wp is None or wp < min_w52:
            return False
    max_w52 = criteria.get("max_w52_perf")
    if max_w52 is not None:
        wp = etf.get("w52_perf")
        if wp is not None and wp > max_w52:
            return False

    # Distance from 52-week high
    max_dist_high = criteria.get("max_w52_dist_high")
    if max_dist_high is not None:
        dh = etf.get("w52_dist_high")
        if dh is None or dh > max_dist_high:
            return False

    return True


def screen_etfs(criteria, on_progress=None, on_match=None):
    """Screen ETFs against criteria using thread pool."""
    tickers = list(ETF_UNIVERSE)

    # Pre-filter by category (free, no API call)
    categories = criteria.get("categories")
    if categories:
        tickers = [s for s in tickers if _CATEGORY_MAP.get(s) in categories]

    # Pre-filter by asset class (free, no API call)
    asset_classes = criteria.get("asset_classes")
    if asset_classes:
        tickers = [s for s in tickers if _ASSET_CLASS_MAP.get(s) in asset_classes]

    total = len(tickers)
    processed = 0
    lock = threading.Lock()
    matches = []

    def process(symbol):
        nonlocal processed
        data = get_etf_data(symbol)
        with lock:
            processed += 1
            if on_progress:
                on_progress(processed, total)
            if data and _passes_criteria(data, criteria):
                matches.append(data)
                if on_match:
                    on_match(data)

    with ThreadPoolExecutor(max_workers=20) as executor:
        list(executor.map(process, tickers))

    return matches


def get_etf_detail(symbol):
    """Get detailed ETF info including holdings and sector breakdown."""
    t, info = _fetch_ticker_info(symbol)
    if t is None or info is None:
        return None

    try:
        def _safe(key, scale=1, decimals=2):
            v = info.get(key)
            if v is None:
                return None
            try:
                return round(float(v) * scale, decimals)
            except (TypeError, ValueError):
                return None

        current_price = (
            info.get("currentPrice")
            or info.get("regularMarketPrice")
            or info.get("previousClose")
            or info.get("navPrice")
        )

        expense_ratio_raw = info.get("annualReportExpenseRatio") or info.get("expenseRatio")
        expense_ratio = None
        if expense_ratio_raw is not None:
            try:
                expense_ratio = round(float(expense_ratio_raw) * 100, 4)
            except (TypeError, ValueError):
                pass

        total_assets = info.get("totalAssets")

        raw_dy = info.get("yield") or info.get("dividendYield")
        if raw_dy:
            raw_dy = float(raw_dy)
            dividend_yield = round(raw_dy, 2) if raw_dy >= 1 else round(raw_dy * 100, 2)
        else:
            dividend_yield = None

        etf_info = {
            "symbol": symbol,
            "name": info.get("longName") or info.get("shortName") or symbol,
            "price": _safe("currentPrice") or _safe("regularMarketPrice") or _safe("previousClose"),
            "previous_close": _safe("previousClose"),
            "open": _safe("regularMarketOpen"),
            "day_high": _safe("dayHigh") or _safe("regularMarketDayHigh"),
            "day_low": _safe("dayLow") or _safe("regularMarketDayLow"),
            "change": _safe("regularMarketChange"),
            "change_pct": _safe("regularMarketChangePercent"),
            "week_52_high": _safe("fiftyTwoWeekHigh"),
            "week_52_low": _safe("fiftyTwoWeekLow"),
            "expense_ratio": expense_ratio,
            "total_assets": total_assets,
            "dividend_yield": dividend_yield,
            "ytd_return": _safe("ytdReturn", 100),
            "three_year_return": _safe("threeYearAverageReturn", 100),
            "five_year_return": _safe("fiveYearAverageReturn", 100),
            "beta": _safe("beta3Year") or _safe("beta"),
            "volume": info.get("volume") or info.get("regularMarketVolume"),
            "avg_volume": info.get("averageVolume"),
            "category": get_etf_category(symbol),
            "asset_class": get_etf_asset_class(symbol),
            "fund_family": info.get("fundFamily"),
            "inception_date": None,
            "nav_price": _safe("navPrice"),
        }

        # Inception date
        inception_raw = info.get("fundInceptionDate")
        if inception_raw:
            try:
                etf_info["inception_date"] = datetime.fromtimestamp(int(inception_raw)).strftime("%Y-%m-%d")
            except (TypeError, ValueError, OSError):
                pass

        # Compute 1-year return from 52w data
        w52_low = _safe("fiftyTwoWeekLow")
        if w52_low and current_price and w52_low > 0:
            etf_info["one_year_return"] = round((float(current_price) - w52_low) / w52_low * 100, 1)
        else:
            etf_info["one_year_return"] = None

        # Top holdings
        holdings = []
        try:
            # yfinance sometimes has fund_top_holdings or similar
            # Try the newer API
            fund_holdings = t.funds_data
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

        # Sector weights
        sector_weights = []
        try:
            fund_holdings = t.funds_data
            if fund_holdings:
                try:
                    sw = fund_holdings.sector_weightings
                    if sw:
                        for item in sw:
                            if isinstance(item, dict):
                                for sector_name, weight in item.items():
                                    sector_weights.append({
                                        "sector": sector_name.replace("_", " ").title(),
                                        "weight": round(float(weight) * 100, 2)
                                    })
                except Exception:
                    pass
        except Exception:
            pass

        return {
            "info": etf_info,
            "holdings": holdings,
            "sector_weights": sector_weights,
        }

    except Exception as e:
        print(f"Error in get_etf_detail for {symbol}: {e}")
        return None
