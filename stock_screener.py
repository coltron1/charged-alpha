import time
import math
import yfinance as yf
import pandas as pd
import threading
import requests
from io import StringIO
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta


def _norm_cdf(x):
    """Standard normal CDF via math.erf — no scipy needed."""
    return (1.0 + math.erf(x / math.sqrt(2.0))) / 2.0


def _prob_itm_put(S, K, sigma, dte, r=0.045):
    try:
        T = dte / 365.0
        if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
            return None
        d2 = (math.log(S / K) + (r - 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
        return round(_norm_cdf(-d2) * 100, 1)
    except Exception:
        return None

# ── Ticker list cache ────────────────────────────────────────────────────────
_ticker_cache: list = []
_ticker_cache_ts: float = 0.0
_TICKER_CACHE_TTL = 3600
# Also cache sector from Wikipedia table for pre-filtering
_ticker_sectors: dict = {}  # symbol -> GICS sector

# ── Use shared ticker info cache ───────────────────────────────────────────
from yf_utils import fetch_ticker_info as _fetch_ticker_info, safe_float as _safe_float, normalize_div_yield, ticker_info_cache as _info_cache


def get_sp500_tickers():
    global _ticker_cache, _ticker_cache_ts, _ticker_sectors
    if _ticker_cache and (time.time() - _ticker_cache_ts) < _TICKER_CACHE_TTL:
        return _ticker_cache
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
        resp = requests.get(
            "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
            headers=headers, timeout=15
        )
        resp.raise_for_status()
        tables = pd.read_html(StringIO(resp.text))
        df = tables[0]
        df["Symbol"] = df["Symbol"].str.replace(".", "-", regex=False)
        tickers = df["Symbol"].tolist()
        # Build sector map from Wikipedia table (GICS Sector column)
        if "GICS Sector" in df.columns:
            _ticker_sectors = dict(zip(df["Symbol"], df["GICS Sector"]))
        _ticker_cache = tickers
        _ticker_cache_ts = time.time()
        return tickers
    except Exception as e:
        print(f"Failed to fetch S&P 500 list: {e}")
        return _ticker_cache or []


def get_ticker_sector(symbol):
    """Get sector from cached Wikipedia data (fast, no API call)."""
    return _ticker_sectors.get(symbol)


def _calc_historical_pe(ticker_obj, hist_close=None):
    try:
        income = ticker_obj.income_stmt
        if income is None or income.empty:
            return None, 0

        if hist_close is not None:
            hist_close = hist_close.dropna()
            if hist_close.empty:
                return None, 0
            hist = pd.DataFrame({"Close": hist_close})
            hist.index = pd.to_datetime(hist.index)
            if hist.index.tz is not None:
                hist.index = hist.index.tz_localize(None)
        else:
            hist = ticker_obj.history(period="6y")
            if hist.empty:
                return None, 0
            hist.index = hist.index.tz_localize(None)

        eps_row = None
        for label in ["Diluted EPS", "Basic EPS"]:
            if label in income.index:
                eps_row = income.loc[label]
                break

        if eps_row is None:
            try:
                net_income = income.loc["Net Income"]
                shares = income.loc["Diluted Average Shares"]
                eps_row = net_income / shares
            except KeyError:
                return None, 0

        pe_list = []
        for col_date in income.columns:
            eps = eps_row.get(col_date)
            if eps is None or pd.isna(eps) or eps <= 0:
                continue

            ts = pd.Timestamp(col_date)
            window = hist.loc[
                (hist.index >= ts - pd.Timedelta(days=10))
                & (hist.index <= ts + pd.Timedelta(days=10))
            ]
            if window.empty:
                after = hist.loc[hist.index >= ts - pd.Timedelta(days=30)]
                before = hist.loc[hist.index <= ts + pd.Timedelta(days=30)]
                if not after.empty:
                    price = after["Close"].iloc[0]
                elif not before.empty:
                    price = before["Close"].iloc[-1]
                else:
                    continue
            else:
                price = float(window["Close"].mean())

            pe = price / float(eps)
            if 0 < pe < 1000:
                pe_list.append(pe)

        if not pe_list:
            return None, 0
        return round(sum(pe_list) / len(pe_list), 2), len(pe_list)
    except Exception:
        return None, 0


def get_options_data(ticker_obj, current_price):
    try:
        expirations = ticker_obj.options
        if not expirations:
            return {}

        today = pd.Timestamp.now().normalize()

        best_exp = None
        best_diff = float("inf")
        for exp in expirations:
            dte = (pd.Timestamp(exp) - today).days
            if dte < 7:
                continue
            diff = abs(dte - 30)
            if diff < best_diff:
                best_diff = diff
                best_exp = (exp, dte)

        if not best_exp:
            return {}

        exp_str, dte = best_exp
        chain = ticker_obj.option_chain(exp_str)
        puts = chain.puts.copy()

        if puts.empty:
            return {}

        puts["dist"] = abs(puts["strike"] - current_price)
        atm = puts.loc[puts["dist"].idxmin()]

        bid = float(atm.get("bid") or 0)
        ask = float(atm.get("ask") or 0)
        spread_pct = None
        if bid > 0 and ask > 0:
            mid = (bid + ask) / 2
            if mid > 0:
                spread_pct = round((ask - bid) / mid * 100, 1)

        iv_raw = atm.get("impliedVolatility")
        iv = round(float(iv_raw) * 100, 1) if iv_raw and not pd.isna(iv_raw) else None

        oi_raw = atm.get("openInterest")
        oi = int(oi_raw) if oi_raw and not pd.isna(oi_raw) else None

        vol_raw = atm.get("volume")
        vol = int(vol_raw) if vol_raw and not pd.isna(vol_raw) else None

        return {
            "atm_put_iv": iv,
            "atm_put_spread_pct": spread_pct,
            "atm_put_oi": oi,
            "atm_put_volume": vol,
            "atm_strike": round(float(atm["strike"]), 2),
            "options_dte": dte,
        }
    except Exception:
        return {}


def get_stock_data(symbol, fetch_options=False, hist_close=None, need_hist_pe=True):
    t, info = _fetch_ticker_info(symbol)
    if t is None or info is None:
        return None

    try:
        current_price = (
            info.get("currentPrice")
            or info.get("regularMarketPrice")
            or info.get("previousClose")
        )
        if current_price is None:
            return None
        current_price = float(current_price)

        trailing_pe = info.get("trailingPE")

        avg_hist_pe, hist_years = None, 0
        if need_hist_pe:
            if trailing_pe is not None:
                avg_hist_pe, hist_years = _calc_historical_pe(t, hist_close=hist_close)

        pe_discount_pct = None
        if trailing_pe and avg_hist_pe and avg_hist_pe > 0:
            pe_discount_pct = round(
                (avg_hist_pe - trailing_pe) / avg_hist_pe * 100, 1
            )

        pb_raw = info.get("priceToBook")

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

        # Dividend yield — yfinance may return as pct (2.5=2.5%) or decimal (0.025)
        raw_dy = info.get("dividendYield")
        if raw_dy:
            raw_dy = float(raw_dy)
            dividend_yield_pct = round(raw_dy, 2) if raw_dy >= 1 else round(raw_dy * 100, 2)
        else:
            dividend_yield_pct = None

        # Consecutive dividend years — estimate from dividend history
        div_streak = None
        if raw_dy and raw_dy > 0:
            try:
                divs = t.dividends
                if divs is not None and not divs.empty:
                    # Count consecutive years with at least one dividend, going backwards
                    years_with_div = sorted(set(divs.index.year), reverse=True)
                    if years_with_div:
                        streak = 0
                        current_yr = datetime.now().year
                        for yr in range(current_yr, current_yr - 50, -1):
                            if yr in years_with_div:
                                streak += 1
                            else:
                                break
                        div_streak = streak if streak > 0 else None
            except Exception:
                pass

        # Payout ratio - yfinance returns as decimal
        raw_pr = info.get("payoutRatio")
        payout_ratio = round(float(raw_pr) * 100, 1) if raw_pr else None

        # Ex-dividend date
        raw_exdiv = info.get("exDividendDate")
        ex_div_date = None
        if raw_exdiv:
            try:
                ex_div_date = datetime.fromtimestamp(int(raw_exdiv)).strftime("%Y-%m-%d")
            except (TypeError, ValueError, OSError):
                pass

        # Growth metrics
        revenue_growth = _safe_float(info, "revenueGrowth", 100)  # to %
        earnings_growth = _safe_float(info, "earningsGrowth", 100)

        # Financial health
        debt_to_equity = _safe_float(info, "debtToEquity")  # yfinance returns as %, e.g. 150 = 150%
        if debt_to_equity is not None:
            debt_to_equity = round(debt_to_equity / 100, 2)  # convert to ratio
        current_ratio = _safe_float(info, "currentRatio")
        operating_margin = _safe_float(info, "operatingMargins", 100)  # to %

        # FCF yield
        fcf = info.get("freeCashflow")
        mcap = info.get("marketCap")
        fcf_yield = None
        if fcf and mcap and mcap > 0:
            fcf_yield = round(float(fcf) / float(mcap) * 100, 2)

        # Analyst sentiment
        analyst_rec = info.get("recommendationKey")
        analyst_score = _safe_float(info, "recommendationMean")
        analyst_count = info.get("numberOfAnalystOpinions")
        target_price = _safe_float(info, "targetMeanPrice")
        target_upside = None
        if target_price and current_price > 0:
            target_upside = round((target_price - current_price) / current_price * 100, 1)

        result = {
            "symbol": symbol,
            "name": info.get("shortName", symbol),
            "price": round(current_price, 2),
            "trailing_pe": round(float(trailing_pe), 2) if trailing_pe else None,
            "avg_hist_pe": avg_hist_pe,
            "hist_years": hist_years,
            "pe_discount_pct": pe_discount_pct,
            "price_to_book": round(float(pb_raw), 2) if pb_raw else None,
            "sector": info.get("sector", "Unknown"),
            "market_cap": mcap,
            "change_pct": round(float(info.get("regularMarketChangePercent", 0)), 2),
            "volume": info.get("volume") or info.get("regularMarketVolume"),
            "dividend_yield": dividend_yield_pct,
            "div_streak": div_streak,
            "payout_ratio": payout_ratio,
            "ex_div_date": ex_div_date,
            "revenue_growth": round(revenue_growth, 1) if revenue_growth is not None else None,
            "earnings_growth": round(earnings_growth, 1) if earnings_growth is not None else None,
            "w52_perf": w52_perf,
            "w52_dist_high": w52_dist_high,
            "debt_to_equity": debt_to_equity,
            "current_ratio": round(current_ratio, 2) if current_ratio is not None else None,
            "fcf_yield": fcf_yield,
            "operating_margin": round(operating_margin, 1) if operating_margin is not None else None,
            "analyst_rec": analyst_rec,
            "analyst_score": round(analyst_score, 2) if analyst_score is not None else None,
            "analyst_count": int(analyst_count) if analyst_count else None,
            "target_price": round(target_price, 2) if target_price is not None else None,
            "target_upside": target_upside,
            # Options fields
            "atm_put_iv": None,
            "atm_put_spread_pct": None,
            "atm_put_oi": None,
            "atm_put_volume": None,
            "atm_strike": None,
            "options_dte": None,
        }

        if fetch_options:
            result.update(get_options_data(t, current_price))

        return result
    except Exception:
        return None


def get_stock_detail(symbol):
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
        )

        stock_info = {
            "symbol": symbol,
            "name": info.get("longName") or info.get("shortName", symbol),
            "price": _safe("currentPrice") or _safe("regularMarketPrice") or _safe("previousClose"),
            "previous_close": _safe("previousClose"),
            "open": _safe("regularMarketOpen"),
            "day_high": _safe("dayHigh") or _safe("regularMarketDayHigh"),
            "day_low": _safe("dayLow") or _safe("regularMarketDayLow"),
            "change": _safe("regularMarketChange"),
            "change_pct": _safe("regularMarketChangePercent"),
            "week_52_high": _safe("fiftyTwoWeekHigh"),
            "week_52_low": _safe("fiftyTwoWeekLow"),
            "trailing_pe": _safe("trailingPE"),
            "forward_pe": _safe("forwardPE"),
            "eps": _safe("trailingEps"),
            "price_to_book": _safe("priceToBook"),
            "market_cap": info.get("marketCap"),
            "beta": _safe("beta"),
            "dividend_yield": _safe("dividendYield"),
            "avg_volume": info.get("averageVolume"),
            "volume": info.get("volume") or info.get("regularMarketVolume"),
            "sector": info.get("sector"),
            "industry": info.get("industry"),
            "short_ratio": _safe("shortRatio"),
            "target_mean_price": _safe("targetMeanPrice"),
            "analyst_recommendation": info.get("recommendationKey"),
            "analyst_mean_score": _safe("recommendationMean"),
            "analyst_count": info.get("numberOfAnalystOpinions"),
            # Extra fields for detail view
            "revenue_growth": _safe("revenueGrowth", 100),
            "earnings_growth": _safe("earningsGrowth", 100),
            "debt_to_equity": _safe("debtToEquity"),
            "current_ratio": _safe("currentRatio"),
            "operating_margin": _safe("operatingMargins", 100),
            "payout_ratio": _safe("payoutRatio", 100),
            "free_cashflow": info.get("freeCashflow"),
            "target_upside": None,
        }

        # Calculate target upside
        tp = stock_info.get("target_mean_price")
        p = stock_info.get("price")
        if tp and p and p > 0:
            stock_info["target_upside"] = round((tp - p) / p * 100, 1)

        # FCF yield
        fcf = info.get("freeCashflow")
        mc = info.get("marketCap")
        stock_info["fcf_yield"] = round(float(fcf) / float(mc) * 100, 2) if fcf and mc and mc > 0 else None

        # Fetch put chains for all expirations within ~92 days (3 months)
        today = pd.Timestamp.now().normalize()
        all_exps = t.options or []
        near_exps = sorted(
            [(e, (pd.Timestamp(e) - today).days) for e in all_exps
             if 0 < (pd.Timestamp(e) - today).days <= 92],
            key=lambda x: x[1],
        )

        options_by_exp = []
        for exp_str, dte in near_exps:
            try:
                chain = t.option_chain(exp_str)
                puts = chain.puts.copy()
                if puts.empty:
                    continue

                contracts = []
                for _, row in puts.iterrows():
                    strike = float(row["strike"])
                    bid = float(row.get("bid") or 0)
                    ask = float(row.get("ask") or 0)

                    if ask == 0:
                        continue

                    mid = round((bid + ask) / 2, 2)
                    spread_dollar = round(ask - bid, 2)
                    spread_pct = round((ask - bid) / mid * 100, 1) if mid > 0 else None

                    iv_raw = row.get("impliedVolatility")
                    iv = round(float(iv_raw) * 100, 1) if iv_raw and not pd.isna(iv_raw) else None

                    oi_raw = row.get("openInterest")
                    oi = int(oi_raw) if oi_raw and not pd.isna(oi_raw) else 0

                    vol_raw = row.get("volume")
                    volume = int(vol_raw) if vol_raw and not pd.isna(vol_raw) else 0

                    capital = round(strike * 100, 2)
                    annual_return = round((mid / strike) * (365 / dte) * 100, 1) if dte > 0 and strike > 0 else None

                    if spread_pct is None:
                        spread_flag = None
                    elif spread_pct > 20:
                        spread_flag = "bad"
                    elif spread_pct > 10:
                        spread_flag = "wide"
                    else:
                        spread_flag = None

                    if volume == 0:
                        vol_flag = "none"
                    elif volume < 10:
                        vol_flag = "low"
                    else:
                        vol_flag = None

                    sigma = float(iv_raw) if iv_raw and not pd.isna(iv_raw) else None
                    prob_itm = _prob_itm_put(current_price, strike, sigma, dte) if sigma else None

                    contracts.append({
                        "strike": strike,
                        "bid": round(bid, 2),
                        "ask": round(ask, 2),
                        "mid": mid,
                        "spread_dollar": spread_dollar,
                        "spread_pct": spread_pct,
                        "iv": iv,
                        "oi": oi,
                        "volume": volume,
                        "capital": capital,
                        "annual_return_pct": annual_return,
                        "prob_itm": prob_itm,
                        "spread_flag": spread_flag,
                        "vol_flag": vol_flag,
                    })

                if contracts:
                    options_by_exp.append({
                        "expiration": exp_str,
                        "dte": dte,
                        "contracts": sorted(contracts, key=lambda c: c["strike"]),
                    })
            except Exception:
                continue

        return {"info": stock_info, "options": options_by_exp}

    except Exception:
        return None


def _passes_cheap_criteria(stock, criteria):
    """Check all criteria that DON'T require historical PE or options data.
    This runs in Phase 1 to eliminate stocks before expensive API calls."""
    # Sector
    sectors = criteria.get("sectors")
    if sectors and stock.get("sector", "Unknown") not in sectors:
        return False
    # Market cap
    cap_ranges = criteria.get("cap_ranges")
    if cap_ranges:
        mc = stock.get("market_cap")
        if mc is None or not any(lo <= mc <= hi for lo, hi in cap_ranges):
            return False
    # Price range
    min_price = criteria.get("min_price")
    max_price = criteria.get("max_price")
    if min_price is not None and stock["price"] < min_price:
        return False
    if max_price is not None and stock["price"] > max_price:
        return False
    # P/B
    min_pb = criteria.get("min_pb")
    max_pb = criteria.get("max_pb")
    if min_pb is not None or max_pb is not None:
        pb = stock.get("price_to_book")
        if pb is None:
            return False
        if min_pb is not None and pb < min_pb:
            return False
        if max_pb is not None and pb > max_pb:
            return False
    # Dividend yield
    min_div = criteria.get("min_div_yield")
    max_div = criteria.get("max_div_yield")
    if min_div is not None or max_div is not None:
        dy = stock.get("dividend_yield")
        if min_div is not None and (dy is None or dy < min_div):
            return False
        if max_div is not None and dy is not None and dy > max_div:
            return False
    # Payout ratio
    max_payout = criteria.get("max_payout_ratio")
    if max_payout is not None:
        pr = stock.get("payout_ratio")
        if pr is None or pr > max_payout:
            return False
    # Consecutive dividend years
    min_streak = criteria.get("min_div_streak")
    if min_streak is not None:
        ds = stock.get("div_streak")
        if ds is None or ds < min_streak:
            return False
    # Ex-div window
    ex_div_window = criteria.get("ex_div_window")
    if ex_div_window is not None:
        exd = stock.get("ex_div_date")
        if not exd:
            return False
        try:
            ex_date = datetime.strptime(exd, "%Y-%m-%d")
            today = datetime.now()
            if ex_date < today or ex_date > today + timedelta(days=ex_div_window):
                return False
        except ValueError:
            return False
    # Growth
    if criteria.get("min_revenue_growth") is not None:
        rg = stock.get("revenue_growth")
        if rg is None or rg < criteria["min_revenue_growth"]:
            return False
    if criteria.get("min_eps_growth") is not None:
        eg = stock.get("earnings_growth")
        if eg is None or eg < criteria["min_eps_growth"]:
            return False
    # 52-week
    min_w52 = criteria.get("min_w52_perf")
    if min_w52 is not None:
        wp = stock.get("w52_perf")
        if wp is None or wp < min_w52:
            return False
    max_w52 = criteria.get("max_w52_perf")
    if max_w52 is not None:
        wp = stock.get("w52_perf")
        if wp is not None and wp > max_w52:
            return False
    max_dist_high = criteria.get("max_w52_dist_high")
    if max_dist_high is not None:
        dh = stock.get("w52_dist_high")
        if dh is None or dh > max_dist_high:
            return False
    # Financial health
    if criteria.get("max_debt_to_equity") is not None:
        de = stock.get("debt_to_equity")
        if de is None or de > criteria["max_debt_to_equity"]:
            return False
    if criteria.get("min_current_ratio") is not None:
        cr = stock.get("current_ratio")
        if cr is None or cr < criteria["min_current_ratio"]:
            return False
    if criteria.get("min_fcf_yield") is not None:
        fy = stock.get("fcf_yield")
        if fy is None or fy < criteria["min_fcf_yield"]:
            return False
    if criteria.get("min_operating_margin") is not None:
        opm = stock.get("operating_margin")
        if opm is None or opm < criteria["min_operating_margin"]:
            return False
    # Analyst
    allowed_recs = criteria.get("analyst_recs")
    if allowed_recs:
        rec = stock.get("analyst_rec")
        if not rec or rec not in allowed_recs:
            return False
    if criteria.get("min_analyst_count") is not None:
        ac = stock.get("analyst_count")
        if ac is None or ac < criteria["min_analyst_count"]:
            return False
    if criteria.get("min_target_upside") is not None:
        tu = stock.get("target_upside")
        if tu is None or tu < criteria["min_target_upside"]:
            return False
    return True


def screen_stocks(criteria, on_progress=None, on_match=None):
    tickers = get_sp500_tickers()
    if not tickers:
        return []

    # Pre-filter by sector using cached Wikipedia data (no API calls needed)
    sectors = criteria.get("sectors")
    if sectors and _ticker_sectors:
        tickers = [s for s in tickers if _ticker_sectors.get(s) in sectors]

    need_hist_pe = bool(criteria.get("pe_below_historical"))
    need_options = any(
        criteria.get(k) is not None
        for k in ["min_put_iv", "max_put_iv", "max_put_spread_pct",
                   "min_put_oi", "min_put_volume"]
    )

    total = len(tickers)
    processed = 0
    lock = threading.Lock()
    matches = []

    # ── Phase 1: Fetch .info and apply cheap criteria ────────────────────────
    # This fetches only the basic info (one API call per ticker) and filters
    # out stocks that fail non-PE, non-options criteria before doing expensive
    # historical PE or options lookups.
    phase1_survivors = []  # (symbol, data_dict) that passed cheap filters

    def phase1(symbol):
        nonlocal processed
        data = get_stock_data(symbol, fetch_options=False, need_hist_pe=False)
        with lock:
            processed += 1
            if on_progress:
                on_progress(processed, total)
            if data and _passes_cheap_criteria(data, criteria):
                phase1_survivors.append((symbol, data))
                # If no expensive checks needed, it's already a final match
                if not need_hist_pe and not need_options:
                    matches.append(data)
                    if on_match:
                        on_match(data)

    with ThreadPoolExecutor(max_workers=20) as executor:
        list(executor.map(phase1, tickers))

    # If no expensive checks needed, we're done
    if not need_hist_pe and not need_options:
        return matches

    # ── Phase 2: Expensive checks only on survivors ──────────────────────────
    phase2_total = len(phase1_survivors)
    print(f"Phase 2: {phase2_total} survivors need {'hist PE' if need_hist_pe else ''}"
          f"{' + options' if need_options else ''}")

    # Bulk download price history for PE calc (only for survivors, not all 500)
    bulk_close = None
    if need_hist_pe and phase2_total > 0:
        survivor_symbols = [s for s, _ in phase1_survivors]
        try:
            print(f"Bulk-downloading 6y price history for {phase2_total} tickers…")
            raw = yf.download(
                survivor_symbols,
                period="6y",
                auto_adjust=True,
                progress=False,
                threads=True,
            )
            if phase2_total == 1:
                # yf.download returns single-level columns for 1 ticker
                bulk_close = pd.DataFrame({survivor_symbols[0]: raw["Close"]})
            else:
                close_df = raw["Close"]
                if close_df.index.tz is not None:
                    close_df.index = close_df.index.tz_localize(None)
                bulk_close = close_df
            print("Bulk download complete.")
        except Exception as e:
            print(f"Bulk download failed ({e}), falling back to per-ticker history()")

    phase2_processed = 0

    def phase2(item):
        nonlocal phase2_processed
        symbol, data = item

        # Compute historical PE if needed
        if need_hist_pe:
            cached = _info_cache.get(symbol)
            t = cached[0] if cached else yf.Ticker(symbol)
            hist_close = None
            if bulk_close is not None and symbol in bulk_close.columns:
                hist_close = bulk_close[symbol]
            avg_hist_pe, hist_years = _calc_historical_pe(t, hist_close=hist_close)
            data["avg_hist_pe"] = avg_hist_pe
            data["hist_years"] = hist_years
            if avg_hist_pe and data.get("trailing_pe") and avg_hist_pe > 0:
                data["pe_discount_pct"] = round(
                    (avg_hist_pe - data["trailing_pe"]) / avg_hist_pe * 100, 1
                )
            else:
                data["pe_discount_pct"] = None

            # Check PE criteria
            if criteria.get("pe_below_historical"):
                if data["trailing_pe"] is None or data["avg_hist_pe"] is None:
                    return
                if data["trailing_pe"] >= data["avg_hist_pe"]:
                    return
                min_discount = criteria.get("pe_min_discount_pct") or 0
                if (data["pe_discount_pct"] or 0) < min_discount:
                    return

        # Fetch options if needed
        if need_options:
            cached = _info_cache.get(symbol)
            t = cached[0] if cached else yf.Ticker(symbol)
            current_price = data["price"]
            opts = get_options_data(t, current_price)
            data.update(opts)

            # Check options criteria
            min_iv = criteria.get("min_put_iv")
            max_iv = criteria.get("max_put_iv")
            max_spread = criteria.get("max_put_spread_pct")
            min_oi = criteria.get("min_put_oi")
            min_put_vol = criteria.get("min_put_volume")
            iv = data.get("atm_put_iv")
            if min_iv is not None and (iv is None or iv < min_iv):
                return
            if max_iv is not None and (iv is None or iv > max_iv):
                return
            if max_spread is not None:
                spread = data.get("atm_put_spread_pct")
                if spread is None or spread > max_spread:
                    return
            if min_oi is not None:
                oi = data.get("atm_put_oi")
                if oi is None or oi < min_oi:
                    return
            if min_put_vol is not None:
                vol = data.get("atm_put_volume")
                if vol is None or vol < min_put_vol:
                    return

        with lock:
            phase2_processed += 1
            matches.append(data)
            if on_match:
                on_match(data)

    with ThreadPoolExecutor(max_workers=10) as executor:
        list(executor.map(phase2, phase1_survivors))

    return matches
