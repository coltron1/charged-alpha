import yfinance as yf
from concurrent.futures import ThreadPoolExecutor, as_completed

REITS = {
    # Residential
    "AVB": "Residential", "EQR": "Residential", "ESS": "Residential", "MAA": "Residential",
    "UDR": "Residential", "CPT": "Residential", "INVH": "Residential", "AMH": "Residential",
    # Office
    "BXP": "Office", "VNO": "Office", "SLG": "Office", "KRC": "Office",
    "HIW": "Office", "CUZ": "Office", "DEI": "Office",
    # Retail
    "SPG": "Retail", "REG": "Retail", "FRT": "Retail", "KIM": "Retail",
    "BRX": "Retail", "NNN": "Retail", "O": "Retail", "ADC": "Retail",
    # Industrial
    "PLD": "Industrial", "REXR": "Industrial", "FR": "Industrial", "EGP": "Industrial",
    "STAG": "Industrial", "TRNO": "Industrial",
    # Healthcare
    "WELL": "Healthcare", "VTR": "Healthcare", "PEAK": "Healthcare", "OHI": "Healthcare",
    "HR": "Healthcare", "MPW": "Healthcare", "SBRA": "Healthcare",
    # Data Center
    "EQIX": "Data Center", "DLR": "Data Center", "AMT": "Data Center",
    "CCI": "Data Center", "SBAC": "Data Center",
    # Specialty
    "PSA": "Specialty", "EXR": "Specialty", "CUBE": "Specialty", "LSI": "Specialty",
    "VICI": "Specialty", "GLPI": "Specialty", "IRM": "Specialty",
    # Diversified
    "ARE": "Diversified", "WPC": "Diversified", "STORE": "Diversified",
    "BNL": "Diversified", "EPRT": "Diversified",
}

def screen_reits(criteria, on_progress=None, on_match=None):
    symbols = list(REITS.keys())
    total = len(symbols)
    if on_progress:
        on_progress(0, total)

    processed = [0]

    def check_reit(sym):
        sector = REITS[sym]

        # Sector filter
        if criteria.get("sectors") and sector not in criteria["sectors"]:
            return None

        try:
            t = yf.Ticker(sym)
            info = t.info

            price = info.get("regularMarketPrice") or info.get("previousClose") or 0
            pe = info.get("trailingPE")
            mcap = info.get("marketCap") or 0
            div_yield = info.get("dividendYield")
            if div_yield:
                # yfinance returns as percentage (4.41 = 4.41%) or decimal (0.0441)
                if div_yield < 1:
                    div_yield = round(div_yield * 100, 2)
                else:
                    div_yield = round(div_yield, 2)
            de = info.get("debtToEquity")
            if de:
                de = round(de / 100, 2) if de > 10 else round(de, 2)
            payout = info.get("payoutRatio")
            if payout:
                payout = round(payout * 100, 1)
            w52_high = info.get("fiftyTwoWeekHigh") or 0
            w52_low = info.get("fiftyTwoWeekLow") or 0
            w52_perf = None
            if w52_low and w52_low > 0:
                w52_perf = round((price - w52_low) / w52_low * 100, 1)

            cr = criteria
            if cr.get("min_price") is not None and price < cr["min_price"]:
                return None
            if cr.get("max_price") is not None and price > cr["max_price"]:
                return None
            if cr.get("min_div_yield") is not None and (div_yield is None or div_yield < cr["min_div_yield"]):
                return None
            if cr.get("max_div_yield") is not None and (div_yield is not None and div_yield > cr["max_div_yield"]):
                return None
            if cr.get("min_pe") is not None and (pe is None or pe < cr["min_pe"]):
                return None
            if cr.get("max_pe") is not None and (pe is not None and pe > cr["max_pe"]):
                return None
            if cr.get("max_debt_to_equity") is not None and (de is not None and de > cr["max_debt_to_equity"]):
                return None
            if cr.get("min_market_cap") is not None and mcap < cr["min_market_cap"]:
                return None
            if cr.get("min_w52_perf") is not None and (w52_perf is None or w52_perf < cr["min_w52_perf"]):
                return None
            if cr.get("max_w52_perf") is not None and (w52_perf is not None and w52_perf > cr["max_w52_perf"]):
                return None

            return {
                "symbol": sym,
                "name": info.get("shortName", sym),
                "sector": sector,
                "price": round(price, 2),
                "div_yield": div_yield,
                "pe": round(pe, 2) if pe else None,
                "market_cap": mcap,
                "debt_to_equity": de,
                "payout_ratio": payout,
                "w52_perf": w52_perf,
                "w52_high": round(w52_high, 2),
                "w52_low": round(w52_low, 2),
            }
        except Exception:
            return None

    with ThreadPoolExecutor(max_workers=15) as pool:
        futures = {pool.submit(check_reit, sym): sym for sym in symbols}
        for future in as_completed(futures):
            processed[0] += 1
            if on_progress:
                on_progress(processed[0], total)
            result = future.result()
            if result and on_match:
                on_match(result)


def get_reit_chart(symbol, range_key="1y"):
    params = {
        "1m": dict(period="1mo", interval="1d"),
        "6m": dict(period="6mo", interval="1d"),
        "1y": dict(period="1y", interval="1d"),
        "5y": dict(period="5y", interval="1wk"),
    }
    p = params.get(range_key, params["1y"])
    try:
        t = yf.Ticker(symbol)
        hist = t.history(**p)
        if hist.empty:
            return None
        if hist.index.tz is not None:
            hist.index = hist.index.tz_localize(None)
        labels = hist.index.strftime("%Y-%m-%d").tolist()
        prices = [round(float(v), 2) for v in hist["Close"]]
        return {"labels": labels, "prices": prices}
    except Exception:
        return None
