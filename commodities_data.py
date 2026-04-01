import yfinance as yf
from concurrent.futures import ThreadPoolExecutor, as_completed
from yf_utils import TTLCache, fetch_chart

_cache = TTLCache(default_ttl=300, max_size=50)

COMMODITIES = {
    "CL=F":  {"name": "Crude Oil (WTI)", "category": "Energy"},
    "BZ=F":  {"name": "Brent Crude", "category": "Energy"},
    "NG=F":  {"name": "Natural Gas", "category": "Energy"},
    "HO=F":  {"name": "Heating Oil", "category": "Energy"},
    "GC=F":  {"name": "Gold", "category": "Metals"},
    "SI=F":  {"name": "Silver", "category": "Metals"},
    "HG=F":  {"name": "Copper", "category": "Metals"},
    "PL=F":  {"name": "Platinum", "category": "Metals"},
    "PA=F":  {"name": "Palladium", "category": "Metals"},
    "ZC=F":  {"name": "Corn", "category": "Agriculture"},
    "ZW=F":  {"name": "Wheat", "category": "Agriculture"},
    "ZS=F":  {"name": "Soybeans", "category": "Agriculture"},
    "KC=F":  {"name": "Coffee", "category": "Agriculture"},
    "SB=F":  {"name": "Sugar", "category": "Agriculture"},
    "CC=F":  {"name": "Cocoa", "category": "Agriculture"},
    "CT=F":  {"name": "Cotton", "category": "Agriculture"},
    "LE=F":  {"name": "Live Cattle", "category": "Livestock"},
    "HE=F":  {"name": "Lean Hogs", "category": "Livestock"},
}

def get_all_commodities():
    cached = _cache.get("all")
    if cached:
        return cached

    results = []

    def fetch(ticker):
        meta = COMMODITIES[ticker]
        try:
            t = yf.Ticker(ticker)
            hist_1m = t.history(period="1mo", interval="1d")
            if hist_1m.empty:
                return None

            current = float(hist_1m["Close"].iloc[-1])
            prev_day = float(hist_1m["Close"].iloc[-2]) if len(hist_1m) > 1 else current

            # 1 week change
            w1_price = float(hist_1m["Close"].iloc[-6]) if len(hist_1m) >= 6 else float(hist_1m["Close"].iloc[0])
            # 1 month change
            m1_price = float(hist_1m["Close"].iloc[0])

            change_1d = round((current - prev_day) / prev_day * 100, 2) if prev_day else 0
            change_1w = round((current - w1_price) / w1_price * 100, 2) if w1_price else 0
            change_1m = round((current - m1_price) / m1_price * 100, 2) if m1_price else 0

            return {
                "ticker": ticker,
                "name": meta["name"],
                "category": meta["category"],
                "price": round(current, 2),
                "change_1d": change_1d,
                "change_1w": change_1w,
                "change_1m": change_1m,
            }
        except Exception:
            return None

    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = {pool.submit(fetch, t): t for t in COMMODITIES}
        for f in as_completed(futures):
            r = f.result()
            if r:
                results.append(r)

    # Sort by category order
    cat_order = {"Energy": 0, "Metals": 1, "Agriculture": 2, "Livestock": 3}
    results.sort(key=lambda x: (cat_order.get(x["category"], 99), x["name"]))
    _cache.set("all", results)
    return results


def get_commodity_chart(ticker, range_key="1y"):
    return fetch_chart(ticker, range_key)
