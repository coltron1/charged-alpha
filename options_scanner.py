import yfinance as yf
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

DEFAULT_SYMBOLS = [
    "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "TSLA", "JPM", "V", "UNH",
    "XOM", "JNJ", "WMT", "PG", "MA", "HD", "CVX", "MRK", "ABBV", "PEP",
    "KO", "COST", "BAC", "AVGO", "TMO",
]

def scan_options(criteria, on_progress=None, on_match=None):
    symbols = criteria.get("symbols") or DEFAULT_SYMBOLS
    total = len(symbols)
    if on_progress:
        on_progress(0, total)

    opt_type = criteria.get("option_type", "both")
    min_oi = criteria.get("min_oi")
    min_volume = criteria.get("min_volume")
    max_spread_pct = criteria.get("max_spread_pct")
    min_dte = criteria.get("min_dte")
    max_dte = criteria.get("max_dte")
    min_vol_oi = criteria.get("min_vol_oi")
    unusual_only = criteria.get("unusual_only", False)
    today = datetime.now().date()

    processed = [0]

    def process_symbol(sym):
        results = []
        try:
            t = yf.Ticker(sym)
            exps = t.options
            if not exps:
                return results
            for exp_str in exps[:12]:  # first 12 expirations for wider DTE range
                exp_date = datetime.strptime(exp_str, "%Y-%m-%d").date()
                dte = (exp_date - today).days
                if dte < 0:
                    continue
                if min_dte is not None and dte < min_dte:
                    continue
                if max_dte is not None and dte > max_dte:
                    continue

                chain = t.option_chain(exp_str)
                frames = []
                if opt_type in ("both", "call"):
                    calls = chain.calls.copy()
                    calls["type"] = "Call"
                    frames.append(calls)
                if opt_type in ("both", "put"):
                    puts = chain.puts.copy()
                    puts["type"] = "Put"
                    frames.append(puts)

                for df in frames:
                    for _, row in df.iterrows():
                        oi = int(row.get("openInterest", 0) or 0)
                        vol = int(row.get("volume", 0) or 0)
                        bid = float(row.get("bid", 0) or 0)
                        ask = float(row.get("ask", 0) or 0)
                        iv = float(row.get("impliedVolatility", 0) or 0)
                        strike = float(row.get("strike", 0))

                        spread_pct = ((ask - bid) / ask * 100) if ask > 0 else 999
                        vol_oi = round(vol / oi, 2) if oi > 0 else 0

                        if min_oi is not None and oi < min_oi:
                            continue
                        if min_volume is not None and vol < min_volume:
                            continue
                        if max_spread_pct is not None and spread_pct > max_spread_pct:
                            continue
                        if min_vol_oi is not None and vol_oi < min_vol_oi:
                            continue
                        if unusual_only and vol_oi < 3:
                            continue

                        results.append({
                            "symbol": sym,
                            "type": row["type"],
                            "strike": round(strike, 2),
                            "expiry": exp_str,
                            "dte": dte,
                            "bid": round(bid, 2),
                            "ask": round(ask, 2),
                            "spread_pct": round(spread_pct, 2),
                            "volume": vol,
                            "open_interest": oi,
                            "vol_oi": vol_oi,
                            "iv": round(iv * 100, 1),
                            "unusual": vol_oi >= 3,
                        })
        except Exception:
            pass
        return results

    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = {pool.submit(process_symbol, sym): sym for sym in symbols}
        for future in as_completed(futures):
            processed[0] += 1
            if on_progress:
                on_progress(processed[0], total)
            matches = future.result()
            if on_match:
                for m in matches:
                    on_match(m)
