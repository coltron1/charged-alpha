#!/usr/bin/env python3
"""Refresh dated research profiles without putting provider calls in page loads."""
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from stock_research import BUSINESS_GROUPS, SNAPSHOT_PATH, normalize_profile, number, read_registry, age_days
from yf_utils import fetch_ticker_info


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbols", help="Comma-separated symbols; defaults to the catalog and reviewed peer universe")
    parser.add_argument("--limit", type=int, default=0, help="Refresh oldest profiles first; zero refreshes all")
    parser.add_argument("--max-age-hours", type=float, default=0, help="Skip a successful recent full refresh; zero forces refresh")
    args = parser.parse_args()
    registry = read_registry()
    if not args.symbols and not args.limit and args.max_age_hours > 0 and registry.get("full_refreshed_at") and 0 <= age_days(registry["full_refreshed_at"]) * 24 < args.max_age_hours:
        print("Research snapshots are within the requested refresh interval; no changes.")
        return 0
    profiles = dict(registry.get("profiles", {}))
    if args.symbols:
        symbols = set(args.symbols.upper().split(","))
    else:
        from app import _shows_context
        symbols = {s["yf_symbol"] for s in _shows_context()["show_library"]["stocks"]}
        symbols.update(s for members in BUSINESS_GROUPS.values() for s in members)
    symbols = sorted(symbols, key=lambda s: (profiles.get(s, {}).get("observed_at", ""), s))
    if args.limit:
        symbols = symbols[:args.limit]
    now = datetime.now(timezone.utc).isoformat()
    fx = {"USD": 1}
    # Rates are USD per currency unit. Unavailable conversions stay unavailable.
    for currency in ("CAD", "EUR", "GBP", "JPY", "CNY", "TWD", "HKD", "CHF", "AUD", "KRW"):
        try:
            _, info = fetch_ticker_info(currency + "USD=X", max_retries=1)
            rate = number(info.get("regularMarketPrice"))
            if rate and rate > 0:
                fx[currency] = rate
        except Exception:
            pass
    def fetch(symbol):
        _, info = fetch_ticker_info(symbol, max_retries=2)
        if not info or not info.get("industry") or not number(info.get("marketCap")):
            return symbol, None
        return symbol, normalize_profile(symbol, info, now, fx)
    failures = []
    with ThreadPoolExecutor(max_workers=4) as pool:
        jobs = {pool.submit(fetch, symbol): symbol for symbol in symbols}
        for index, job in enumerate(as_completed(jobs), 1):
            try:
                symbol, profile = job.result()
                if profile:
                    profiles[symbol] = profile
                else:
                    failures.append(symbol)
            except Exception:
                failures.append(jobs[job])
            if index % 100 == 0:
                print(f"Refreshed {index}/{len(symbols)}", flush=True)
    full_refreshed_at = now if not args.symbols and not args.limit and len(failures) < len(symbols) / 2 else registry.get("full_refreshed_at")
    output = {"schema_version": 1, "refreshed_at": now, "full_refreshed_at": full_refreshed_at, "fx": fx, "fx_observed_at": now,
              "failed_symbols": sorted((set(registry.get("failed_symbols", [])) - set(symbols)) | set(failures)),
              "profiles": dict(sorted(profiles.items()))}
    SNAPSHOT_PATH.parent.mkdir(exist_ok=True)
    temporary = SNAPSHOT_PATH.with_suffix(".tmp")
    temporary.write_text(json.dumps(output, indent=2, ensure_ascii=True, allow_nan=False) + "\n")
    temporary.replace(SNAPSHOT_PATH)
    print(f"Saved {len(profiles)} profiles; {len(failures)} refresh failures (last valid snapshots retained).")
    return 1 if len(failures) == len(symbols) else 0


if __name__ == "__main__":
    raise SystemExit(main())
