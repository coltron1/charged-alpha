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

FX_CURRENCIES = ("CAD", "EUR", "GBP", "JPY", "CNY", "TWD", "HKD", "CHF", "AUD", "KRW")


def refresh_fx(registry, observed_at):
    """Return retained registry rates, fresh rates safe for new conversions, dates, failures.

    Retained rates are provenance only. Callers must pass ``fresh`` rather than
    ``merged`` to normalize_profile so an old conversion is never represented as new.
    """
    previous = registry.get("fx") if isinstance(registry.get("fx"), dict) else {}
    previous_dates = registry.get("fx_observed_at_by_currency")
    previous_dates = previous_dates if isinstance(previous_dates, dict) else {}
    legacy_date = registry.get("fx_observed_at")
    legacy_date = legacy_date if isinstance(legacy_date, str) and legacy_date else None
    merged, dates = {}, {}
    for currency, raw_rate in previous.items():
        rate = number(raw_rate)
        if not isinstance(currency, str) or not rate or rate <= 0:
            continue
        merged[currency] = rate
        date = previous_dates.get(currency)
        if isinstance(date, str) and date:
            dates[currency] = date
        elif legacy_date:
            dates[currency] = legacy_date
    merged["USD"] = 1
    if "USD" not in dates:
        dates["USD"] = legacy_date or observed_at
    fresh, failed = {"USD": 1}, []
    # Rates are USD per currency unit. Unavailable conversions retain their
    # prior value/date for provenance but are unavailable to new profiles.
    for currency in FX_CURRENCIES:
        try:
            _, info = fetch_ticker_info(currency + "USD=X", max_retries=1)
            rate = number(info.get("regularMarketPrice")) if info else None
        except Exception:
            rate = None
        if rate and rate > 0:
            fresh[currency] = merged[currency] = rate
            dates[currency] = observed_at
        else:
            failed.append(currency)
    return merged, fresh, dates, failed, (legacy_date if failed else observed_at)


def refreshed_profile(symbol, info, observed_at, fresh_fx, previous=None):
    """Normalize with fresh FX, retaining an old profile if FX alone would degrade it."""
    profile = normalize_profile(symbol, info, observed_at, fresh_fx)
    previous = previous if isinstance(previous, dict) else {}
    dependencies = (("market_cap_usd", "marketCap", profile.get("currency")),
                    ("revenue_usd", "totalRevenue", profile.get("financial_currency")))
    for output, source, currency in dependencies:
        if (previous.get(output) is not None and profile.get(output) is None
                and number(info.get(source)) is not None and currency and currency not in fresh_fx):
            return None
    return profile


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
    fx, fresh_fx, fx_dates, failed_fx, fx_observed_at = refresh_fx(registry, now)
    print(f"FX refreshed {len(FX_CURRENCIES) - len(failed_fx)}/{len(FX_CURRENCIES)}"
          + (f"; unavailable: {','.join(failed_fx)}" if failed_fx else ""), flush=True)
    def fetch(symbol):
        _, info = fetch_ticker_info(symbol, max_retries=2)
        if not info or not info.get("industry") or not number(info.get("marketCap")):
            return symbol, None
        return symbol, refreshed_profile(symbol, info, now, fresh_fx, profiles.get(symbol))
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
    full_refreshed_at = now if not args.symbols and not args.limit and not failed_fx and len(failures) < len(symbols) / 2 else registry.get("full_refreshed_at")
    output = {"schema_version": 1, "refreshed_at": now, "full_refreshed_at": full_refreshed_at,
              "fx": fx, "fx_observed_at": fx_observed_at,
              "fx_observed_at_by_currency": dict(sorted(fx_dates.items())), "failed_fx": sorted(failed_fx),
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
