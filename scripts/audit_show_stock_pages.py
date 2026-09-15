#!/usr/bin/env python3
"""Validate the generated Charged Alpha stock-page library without web calls."""

from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app import (  # noqa: E402
    _episode_published_sort_key,
    _is_placeholder_show_company,
    _is_placeholder_show_sector,
    build_show_library,
    load_shows_catalog,
)
from app import app as flask_app  # noqa: E402
from stock_research import age_days, number, read_registry  # noqa: E402


def main() -> int:
    catalog = load_shows_catalog()
    library = build_show_library(
        catalog.get("episodes", []),
        catalog.get("stock_metadata", {}),
    )
    stock_metadata = catalog.get("stock_metadata", {})
    errors = []
    warnings = []
    profiles = read_registry().get("profiles", {})
    slugs = set()
    youtube_pages = 0

    for stock in library.get("stocks", []):
        ticker = stock.get("ticker") or ""
        slug = stock.get("slug") or ""
        if not ticker or not slug:
            errors.append(f"Stock page has a blank ticker or slug: {stock!r}")
            continue
        if slug in slugs:
            errors.append(f"Duplicate stock-page slug: {slug}")
        slugs.add(slug)
        if _is_placeholder_show_company(
            stock.get("company"),
            ticker,
            stock.get("company_is_ticker", False),
        ):
            errors.append(f"/shows/{slug} has unresolved company identity")
        if _is_placeholder_show_sector(stock.get("sector")):
            errors.append(f"/shows/{slug} has unresolved sector identity")
        if not stock.get("episodes"):
            errors.append(f"/shows/{slug} has no episode timeline")
            continue

        metadata = stock_metadata.get(ticker, {})
        historical_listing = bool(
            metadata.get("market_data_note") and not metadata.get("yf_symbol")
        )
        profile = profiles.get(stock.get("yf_symbol"))
        if not profile and not historical_listing:
            errors.append(
                f"/shows/{slug} has no dated research profile for {stock.get('yf_symbol')}"
            )
        elif profile:
            if not profile.get("observed_at"):
                errors.append(f"/shows/{slug} profile has no observation date")
            if number(profile.get("price")) is None:
                errors.append(f"/shows/{slug} profile has no current quote")
            if number(profile.get("market_cap_usd")) is None:
                errors.append(f"/shows/{slug} profile has no USD market cap")
            if age_days(profile.get("observed_at")) > 2:
                warnings.append(f"/shows/{slug} snapshot is more than two days old")

        latest_youtube = next(
            (episode for episode in stock["episodes"] if episode.get("youtube_url")),
            None,
        )
        if latest_youtube:
            youtube_pages += 1
            if stock.get("latest_youtube_url") != latest_youtube.get("youtube_url"):
                errors.append(f"/shows/{slug} has a stale latest YouTube link")
            expected = max(
                [episode for episode in stock["episodes"] if episode.get("youtube_url")],
                key=_episode_published_sort_key,
            )
            if latest_youtube.get("youtube_url") != expected.get("youtube_url"):
                errors.append(f"/shows/{slug} does not choose its newest published YouTube episode")

    # Page rendering is provider-free and uses the committed snapshot registry.
    # Render every route so empty profile fallbacks cannot pass unnoticed.
    rendered_pages = 0
    with flask_app.test_client() as client:
        for slug in sorted(slugs):
            response = client.get(f"/shows/{slug}")
            if response.status_code != 200:
                errors.append(f"/shows/{slug} returned HTTP {response.status_code}")
                continue
            body = response.get_data()
            if any(anchor not in body for anchor in (b'id="latest"', b'id="peers"', b'id="financials"', b'id="valuation"', b'id="archive"')):
                errors.append(f"/shows/{slug} omitted a required stock-analysis section")
                continue
            rendered_pages += 1

    if errors:
        print("Stock page catalog audit failed:")
        for error in errors:
            print(f"- {error}")
        return 1

    if warnings:
        print("Stock page catalog audit warnings:")
        for warning in warnings:
            print(f"- {warning}")

    print(
        "Stock page catalog audit passed: "
        f"{len(slugs)} generated pages rendered, {youtube_pages} with a primary YouTube episode, "
        f"{len(profiles)} dated research profiles available."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
