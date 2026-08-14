#!/usr/bin/env python3
"""Validate the generated Charged Alpha stock-page library without web calls."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch


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


def _render_detail(symbol, allow_fetch=True):
    """Deterministic detail fixture used to exercise every dynamic page route."""
    return {
        "info": {
            "symbol": symbol,
            "name": f"{symbol} Holdings",
            "price": 100.0,
            "previous_close": 99.0,
            "change": 1.0,
            "change_pct": 1.01,
            "market_cap": 1_000_000_000,
            "volume": 1_000_000,
            "week_52_low": 75.0,
            "week_52_high": 125.0,
            "forward_pe": 20.0,
            "revenue_growth": 8.0,
            "operating_margin": 15.0,
            "profit_margin": 10.0,
            "return_on_equity": 12.0,
            "fcf_yield": 4.0,
            "debt_to_equity": 50.0,
            "current_ratio": 1.5,
            "target_upside": 10.0,
        },
        "options": [],
    }


def main() -> int:
    catalog = load_shows_catalog()
    library = build_show_library(
        catalog.get("episodes", []),
        catalog.get("stock_metadata", {}),
    )
    errors = []
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

    # The library uses a shared template, so render every generated route with
    # deterministic market data. This catches missing context/template branches
    # without making a thousand outbound Yahoo requests during a catalog audit.
    rendered_pages = 0
    with patch("app._cached_show_stock_detail", side_effect=_render_detail):
        with flask_app.test_client() as client:
            for slug in sorted(slugs):
                response = client.get(f"/shows/{slug}")
                if response.status_code != 200:
                    errors.append(f"/shows/{slug} returned HTTP {response.status_code}")
                    continue
                body = response.get_data()
                if b"Market context for" not in body or b"Episode archive for" not in body:
                    errors.append(f"/shows/{slug} omitted a required stock-analysis section")
                    continue
                rendered_pages += 1

    if errors:
        print("Stock page catalog audit failed:")
        for error in errors:
            print(f"- {error}")
        return 1

    print(
        "Stock page catalog audit passed: "
        f"{len(slugs)} generated pages rendered, {youtube_pages} with a primary YouTube episode."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
