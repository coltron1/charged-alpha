#!/usr/bin/env python3
"""Audit Charged Alpha video JSON-LD before deploys."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from app import (  # noqa: E402
    _canonical_url,
    _shows_page_structured_data,
    _stock_page_structured_data,
    _video_object_schema,
    build_show_library,
    load_shows_catalog,
)


def iter_video_objects(value):
    if isinstance(value, dict):
        if value.get("@type") == "VideoObject":
            yield value
        for child in value.values():
            yield from iter_video_objects(child)
    elif isinstance(value, list):
        for child in value:
            yield from iter_video_objects(child)


def catalog_items_missing_dates(shows_data):
    missing = []
    for episode in shows_data.get("episodes", []):
        if episode.get("youtube_url") and not episode.get("published_at"):
            missing.append(
                f"episode {episode.get('ticker')} {episode.get('quarter')}: {episode.get('youtube_url')}"
            )

    for section in shows_data.get("video_sections", []) or []:
        for video in section.get("videos", []) or []:
            if video.get("youtube_url") and not video.get("published_at"):
                missing.append(
                    f"{section.get('title')} video: {video.get('youtube_url')}"
                )
    return missing


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate Charged Alpha VideoObject JSON-LD.")
    parser.add_argument(
        "--strict-catalog-dates",
        action="store_true",
        help="Fail when any catalog YouTube URL is missing published_at, even if schema skips it.",
    )
    args = parser.parse_args()

    shows_data = load_shows_catalog()
    show_library = build_show_library(
        shows_data.get("episodes", []),
        shows_data.get("stock_metadata", {}),
    )
    errors = []

    if _video_object_schema("Missing date", "https://www.youtube.com/watch?v=dQw4w9WgXcQ", "") is not None:
        errors.append("_video_object_schema emitted VideoObject without uploadDate.")

    for path in ("/", "/shows"):
        video_objects = list(iter_video_objects(_shows_page_structured_data(path, show_library)))
        if video_objects:
            errors.append(f"{path} emitted {len(video_objects)} VideoObject item(s); library pages should emit collection schema only.")

    stock_video_count = 0
    for stock in show_library.get("stocks", []):
        seo_meta = {
            "title": f"{stock['company']} ({stock['ticker']}) Stock Library - Charged Alpha",
            "description": f"Charged Alpha earnings episodes for {stock['company']} ({stock['ticker']}).",
            "canonical_url": _canonical_url(f"/shows/{stock['slug']}"),
        }
        video_objects = list(iter_video_objects(_stock_page_structured_data(stock, seo_meta)))
        stock_video_count += len(video_objects)
        if len(video_objects) > 1:
            errors.append(f"/shows/{stock['slug']} emitted {len(video_objects)} VideoObject items; expected at most one primary video.")
        for video in video_objects:
            missing_fields = [
                field
                for field in ("name", "thumbnailUrl", "uploadDate")
                if not video.get(field)
            ]
            if missing_fields:
                errors.append(
                    f"/shows/{stock['slug']} VideoObject missing {', '.join(missing_fields)}."
                )

    missing_dates = catalog_items_missing_dates(shows_data)
    if missing_dates:
        print("Catalog YouTube URLs missing published_at:")
        for item in missing_dates:
            print(f"  - {item}")
        if args.strict_catalog_dates:
            errors.append(f"{len(missing_dates)} catalog YouTube URL(s) are missing published_at.")

    if errors:
        print("Video structured data audit failed:")
        for error in errors:
            print(f"  - {error}")
        return 1

    print(
        "Video structured data audit passed: "
        f"{stock_video_count} stock VideoObject item(s), all with uploadDate."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
