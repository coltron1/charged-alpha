#!/usr/bin/env python3
"""Sync Charged Alpha show links from YouTube, Podbean, and Spotify.

This is a maintenance script, not app runtime code. It keeps the stock episode
catalog current by comparing the public channel/feed exports against the local
JSON catalog and prepending only missing content.
"""

from __future__ import annotations

import argparse
import html
import json
import re
import subprocess
import sys
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path


DEFAULT_CATALOG = Path("data/shows_catalog.json")
DEFAULT_YOUTUBE_CHANNEL = "https://www.youtube.com/@ChargedAlpha"
DEFAULT_YOUTUBE_RSS = "https://www.youtube.com/feeds/videos.xml?channel_id=UC4ZDZpC0OvoN4cCGoUSofuA"
DEFAULT_PODBEAN_FEED = "https://feed.podbean.com/chargedalpha/feed.xml"
DEFAULT_SPOTIFY_SHOW = "https://open.spotify.com/show/72TRXJNznbvrpM72hdVN3b"

STOCK_TITLE_RE = re.compile(r"^([A-Z][A-Z0-9.\-]{0,12}) Stock:\s+(.+)$", re.IGNORECASE)
QUARTER_RE = re.compile(r"\(?\b(Q[1-4]\s+(?:FY)?\d{4})\b\)?", re.IGNORECASE)
SPOTIFY_EPISODE_RE = re.compile(
    r'<a href="(/episode/[^"]+)"><h4[^>]*data-testid="episodeTitle"[^>]*>(.*?)</h4>',
    re.DOTALL,
)
YOUTUBE_PAGE_DATE_RE = re.compile(
    r'"(?:publishDate|uploadDate|datePublished)":"([^"]+)"'
)
TICKER_RE = re.compile(r"\b[A-Z][A-Z0-9.]{1,5}\b")
TICKER_STOPWORDS = {
    "ADJ",
    "AI",
    "ARR",
    "BPS",
    "CEO",
    "EPS",
    "EV",
    "FCF",
    "FY",
    "GAAP",
    "IPO",
    "QOQ",
    "THE",
    "USA",
    "YOY",
}


@dataclass(frozen=True)
class VideoRow:
    video_id: str
    url: str
    title: str


@dataclass(frozen=True)
class PodcastItem:
    title: str
    url: str
    published_at: str


def fetch_text(url: str) -> str:
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "charged-alpha-catalog-sync/1.0"},
    )
    with urllib.request.urlopen(request, timeout=45) as response:
        return response.read().decode("utf-8")


def run_ytdlp_flat(url: str) -> list[VideoRow]:
    command = [
        sys.executable,
        "-m",
        "yt_dlp",
        "--flat-playlist",
        "--print",
        "%(id)s\t%(webpage_url)s\t%(title)s",
        url,
    ]
    try:
        result = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
            timeout=180,
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
        stderr = getattr(exc, "stderr", "") or ""
        raise SystemExit(
            "Unable to read YouTube with yt-dlp. Install/update it with:\n"
            "  python3 -m pip install --user -U yt-dlp\n\n"
            f"yt-dlp error:\n{stderr.strip()}"
        ) from exc

    rows: list[VideoRow] = []
    for line in result.stdout.splitlines():
        parts = line.split("\t", 2)
        if len(parts) != 3:
            continue
        rows.append(VideoRow(video_id=parts[0], url=parts[1], title=parts[2]))
    return rows


def parse_iso_from_rss_date(value: str) -> str:
    if not value:
        return ""
    try:
        return parsedate_to_datetime(value).astimezone(timezone.utc).isoformat()
    except (TypeError, ValueError):
        return ""


def parse_youtube_rss_dates(feed_xml: str) -> dict[str, str]:
    dates: dict[str, str] = {}
    root = ET.fromstring(feed_xml)
    ns = {"a": "http://www.w3.org/2005/Atom"}
    for entry in root.findall("a:entry", ns):
        link_el = entry.find("a:link", ns)
        if link_el is None:
            continue
        url = link_el.attrib.get("href", "")
        published_at = entry.findtext("a:published", default="", namespaces=ns)
        if url and published_at:
            dates[url] = published_at
    return dates


def parse_ytdlp_upload_date(timestamp: str, upload_date: str) -> str:
    if timestamp and timestamp != "NA":
        try:
            return datetime.fromtimestamp(int(float(timestamp)), tz=timezone.utc).isoformat()
        except (TypeError, ValueError):
            pass
    if upload_date and upload_date != "NA":
        try:
            return datetime.strptime(upload_date, "%Y%m%d").replace(tzinfo=timezone.utc).isoformat()
        except (TypeError, ValueError):
            pass
    return ""


def parse_youtube_page_date(value: str) -> str:
    if not value:
        return ""
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).isoformat()
    except ValueError:
        pass
    try:
        return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc).isoformat()
    except ValueError:
        return ""


def run_ytdlp_dates(rows: list[VideoRow]) -> dict[str, str]:
    missing_rows = [row for row in rows if row.url]
    if not missing_rows:
        return {}

    command = [
        sys.executable,
        "-m",
        "yt_dlp",
        "--skip-download",
        "--ignore-errors",
        "--no-warnings",
        "--print",
        "%(id)s\t%(timestamp)s\t%(upload_date)s",
        *[row.url for row in missing_rows],
    ]
    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        timeout=600,
    )

    dates: dict[str, str] = {}
    for line in result.stdout.splitlines():
        parts = line.split("\t")
        if len(parts) != 3:
            continue
        video_id, timestamp, upload_date = parts
        parsed = parse_ytdlp_upload_date(timestamp, upload_date)
        if video_id and parsed:
            dates[video_id] = parsed
    return dates


def scrape_youtube_page_dates(rows: list[VideoRow]) -> dict[str, str]:
    dates: dict[str, str] = {}
    for row in rows:
        if not row.url:
            continue
        try:
            request = urllib.request.Request(
                row.url,
                headers={"User-Agent": "Mozilla/5.0"},
            )
            page = urllib.request.urlopen(request, timeout=30).read().decode("utf-8", "ignore")
        except Exception:
            continue

        for raw_date in YOUTUBE_PAGE_DATE_RE.findall(page):
            parsed = parse_youtube_page_date(raw_date)
            if parsed:
                dates[row.url] = parsed
                break
    return dates


def parse_stock_title(title: str) -> tuple[str, str] | None:
    match = STOCK_TITLE_RE.match(title.strip())
    if not match:
        return None
    quarter_match = QUARTER_RE.search(match.group(2))
    if not quarter_match:
        return None
    quarter = re.sub(r"\s+", " ", quarter_match.group(1)).strip()
    return match.group(1).upper(), quarter


def normalize_title(title: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", title.lower()).strip()


def parse_podbean_items(feed_xml: str) -> tuple[dict[tuple[str, str], PodcastItem], dict[str, PodcastItem]]:
    root = ET.fromstring(feed_xml)
    channel = root.find("channel")
    if channel is None:
        return {}, {}

    stock_items: dict[tuple[str, str], PodcastItem] = {}
    titled_items: dict[str, PodcastItem] = {}
    for item in channel.findall("item"):
        title = (item.findtext("title") or "").strip()
        url = (item.findtext("link") or "").strip()
        published_at = parse_iso_from_rss_date((item.findtext("pubDate") or "").strip())
        if not title or not url:
            continue

        podcast = PodcastItem(title=title, url=url, published_at=published_at)
        titled_items.setdefault(title, podcast)
        parsed = parse_stock_title(title)
        if parsed:
            stock_items.setdefault(parsed, podcast)
    return stock_items, titled_items


def parse_spotify_items(show_html: str) -> tuple[dict[tuple[str, str], PodcastItem], dict[str, PodcastItem]]:
    stock_items: dict[tuple[str, str], PodcastItem] = {}
    titled_items: dict[str, PodcastItem] = {}

    for href, raw_title in SPOTIFY_EPISODE_RE.findall(show_html):
        title = re.sub(r"<[^>]+>", "", raw_title)
        title = html.unescape(title).strip()
        if not title:
            continue

        url = f"https://open.spotify.com{href}"
        podcast = PodcastItem(title=title, url=url, published_at="")
        titled_items.setdefault(title, podcast)
        parsed = parse_stock_title(title)
        if parsed:
            stock_items.setdefault(parsed, podcast)

    return stock_items, titled_items


def collect_existing_urls(catalog: dict) -> set[str]:
    urls: set[str] = set()
    for episode in catalog.get("episodes", []):
        for key in ("youtube_url", "spotify_url", "podbean_url", "apple_url", "amazon_url"):
            if episode.get(key):
                urls.add(episode[key])
    for section in catalog.get("video_sections", []) or []:
        for video in section.get("videos", []) or []:
            for key in ("youtube_url", "spotify_url", "podbean_url"):
                if video.get(key):
                    urls.add(video[key])
    return urls


def collect_stock_metadata(catalog: dict) -> tuple[dict[str, str], dict[str, str]]:
    companies: dict[str, str] = {}
    sectors: dict[str, str] = {}
    for episode in catalog.get("episodes", []):
        ticker = (episode.get("ticker") or "").upper().strip()
        if not ticker:
            continue
        companies.setdefault(ticker, episode.get("company") or ticker)
        sectors.setdefault(ticker, episode.get("sector") or "Unclassified")
    return companies, sectors


def newest_unlinked(rows: list[VideoRow], existing_urls: set[str], scan_all: bool) -> list[VideoRow]:
    pending: list[VideoRow] = []
    for row in rows:
        if row.url in existing_urls:
            if not scan_all:
                break
            continue
        pending.append(row)
    return pending


def extract_tickers(title: str) -> list[str]:
    seen: set[str] = set()
    tickers: list[str] = []
    for match in TICKER_RE.findall(title):
        ticker = match.upper()
        if ticker in TICKER_STOPWORDS or ticker.startswith("Q") and ticker[1:].isdigit():
            continue
        if ticker not in seen:
            seen.add(ticker)
            tickers.append(ticker)
    return tickers[:8]


def find_section(catalog: dict, title: str) -> dict | None:
    for section in catalog.get("video_sections", []) or []:
        if section.get("title") == title:
            return section
    return None


def build_stock_episode(
    row: VideoRow,
    key: tuple[str, str],
    podbean_items: dict[tuple[str, str], PodcastItem],
    spotify_items: dict[tuple[str, str], PodcastItem],
    youtube_dates: dict[str, str],
    companies: dict[str, str],
    sectors: dict[str, str],
    duplicate_stock_keys: set[tuple[str, str]],
    used_podbean_urls: set[str],
    used_spotify_urls: set[str],
) -> dict:
    ticker, quarter = key
    podbean = podbean_items.get(key)
    spotify = spotify_items.get(key)

    if key in duplicate_stock_keys:
        row_title = normalize_title(row.title)
        if podbean and row_title != normalize_title(podbean.title):
            podbean = None
        if spotify and row_title != normalize_title(spotify.title):
            spotify = None

    if podbean and podbean.url in used_podbean_urls:
        podbean = None
    if spotify and spotify.url in used_spotify_urls:
        spotify = None

    if podbean:
        used_podbean_urls.add(podbean.url)
    if spotify:
        used_spotify_urls.add(spotify.url)

    podcast_title = podbean.title if podbean else spotify.title if spotify else row.title
    podcast_date = podbean.published_at if podbean else ""
    return {
        "ticker": ticker,
        "company": companies.get(ticker, ticker),
        "sector": sectors.get(ticker, "Unclassified"),
        "quarter": quarter,
        "episode_number": "",
        "title": podcast_title,
        "published_at": podcast_date or youtube_dates.get(row.url, ""),
        "status": "youtube_podcast_complete" if podbean or spotify else "youtube_complete",
        "youtube_url": row.url,
        "spotify_url": spotify.url if spotify else "",
        "apple_url": "",
        "google_url": "",
        "iheart_url": "",
        "podbean_url": podbean.url if podbean else "",
        "has_episode": True,
        "amazon_url": "",
    }


def enrich_new_episode_metadata(episodes: list[dict]) -> None:
    tickers = sorted(
        {
            (episode.get("ticker") or "").upper()
            for episode in episodes
            if episode.get("ticker")
            and (
                episode.get("company") == episode.get("ticker")
                or episode.get("sector") == "Unclassified"
            )
        }
    )
    if not tickers:
        return

    try:
        from yf_utils import fetch_ticker_info
    except Exception:
        return

    print("Fetching company metadata for new tickers...")
    metadata: dict[str, dict] = {}
    for ticker in tickers:
        _ticker_obj, info = fetch_ticker_info(ticker, max_retries=1)
        if info:
            metadata[ticker] = info

    for episode in episodes:
        ticker = (episode.get("ticker") or "").upper()
        info = metadata.get(ticker) or {}
        company = info.get("longName") or info.get("shortName")
        sector = info.get("sector")
        if company and episode.get("company") == ticker:
            episode["company"] = company
        if sector and episode.get("sector") == "Unclassified":
            episode["sector"] = sector


def build_extra_video(
    row: VideoRow,
    section: str,
    youtube_dates: dict[str, str],
    titled_podcasts: dict[str, PodcastItem],
    titled_spotify: dict[str, PodcastItem],
    summary: str,
) -> dict:
    podcast = titled_podcasts.get(row.title)
    spotify = titled_spotify.get(row.title)
    return {
        "title": row.title,
        "youtube_url": row.url,
        "spotify_url": spotify.url if spotify else "",
        "podbean_url": podcast.url if podcast else "",
        "published_at": (podcast.published_at if podcast else "") or youtube_dates.get(row.url, ""),
        "section": section,
        "tickers": extract_tickers(row.title),
        "summary": summary,
    }


def sync_catalog(args: argparse.Namespace) -> dict:
    catalog_path = Path(args.catalog)
    catalog = json.loads(catalog_path.read_text())
    existing_urls = collect_existing_urls(catalog)
    companies, sectors = collect_stock_metadata(catalog)

    print("Fetching YouTube videos...")
    videos = run_ytdlp_flat(f"{args.youtube_channel.rstrip('/')}/videos")
    print("Fetching YouTube Shorts...")
    shorts = run_ytdlp_flat(f"{args.youtube_channel.rstrip('/')}/shorts")
    print("Fetching YouTube RSS dates...")
    youtube_dates = parse_youtube_rss_dates(fetch_text(args.youtube_rss))
    print("Fetching Podbean feed...")
    podcast_items, titled_podcasts = parse_podbean_items(fetch_text(args.podbean_feed))
    print("Fetching Spotify show page...")
    try:
        spotify_items, titled_spotify = parse_spotify_items(fetch_text(args.spotify_show))
    except Exception as exc:
        print(f"Warning: unable to read Spotify show page: {exc}", file=sys.stderr)
        spotify_items, titled_spotify = {}, {}

    pending_videos = newest_unlinked(videos, existing_urls, args.scan_all)
    pending_shorts = newest_unlinked(shorts, existing_urls, args.scan_all)
    pending_without_rss_dates = [
        row
        for row in [*pending_videos, *pending_shorts]
        if not youtube_dates.get(row.url)
    ]
    if pending_without_rss_dates:
        print("Fetching upload dates for pending YouTube rows...")
        metadata_dates = run_ytdlp_dates(pending_without_rss_dates)
        for row in pending_without_rss_dates:
            if metadata_dates.get(row.video_id):
                youtube_dates[row.url] = metadata_dates[row.video_id]
        still_missing_dates = [
            row
            for row in pending_without_rss_dates
            if not youtube_dates.get(row.url)
        ]
        if still_missing_dates:
            print("Scraping YouTube pages for remaining upload dates...")
            page_dates = scrape_youtube_page_dates(still_missing_dates)
            youtube_dates.update(page_dates)

    new_episodes: list[dict] = []
    new_explainers: list[dict] = []
    pending_stock_keys = [
        parsed
        for row in pending_videos
        if (parsed := parse_stock_title(row.title)) is not None
    ]
    duplicate_stock_keys = {
        key
        for key in pending_stock_keys
        if pending_stock_keys.count(key) > 1
    }
    used_podbean_urls = set(existing_urls)
    used_spotify_urls = set(existing_urls)
    for row in pending_videos:
        parsed = parse_stock_title(row.title)
        if parsed:
            new_episodes.append(
                build_stock_episode(
                    row,
                    parsed,
                    podcast_items,
                    spotify_items,
                    youtube_dates,
                    companies,
                    sectors,
                    duplicate_stock_keys,
                    used_podbean_urls,
                    used_spotify_urls,
                )
            )
        else:
            new_explainers.append(
                build_extra_video(
                    row,
                    "Market and Sector Explainers",
                    youtube_dates,
                    titled_podcasts,
                    titled_spotify,
                    "A special-topic Charged Alpha video connected to current stock and earnings research.",
                )
            )

    new_shorts = [
        build_extra_video(
            row,
            "Shorts and Clips",
            youtube_dates,
            titled_podcasts,
            titled_spotify,
            "A fast Charged Alpha clip pointing viewers into the latest earnings coverage.",
        )
        for row in pending_shorts
    ]

    new_episodes.sort(key=lambda episode: episode.get("published_at") or "", reverse=True)
    enrich_new_episode_metadata(new_episodes)
    catalog["episodes"] = new_episodes + catalog.get("episodes", [])

    explainer_section = find_section(catalog, "Market and Sector Explainers")
    if explainer_section is not None:
        explainer_section.setdefault("videos", [])
        explainer_section["videos"] = new_explainers + explainer_section["videos"]

    shorts_section = find_section(catalog, "Shorts and Clips")
    if shorts_section is not None:
        shorts_section.setdefault("videos", [])
        shorts_section["videos"] = new_shorts + shorts_section["videos"]

    catalog["last_synced_at"] = datetime.now().astimezone().isoformat(timespec="seconds")

    summary = {
        "stock_episodes": len(new_episodes),
        "explainers": len(new_explainers),
        "shorts": len(new_shorts),
        "stock_tickers": [episode["ticker"] for episode in new_episodes],
        "catalog_path": str(catalog_path),
    }

    if args.dry_run:
        print(json.dumps(summary, indent=2))
        return summary

    catalog_path.write_text(json.dumps(catalog, indent=2, ensure_ascii=False) + "\n")
    print(json.dumps(summary, indent=2))
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync Charged Alpha show catalog links.")
    parser.add_argument("--catalog", default=str(DEFAULT_CATALOG), help="Path to shows_catalog.json.")
    parser.add_argument("--youtube-channel", default=DEFAULT_YOUTUBE_CHANNEL)
    parser.add_argument("--youtube-rss", default=DEFAULT_YOUTUBE_RSS)
    parser.add_argument("--podbean-feed", default=DEFAULT_PODBEAN_FEED)
    parser.add_argument("--spotify-show", default=DEFAULT_SPOTIFY_SHOW)
    parser.add_argument(
        "--scan-all",
        action="store_true",
        help="Scan the whole YouTube tab instead of stopping at the first already-linked upload.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print additions without writing the catalog.")
    return parser.parse_args()


if __name__ == "__main__":
    sync_catalog(parse_args())
