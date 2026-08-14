#!/usr/bin/env python3
"""Sync Charged Alpha show links from YouTube, Podbean, Spotify, and Apple Podcasts.

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


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


DEFAULT_CATALOG = Path("data/shows_catalog.json")
DEFAULT_YOUTUBE_CHANNEL = "https://www.youtube.com/@ChargedAlpha"
DEFAULT_YOUTUBE_RSS = "https://www.youtube.com/feeds/videos.xml?channel_id=UC4ZDZpC0OvoN4cCGoUSofuA"
DEFAULT_PODBEAN_FEED = "https://feed.podbean.com/chargedalpha/feed.xml"
DEFAULT_SPOTIFY_SHOW = "https://open.spotify.com/show/72TRXJNznbvrpM72hdVN3b"
DEFAULT_APPLE_LOOKUP = "https://itunes.apple.com/lookup?id=1891551459&entity=podcastEpisode&limit=200&country=us"

TICKER_STOCK_TITLE_RE = re.compile(r"^([A-Z][A-Z0-9.\-]{0,12}) Stock:\s+(.+)$")
COMPANY_TICKER_STOCK_TITLE_RE = re.compile(
    r"^.+? Stock \(([A-Z][A-Z0-9.\-]{0,12})\):\s+(.+)$"
)
PERIOD_RE = re.compile(r"\b(Q[1-4]\s+(?:FY)?\d{4}|FY\s?\d{4})\b", re.IGNORECASE)
PODCAST_IDENTITY_RE = re.compile(
    r"\(([A-Z][A-Z0-9.\-]{0,12})\)\s+"
    r"(Q[1-4]\s+(?:FY)?\d{4}|FY\s?\d{4})\b"
)
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
    ticker: str = ""
    quarter: str = ""
    guid: str = ""


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


def normalize_period(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip().upper()


def parse_stock_title(title: str, fallback_text: str = "") -> tuple[str, str] | None:
    clean_title = title.strip()
    ticker = ""

    ticker_match = TICKER_STOCK_TITLE_RE.match(clean_title)
    if ticker_match:
        ticker = ticker_match.group(1)
    else:
        company_ticker_match = COMPANY_TICKER_STOCK_TITLE_RE.match(clean_title)
        if company_ticker_match:
            ticker = company_ticker_match.group(1)

    period_match = PERIOD_RE.search(clean_title)
    period = normalize_period(period_match.group(1)) if period_match else ""

    metadata_match = PODCAST_IDENTITY_RE.search(fallback_text)
    if metadata_match:
        ticker = ticker or metadata_match.group(1)
        period = period or normalize_period(metadata_match.group(2))

    if not ticker:
        return None
    return ticker.upper(), period


def normalize_title(title: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", title.lower()).strip()


def find_titled_match(
    title: str,
    titled_items: dict[str, PodcastItem],
) -> PodcastItem | None:
    normalized = normalize_title(title)
    exact = titled_items.get(normalized)
    if exact:
        return exact
    if len(normalized) < 36:
        return None

    prefix_matches = [
        (candidate_title, item)
        for candidate_title, item in titled_items.items()
        if candidate_title.startswith(normalized)
    ]
    if not prefix_matches:
        return None
    return min(prefix_matches, key=lambda match: len(match[0]))[1]


def resolve_stock_key(
    title: str,
    titled_podcasts: dict[str, PodcastItem],
) -> tuple[str, str] | None:
    parsed = parse_stock_title(title)
    podcast = find_titled_match(title, titled_podcasts)

    if podcast and podcast.ticker:
        parsed_period = parsed[1] if parsed else ""
        return podcast.ticker, podcast.quarter or parsed_period or "Current"
    if parsed:
        return parsed[0], parsed[1] or "Current"
    return None


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

        description = html.unescape(
            re.sub(r"<[^>]+>", " ", item.findtext("description") or "")
        )
        parsed = parse_stock_title(title, description)
        podcast = PodcastItem(
            title=title,
            url=url,
            published_at=published_at,
            ticker=parsed[0] if parsed else "",
            quarter=parsed[1] if parsed else "",
            guid=(item.findtext("guid") or "").strip(),
        )
        titled_items.setdefault(normalize_title(title), podcast)
        if parsed:
            stock_items.setdefault(parsed, podcast)
    return stock_items, titled_items


def parse_apple_items(payload: str) -> tuple[dict[str, PodcastItem], dict[str, PodcastItem]]:
    """Return Apple Podcast episodes keyed by RSS GUID and normalized title."""
    try:
        results = json.loads(payload).get("results", [])
    except json.JSONDecodeError:
        return {}, {}

    guid_items: dict[str, PodcastItem] = {}
    titled_items: dict[str, PodcastItem] = {}
    for item in results:
        if item.get("kind") != "podcast-episode":
            continue

        title = (item.get("trackName") or "").strip()
        url = (item.get("trackViewUrl") or "").strip()
        if not title or not url:
            continue

        parsed = parse_stock_title(title)
        podcast = PodcastItem(
            title=title,
            url=url,
            published_at=(item.get("releaseDate") or "").strip(),
            ticker=parsed[0] if parsed else "",
            quarter=parsed[1] if parsed else "",
            guid=(item.get("episodeGuid") or "").strip(),
        )
        titled_items.setdefault(normalize_title(title), podcast)
        if podcast.guid:
            guid_items.setdefault(podcast.guid, podcast)

    return guid_items, titled_items


def parse_spotify_items(show_html: str) -> tuple[dict[tuple[str, str], PodcastItem], dict[str, PodcastItem]]:
    stock_items: dict[tuple[str, str], PodcastItem] = {}
    titled_items: dict[str, PodcastItem] = {}

    for href, raw_title in SPOTIFY_EPISODE_RE.findall(show_html):
        title = re.sub(r"<[^>]+>", "", raw_title)
        title = html.unescape(title).strip()
        if not title:
            continue

        url = f"https://open.spotify.com{href}"
        parsed = parse_stock_title(title)
        podcast = PodcastItem(
            title=title,
            url=url,
            published_at="",
            ticker=parsed[0] if parsed else "",
            quarter=parsed[1] if parsed else "",
        )
        titled_items.setdefault(normalize_title(title), podcast)
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


def is_placeholder_company(value: object, ticker: str, allow_ticker_name: bool = False) -> bool:
    text = str(value or "").strip()
    return not text or (not allow_ticker_name and text.upper() == ticker.upper())


def is_placeholder_sector(value: object) -> bool:
    return not str(value or "").strip() or str(value).strip().lower() == "unclassified"


def normalized_stock_metadata(catalog: dict) -> dict[str, dict]:
    profiles = catalog.get("stock_metadata") or {}
    if not isinstance(profiles, dict):
        return {}
    return {
        str(ticker).upper(): dict(profile)
        for ticker, profile in profiles.items()
        if isinstance(profile, dict) and str(ticker).strip()
    }


def _set_stock_profile_value(profiles: dict[str, dict], ticker: str, key: str, value: object) -> bool:
    if value in (None, ""):
        return False
    profile = dict(profiles.get(ticker) or {})
    if profile.get(key) == value:
        return False
    profile[key] = value
    profiles[ticker] = profile
    return True


def profile_has_resolved_company(profile: dict, ticker: str) -> bool:
    return not is_placeholder_company(
        profile.get("company"),
        ticker,
        bool(profile.get("company_is_ticker")),
    )


def metadata_published_at(value: object) -> datetime:
    """Normalize catalog dates before choosing the newest historical metadata."""
    text = str(value or "").strip()
    if not text:
        return datetime.min.replace(tzinfo=timezone.utc)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return datetime.min.replace(tzinfo=timezone.utc)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def collect_stock_metadata(catalog: dict) -> tuple[dict[str, str], dict[str, str]]:
    """Resolve stable identity from explicit profiles, then valid episode history.

    The catalog is prepended on every sync, so ``setdefault`` made a newly added
    placeholder overwrite richer historical metadata. Profiles are canonical;
    otherwise prefer the newest non-placeholder episode field.
    """
    profiles = normalized_stock_metadata(catalog)
    companies: dict[str, str] = {}
    sectors: dict[str, str] = {}
    company_dates: dict[str, datetime] = {}
    sector_dates: dict[str, datetime] = {}

    for ticker, profile in profiles.items():
        company = profile.get("company")
        sector = profile.get("sector")
        if profile_has_resolved_company(profile, ticker):
            companies[ticker] = str(company).strip()
        if not is_placeholder_sector(sector):
            sectors[ticker] = str(sector).strip()

    for episode in catalog.get("episodes", []):
        ticker = (episode.get("ticker") or "").upper().strip()
        if not ticker:
            continue
        published_at = metadata_published_at(episode.get("published_at"))
        company = episode.get("company")
        sector = episode.get("sector")
        if (
            not is_placeholder_company(company, ticker)
            and (
                ticker not in companies
                or published_at > company_dates.get(ticker, datetime.min.replace(tzinfo=timezone.utc))
            )
        ):
            companies[ticker] = str(company).strip()
            company_dates[ticker] = published_at
        if (
            not is_placeholder_sector(sector)
            and (
                ticker not in sectors
                or published_at > sector_dates.get(ticker, datetime.min.replace(tzinfo=timezone.utc))
            )
        ):
            sectors[ticker] = str(sector).strip()
            sector_dates[ticker] = published_at

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
    titled_podcasts: dict[str, PodcastItem],
    titled_spotify: dict[str, PodcastItem],
    apple_items_by_guid: dict[str, PodcastItem],
    titled_apple: dict[str, PodcastItem],
    youtube_dates: dict[str, str],
    companies: dict[str, str],
    sectors: dict[str, str],
    duplicate_stock_keys: set[tuple[str, str]],
    used_podbean_urls: set[str],
    used_spotify_urls: set[str],
) -> dict:
    ticker, quarter = key
    podbean = find_titled_match(row.title, titled_podcasts) or podbean_items.get(key)
    spotify = find_titled_match(row.title, titled_spotify) or spotify_items.get(key)

    if key in duplicate_stock_keys:
        row_title = normalize_title(row.title)
        if podbean and not normalize_title(podbean.title).startswith(row_title):
            podbean = None
        if spotify and not normalize_title(spotify.title).startswith(row_title):
            spotify = None

    if podbean and podbean.url in used_podbean_urls:
        podbean = None
    if spotify and spotify.url in used_spotify_urls:
        spotify = None

    if podbean:
        used_podbean_urls.add(podbean.url)
    if spotify:
        used_spotify_urls.add(spotify.url)

    apple = (
        apple_items_by_guid.get(podbean.guid)
        if podbean and podbean.guid
        else find_titled_match(row.title, titled_apple)
    )

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
        "apple_url": apple.url if apple else "",
        "google_url": "",
        "iheart_url": "",
        "podbean_url": podbean.url if podbean else "",
        "has_episode": True,
        "amazon_url": "",
    }


def enrich_new_episode_metadata(
    episodes: list[dict],
    stock_metadata: dict[str, dict] | None = None,
) -> dict[str, int]:
    stock_metadata = stock_metadata if stock_metadata is not None else {}
    tickers = sorted(
        {
            (episode.get("ticker") or "").upper()
            for episode in episodes
            if episode.get("ticker")
            and (
                is_placeholder_company(episode.get("company"), episode.get("ticker") or "")
                or is_placeholder_sector(episode.get("sector"))
            )
        }
    )
    if not tickers:
        return {"lookups": 0, "profiles_updated": 0}

    try:
        from yf_utils import fetch_ticker_info
    except Exception:
        return {"lookups": 0, "profiles_updated": 0}

    print("Fetching company metadata for new tickers...")
    metadata: dict[str, dict] = {}
    for ticker in tickers:
        _ticker_obj, info = fetch_ticker_info(ticker, max_retries=2)
        if info:
            metadata[ticker] = info

    profiles_updated = 0
    for episode in episodes:
        ticker = (episode.get("ticker") or "").upper()
        info = metadata.get(ticker) or {}
        company = info.get("longName") or info.get("shortName")
        sector = info.get("sector")
        if company and is_placeholder_company(episode.get("company"), ticker):
            episode["company"] = company
        if sector and is_placeholder_sector(episode.get("sector")):
            episode["sector"] = sector
        if company and not is_placeholder_company(company, ticker):
            profiles_updated += int(_set_stock_profile_value(stock_metadata, ticker, "company", company))
        if sector and not is_placeholder_sector(sector):
            profiles_updated += int(_set_stock_profile_value(stock_metadata, ticker, "sector", sector))

    return {"lookups": len(tickers), "profiles_updated": profiles_updated}


def refresh_catalog_stock_metadata(catalog: dict, metadata_limit: int = 0) -> dict[str, int]:
    """Persist canonical company/sector identity for existing catalog pages.

    This is intentionally opt-in because a first repair can require many Yahoo
    lookups. Once profiles are stored, future nightly syncs only fetch metadata
    for truly new tickers.
    """
    profiles = normalized_stock_metadata(catalog)
    companies, sectors = collect_stock_metadata(catalog)
    tickers = sorted(
        {
            (episode.get("ticker") or "").upper().strip()
            for episode in catalog.get("episodes", [])
            if (episode.get("ticker") or "").strip()
        }
    )

    updated = 0
    for ticker in tickers:
        company = companies.get(ticker)
        sector = sectors.get(ticker)
        if company and not is_placeholder_company(company, ticker):
            updated += int(_set_stock_profile_value(profiles, ticker, "company", company))
        if sector and not is_placeholder_sector(sector):
            updated += int(_set_stock_profile_value(profiles, ticker, "sector", sector))

    unresolved = [
        ticker
        for ticker in tickers
        if not profile_has_resolved_company(profiles.get(ticker) or {}, ticker)
        or is_placeholder_sector((profiles.get(ticker) or {}).get("sector"))
    ]
    selected = unresolved[:metadata_limit] if metadata_limit > 0 else unresolved

    lookups = 0
    if selected:
        try:
            from yf_utils import fetch_ticker_info
        except Exception:
            fetch_ticker_info = None

        if fetch_ticker_info is not None:
            print(f"Refreshing company metadata for {len(selected)} existing ticker(s)...")
            for index, ticker in enumerate(selected, start=1):
                _ticker_obj, info = fetch_ticker_info(ticker, max_retries=1)
                lookups += 1
                info = info or {}
                company = info.get("longName") or info.get("shortName")
                sector = info.get("sector")
                if company and not is_placeholder_company(company, ticker):
                    updated += int(_set_stock_profile_value(profiles, ticker, "company", company))
                if sector and not is_placeholder_sector(sector):
                    updated += int(_set_stock_profile_value(profiles, ticker, "sector", sector))
                if index % 25 == 0 or index == len(selected):
                    print(f"  Metadata checked: {index}/{len(selected)}")

    if profiles:
        catalog["stock_metadata"] = dict(sorted(profiles.items()))

    unresolved_after = sum(
        1
        for ticker in tickers
        if not profile_has_resolved_company(profiles.get(ticker) or {}, ticker)
        or is_placeholder_sector((profiles.get(ticker) or {}).get("sector"))
    )
    return {
        "profiles": len(profiles),
        "lookups": lookups,
        "updated": updated,
        "unresolved": unresolved_after,
    }


def build_extra_video(
    row: VideoRow,
    section: str,
    youtube_dates: dict[str, str],
    titled_podcasts: dict[str, PodcastItem],
    titled_spotify: dict[str, PodcastItem],
    apple_items_by_guid: dict[str, PodcastItem],
    titled_apple: dict[str, PodcastItem],
    summary: str,
) -> dict:
    podcast = find_titled_match(row.title, titled_podcasts)
    spotify = find_titled_match(row.title, titled_spotify)
    apple = (
        apple_items_by_guid.get(podcast.guid)
        if podcast and podcast.guid
        else find_titled_match(row.title, titled_apple)
    )
    return {
        "title": row.title,
        "youtube_url": row.url,
        "spotify_url": spotify.url if spotify else "",
        "podbean_url": podcast.url if podcast else "",
        "apple_url": apple.url if apple else "",
        "published_at": (podcast.published_at if podcast else "") or youtube_dates.get(row.url, ""),
        "section": section,
        "tickers": extract_tickers(row.title),
        "summary": summary,
    }


def backfill_catalog_platform_links(
    catalog: dict,
    titled_podcasts: dict[str, PodcastItem],
    titled_spotify: dict[str, PodcastItem],
    apple_items_by_guid: dict[str, PodcastItem],
    titled_apple: dict[str, PodcastItem],
) -> dict[str, int]:
    """Fill only links confirmed by the public platform feeds already in use."""
    podbean_by_url = {item.url: item for item in titled_podcasts.values()}
    updated = {"podbean": 0, "spotify": 0, "apple": 0}

    for episode in catalog.get("episodes", []):
        title = episode.get("title") or ""
        podbean = podbean_by_url.get(episode.get("podbean_url") or "")
        if not podbean:
            podbean = find_titled_match(title, titled_podcasts)
            if podbean and not episode.get("podbean_url"):
                episode["podbean_url"] = podbean.url
                updated["podbean"] += 1

        if not episode.get("spotify_url"):
            spotify = find_titled_match(title, titled_spotify)
            if spotify:
                episode["spotify_url"] = spotify.url
                updated["spotify"] += 1

        if not episode.get("apple_url"):
            apple = (
                apple_items_by_guid.get(podbean.guid)
                if podbean and podbean.guid
                else find_titled_match(title, titled_apple)
            )
            if apple:
                episode["apple_url"] = apple.url
                updated["apple"] += 1

    for section in catalog.get("video_sections", []) or []:
        for video in section.get("videos", []) or []:
            title = video.get("title") or ""
            podbean = podbean_by_url.get(video.get("podbean_url") or "")
            if not podbean:
                podbean = find_titled_match(title, titled_podcasts)
                if podbean and not video.get("podbean_url"):
                    video["podbean_url"] = podbean.url
                    updated["podbean"] += 1

            if not video.get("spotify_url"):
                spotify = find_titled_match(title, titled_spotify)
                if spotify:
                    video["spotify_url"] = spotify.url
                    updated["spotify"] += 1

            if not video.get("apple_url"):
                apple = (
                    apple_items_by_guid.get(podbean.guid)
                    if podbean and podbean.guid
                    else find_titled_match(title, titled_apple)
                )
                if apple:
                    video["apple_url"] = apple.url
                    updated["apple"] += 1

    return updated


def sync_catalog(args: argparse.Namespace) -> dict:
    catalog_path = Path(args.catalog)
    catalog = json.loads(catalog_path.read_text())
    catalog_content_before = json.dumps(
        {key: value for key, value in catalog.items() if key != "last_synced_at"},
        sort_keys=True,
        ensure_ascii=False,
    )
    existing_urls = collect_existing_urls(catalog)
    stock_metadata = normalized_stock_metadata(catalog)
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
    print("Fetching Apple Podcasts episode links...")
    try:
        apple_items_by_guid, titled_apple = parse_apple_items(fetch_text(args.apple_lookup))
    except Exception as exc:
        print(f"Warning: unable to read Apple Podcasts: {exc}", file=sys.stderr)
        apple_items_by_guid, titled_apple = {}, {}

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
        if (parsed := resolve_stock_key(row.title, titled_podcasts)) is not None
    ]
    duplicate_stock_keys = {
        key
        for key in pending_stock_keys
        if pending_stock_keys.count(key) > 1
    }
    used_podbean_urls = set(existing_urls)
    used_spotify_urls = set(existing_urls)
    for row in pending_videos:
        parsed = resolve_stock_key(row.title, titled_podcasts)
        if parsed:
            new_episodes.append(
                build_stock_episode(
                    row,
                    parsed,
                    podcast_items,
                    spotify_items,
                    titled_podcasts,
                    titled_spotify,
                    apple_items_by_guid,
                    titled_apple,
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
                    apple_items_by_guid,
                    titled_apple,
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
            apple_items_by_guid,
            titled_apple,
            "A fast Charged Alpha clip pointing viewers into the latest earnings coverage.",
        )
        for row in pending_shorts
    ]

    new_episodes.sort(key=lambda episode: episode.get("published_at") or "", reverse=True)
    new_metadata_summary = enrich_new_episode_metadata(new_episodes, stock_metadata)
    catalog["episodes"] = new_episodes + catalog.get("episodes", [])
    if stock_metadata:
        catalog["stock_metadata"] = dict(sorted(stock_metadata.items()))

    explainer_section = find_section(catalog, "Market and Sector Explainers")
    if explainer_section is not None:
        explainer_section.setdefault("videos", [])
        explainer_section["videos"] = new_explainers + explainer_section["videos"]

    shorts_section = find_section(catalog, "Shorts and Clips")
    if shorts_section is not None:
        shorts_section.setdefault("videos", [])
        shorts_section["videos"] = new_shorts + shorts_section["videos"]

    backfilled_links = backfill_catalog_platform_links(
        catalog,
        titled_podcasts,
        titled_spotify,
        apple_items_by_guid,
        titled_apple,
    )
    metadata_summary = {
        "profiles": len(stock_metadata),
        "lookups": new_metadata_summary["lookups"],
        "updated": new_metadata_summary["profiles_updated"],
        "unresolved": None,
    }
    if args.refresh_stock_metadata:
        metadata_summary = refresh_catalog_stock_metadata(
            catalog,
            metadata_limit=args.metadata_limit,
        )

    catalog_content_changed = json.dumps(
        {key: value for key, value in catalog.items() if key != "last_synced_at"},
        sort_keys=True,
        ensure_ascii=False,
    ) != catalog_content_before
    if catalog_content_changed:
        catalog["last_synced_at"] = datetime.now().astimezone().isoformat(timespec="seconds")

    summary = {
        "stock_episodes": len(new_episodes),
        "explainers": len(new_explainers),
        "shorts": len(new_shorts),
        "stock_tickers": [episode["ticker"] for episode in new_episodes],
        "backfilled_links": backfilled_links,
        "metadata": metadata_summary,
        "catalog_changed": catalog_content_changed,
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
    parser.add_argument("--apple-lookup", default=DEFAULT_APPLE_LOOKUP)
    parser.add_argument(
        "--scan-all",
        action="store_true",
        help="Scan the whole YouTube tab instead of stopping at the first already-linked upload.",
    )
    parser.add_argument(
        "--refresh-stock-metadata",
        action="store_true",
        help="Backfill canonical company and sector profiles for existing stock pages.",
    )
    parser.add_argument(
        "--metadata-limit",
        type=int,
        default=0,
        help="Maximum existing ticker profiles to query during --refresh-stock-metadata (0 means all).",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print additions without writing the catalog.")
    return parser.parse_args()


if __name__ == "__main__":
    sync_catalog(parse_args())
