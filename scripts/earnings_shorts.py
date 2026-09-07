"""Associate cataloged Shorts with a single, dated full-length earnings video."""

import re
import hashlib
import json
from pathlib import Path
from collections import defaultdict
from datetime import datetime, timezone
from urllib.parse import parse_qs, urlparse


REPORTING_PERIOD_RE = re.compile(r"\b(Q[1-4]|H[12])\s+(?:FY\s*)?(\d{4})\b|\bFY\s*(\d{4})\b", re.I)
AMBIGUOUS_SYMBOLS = {"AI", "ALL", "ARE", "BE", "FOR", "IT", "ON", "OR", "SO", "THE", "NOW", "NET"}
COMPANY_ALIASES = {"PANW": ("Palo Alto",)}


def youtube_id(url):
    parsed = urlparse(url or "")
    host = (parsed.hostname or "").lower()
    parts = parsed.path.strip("/").split("/")
    if host == "youtu.be":
        return parts[0]
    if host == "youtube.com" or host.endswith(".youtube.com"):
        if len(parts) == 2 and parts[0] in {"shorts", "embed"}:
            return parts[1]
        return parse_qs(parsed.query).get("v", [""])[0]
    return ""


def reporting_periods(text):
    return {
        (period.upper(), year) if period else ("FY", annual_year)
        for period, year, annual_year in REPORTING_PERIOD_RE.findall(text or "")
    }


def published_date(value):
    try:
        parsed = datetime.fromisoformat((value or "").replace("Z", "+00:00"))
        return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed
    except (TypeError, ValueError):
        return None


def normalized_name(value):
    return re.sub(r"[^a-z0-9]+", " ", (value or "").lower()).strip()


CHANNEL_ID = "UC4ZDZpC0OvoN4cCGoUSofuA"
STUDIO_SUFFIX = " — Animated Studio Edition"


def description_video_ids(description):
    return {
        video_id for url in re.findall(r"https?://[^\s<>]+", description or "")
        if re.fullmatch(r"[A-Za-z0-9_-]{11}", video_id := youtube_id(url.rstrip(".,);")))
    }


def validate_link_record(source_id, record):
    if not isinstance(record, dict):
        raise ValueError("Episode link evidence record must be an object")
    valid = (
        re.fullmatch(r"[A-Za-z0-9_-]{11}", source_id or "")
        and record.get("source_video_id") == source_id
        and youtube_id(record.get("source_url")) == source_id
        and record.get("channel_id") == CHANNEL_ID
        and isinstance(record.get("title"), str) and record["title"].strip()
        and isinstance(record.get("description"), str)
        and record.get("description_sha256") == hashlib.sha256(record["description"].encode("utf-8")).hexdigest()
        and published_date(record.get("fetched_at")) is not None
        and record.get("relation") in {"short_earnings", "studio_primary"}
        and isinstance(record.get("target_video_id"), str)
        and record["target_video_id"] != source_id
        and record["target_video_id"] in description_video_ids(record["description"])
    )
    if not valid:
        raise ValueError("Episode link evidence identity, description hash or direct target is invalid: " + str(source_id))
    if record["relation"] == "short_earnings":
        labelled = set()
        for line in record["description"].splitlines():
            if re.search(r"\bfull\s+(?:breakdown|analysis|earnings (?:breakdown|analysis))\b", line, re.I):
                labelled.update(description_video_ids(line))
        if record["target_video_id"] not in labelled:
            raise ValueError("Short evidence requires an explicit full-breakdown or analysis link")
    if record["relation"] == "studio_primary":
        labelled = re.findall(r"(?im)^Watch the original presentation:\s*(https?://\S+)", record["description"])
        if {youtube_id(url.rstrip(".,);")) for url in labelled} != {record["target_video_id"]}:
            raise ValueError("Studio evidence requires the exact labelled original-presentation link")
    return record


def load_episode_link_evidence(path):
    """Load reviewed public descriptions; never fetch, infer or overwrite evidence."""
    path = Path(path)
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("schema_version") != 1 or not isinstance(payload.get("records"), dict):
        raise ValueError("Episode link evidence schema must be version 1 with records")
    return {key: validate_link_record(key, record) for key, record in payload["records"].items()}


def is_studio_edition(episode):
    return (episode.get("title") or "").endswith(STUDIO_SUFFIX)


def verified_description_target(item, primary_by_id, evidence, relation):
    source_id = youtube_id(item.get("youtube_url"))
    record = (evidence or {}).get(source_id)
    if record is None:
        return None
    validate_link_record(source_id, record)
    if record["relation"] != relation or record["title"] != (item.get("title") or ""):
        return None
    # Multiple cited earnings reports are ambiguous; a declared target alone
    # cannot override the actual source description.
    targets = description_video_ids(record["description"]) & primary_by_id.keys()
    if targets != {record["target_video_id"]}:
        return None
    target = primary_by_id[record["target_video_id"]]
    if not reporting_periods(target.get("quarter")):
        return None
    return target


def bind_studio_editions(catalog, evidence):
    """Keep both rows; associate/label a studio quarter only by its exact primary ID."""
    originals = {youtube_id(ep.get("youtube_url")): ep for ep in catalog.get("episodes", [])
                 if not is_studio_edition(ep) and youtube_id(ep.get("youtube_url"))}
    summary = {"verified": 0, "newly_linked": 0, "quarter_corrected": 0, "unverified": 0}
    for episode in catalog.get("episodes", []):
        if not is_studio_edition(episode):
            continue
        primary = verified_description_target(episode, originals, evidence, "studio_primary")
        if not primary or primary.get("ticker") != episode.get("ticker") or not reporting_periods(primary.get("quarter")):
            summary["unverified"] += 1
            continue
        summary["verified"] += 1
        summary["newly_linked"] += int(episode.get("studio_primary_youtube_url") != primary["youtube_url"])
        summary["quarter_corrected"] += int(episode.get("quarter") != primary["quarter"])
        episode["studio_primary_youtube_url"] = primary["youtube_url"]
        episode["studio_primary_link_evidence"] = youtube_id(episode["youtube_url"])
        episode["quarter"] = primary["quarter"]
    return summary


def build_earnings_index(episodes):
    by_ticker = defaultdict(list)
    aliases = defaultdict(set)
    for episode in episodes:
        # Studio editions remain in the archive, but never compete with their
        # original presentation as a second earnings event.
        if is_studio_edition(episode):
            continue
        ticker = (episode.get("ticker") or "").upper()
        if not ticker or not youtube_id(episode.get("youtube_url")):
            continue
        by_ticker[ticker].append(episode)
        company = normalized_name(episode.get("company"))
        company = re.sub(r"(?: (?:inc|incorporated|corp|corporation|plc|limited|ltd|holdings|holding|group|company|co))+$", "", company)
        names = [company]
        # Branded titles supply verified short names such as Credo and MongoDB.
        title_brand = re.match(r"^(.+?)(?: Stock\b| \(" + re.escape(ticker) + r"\))", episode.get("title") or "")
        if title_brand:
            brand = title_brand.group(1)
            if brand != ticker and len(brand) >= 2:
                aliases[normalized_name(brand)].add(ticker)
        for name in names:
            if len(name) >= 3 and name != ticker.lower():
                aliases[name].add(ticker)
    for ticker, names in COMPANY_ALIASES.items():
        if ticker in by_ticker:
            for name in names:
                aliases[normalized_name(name)].add(ticker)
    return by_ticker, aliases


def match_short(short, by_ticker, aliases):
    title = short.get("title") or ""
    date = published_date(short.get("published_at"))
    if not date:
        return None, "missing_date"
    periods = reporting_periods(title)
    if not periods and re.search(r"\b(?:options?|explained)\b|\bimplied move\b|\bstock split\b", title, re.I):
        return None, "general_research"
    primary = re.match(r"^([A-Z][A-Z0-9.-]{0,12}) Stock\b|^([A-Z][A-Z0-9.-]{1,12}) [+$-]?\d", title)
    if primary:
        tickers = {primary.group(1) or primary.group(2)}
    else:
        marked = set(re.findall(r"(?:[$(])([A-Z][A-Z0-9.-]*)\b", title))
        tickers = marked & by_ticker.keys()
        if not tickers:
            hashtags = set(re.findall(r"#([A-Z][A-Z0-9.-]*)\b", title))
            tickers = (hashtags - {"AI"}) & by_ticker.keys()
        if not tickers:
            normalized = " " + normalized_name(title) + " "
            names = [name for name in aliases if " " + name + " " in normalized]
            # Coca-Cola FEMSA is not the Coca-Cola Company; prefer the full name.
            names = [name for name in names if not any(
                name != other and " " + name + " " in " " + other + " " for other in names
            )]
            tickers = set().union(*(
                aliases[name] for name in names
            ))
        if not tickers:
            tickers = {
                token for token in re.findall(r"\b[A-Z][A-Z0-9.-]*\b", title)
                if token in by_ticker and token not in AMBIGUOUS_SYMBOLS and len(token) > 1
            }
    if len(tickers) != 1:
        return None, "ambiguous_stock" if tickers else "unknown_stock"
    if len(periods) > 1:
        return None, "ambiguous_period"
    candidates = {}
    for episode in by_ticker.get(next(iter(tickers)), []):
        episode_date = published_date(episode.get("published_at"))
        if not episode_date:
            continue
        distance = abs((date - episode_date).total_seconds())
        if distance > (14 if periods else 3) * 86400:
            continue
        if not periods and distance > 3600:
            ignored = {"stock", "stocks", "earnings", "analysis", "shorts", "with", "from", "that", "this", "what", "your", "have", "more"}
            ignored.update(normalized_name(episode.get("company")).split())
            ignored.add(episode["ticker"].lower())
            shared = set(normalized_name(title).split()) & set(normalized_name(episode.get("title")).split())
            if len({word for word in shared - ignored if len(word) >= 4}) < 2:
                continue
        episode_periods = reporting_periods(episode.get("quarter")) or reporting_periods(episode.get("title"))
        if not episode_periods:
            continue
        if periods and periods != episode_periods:
            # Annual and Q4 coverage can describe the same year-end report.
            year_end = len(periods) == len(episode_periods) == 1 and {
                next(iter(periods))[0], next(iter(episode_periods))[0]
            } == {"FY", "Q4"} and next(iter(periods))[1] == next(iter(episode_periods))[1]
            if not year_end:
                continue
        candidates[youtube_id(episode["youtube_url"])] = episode
    if len(candidates) > 1:
        ranked = sorted(candidates.values(), key=lambda ep: abs((date - published_date(ep["published_at"])).total_seconds()))
        closest = abs((date - published_date(ranked[0]["published_at"])).total_seconds())
        runner_up = abs((date - published_date(ranked[1]["published_at"])).total_seconds())
        if (closest <= 3600 and runner_up >= 6 * 3600) or (closest <= 86400 and runner_up - closest >= 2 * 86400):
            return ranked[0], "stock_period_date_nearest" if periods else "stock_date_nearest"
    if len(candidates) != 1:
        return None, "ambiguous_episode" if candidates else "no_matching_episode"
    return next(iter(candidates.values())), "stock_period_date" if periods else "stock_date"


def link_earnings_shorts(catalog, evidence=None):
    """Recheck every Short each run, including clips uploaded before the long video."""
    by_ticker, aliases = build_earnings_index(catalog.get("episodes", []))
    originals = {youtube_id(ep["youtube_url"]): ep for items in by_ticker.values() for ep in items}
    summary = {"linked": 0, "newly_linked": 0, "updated": 0, "unmatched": 0, "unmatched_reasons": {}}
    for section in catalog.get("video_sections", []):
        if section.get("title") != "Shorts and Clips":
            continue
        for short in section.get("videos", []):
            before = (short.get("earnings_youtube_url"), short.get("earnings_match"))
            episode = verified_description_target(short, originals, evidence, "short_earnings")
            if episode is not None:
                reason = "verified_description_link"
            else:
                episode, reason = match_short(short, by_ticker, aliases)
            if episode:
                short["earnings_youtube_url"] = episode["youtube_url"]
                short["earnings_match"] = reason
                summary["linked"] += 1
                summary["newly_linked"] += int(not before[0])
            else:
                short.pop("earnings_youtube_url", None)
                short.pop("earnings_match", None)
                summary["unmatched"] += 1
                reasons = summary["unmatched_reasons"]
                reasons[reason] = reasons.get(reason, 0) + 1
            after = (short.get("earnings_youtube_url"), short.get("earnings_match"))
            summary["updated"] += int(before != after)
    return summary
