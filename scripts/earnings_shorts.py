"""Associate cataloged Shorts with a single, dated full-length earnings video."""

import re
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


def build_earnings_index(episodes):
    by_ticker = defaultdict(list)
    aliases = defaultdict(set)
    for episode in episodes:
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


def link_earnings_shorts(catalog):
    """Recheck every Short each run, including clips uploaded before the long video."""
    by_ticker, aliases = build_earnings_index(catalog.get("episodes", []))
    summary = {"linked": 0, "newly_linked": 0, "updated": 0, "unmatched": 0, "unmatched_reasons": {}}
    for section in catalog.get("video_sections", []):
        if section.get("title") != "Shorts and Clips":
            continue
        for short in section.get("videos", []):
            before = (short.get("earnings_youtube_url"), short.get("earnings_match"))
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
