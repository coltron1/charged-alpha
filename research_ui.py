"""Presentation helpers for the research-first website; never alter packet bytes."""
from datetime import datetime


def publication_date(value):
    for pattern in ("%Y-%m-%d", "%B %d, %Y"):
        try:
            return datetime.strptime(str(value or "")[:10] if pattern == "%Y-%m-%d" else str(value), pattern).date().isoformat()
        except (TypeError, ValueError):
            pass
    return ""


def research_listing(stocks, packets):
    by_ticker = {}
    for packet in packets:
        by_ticker.setdefault(packet["ticker"].upper(), []).append(packet)
    result = []
    for original in stocks:
        stock = dict(original)
        candidates = by_ticker.get(stock["ticker"].upper(), [])
        packet = max(candidates, key=lambda p: (p["year"], p["quarter"]), default=None)
        stock["packet"] = ({key: packet.get(key) for key in ("slug", "title", "description", "period", "source_published")} if packet else None)
        stock["research_date"] = max(publication_date(stock.get("latest_video_published_at") or stock.get("latest_published_at")), publication_date(packet.get("source_published")) if packet else "")
        result.append(stock)
    result.sort(key=lambda s: s["ticker"])
    return sorted(result, key=lambda s: s["research_date"], reverse=True)


def latest_episode_podcasts(stock, video_id):
    """Inherit only from a verified primary/studio association, never ticker alone."""
    episodes = stock.get("episodes", [])
    latest = next((e for e in episodes if e.get("youtube_url") == stock.get("latest_youtube_url")), None)
    if not latest:
        return []
    eligible = [latest]
    primary_id = video_id(latest.get("studio_primary_youtube_url"))
    if primary_id and latest.get("studio_primary_link_evidence") == video_id(latest.get("youtube_url")):
        eligible += [e for e in episodes if video_id(e.get("youtube_url")) == primary_id and e.get("quarter") == latest.get("quarter")]
    links = []
    for field, label in (("spotify_url", "Spotify"), ("podbean_url", "Podbean"), ("apple_url", "Apple Podcasts"), ("amazon_url", "Amazon Music"), ("iheart_url", "iHeartRadio"), ("google_url", "YouTube Music")):
        source = next((e for e in eligible if e.get(field)), None)
        if source:
            links.append({"label": label, "url": source[field], "edition": "Presentation audio" if source is not latest else "Episode audio"})
    return links
