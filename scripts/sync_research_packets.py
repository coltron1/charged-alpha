#!/usr/bin/env python3
"""Import finalized READY packets; default mode is offline and create-only.

The explicit unpublished-replacement mode refreshes Git read-only and shares the
queue's packet-publication lock. No mode publishes or rewrites producer inputs.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import fcntl
from html.parser import HTMLParser
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import tempfile
import unicodedata

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from research_packets import (DEFAULT_INDEX, load_packets, packet_html_path, packet_sort_key,
                              period_identity, reject_symlinks, require, safe_path, sha256,
                              validate_record, validate_what_changed)

DEFAULT_QUEUE = Path.home() / "Desktop/CHARGED ALPHA EPISODES/_studio_queue"
PACKET_REQUIRED_FROM = datetime(2026, 9, 8, tzinfo=timezone.utc)
HIGHLIGHTS_REQUIRED_FROM = datetime(2026, 9, 11, tzinfo=timezone.utc)
DEFERRED_ORIGIN_DIR = "_deferred_short_origin"
IMMUTABLE_FIELDS = ("key", "ticker", "company", "period", "year", "quarter", "fiscal", "slug",
                    "canonical_url", "title", "page_title", "description", "sha256", "source_episode")
REPLACEMENT_ORIGIN = "https://github.com/coltron1/charged-alpha.git"
REPLACEMENT_REPORT = "charged-alpha-unpublished-packet-replacement-review"


# Source-bound correction to the CRWV Q3 stock-page summary. The packet itself
# remains byte-identical; published HTML receives the same attribution correction.
def reviewed_highlight_projection(episode, html_sha256, content_sha256, highlights):
    if episode != "CRWV-Q3-2026":
        return highlights
    require(html_sha256 == "ad9db4c34fdd59a58310cbf3bc71db16e0981e93502f6f0e3390b82f5f097239"
            and content_sha256 == "e04bc447ca6e045ab66f5174a41047b957a6a764e38ccf3c6b9f17f3b63779f2",
            "CRWV reviewed highlight source differs")
    require(isinstance(highlights, dict) and len(highlights.get("items", [])) == 5,
            "CRWV reviewed highlights differ")
    old = "Q3 statements are unaudited and unresolved material control weaknesses remain a live risk; no remediation is assumed."
    item = highlights["items"][4]
    require(item == {"title": "The evidence has limits.", "detail": old},
            "CRWV reviewed highlight preimage differs")
    projected = {**highlights, "items": [dict(value) for value in highlights["items"]]}
    projected["items"][4]["detail"] = ("Q3 statements are unaudited. The latest quarterly filing reviewed, for Q2, "
        "reported material control weaknesses; the Q3 release supplied no controls assessment.")
    return validate_what_changed(projected)


def reviewed_html_projection(episode, html_sha256, content_sha256, raw_html):
    if episode != "CRWV-Q3-2026":
        return raw_html
    require(html_sha256 == "ad9db4c34fdd59a58310cbf3bc71db16e0981e93502f6f0e3390b82f5f097239"
            and content_sha256 == "e04bc447ca6e045ab66f5174a41047b957a6a764e38ccf3c6b9f17f3b63779f2"
            and sha256(raw_html) == html_sha256, "CRWV reviewed HTML source differs")
    old = b"Q3 statements are unaudited and unresolved material control weaknesses remain a live risk; no remediation is assumed."
    new = b"Q3 statements are unaudited. The latest quarterly filing reviewed, for Q2, reported material control weaknesses; the Q3 release supplied no controls assessment."
    require(raw_html.count(old) == 1, "CRWV reviewed HTML preimage differs")
    return raw_html.replace(old, new, 1)


class HeadMetadata(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.descriptions, self.titles, self.canonicals = [], [], []
        self.in_head = self.in_title = False

    def handle_starttag(self, tag, attrs):
        values = dict(attrs)
        if tag == "head":
            self.in_head = True
        if not self.in_head:
            return
        if tag == "title":
            self.in_title = True
            self.titles.append("")
        if tag == "meta" and values.get("name", "").lower() == "description":
            self.descriptions.append(values.get("content", ""))
        if tag == "link" and "canonical" in values.get("rel", "").lower().split():
            self.canonicals.append(values.get("href", ""))

    def handle_endtag(self, tag):
        if tag == "title":
            self.in_title = False
        if tag == "head":
            self.in_head = self.in_title = False

    def handle_data(self, data):
        if self.in_head and self.in_title:
            self.titles[-1] += data


def normalized_period(value):
    """Canonicalize display whitespace only for a complete quarterly period token."""
    require(isinstance(value, str), "Packet period must be text")
    display = " ".join(value.upper().split())
    match = re.fullmatch(r"Q\s*([1-4])(?:\s+|-)\s*((?:FY)?20\d{2})", display)
    if match:
        return f"Q{match.group(1)} {match.group(2)}"
    legacy = " ".join(value.upper().replace("-", " ").split())
    return legacy if re.fullmatch(r"(?:H[12] (?:FY)?20\d{2}|FY20\d{2})", legacy) else display


class HighlightText(HTMLParser):
    """Extract readable text and the emphasized lead from one packet highlight."""
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.parts, self.emphasis, self.emphasis_depth = [], [], 0

    def handle_starttag(self, tag, attrs):
        if tag.lower() in ("b", "strong"):
            self.emphasis_depth += 1

    def handle_endtag(self, tag):
        if tag.lower() in ("b", "strong") and self.emphasis_depth:
            self.emphasis_depth -= 1

    def handle_data(self, data):
        text = " ".join(data.split())
        if not text:
            return
        self.parts.append(text)
        if self.emphasis_depth:
            self.emphasis.append(text)


class SummaryText(HTMLParser):
    """Remove only balanced, attribute-free emphasis from a packet summary."""
    ALLOWED = {"b", "strong"}

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.parts, self.stack = [], []

    def handle_starttag(self, tag, attrs):
        tag = tag.lower()
        require(tag in self.ALLOWED and not attrs,
                "Packet what_changed summary contains unsupported markup")
        self.stack.append(tag)

    def handle_endtag(self, tag):
        tag = tag.lower()
        require(tag in self.ALLOWED and self.stack and self.stack[-1] == tag,
                "Packet what_changed summary contains malformed markup")
        self.stack.pop()

    def handle_startendtag(self, tag, attrs):
        raise ValueError("Packet what_changed summary contains unsupported markup")

    def handle_data(self, data):
        self.parts.append(data)

    def handle_comment(self, data):
        raise ValueError("Packet what_changed summary contains unsupported markup")

    def handle_decl(self, decl):
        raise ValueError("Packet what_changed summary contains unsupported markup")

    def unknown_decl(self, data):
        raise ValueError("Packet what_changed summary contains unsupported markup")

    def handle_pi(self, data):
        raise ValueError("Packet what_changed summary contains unsupported markup")


def plain_summary(value):
    """Return exact visible summary text for the two supported emphasis tags."""
    require(isinstance(value, str), "Invalid packet what_changed summary")
    parser = SummaryText()
    parser.feed(value)
    parser.close()
    require(not parser.stack, "Packet what_changed summary contains malformed markup")
    result = "".join(parser.parts)
    require(result.strip() == result and 1 <= len(result) <= 500,
            "Invalid packet what_changed summary")
    require("<" not in result and ">" not in result and "\n" not in result and "\r" not in result,
            "Packet what_changed summary must be plain text")
    return result


def normalize_highlight_detail(value):
    """Restore narrowly proven display spaces without changing financial tokens."""
    require(isinstance(value, str), "Packet highlight detail must be text")
    # Packet prose may use a standalone greater-than comparator directly before
    # a dollar amount.  Convert only that financial token to plain words so the
    # derived What’s Changed detail remains strict plain text; leave the immutable
    # packet document and HTML untouched.  Operators such as >= and markup-like
    # angle brackets deliberately remain invalid for validate_what_changed().
    value = re.sub(r"(?<!\S)>[ \t]*(?=\$\d)", "more than ", value)
    # These word/number boundaries are present in the retained packet corpus. Keep
    # the vocabulary explicit so product names such as iPhone16 are not rewritten.
    value = re.sub(r"\b(rose|but|grew|grows|equals|mid|September)(?=\d)", r"\1 ", value)
    # Lowercase prose followed by a currency value needs a separator. Uppercase
    # currency prefixes such as C$ and US$ deliberately do not match.
    value = re.sub(r"(?<=[a-z])(?=\$)", " ", value)
    value = re.sub(r"(?<=[,;:])(?=\$)", " ", value)
    value = re.sub(r"\b(in)(?=FY\d)", r"\1 ", value)
    value = re.sub(r"\b(FY\d{2})(?=(?:FCF|EPS|EBITDA|GAAP)\b)", r"\1 ", value)
    return value


def normalized_what_changed(value):
    """Canonical comparison view; callers retain the original registry object."""
    validate_what_changed(value)
    return {
        "summary": value["summary"],
        "items": [
            {"title": item["title"], "detail": normalize_highlight_detail(item["detail"])}
            for item in value["items"]
        ],
    }


def packet_highlights(content, *, required=False):
    """Create the shared plain-text summary from the finalized packet's fast read."""
    values = content.get("five_things")
    if values is None:
        require(not required, "Packet five_things are required for What’s Changed")
        return None
    require(isinstance(values, list) and 3 <= len(values) <= 5,
            "Packet five_things must contain three to five highlights")
    items = []
    for value in values:
        require(isinstance(value, str) and value.strip(), "Invalid packet five_things item")
        parser = HighlightText()
        parser.feed(value)
        parser.close()
        plain, title = " ".join(parser.parts), " ".join(parser.emphasis)
        require(plain and title and plain.startswith(title),
                "Each packet five_things item needs an emphasized lead")
        detail = normalize_highlight_detail(plain[len(title):].strip())
        require(detail, "Each packet five_things item needs supporting detail")
        items.append({"title": title, "detail": detail})
    summary = plain_summary(content.get("meta", {}).get("description"))
    result = {"summary": summary, "items": items}
    return validate_what_changed(result)


def json_object(raw, label):
    value = json.loads(raw)
    require(isinstance(value, dict), f"{label} must be a JSON object")
    return value


def canonical_company_name(value):
    """Compare display/legal spelling only; never infer a company from its ticker."""
    require(isinstance(value, str) and value.strip(), "Packet company identity missing")
    value = unicodedata.normalize("NFKC", value).translate(str.maketrans({"‘": "'", "’": "'"}))
    value = " ".join(value.split()).casefold()
    # A standalone ampersand is the display spelling of the same conjunction.
    # Preserve embedded brand punctuation (for example AT&T) and every other word.
    value = re.sub(r"(?<!\S)&(?!\S)", "and", value)
    value = re.sub(r",\s*inc\.?$", "", value).strip()
    require(value, "Packet company identity missing")
    return value


def staged_date(handoff, ready):
    values = [ready.decode("utf-8").strip()]
    if handoff.get("staged_at"):
        values.append(handoff["staged_at"])
    dates = []
    for value in values:
        require(isinstance(value, str) and value, "Missing source staging date")
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError("Invalid source staging date") from exc
        require(parsed.tzinfo is not None, "Source staging date needs a timezone")
        dates.append((parsed.astimezone(timezone.utc), value))
    # READY is written last: a newly restaged folder cannot hide a required packet
    # behind a historical handoff timestamp.
    return max(dates)


def source_date(value, label):
    require(isinstance(value, str) and value, f"Missing {label}")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"Invalid {label}") from exc
    require(parsed.tzinfo is not None, f"{label} needs a timezone")
    return parsed.astimezone(timezone.utc)


def retained_legacy_origin(queue, handoff, raw):
    """Validate cryptographic proof that only an owed Short restaged a pre-cutoff handoff."""
    origin = handoff.get("deferred_short_origin")
    require(isinstance(origin, dict), "Restaged packet-null handoff lacks retained origin proof")
    require(set(origin) == {"schema_version", "directory", "ready_at", "staged_at", "sha256"}
            and origin.get("schema_version") == 1 and origin.get("directory") == DEFERRED_ORIGIN_DIR,
            "Invalid retained origin provenance")
    hashes = origin.get("sha256")
    names = ("READY", "handoff.json", "LONGFORM_DONE")
    require(isinstance(hashes, dict) and set(hashes) == set(names)
            and all(isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value)
                    for value in hashes.values()), "Invalid retained origin hashes")
    retained = {}
    for name in names:
        relative = f"{DEFERRED_ORIGIN_DIR}/{name}"
        retained[name] = safe_path(queue, relative).read_bytes()
        raw[relative] = retained[name]
        require(sha256(retained[name]) == hashes[name], f"Retained origin {name} hash differs")
    try:
        ready_at = retained["READY"].decode("utf-8").strip()
    except UnicodeDecodeError as exc:
        raise ValueError("Invalid retained origin READY timestamp") from exc
    original = json_object(retained["handoff.json"], "Retained origin handoff")
    require(origin["ready_at"] == ready_at and origin["staged_at"] == original.get("staged_at"),
            "Retained origin timestamps differ from provenance")
    require(source_date(ready_at, "retained READY timestamp") < PACKET_REQUIRED_FROM
            and source_date(original.get("staged_at"), "retained handoff timestamp") < PACKET_REQUIRED_FROM,
            "Retained packet-null origin is not before the packet requirement")
    require(original.get("short") is None and original.get("packet") is None,
            "Retained origin was not packet-null and Short-null")
    identity = period_identity(handoff.get("ticker"), handoff.get("period"))
    require(handoff.get("episode") == identity["key"].replace(":", "-"),
            "Deferred Short episode/ticker/period identity differs")
    primary = handoff.get("primary")
    require(isinstance(primary, dict) and isinstance(primary.get("youtube_long"), str)
            and re.fullmatch(r"[A-Za-z0-9_-]{11}", primary["youtube_long"]),
            "Deferred Short primary identity is invalid")
    require(set(handoff) == set(original) | {"deferred_short_origin"},
            "Restaged handoff fields differ from retained origin")
    for key, value in original.items():
        if key not in ("short", "staged_at"):
            require(handoff.get(key) == value, f"Restaged handoff {key} differs from retained origin")
    require(isinstance(handoff.get("short"), dict), "Deferred Short metadata is missing")

    raw["LONGFORM_DONE"] = safe_path(queue, "LONGFORM_DONE").read_bytes()
    require(raw["LONGFORM_DONE"] == retained["LONGFORM_DONE"],
            "Current long completion differs from retained origin")
    long_done = json_object(raw["LONGFORM_DONE"], "Long-form completion")
    require(long_done.get("episode_id") == handoff.get("episode") and long_done.get("cut_id") == "studio-v1"
            and isinstance(long_done.get("video_id"), str)
            and re.fullmatch(r"[A-Za-z0-9_-]{11}", long_done["video_id"])
            and isinstance(long_done.get("output_sha256"), str)
            and re.fullmatch(r"[0-9a-f]{64}", long_done["output_sha256"]),
            "Retained long completion identity is invalid")

    raw["SHORT_ADDED"] = safe_path(queue, "SHORT_ADDED").read_bytes()
    raw["producer_staging_receipt.json"] = safe_path(queue, "producer_staging_receipt.json").read_bytes()
    receipt = json_object(raw["producer_staging_receipt.json"], "Producer staging receipt")
    try:
        current_ready = raw["READY"].decode("utf-8").strip()
        current_short_added = raw["SHORT_ADDED"].decode("utf-8").strip()
    except UnicodeDecodeError as exc:
        raise ValueError("Invalid deferred Short staging marker") from exc
    require(receipt.get("schema_version") == 1 and receipt.get("episode") == handoff.get("episode")
            and receipt.get("operation") == "add_deferred_short"
            and receipt.get("staged_at") == handoff.get("staged_at") == current_ready == current_short_added,
            "Deferred Short staging markers differ")
    require(receipt.get("retained_origin_sha256") == hashes,
            "Producer staging receipt does not bind retained origin")
    staged_hashes = receipt.get("staged_file_sha256")
    require(isinstance(staged_hashes, dict), "Producer staging receipt lacks staged hashes")
    bound = {"handoff.json": raw["handoff.json"], "LONGFORM_DONE": raw["LONGFORM_DONE"]}
    # Canonical staging omits files named READY from this general inventory,
    # including the retained marker. Its exact bytes were already verified above
    # against both handoff provenance and receipt.retained_origin_sha256.
    bound.update({f"{DEFERRED_ORIGIN_DIR}/{name}": retained[name] for name in names if name != "READY"})
    retained_ready = f"{DEFERRED_ORIGIN_DIR}/READY"
    if retained_ready in staged_hashes:
        bound[retained_ready] = retained["READY"]
    require(all(staged_hashes.get(name) == sha256(value) for name, value in bound.items()),
            "Producer staging receipt hash binding differs")
    return {"status": "legacy_no_packet", "source_episode": handoff["episode"],
            "queue_dir": str(queue), "reason": "Validated retained pre-cutoff packet-null origin"}


def source_episode(queue, handoff, raw):
    """Resolve only the canonical folder or the completion helper's bound sibling."""
    episode = handoff.get("episode")
    require(isinstance(episode, str) and episode, "Missing source episode")
    if queue.name == episode:
        return episode
    sibling = re.fullmatch(re.escape(episode) + r"--studio-both-([A-Za-z0-9_-]{11})", queue.name)
    require(queue.parent.name == "_done" and sibling is not None,
            "Source episode differs from READY folder")
    raw["DONE"] = safe_path(queue, "DONE").read_bytes()
    done = json_object(raw["DONE"], "Archive completion")
    require(done.get("schema_version") == 2 and done.get("episode_id") == episode
            and done.get("ticker") == handoff.get("ticker"), "Archive completion identity differs")
    videos = []
    for field, cut in (("long_form", "studio-v1"), ("short", "studio-short-v1")):
        payload = done.get(field)
        require(isinstance(payload, dict) and payload.get("episode_id") == episode
                and payload.get("ticker") == handoff.get("ticker") and payload.get("cut_id") == cut,
                "Archive cut identity differs")
        video = payload.get("video_id")
        require(isinstance(video, str) and re.fullmatch(r"[A-Za-z0-9_-]{11}", video),
                "Invalid archive video ID")
        videos.append(video)
    require(videos[0] != videos[1] and videos[1] == sibling.group(1),
            "Archive suffix differs from completed Short")
    return episode


def read_candidate(queue):
    """Validate the producer's finalized packet and capture its exact source bytes."""
    queue = reject_symlinks(queue)
    require("_MEDIA_ARCHIVE_DELETE_ME" not in queue.parts, "Deletion archive cannot be a source")
    raw = {"READY": safe_path(queue, "READY").read_bytes(),
           "handoff.json": safe_path(queue, "handoff.json").read_bytes()}
    handoff = json_object(raw["handoff.json"], "Handoff")
    episode = source_episode(queue, handoff, raw)
    when, staged_at = staged_date(handoff, raw["READY"])
    packet = handoff.get("packet")
    if packet is None:
        if when < PACKET_REQUIRED_FROM:
            return {"status": "legacy_no_packet", "source_episode": episode, "queue_dir": str(queue),
                    "reason": "Older packet-null handoff is valid"}
        if "deferred_short_origin" in handoff:
            result = retained_legacy_origin(queue, handoff, raw)
            require(result["source_episode"] == episode, "Retained origin episode identity differs")
            return result
        return {"status": "pending_source", "source_episode": episode, "queue_dir": str(queue),
                "reason": "no packet"}
    require(isinstance(packet, dict) and packet.get("finalized") is True, "Packet must be explicitly finalized")
    identity = period_identity(handoff.get("ticker"), handoff.get("period"))
    require(handoff["episode"] == identity["key"].replace(":", "-"), "Episode ticker/period identity differs")
    require(packet.get("slug") == identity["slug"], "Packet slug differs from ticker/period")
    canonical = f"https://chargedalpha.com/research/{identity['slug']}"
    require(packet.get("canonical_url") == canonical, "Packet canonical URL differs")
    directory, filename = packet.get("dir"), packet.get("file")
    require(isinstance(directory, str) and directory, "Missing packet directory")
    require(filename == f"{identity['slug']}-research-packet.html", "Unexpected packet HTML filename")
    names = {"html": f"{directory}/{filename}", "meta": f"{directory}/packet_meta.json",
             "content": f"{directory}/packet.json"}
    for name in names.values():
        raw[name] = safe_path(queue, name).read_bytes()
    meta = json_object(raw[names["meta"]], "Packet metadata")
    content = json_object(raw[names["content"]], "Packet content")
    cm = content.get("meta")
    require(isinstance(cm, dict), "Packet content metadata missing")
    digest = sha256(raw[names["html"]])
    require(meta.get("finalized") is True and digest == packet.get("sha256") == meta.get("sha256"),
            "Finalized packet SHA256 differs from handoff/metadata")
    require(meta.get("bytes") == len(raw[names["html"]]), "Packet byte count differs")
    for key in ("slug", "canonical_url", "title", "page_title"):
        require(packet.get(key) == meta.get(key), f"Packet {key} differs from metadata")
    require(meta.get("file") == filename, "Packet metadata filename differs")
    require(handoff.get("ticker") == meta.get("ticker") == cm.get("ticker"),
            "Packet ticker identity differs")
    require(meta.get("period") == cm.get("period"), "Packet period document/metadata differs")
    require(normalized_period(handoff.get("period")) == normalized_period(meta.get("period")),
            "Packet period identity differs")
    handoff_company = canonical_company_name(handoff.get("company"))
    packet_company = canonical_company_name(cm.get("company"))
    # Source-verified legal/display pair for GE Vernova; do not generalize suffix stripping.
    gev_names = {"ge vernova", "ge vernova inc."}
    same_gev = handoff.get("ticker") == "GEV" and {handoff_company, packet_company} <= gev_names
    require(handoff_company == packet_company or same_gev, "Packet company identity differs")
    for key in ("slug", "title", "page_title"):
        require(packet.get(key) == cm.get(key), f"Packet content {key} differs")
    parsed = HeadMetadata()
    parsed.feed(raw[names["html"]].decode("utf-8"))
    require(len(parsed.descriptions) == 1 and parsed.descriptions[0].strip(), "Exactly one HTML meta description required")
    require(len(parsed.titles) == 1 and parsed.titles[0].strip() == packet["page_title"], "HTML page title differs")
    require(parsed.descriptions[0] == cm.get("description"), "HTML description differs from packet content")
    require(not parsed.canonicals or parsed.canonicals == [canonical], "HTML canonical URL differs")
    what_changed = packet_highlights(content, required=when >= HIGHLIGHTS_REQUIRED_FROM)
    what_changed = reviewed_highlight_projection(episode, digest, sha256(raw[names["content"]]), what_changed)
    primary, links = handoff.get("primary", {}), meta.get("links", {})
    require(isinstance(primary, dict) and isinstance(links, dict), "Invalid source video links")
    require(content.get("links") == links, "Packet video links differ from metadata")
    videos = {}
    for name in ("youtube_long", "youtube_short", "youtube_studio", "youtube_studio_short"):
        value = links.get(name)
        other = primary.get(name) if name in ("youtube_long", "youtube_short") else None
        require(value in (None, "") or isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9_-]{11}", value), "Invalid packet video ID")
        require(other in (None, "") or isinstance(other, str) and re.fullmatch(r"[A-Za-z0-9_-]{11}", other), "Invalid handoff video ID")
        require(not value or not other or value == other, "Conflicting primary video IDs")
        videos[("primary_" if name in ("youtube_long", "youtube_short") else "") + name] = value or other or None
    published_html = reviewed_html_projection(episode, digest, sha256(raw[names["content"]]), raw[names["html"]])
    record = {**identity, "ticker": handoff["ticker"], "company": handoff["company"],
              "period": handoff["period"], "canonical_url": canonical, "title": packet["title"],
              "page_title": packet["page_title"], "description": parsed.descriptions[0], "sha256": sha256(published_html),
              "html_file": f"research_packets/{identity['slug']}.html", "source_episode": episode,
              "source_staged_at": staged_at, "source_sha256": {name: sha256(data) for name, data in raw.items()},
              **videos}
    if episode == "CRWV-Q3-2026":
        record["publication_revision"] = {"kind": "source_bound_disclosure_attribution", "source_html_sha256": digest, "source_content_sha256": sha256(raw[names["content"]]), "published_html_sha256": sha256(published_html)}
    if what_changed:
        record["what_changed"] = what_changed
    if cm.get("published"):
        require(isinstance(cm["published"], str), "Invalid source publication date")
        record["source_published"] = cm["published"]
    for field in ("words", "figures", "tables"):
        if field in meta:
            require(type(meta[field]) is int and meta[field] >= 0, f"Invalid packet {field} count")
            record[field] = meta[field]
    validate_record(record)
    return {"status": "validated", "queue_dir": str(queue), "source_episode": episode,
            "record": record, "source_bytes": raw, "html_bytes": published_html}


def ready_folders(queue_root):
    root = reject_symlinks(queue_root)
    require(root.is_dir(), "Queue root is unavailable")
    folders = []
    for container in (root, root / "_done"):
        if not container.exists():
            continue
        reject_symlinks(container)
        for folder in container.iterdir():
            if folder.name.startswith("_") or not folder.is_dir():
                continue
            if (folder / "READY").exists() or (folder / "READY").is_symlink():
                folders.append(folder)
    return sorted(folders, key=lambda p: ((p / "READY").lstat().st_mtime_ns, str(p)))


def unchanged(candidate):
    queue = Path(candidate["queue_dir"])
    require(all(safe_path(queue, name).read_bytes() == raw for name, raw in candidate["source_bytes"].items()),
            "READY packet changed during import")


# Exact, independently reviewed ANET archived display revision.  Historical source
# remains immutable; this exception cannot authorize a different quarter or bytes.
ANET_DISPLAY_LINEAGE = {
    "id": "anet-q3-2026-display-20261111-01",
    "predecessor_html_sha256": "a4f5f1051098d7dab883b58851dcc26ff2f518754b9113d986b46de2ae891fd5",
    "successor_html_sha256": "da74f5f8a4a1abadbcf6c8ab550ff05e7402af3843e9f9413c388b2df62c2ad3",
    "candidate_meta_sha256": "8929ac65c1ef399fbe9053a7332d687f73e4d37c3f17849ed0916666bff69cbe",
    "candidate_review_sha256": "bd59387d659efaa126a6df795c3e969af95e227d1f8f3150ef2e440d3eab352a",
}
ANET_ARCHIVE_SOURCES = {
    "READY": "fdeee948b1643786c1dd4739399b3cb7affb16506528c20a847bc025e8912020",
    "handoff.json": "533f02e3e58159452a9478fbf266016fae765a42398d9b66662003cb88912022",
    "packet/anet-q3-2026-research-packet.html": ANET_DISPLAY_LINEAGE["predecessor_html_sha256"],
    "packet/packet_meta.json": "cab1b32f84fe611626eab78c915e413eb950538bfb91c5052c7ac74272276c48",
    "packet/packet.json": "a311af56aa44c47ac42317ba1cad1697f0bf17ac73df8b80d90857e4485deb8c",
}

def is_exact_anet_archived_revision(candidate, registered):
    record = candidate["record"]
    queue = Path(candidate["queue_dir"])
    return (queue.name == "ANET-Q3-2026" and queue.parent.name == "_done"
            and record.get("key") == registered.get("key") == "ANET:Q3-2026"
            and record.get("sha256") == ANET_DISPLAY_LINEAGE["predecessor_html_sha256"]
            and registered.get("sha256") == ANET_DISPLAY_LINEAGE["successor_html_sha256"]
            and registered.get("display_revision") == ANET_DISPLAY_LINEAGE
            and record.get("source_sha256") == registered.get("source_sha256") == ANET_ARCHIVE_SOURCES
            and {k: v for k, v in record.items() if k not in
                 {"sha256", "primary_youtube_long", "primary_youtube_short", "youtube_studio", "youtube_studio_short"}}
                == {k: v for k, v in registered.items() if k not in
                 {"sha256", "display_revision", "primary_youtube_long", "primary_youtube_short", "youtube_studio", "youtube_studio_short"}})


def classify(candidate, records, index):
    record = candidate["record"]
    matched = [p for p in records if p["key"] == record["key"] or p["slug"] == record["slug"]]
    if matched:
        require(len(matched) == 1 and (
                    all(matched[0][k] == record[k] for k in IMMUTABLE_FIELDS)
                    or is_exact_anet_archived_revision(candidate, matched[0])),
                "Immutable ticker/period packet conflicts with registry; no overwrite")
        packet_html_path(matched[0], index)
        if record.get("what_changed"):
            if matched[0].get("what_changed"):
                require(normalized_what_changed(matched[0]["what_changed"]) ==
                        normalized_what_changed(record["what_changed"]),
                        "Packet What’s Changed differs from registered source")
            else:
                old_content = [value for name, value in matched[0].get("source_sha256", {}).items()
                               if name.endswith("/packet.json")]
                new_content = [value for name, value in record.get("source_sha256", {}).items()
                               if name.endswith("/packet.json")]
                require(len(old_content) == len(new_content) == 1 and old_content == new_content,
                        "Packet content hash differs; refusing metadata enrichment")
                return "ready_to_enrich"
        return "already_imported"
    path = safe_path(index.parent, record["html_file"], must_exist=False)
    if path.exists():
        require(path.is_file() and sha256(path.read_bytes()) == record["sha256"], "Existing unindexed HTML conflicts; no overwrite")
    return "ready_to_import"


def index_bytes(records):
    return (json.dumps({"schema_version": 1, "packets": sorted(records, key=packet_sort_key)},
                       ensure_ascii=False, indent=2) + "\n").encode("utf-8")


def atomic_index(index, records):
    payload = index_bytes(records)
    fd, name = tempfile.mkstemp(prefix=".research-packets-", suffix=".json", dir=index.parent)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(payload); stream.flush(); os.fsync(stream.fileno())
        os.chmod(name, 0o644)
        os.replace(name, index)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def enriched_records(records, record):
    """Add derived summary metadata without changing immutable packet bytes."""
    require(record.get("what_changed"), "Missing What’s Changed metadata")
    updated, found = [], False
    for existing in records:
        if existing["key"] == record["key"]:
            require(not existing.get("what_changed"), "What’s Changed is already registered")
            existing = {**existing, "what_changed": record["what_changed"]}
            found = True
        updated.append(existing)
    require(found, "Packet enrichment target is missing")
    return updated


def install_html(path, raw):
    if path.exists():
        require(path.read_bytes() == raw, "Existing HTML differs; no overwrite")
        return
    fd, name = tempfile.mkstemp(prefix=".packet-", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(raw); stream.flush(); os.fsync(stream.fileno())
        os.chmod(name, 0o644)
        os.link(name, path)  # Atomic create-only; never replaces a historical page.
    finally:
        os.unlink(name)


def sync_packets(queue_root=DEFAULT_QUEUE, index_path=DEFAULT_INDEX, *, execute=False, episode=None):
    index = reject_symlinks(index_path)
    records = load_packets(index)
    results, candidates, planned = [], [], []
    for queue in ready_folders(queue_root):
        if episode and queue.name != episode and not (
                queue.parent.name == "_done" and re.fullmatch(
                    re.escape(episode) + r"--studio-both-[A-Za-z0-9_-]{11}", queue.name)):
            continue
        try:
            candidate = read_candidate(queue)
            if candidate["status"] != "validated":
                results.append(candidate); continue
            matches = [p for p in planned if p["key"] == candidate["record"]["key"]
                       or p["slug"] == candidate["record"]["slug"]]
            if matches:
                require(len(matches) == 1 and all(matches[0][k] == candidate["record"][k] for k in IMMUTABLE_FIELDS),
                        "Conflicting duplicate READY packets; no overwrite")
                candidate["status"] = "duplicate_ready"
            else:
                candidate["status"] = classify(candidate, records, index)
            candidates.append(candidate)
            if candidate["status"] == "ready_to_import":
                # Also detect duplicate conflicts within this scan without writing.
                planned.append(candidate["record"])
        except (ValueError, OSError, KeyError, TypeError) as exc:
            results.append({"status": "error", "queue_dir": str(queue), "source_episode": queue.name, "reason": str(exc)})
    if execute and candidates:
        index.parent.mkdir(parents=True, exist_ok=True)
        directory = safe_path(index.parent, "research_packets", must_exist=False)
        directory.mkdir(exist_ok=True)
        lock = os.open(directory, os.O_RDONLY)
        try:
            fcntl.flock(lock, fcntl.LOCK_EX)
            records = load_packets(index)
            for candidate in candidates:
                try:
                    unchanged(candidate)
                    status = classify(candidate, records, index)
                    if status == "ready_to_import":
                        install_html(safe_path(index.parent, candidate["record"]["html_file"], must_exist=False), candidate["html_bytes"])
                        unchanged(candidate)
                        next_records = [*records, candidate["record"]]
                        atomic_index(index, next_records)
                        records = next_records
                        status = "imported"
                    elif status == "ready_to_enrich":
                        records = enriched_records(records, candidate["record"])
                        atomic_index(index, records)
                        status = "enriched"
                    candidate["status"] = status
                except (ValueError, OSError, KeyError, TypeError) as exc:
                    candidate["status"], candidate["reason"] = "error", str(exc)
        finally:
            os.close(lock)
    results.extend({k: v for k, v in c.items() if k not in ("source_bytes", "html_bytes")} for c in candidates)
    return {"mode": "execute" if execute else "plan", "index_path": str(index), "packets": results,
            "errors": sum(p["status"] == "error" for p in results),
            "pending_source": sum(p["status"] == "pending_source" for p in results),
            "imported": sum(p["status"] == "imported" for p in results),
            "enriched": sum(p["status"] == "enriched" for p in results)}


def replacement_git(root, *args, missing=False):
    """Use the actual repository, with no caller-supplied Git repository overrides."""
    environment = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
    environment["GIT_TERMINAL_PROMPT"] = "0"
    result = subprocess.run(["git", "-C", str(root), *args], stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, env=environment, timeout=35)
    absent = missing and ((args[0] == "ls-files" and result.returncode == 1)
                          or (args[0] == "cat-file" and result.returncode == 128
                              and (b"does not exist in" in result.stderr
                                   or b"exists on disk, but not in" in result.stderr)))
    require(result.returncode == 0 or absent, "Unpublished replacement Git check failed")
    return result.stdout if result.returncode == 0 else None


def packet_source_hashes(record):
    values = record.get("source_sha256", {})
    result = {}
    for key, suffix in (("html", ".html"), ("content", "/packet.json"), ("meta", "/packet_meta.json")):
        matches = [value for name, value in values.items() if name.endswith(suffix)]
        require(len(matches) == 1, "Unpublished candidate needs three exact source packet hashes")
        result[key] = matches[0]
    require(result["html"] == record["sha256"], "Unpublished HTML/source identity differs")
    return result


def replace_bytes(path, raw):
    fd, temporary = tempfile.mkstemp(prefix=".packet-replacement-", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(raw); stream.flush(); os.fsync(stream.fileno())
        os.chmod(temporary, 0o644)
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def preserve_bytes(path, raw):
    reject_symlinks(path)
    if path.exists():
        require(path.is_file() and path.read_bytes() == raw, "Preserved replacement evidence differs")
    else:
        with path.open("xb") as stream:
            stream.write(raw); stream.flush(); os.fsync(stream.fileno())


def unpublished_queue(queue_root, queue, episode):
    require(not any((queue / name).exists() or (queue / name).is_symlink()
                    for name in ("PACKET_DONE", "DONE", "SHORT_DONE", "LONGFORM_DONE")),
            "A published/completed packet cannot be replaced")
    archived = queue_root / "_done"
    require(not (archived / episode).exists()
            and not any(archived.glob(episode + "--*")), "Archived episodes cannot be replaced")


def replace_unpublished_candidate(queue_root, index_path, episode, review_path, *, execute=False):
    """Share the marker writer's queue lock, without waiting on another owner."""
    require(episode and review_path, "Replacement requires one episode and an exact Terra review")
    queue_root = reject_symlinks(queue_root)
    queue = safe_path(queue_root, episode, must_exist=False)
    require(queue.is_dir(), "Exact active READY queue required")
    path = reject_symlinks(queue / ".packet-publication.lock")
    fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        require(stat.S_ISREG(os.fstat(fd).st_mode), "Packet publication lock must be a regular file")
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ValueError("resource_busy: queue packet publication lock") from None
        return _replace_unpublished_candidate(queue_root, index_path, episode, review_path, execute=execute)
    finally:
        os.close(fd)


def _replace_unpublished_candidate(queue_root, index_path, episode, review_path, *, execute=False):
    """Opt-in, reviewed SVG-only correction of one never-published local candidate.

    The default importer remains create-only. This route refreshes origin/main
    read-only and requires a Terra review binding the exact old index, old/new
    three-file packet hashes and full source inventories, base commit, evidence,
    and an outside-Git preservation directory. READY, handoff and packet source
    bytes are unchanged; the outer wrapper uses the protocol's coordination lock.
    """
    require(episode and review_path, "Replacement requires one episode and an exact Terra review")
    queue_root, index = reject_symlinks(queue_root), reject_symlinks(index_path)
    require(queue_root.name == "_studio_queue", "Replacement requires the canonical queue root")
    producer = queue_root.resolve().parent
    scheduled = producer / "_studio_motion_codex/production/scheduled_runs"
    review_path = reject_symlinks(review_path)
    require(review_path.is_absolute() and review_path.resolve().is_relative_to(scheduled.resolve()),
            "Replacement review must remain in the producer's scheduled run")
    review_raw = review_path.read_bytes()
    review = json_object(review_raw, "Replacement review")
    reviewer = review.get("reviewer", review.get("reviewed_by", ""))
    require(review.get("schema_version") == 1 and review.get("report_type") == REPLACEMENT_REPORT
            and review.get("approved") is True and review.get("model") == "gpt-6.1-terra"
            and isinstance(reviewer, str)
            and re.fullmatch(r"Terra(?: 6\.1)? /root(?:/[a-z0-9_]+)+", reviewer),
            "An actual exact Terra replacement approval is required")
    require(review.get("episode_id") == episode, "Replacement review episode differs")
    evidence = review.get("evidence")
    require(isinstance(evidence, list) and len(evidence) >= 2,
            "Replacement review must bind source-display and canonical queue-revision evidence")
    evidence_paths, evidence_bindings = set(), []
    for item in evidence:
        path = reject_symlinks(item["path"])
        require(path.is_absolute() and path.resolve().is_relative_to(producer)
                and path.is_file() and sha256(path.read_bytes()) == item["sha256"],
                "Replacement review evidence changed")
        evidence_paths.add(str(path))
        evidence_bindings.append((path, item["sha256"]))
    require(len(evidence_paths) == len(evidence), "Duplicate replacement review evidence")
    queue = safe_path(queue_root, episode, must_exist=False)
    require(queue.is_dir() and queue.parent == queue_root, "Exact active READY queue required")
    unpublished_queue(queue_root, queue, episode)
    candidate = read_candidate(queue)
    require(candidate["status"] == "validated", "A finalized READY replacement is required")
    new = candidate["record"]
    require(candidate["source_episode"] == episode and review.get("new_source_sha256") == new["source_sha256"]
            and review.get("new_packet_sha256") == packet_source_hashes(new),
            "Reviewed new packet/source hashes differ")
    root = index.parent.parent.resolve()
    require(index.resolve() == root / "data/research_packets.json", "Canonical site registry required")
    require(replacement_git(root, "rev-parse", "--show-toplevel").decode().strip() == str(root),
            "Unpublished replacement repository differs")
    require(replacement_git(root, "remote", "get-url", "origin").decode().strip() == REPLACEMENT_ORIGIN,
            "Unpublished replacement origin differs")
    replacement_git(root, "fetch", "--no-tags", "origin", "main")
    base = review.get("base_commit")
    require(isinstance(base, str) and re.fullmatch(r"[0-9a-f]{40}", base)
            and replacement_git(root, "rev-parse", "HEAD").decode().strip() == base
            and replacement_git(root, "rev-parse", "refs/remotes/origin/main").decode().strip() == base,
            "Fresh origin/main and HEAD must match the reviewed base")
    html = safe_path(index.parent, new["html_file"])
    relative_html = str(html.relative_to(root))
    require(replacement_git(root, "ls-files", "--error-unmatch", "--", relative_html, missing=True) is None,
            "Tracked packet HTML cannot be replaced")
    require(not replacement_git(root, "log", "--all", "--format=%H", "--", relative_html).strip(),
            "Packet HTML has Git history and is not unpublished")
    for ref in ("HEAD", "refs/remotes/origin/main"):
        require(replacement_git(root, "cat-file", "-e", ref + ":" + relative_html, missing=True) is None,
                "Published packet HTML cannot be replaced")
        document = json.loads(replacement_git(root, "show", ref + ":data/research_packets.json"))
        require(not any(p["key"] == new["key"] or p["slug"] == new["slug"]
                        for p in document["packets"]), "Published packet registry identity cannot be replaced")
    preserve = reject_symlinks(review["preserve_directory"])
    require(preserve.is_absolute() and preserve.resolve().is_relative_to(scheduled.resolve())
            and not preserve.resolve().is_relative_to(root), "Preserve old bytes outside the Git checkout")
    lock = os.open(html.parent, os.O_RDONLY)
    try:
        fcntl.flock(lock, fcntl.LOCK_EX)
        journal_path = reject_symlinks(preserve / "journal.json")
        journal = json_object(journal_path.read_bytes(), "Replacement journal") if journal_path.exists() else None
        if journal:
            require(journal.get("review_sha256") == sha256(review_raw)
                    and journal.get("episode_id") == episode, "Replacement journal belongs to another approval")
            old_index = safe_path(preserve, "old-index.json").read_bytes()
            old_html = safe_path(preserve, "old-packet.html").read_bytes()
        else:
            old_index, old_html = index.read_bytes(), html.read_bytes()
        require(sha256(old_index) == review.get("expected_index_sha256"), "Reviewed registry hash changed")
        document = json_object(old_index, "Old registry")
        require(document.get("schema_version") == 1 and isinstance(document.get("packets"), list),
                "Invalid preserved registry")
        records = [validate_record(p) for p in document["packets"]]
        matched = [p for p in records if p["key"] == new["key"] or p["slug"] == new["slug"]]
        require(len(matched) == 1, "Exactly one unpublished registry candidate required")
        old = matched[0]
        require(review.get("old_source_sha256") == old.get("source_sha256")
                and review.get("old_packet_sha256") == packet_source_hashes(old)
                and sha256(old_html) == old["sha256"], "Reviewed old packet/source hashes differ")
        changing = {"sha256", "source_sha256", "source_staged_at"}
        require({k: v for k, v in old.items() if k not in changing} ==
                {k: v for k, v in new.items() if k not in changing},
                "Replacement changed packet identity, financial summary, links or other metadata")
        svg = re.compile(rb"<svg\b[^>]*>.*?</svg\s*>", re.I | re.S)
        old_svgs, new_svgs = svg.findall(old_html), svg.findall(candidate["html_bytes"])
        require(old_svgs and len(old_svgs) == len(new_svgs)
                and svg.sub(b"<svg/>\n", old_html) == svg.sub(b"<svg/>\n", candidate["html_bytes"]),
                "Only source-reviewed SVG display content may change")
        require(old_html != candidate["html_bytes"], "Unpublished replacement must change reviewed SVG bytes")
        updated = [new if p["key"] == old["key"] else p for p in records]
        next_index = index_bytes(updated)
        current_index, current_html = index.read_bytes(), html.read_bytes()
        if journal:
            require(journal.get("old_index_sha256") == sha256(old_index)
                    and journal.get("new_index_sha256") == sha256(next_index)
                    and journal.get("old_html_sha256") == sha256(old_html)
                    and journal.get("new_html_sha256") == new["sha256"], "Replacement journal hashes differ")
            require(current_index in (old_index, next_index) and current_html in (old_html, candidate["html_bytes"]),
                    "Replacement state requires explicit reconciliation")
        else:
            require(current_index == old_index and current_html == old_html, "Unpublished candidate changed")
        complete = current_index == next_index and current_html == candidate["html_bytes"]
        require(complete or current_index == old_index, "Unexpected partial replacement state")
        result = {"mode": "execute" if execute else "plan", "network_called": True,
                  "network_scope": "read-only origin/main refresh", "execution_ready": True,
                  "status": "already_replaced" if complete else "ready_to_replace_unpublished",
                  "episode_id": episode, "old_packet_sha256": packet_source_hashes(old),
                  "new_packet_sha256": packet_source_hashes(new), "other_packet_records_preserved": len(records) - 1,
                  "review_sha256": sha256(review_raw), "preserve_directory": str(preserve),
                  "expected_index_sha256": sha256(old_index), "new_index_sha256": sha256(next_index)}
        unchanged(candidate)
        require(review_path.read_bytes() == review_raw, "Replacement approval changed")

        def recheck_before_mutation():
            # The caller still holds the queue's .packet-publication.lock and
            # the site directory lock. Revalidate after Git/network/backup IO.
            unpublished_queue(queue_root, queue, episode)
            require(replacement_git(root, "rev-parse", "HEAD").decode().strip() == base
                    and replacement_git(root, "rev-parse", "refs/remotes/origin/main").decode().strip() == base,
                    "Reviewed Git base changed before replacement")
            require(replacement_git(root, "ls-files", "--error-unmatch", "--", relative_html, missing=True) is None,
                    "Packet was tracked before replacement")
            require(not replacement_git(root, "log", "--all", "--format=%H", "--", relative_html).strip(),
                    "Packet entered Git history before replacement")
            unchanged(candidate)
            require(review_path.read_bytes() == review_raw and all(
                sha256(reject_symlinks(p).read_bytes()) == digest for p, digest in evidence_bindings),
                "Replacement approval/evidence changed before execution")

        if execute and not complete:
            recheck_before_mutation()
            preserve.mkdir(parents=True, exist_ok=True)
            preserve_bytes(preserve / "old-index.json", old_index)
            preserve_bytes(preserve / "old-packet.html", old_html)
            preserve_bytes(preserve / "review.json", review_raw)
            journal = {"episode_id": episode, "review_sha256": sha256(review_raw),
                       "old_index_sha256": sha256(old_index), "new_index_sha256": sha256(next_index),
                       "old_html_sha256": sha256(old_html), "new_html_sha256": new["sha256"], "phase": "prepared"}
            replace_bytes(journal_path, (json.dumps(journal, indent=2) + "\n").encode())
            recheck_before_mutation()
            require(index.read_bytes() == old_index and html.read_bytes() in (old_html, candidate["html_bytes"]),
                    "Candidate changed before replacement")
            replace_bytes(html, candidate["html_bytes"])
            recheck_before_mutation()
            require(review_path.read_bytes() == review_raw and index.read_bytes() == old_index
                    and html.read_bytes() == candidate["html_bytes"],
                    "Replacement inputs changed before registry commit")
            atomic_index(index, updated)
            journal["phase"] = "complete"
            replace_bytes(journal_path, (json.dumps(journal, indent=2) + "\n").encode())
            result["status"] = "replaced_unpublished"
        return result
    finally:
        os.close(lock)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--queue-root", type=Path, default=DEFAULT_QUEUE)
    parser.add_argument("--index", type=Path, default=DEFAULT_INDEX)
    parser.add_argument("--episode", help="Optionally select one exact READY episode ID")
    parser.add_argument("--execute", action="store_true", help="Copy validated HTML and atomically update registry")
    parser.add_argument("--replace-unpublished-candidate", action="store_true",
                        help="Opt in to one exact Terra-reviewed, never-published SVG display correction")
    parser.add_argument("--replacement-review", type=Path, help="Exact scheduled-run Terra replacement approval")
    args = parser.parse_args()
    if args.replace_unpublished_candidate:
        if not args.episode or not args.replacement_review:
            parser.error("--replace-unpublished-candidate requires --episode and --replacement-review")
        try:
            result = replace_unpublished_candidate(args.queue_root, args.index, args.episode,
                                                   args.replacement_review, execute=args.execute)
        except (ValueError, OSError, KeyError, TypeError, subprocess.SubprocessError) as exc:
            print(json.dumps({"status": "blocked", "error": str(exc)}))
            return 1
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    if args.replacement_review:
        parser.error("--replacement-review requires --replace-unpublished-candidate")
    result = sync_packets(args.queue_root, args.index, execute=args.execute, episode=args.episode)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 1 if result["errors"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
