#!/usr/bin/env python3
"""Plan or import finalized READY research packets. No network, queue writes, or deploys."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import fcntl
from html.parser import HTMLParser
import json
import os
from pathlib import Path
import re
import sys
import tempfile

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from research_packets import (DEFAULT_INDEX, load_packets, packet_html_path, packet_sort_key,
                              period_identity, reject_symlinks, require, safe_path, sha256,
                              validate_record)

DEFAULT_QUEUE = Path.home() / "Desktop/CHARGED ALPHA EPISODES/_studio_queue"
PACKET_REQUIRED_FROM = datetime(2026, 9, 8, tzinfo=timezone.utc)
IMMUTABLE_FIELDS = ("key", "ticker", "company", "period", "year", "quarter", "fiscal", "slug",
                    "canonical_url", "title", "page_title", "description", "sha256", "source_episode")


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


def json_object(raw, label):
    value = json.loads(raw)
    require(isinstance(value, dict), f"{label} must be a JSON object")
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
        return {"status": "pending_source" if when >= PACKET_REQUIRED_FROM else "legacy_no_packet",
                "source_episode": episode, "queue_dir": str(queue),
                "reason": "no packet" if when >= PACKET_REQUIRED_FROM else "Older packet-null handoff is valid"}
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
    for key in ("ticker", "period"):
        require(handoff.get(key) == meta.get(key) == cm.get(key), f"Packet {key} identity differs")
    require(handoff.get("company") == cm.get("company"), "Packet company identity differs")
    for key in ("slug", "title", "page_title"):
        require(packet.get(key) == cm.get(key), f"Packet content {key} differs")
    parsed = HeadMetadata()
    parsed.feed(raw[names["html"]].decode("utf-8"))
    require(len(parsed.descriptions) == 1 and parsed.descriptions[0].strip(), "Exactly one HTML meta description required")
    require(len(parsed.titles) == 1 and parsed.titles[0].strip() == packet["page_title"], "HTML page title differs")
    require(parsed.descriptions[0] == cm.get("description"), "HTML description differs from packet content")
    require(not parsed.canonicals or parsed.canonicals == [canonical], "HTML canonical URL differs")
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
    record = {**identity, "ticker": handoff["ticker"], "company": handoff["company"],
              "period": handoff["period"], "canonical_url": canonical, "title": packet["title"],
              "page_title": packet["page_title"], "description": parsed.descriptions[0], "sha256": digest,
              "html_file": f"research_packets/{identity['slug']}.html", "source_episode": episode,
              "source_staged_at": staged_at, "source_sha256": {name: sha256(data) for name, data in raw.items()},
              **videos}
    if cm.get("published"):
        require(isinstance(cm["published"], str), "Invalid source publication date")
        record["source_published"] = cm["published"]
    for field in ("words", "figures", "tables"):
        if field in meta:
            require(type(meta[field]) is int and meta[field] >= 0, f"Invalid packet {field} count")
            record[field] = meta[field]
    validate_record(record)
    return {"status": "validated", "queue_dir": str(queue), "source_episode": episode,
            "record": record, "source_bytes": raw, "html_bytes": raw[names["html"]]}


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


def classify(candidate, records, index):
    record = candidate["record"]
    matched = [p for p in records if p["key"] == record["key"] or p["slug"] == record["slug"]]
    if matched:
        require(len(matched) == 1 and all(matched[0][k] == record[k] for k in IMMUTABLE_FIELDS),
                "Immutable ticker/period packet conflicts with registry; no overwrite")
        packet_html_path(matched[0], index)
        return "already_imported"
    path = safe_path(index.parent, record["html_file"], must_exist=False)
    if path.exists():
        require(path.is_file() and sha256(path.read_bytes()) == record["sha256"], "Existing unindexed HTML conflicts; no overwrite")
    return "ready_to_import"


def atomic_index(index, records):
    payload = (json.dumps({"schema_version": 1, "packets": sorted(records, key=packet_sort_key)},
                          ensure_ascii=False, indent=2) + "\n").encode("utf-8")
    fd, name = tempfile.mkstemp(prefix=".research-packets-", suffix=".json", dir=index.parent)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(payload); stream.flush(); os.fsync(stream.fileno())
        os.chmod(name, 0o644)
        os.replace(name, index)
    finally:
        if os.path.exists(name):
            os.unlink(name)


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
                    candidate["status"] = status
                except (ValueError, OSError, KeyError, TypeError) as exc:
                    candidate["status"], candidate["reason"] = "error", str(exc)
        finally:
            os.close(lock)
    results.extend({k: v for k, v in c.items() if k not in ("source_bytes", "html_bytes")} for c in candidates)
    return {"mode": "execute" if execute else "plan", "index_path": str(index), "packets": results,
            "errors": sum(p["status"] == "error" for p in results),
            "pending_source": sum(p["status"] == "pending_source" for p in results),
            "imported": sum(p["status"] == "imported" for p in results)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--queue-root", type=Path, default=DEFAULT_QUEUE)
    parser.add_argument("--index", type=Path, default=DEFAULT_INDEX)
    parser.add_argument("--episode", help="Optionally select one exact READY episode ID")
    parser.add_argument("--execute", action="store_true", help="Copy validated HTML and atomically update registry")
    args = parser.parse_args()
    result = sync_packets(args.queue_root, args.index, execute=args.execute, episode=args.episode)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 1 if result["errors"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
