"""Read the immutable research-packet registry used by the public website."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import re

DEFAULT_INDEX = Path(__file__).resolve().parent / "data/research_packets.json"
SLUG_RE = re.compile(r"[a-z0-9]+(?:-[a-z0-9]+)*")
TICKER_RE = re.compile(r"[A-Z][A-Z0-9.\-]{0,12}")
SHA_RE = re.compile(r"[0-9a-f]{64}")


def require(condition, message):
    if not condition:
        raise ValueError(message)


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def reject_symlinks(path):
    path = Path(path).absolute()
    require(not any(p.is_symlink() for p in (path, *path.parents)), f"Symlink path refused: {path}")
    return path


def safe_path(root, relative, *, must_exist=True):
    root = reject_symlinks(root)
    require(isinstance(relative, str) and relative and "\\" not in relative, "Invalid relative packet path")
    rel = Path(relative)
    require(not rel.is_absolute() and ".." not in rel.parts, "Unsafe packet path")
    path = reject_symlinks(root / rel)
    require(root.resolve() in path.resolve().parents, "Packet path escapes its root")
    if must_exist:
        require(path.is_file(), f"Missing packet file: {path}")
    return path


def _period_data(ticker, period):
    require(isinstance(ticker, str) and TICKER_RE.fullmatch(ticker), "Invalid packet ticker")
    require(isinstance(period, str), "Invalid packet period")
    quarter_match = re.fullmatch(r"Q([1-4])\s+(FY\s*)?(\d{4})", period)
    annual_match = re.fullmatch(r"FY(\d{4})", period)
    half_match = re.fullmatch(r"H([1-2])\s+(FY\s*)?(\d{4})", period)
    require(quarter_match or annual_match or half_match,
            "Packet period must identify a quarter, half year, or fiscal year")
    if quarter_match:
        period_code = f"Q{quarter_match[1]}"
        quarter, fiscal, year = int(quarter_match[1]), bool(quarter_match[2]), int(quarter_match[3])
        order = quarter
    elif annual_match:
        period_code = "FY"
        quarter, fiscal, year, order = 0, True, int(annual_match[1]), 5
    else:
        period_code = f"H{half_match[1]}"
        quarter, fiscal, year = 0, bool(half_match[2]), int(half_match[3])
        order = 2.5 if period_code == "H1" else 4.5
    require(2000 <= year <= 2199, "Invalid packet reporting year")
    normalized = f"FY{year}" if annual_match else f"{period_code}-{'FY' if fiscal else ''}{year}"
    slug = re.sub(r"[.\-]+", "-", ticker.lower()) + "-" + normalized.lower()
    return ({"key": f"{ticker}:{normalized}", "year": year, "quarter": quarter,
             "fiscal": fiscal, "slug": slug}, order)


def period_identity(ticker, period):
    return _period_data(ticker, period)[0]


def validate_what_changed(value):
    """Validate the plain-text summary shared by stock pages and alert emails."""
    require(isinstance(value, dict), "Packet what_changed must be an object")
    require(set(value) == {"summary", "items"}, "Packet what_changed fields differ")
    summary = value["summary"]
    require(isinstance(summary, str) and summary.strip() == summary and 1 <= len(summary) <= 500,
            "Invalid packet what_changed summary")
    require("<" not in summary and ">" not in summary and "\n" not in summary and "\r" not in summary,
            "Packet what_changed summary must be plain text")
    items = value["items"]
    require(isinstance(items, list) and 3 <= len(items) <= 5,
            "Packet what_changed requires three to five items")
    for item in items:
        require(isinstance(item, dict) and set(item) == {"title", "detail"},
                "Invalid packet what_changed item")
        for field, maximum in (("title", 180), ("detail", 500)):
            text = item[field]
            require(isinstance(text, str) and text.strip() == text and 1 <= len(text) <= maximum,
                    f"Invalid packet what_changed {field}")
            require("<" not in text and ">" not in text and "\n" not in text and "\r" not in text,
                    f"Packet what_changed {field} must be plain text")
    return value


def validate_record(packet):
    require(isinstance(packet, dict), "Packet registry record must be an object")
    expected = period_identity(packet.get("ticker"), packet.get("period"))
    for key, value in expected.items():
        require(packet.get(key) == value, f"Packet registry {key} identity differs")
    slug = packet["slug"]
    require(SLUG_RE.fullmatch(slug), "Unsafe packet slug")
    require(packet.get("canonical_url") == f"https://chargedalpha.com/research/{slug}", "Invalid packet canonical URL")
    require(packet.get("html_file") == f"research_packets/{slug}.html", "Invalid registered HTML path")
    require(isinstance(packet.get("sha256"), str) and SHA_RE.fullmatch(packet["sha256"]), "Invalid packet SHA256")
    for field in ("company", "title", "page_title", "description", "source_episode"):
        require(isinstance(packet.get(field), str) and packet[field].strip(), f"Missing packet {field}")
    if "what_changed" in packet:
        validate_what_changed(packet["what_changed"])
    return packet


def packet_sort_key(packet):
    identity, order = _period_data(packet["ticker"], packet["period"])
    return (-identity["year"], -order, packet["ticker"], not identity["fiscal"], identity["slug"])


def load_packets(index_path=None):
    """Return validated records, newest reporting year/quarter first; absent => []."""
    path = reject_symlinks(index_path or DEFAULT_INDEX)
    if not path.exists():
        return []
    document = json.loads(path.read_bytes())
    require(isinstance(document, dict) and document.get("schema_version") == 1
            and isinstance(document.get("packets"), list), "Invalid research packet registry")
    packets = [validate_record(p) for p in document["packets"]]
    for field in ("key", "slug"):
        require(len({p[field] for p in packets}) == len(packets), f"Duplicate packet {field} in registry")
    return sorted(packets, key=packet_sort_key)


def packets_for_ticker(ticker, index_path=None):
    ticker = str(ticker).strip().upper()
    return [p for p in load_packets(index_path) if p["ticker"] == ticker]


def packet_for_slug(slug, index_path=None):
    if not isinstance(slug, str) or not SLUG_RE.fullmatch(slug):
        return None
    return next((p for p in load_packets(index_path) if p["slug"] == slug), None)


def packet_html_path(packet, index_path=None):
    """Resolve a registered HTML file and verify its exact bytes before serving."""
    validate_record(packet)
    index = reject_symlinks(index_path or DEFAULT_INDEX)
    path = safe_path(index.parent, packet["html_file"])
    require(sha256(path.read_bytes()) == packet["sha256"], "Registered packet HTML hash mismatch")
    return path
