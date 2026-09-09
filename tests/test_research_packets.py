import copy
import hashlib
import html
import json
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch

from research_packets import (load_packets, packet_for_slug, packet_html_path,
                              packets_for_ticker, period_identity)
from scripts import sync_research_packets as sync


class ResearchPacketTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.queue = self.root / "queue"
        self.queue.mkdir()
        self.index = self.root / "site/data/research_packets.json"

    def fixture(self, ticker="BZUN", period="Q2 2026", *, done=False, description="Exact A & B research."):
        identity = period_identity(ticker, period)
        episode = identity["key"].replace(":", "-")
        folder = self.queue / ("_done" if done else "") / episode
        (folder / "packet").mkdir(parents=True)
        title = f"{ticker} {period} Research Packet — Charged Alpha"
        slug = identity["slug"]
        raw = ("<!doctype html><html><head><title>" + title + "</title>"
               "<meta content=\"" + html.escape(description, quote=True) + "\" name=\"description\">"
               "<style>body{color:#123}</style></head><body><h1>Original packet</h1>"
               "<svg><text>No invented metrics</text></svg></body></html>\n").encode()
        digest = hashlib.sha256(raw).hexdigest()
        packet = {"dir": "packet", "file": f"{slug}-research-packet.html", "slug": slug,
                  "canonical_url": f"https://chargedalpha.com/research/{slug}",
                  "title": f"{ticker}: source analysis", "page_title": title,
                  "sha256": digest, "finalized": True}
        links = {"youtube_long": "abcdefghijk", "youtube_short": None,
                 "youtube_studio": "lmnopqrstuv", "youtube_studio_short": None}
        hand = {"episode": episode, "ticker": ticker, "company": f"{ticker} Company",
                "period": period, "staged_at": "2026-09-08T00:00:00Z",
                "primary": {"youtube_long": links["youtube_long"]}, "packet": packet}
        meta = {k: v for k, v in packet.items() if k != "dir"}
        meta.update(ticker=ticker, period=period, bytes=len(raw), words=100, figures=2, tables=1, links=links)
        content = {"meta": {"ticker": ticker, "company": hand["company"], "period": period,
                            "slug": slug, "title": packet["title"], "page_title": title,
                            "description": description, "published": "September 7, 2026"}, "links": links}
        (folder / "READY").write_text(hand["staged_at"] + "\n")
        (folder / "handoff.json").write_text(json.dumps(hand))
        (folder / "packet/packet_meta.json").write_text(json.dumps(meta))
        (folder / "packet/packet.json").write_text(json.dumps(content))
        (folder / "packet" / packet["file"]).write_bytes(raw)
        return folder, raw

    def import_all(self, execute=True):
        return sync.sync_packets(self.queue, self.index, execute=execute)

    def mutate(self, path, callback):
        value = json.loads(path.read_text()); callback(value); path.write_text(json.dumps(value))

    def both_archive(self, source, video_id="Cead9QgqB5U"):
        handoff = json.loads((source / "handoff.json").read_text())
        episode, ticker = handoff["episode"], handoff["ticker"]
        target = self.queue / "_done" / (episode + "--studio-both-" + video_id)
        target.parent.mkdir(exist_ok=True)
        source.rename(target)
        done = {"schema_version": 2, "episode_id": episode, "ticker": ticker}
        for field, cut, video in (("long_form", "studio-v1", "cxhzMaX5Ak4"),
                                  ("short", "studio-short-v1", video_id)):
            done[field] = {"episode_id": episode, "ticker": ticker, "cut_id": cut, "video_id": video}
        (target / "DONE").write_text(json.dumps(done))
        return target

    def test_plan_writes_nothing_and_execute_preserves_exact_bytes_and_metadata(self):
        source, raw = self.fixture()
        before = {str(p): p.read_bytes() for p in source.rglob("*") if p.is_file()}
        plan = self.import_all(False)
        self.assertEqual(plan["packets"][0]["status"], "ready_to_import")
        self.assertFalse(self.index.parent.exists())
        result = self.import_all()
        self.assertEqual(result["imported"], 1)
        packet = load_packets(self.index)[0]
        self.assertEqual(packet_html_path(packet, self.index).read_bytes(), raw)
        self.assertEqual(packet["description"], "Exact A & B research.")
        self.assertEqual((packet["figures"], packet["tables"], packet["words"]), (2, 1, 100))
        self.assertEqual(packet["primary_youtube_long"], "abcdefghijk")
        self.assertEqual(packet["source_published"], "September 7, 2026")
        self.assertEqual(before, {str(p): p.read_bytes() for p in source.rglob("*") if p.is_file()})

    def test_idempotent_import_has_no_file_or_timestamp_churn(self):
        self.fixture(); self.import_all()
        files = {p: (p.read_bytes(), p.stat().st_mtime_ns) for p in self.index.parent.rglob("*") if p.is_file()}
        result = self.import_all()
        self.assertEqual(result["imported"], 0)
        self.assertEqual(result["packets"][0]["status"], "already_imported")
        self.assertEqual(files, {p: (p.read_bytes(), p.stat().st_mtime_ns) for p in files})

    def test_casy_display_legal_company_spelling_preserves_exact_source(self):
        source, raw = self.fixture("CASY", "Q1 FY2027")
        self.mutate(source / "handoff.json", lambda h: h.update(company="Casey's General Stores, Inc."))
        self.mutate(source / "packet/packet.json", lambda p: p["meta"].update(company="Casey’s General Stores"))
        before = {p: p.read_bytes() for p in source.rglob("*") if p.is_file()}
        self.assertEqual(self.import_all(False)["packets"][0]["status"], "ready_to_import")
        self.assertEqual(self.import_all()["imported"], 1)
        record = load_packets(self.index)[0]
        self.assertEqual(record["company"], "Casey's General Stores, Inc.")
        self.assertEqual(packet_html_path(record, self.index).read_bytes(), raw)
        self.assertEqual(before, {p: p.read_bytes() for p in before})
        self.assertEqual(record["source_sha256"]["packet/packet.json"],
                         hashlib.sha256(before[source / "packet/packet.json"]).hexdigest())

    def test_company_normalization_is_limited_to_declared_formatting(self):
        self.assertEqual(sync.canonical_company_name("  Ｃasey’s\u00a0 General\nStores, INC.  "),
                         sync.canonical_company_name("Casey's General Stores"))
        for name in ("Casey's General Store", "Casey's General Stores Corporation",
                     "Casey's General Stores, LLC", "Casey's General Stores Inc.",
                     "Casey's General Stores, Inc. Holdings"):
            with self.subTest(name=name):
                self.assertNotEqual(sync.canonical_company_name(name),
                                    sync.canonical_company_name("Casey's General Stores"))

    def test_company_substantive_mismatch_or_empty_identity_refuses_import(self):
        for name in ("Different Company", "CASY", "", "  ", None, 42, ", Inc."):
            with self.subTest(name=name):
                shutil.rmtree(self.queue); self.queue.mkdir()
                source, _ = self.fixture("CASY", "Q1 FY2027")
                self.mutate(source / "packet/packet.json", lambda p: p["meta"].update(company=name))
                self.assertEqual(self.import_all()["errors"], 1)
                self.assertFalse(self.index.exists())

    def test_year_quarter_history_and_ticker_queries(self):
        for period in ("Q2 2026", "Q4 FY2025", "Q1 FY2027", "Q3 2026"):
            self.fixture(period=period)
        self.fixture("IOT", "Q4 FY2027")
        self.assertEqual(self.import_all()["imported"], 5)
        packets = packets_for_ticker(" bzun ", self.index)
        self.assertEqual([p["period"] for p in packets], ["Q1 FY2027", "Q3 2026", "Q2 2026", "Q4 FY2025"])
        self.assertTrue(packets[0]["fiscal"])
        self.assertFalse(packets[1]["fiscal"])
        self.assertEqual(packet_for_slug("bzun-q2-2026", self.index)["ticker"], "BZUN")
        self.assertIsNone(packet_for_slug("../../secrets", self.index))

    def test_ready_only_and_done_recovery_without_duplicate_import(self):
        source, _ = self.fixture()
        target = self.queue / "_done" / source.name
        target.parent.mkdir(); shutil.copytree(source, target)
        incomplete, _ = self.fixture("WAIT", "Q2 2026")
        (incomplete / "READY").unlink()
        plan = self.import_all(False)
        self.assertEqual([p["status"] for p in plan["packets"]], ["ready_to_import", "duplicate_ready"])
        self.assertEqual(self.import_all()["imported"], 1)
        shutil.rmtree(source)
        self.assertEqual(self.import_all()["packets"][0]["status"], "already_imported")

    def test_completed_sibling_old_null_packet_and_episode_filter(self):
        source, _ = self.fixture("IOT", "Q2 FY2027")
        self.mutate(source / "handoff.json", lambda h: h.update(packet=None, staged_at="2026-09-07T14:00:00Z"))
        (source / "READY").write_text("2026-09-07T14:00:00Z\n")
        self.both_archive(source)
        report = self.import_all(False)
        self.assertEqual(report["errors"], 0)
        self.assertEqual(report["packets"][0]["status"], "legacy_no_packet")
        self.assertEqual(report["packets"][0]["source_episode"], "IOT-Q2-FY2027")
        self.fixture()  # A filtered recovery need not inspect another episode.
        selected = sync.sync_packets(self.queue, self.index, episode="IOT-Q2-FY2027")
        self.assertEqual(len(selected["packets"]), 1)
        self.assertEqual(selected["packets"][0]["status"], "legacy_no_packet")
        self.assertFalse(self.index.exists())

    def test_completed_sibling_packet_recovers_with_canonical_identity_and_done_hash(self):
        source, raw = self.fixture("IOT", "Q2 FY2027")
        target = self.both_archive(source)
        plan = sync.sync_packets(self.queue, self.index, episode="IOT-Q2-FY2027")
        self.assertEqual(plan["packets"][0]["status"], "ready_to_import")
        self.assertFalse(self.index.exists())
        report = sync.sync_packets(self.queue, self.index, episode="IOT-Q2-FY2027", execute=True)
        self.assertEqual((report["errors"], report["imported"]), (0, 1))
        packet = load_packets(self.index)[0]
        self.assertEqual(packet["source_episode"], "IOT-Q2-FY2027")
        self.assertEqual(packet["source_sha256"]["DONE"], hashlib.sha256((target / "DONE").read_bytes()).hexdigest())
        self.assertEqual(packet_html_path(packet, self.index).read_bytes(), raw)
        self.assertEqual(self.import_all()["packets"][0]["status"], "already_imported")

    def test_completed_sibling_requires_exact_suffix_location_and_done_binding(self):
        mutations = [lambda d: d.update(schema_version=1),
                     lambda d: d.update(episode_id="OTHER-Q2-FY2027"),
                     lambda d: d["short"].update(video_id="zzzzzzzzzzz"),
                     lambda d: d["long_form"].update(ticker="OTHER"),
                     lambda d: d["short"].update(cut_id="studio-v1")]
        for change in mutations:
            with self.subTest(change=change):
                shutil.rmtree(self.queue); self.queue.mkdir()
                source, _ = self.fixture("IOT", "Q2 FY2027")
                target = self.both_archive(source)
                self.mutate(target / "DONE", change)
                self.assertEqual(self.import_all()["errors"], 1)
                self.assertFalse(self.index.exists())
        shutil.rmtree(self.queue); self.queue.mkdir()
        source, _ = self.fixture("IOT", "Q2 FY2027")
        target = self.both_archive(source)
        target = target.rename(self.queue / target.name)
        self.assertEqual(self.import_all()["errors"], 1)  # Suffix is not valid in the active queue.
        target = target.rename(self.queue / "_done" / "IOT-Q2-FY2027--arbitrary-Cead9QgqB5U")
        self.assertEqual(self.import_all()["errors"], 1)
        target = target.rename(target.with_name("IOT-Q2-FY2027--studio-both-Cead9QgqB5U"))
        (target / "DONE").unlink()
        self.assertEqual(self.import_all()["errors"], 1)

    def test_completed_sibling_done_changes_during_import_refuse_commit(self):
        source, _ = self.fixture("IOT", "Q2 FY2027")
        target = self.both_archive(source)
        original = sync.unchanged
        def changed(candidate):
            self.mutate(target / "DONE", lambda d: d["short"].update(video_id="zzzzzzzzzzz"))
            original(candidate)
        with patch.object(sync, "unchanged", side_effect=changed):
            self.assertEqual(self.import_all()["errors"], 1)
        self.assertFalse(self.index.exists())

    def test_hash_or_identity_conflict_does_not_overwrite_and_other_packet_can_import(self):
        first, _ = self.fixture(); self.import_all()
        before = self.index.read_bytes()
        p = first / "packet/packet_meta.json"
        self.mutate(p, lambda m: m.update(sha256="0" * 64))
        self.fixture("IOT", "Q2 FY2027")
        report = self.import_all()
        self.assertEqual((report["errors"], report["imported"]), (1, 1))
        self.assertEqual(packet_for_slug("bzun-q2-2026", self.index)["sha256"], json.loads(before)["packets"][0]["sha256"])

    def test_valid_but_changed_packet_is_an_immutable_conflict(self):
        first, _ = self.fixture(); self.import_all()
        prior = self.index.read_bytes(); old_html = packet_html_path(load_packets(self.index)[0], self.index).read_bytes()
        shutil.rmtree(first); self.fixture(description="A changed finalized analysis.")
        report = self.import_all()
        self.assertEqual(report["errors"], 1)
        self.assertIn("conflicts", report["packets"][0]["reason"])
        self.assertEqual(self.index.read_bytes(), prior)
        self.assertEqual(packet_html_path(load_packets(self.index)[0], self.index).read_bytes(), old_html)

    def test_unfinalized_invalid_slug_canonical_and_metadata_are_rejected(self):
        mutations = [lambda h: h["packet"].update(finalized=False),
                     lambda h: h["packet"].update(slug="../bad"),
                     lambda h: h["packet"].update(canonical_url="https://evil.test/research/bzun-q2-2026"),
                     lambda h: h.update(ticker="IOT"),
                     lambda h: h["packet"].update(dir="../../outside")]
        for change in mutations:
            with self.subTest(change=change):
                shutil.rmtree(self.queue); self.queue.mkdir(); folder, _ = self.fixture()
                self.mutate(folder / "handoff.json", change)
                self.assertEqual(self.import_all()["errors"], 1)
                self.assertFalse(self.index.exists())

    def test_symlink_source_and_destination_cannot_escape(self):
        source, raw = self.fixture()
        html_path = next((source / "packet").glob("*.html"))
        other = self.root / "elsewhere.html"; other.write_bytes(raw)
        html_path.unlink(); html_path.symlink_to(other)
        self.assertEqual(self.import_all()["errors"], 1)
        self.assertFalse(self.index.exists())
        html_path.unlink(); html_path.write_bytes(raw)
        self.index.parent.mkdir(parents=True)
        (self.index.parent / "research_packets").symlink_to(self.root, target_is_directory=True)
        self.assertEqual(self.import_all()["errors"], 1)
        self.assertFalse(self.index.exists())

    def test_missing_new_packet_is_pending_while_older_null_is_valid(self):
        old, _ = self.fixture("OLD", "Q1 2026")
        new, _ = self.fixture("NEW", "Q1 2026")
        self.mutate(old / "handoff.json", lambda h: h.update(packet=None, staged_at="2026-09-07T23:59:59Z"))
        (old / "READY").write_text("2026-09-07T23:59:59Z\n")
        self.mutate(new / "handoff.json", lambda h: h.update(packet=None))
        report = self.import_all()
        self.assertEqual(report["pending_source"], 1)
        self.assertEqual({p["status"] for p in report["packets"]}, {"legacy_no_packet", "pending_source"})
        self.assertFalse(self.index.exists())
        self.assertFalse((new / "BLOCKED").exists())
        (old / "READY").write_text("2026-09-08T00:01:00Z\n")
        self.assertEqual(self.import_all()["pending_source"], 2)

    def test_changed_source_during_execution_refuses_commit(self):
        source, _ = self.fixture()
        original = sync.unchanged
        def changed(candidate):
            (source / "READY").write_text("changed during plan")
            original(candidate)
        with patch.object(sync, "unchanged", side_effect=changed):
            self.assertEqual(self.import_all()["errors"], 1)
        self.assertFalse(self.index.exists())

    def test_failed_index_commit_recovers_exact_orphan_html(self):
        _, raw = self.fixture()
        with patch.object(sync, "atomic_index", side_effect=OSError("simulated interrupted commit")):
            self.assertEqual(self.import_all()["errors"], 1)
        self.assertFalse(self.index.exists())
        self.assertEqual((self.index.parent / "research_packets/bzun-q2-2026.html").read_bytes(), raw)
        self.assertEqual(self.import_all()["imported"], 1)

    def test_serving_detects_corruption_and_registry_rejects_duplicate_history(self):
        self.assertEqual(load_packets(self.index), [])
        self.fixture(); self.import_all()
        packet = load_packets(self.index)[0]
        path = packet_html_path(packet, self.index); path.write_bytes(b"changed")
        with self.assertRaisesRegex(ValueError, "hash mismatch"):
            packet_html_path(packet, self.index)
        self.mutate(self.index, lambda d: d["packets"].append(copy.deepcopy(d["packets"][0])))
        with self.assertRaisesRegex(ValueError, "Duplicate"):
            load_packets(self.index)

    def test_duplicate_description_or_conflicting_primary_links_fail_validation(self):
        source, _ = self.fixture()
        self.mutate(source / "handoff.json", lambda h: h["primary"].update(youtube_long="zzzzzzzzzzz"))
        self.assertEqual(self.import_all()["errors"], 1)
        self.assertFalse(self.index.exists())
        self.mutate(source / "handoff.json", lambda h: h["primary"].update(youtube_long="abcdefghijk"))
        page = next((source / "packet").glob("*.html"))
        raw = page.read_bytes().replace(b"</head>", b'<meta name="description" content="Second description"></head>')
        page.write_bytes(raw)
        digest = hashlib.sha256(raw).hexdigest()
        self.mutate(source / "handoff.json", lambda h: h["packet"].update(sha256=digest))
        self.mutate(source / "packet/packet_meta.json", lambda m: m.update(sha256=digest, bytes=len(raw)))
        report = self.import_all()
        self.assertEqual(report["errors"], 1)
        self.assertIn("Exactly one HTML meta description", report["packets"][0]["reason"])


if __name__ == "__main__":
    unittest.main()
