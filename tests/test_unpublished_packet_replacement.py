"""Real-Git regression fixtures for the opt-in unpublished SVG correction route."""
import copy
import fcntl
import hashlib
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import test_research_packets as packet_fixtures
from scripts import sync_research_packets as sync


class UnpublishedPacketReplacementTests(unittest.TestCase):
    def git(self, *args):
        return subprocess.check_output(["git", "-C", str(self.site), *args], stderr=subprocess.DEVNULL).decode().strip()

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.producer = self.root / "producer"
        self.queue = self.producer / "_studio_queue"
        self.queue.mkdir(parents=True)
        self.site = self.root / "site"
        self.index = self.site / "data/research_packets.json"
        fixture = lambda ticker, period: packet_fixtures.ResearchPacketTests.fixture(self, ticker, period)
        fixture("TSEM", "Q2 2026")
        sync.sync_packets(self.queue, self.index, execute=True)
        self.git("init", "-q", "-b", "main")
        self.git("config", "user.name", "Local regression fixture")
        self.git("config", "user.email", "fixture@example.invalid")
        self.git("add", "data")
        self.git("commit", "-qm", "Published baseline without GLW")
        self.remote = self.root / "remote.git"
        subprocess.check_call(["git", "init", "--bare", "-q", str(self.remote)])
        self.git("remote", "add", "origin", str(self.remote))
        self.git("push", "-qu", "origin", "main")
        self.addCleanup(patch.stopall)
        patch.object(sync, "REPLACEMENT_ORIGIN", str(self.remote)).start()
        self.source, self.old_html = fixture("GLW", "Q3 2026")
        (self.source / ".packet-publication.lock").touch()
        self.episode = "GLW-Q3-2026"
        self.assertEqual(sync.sync_packets(self.queue, self.index, episode=self.episode, execute=True)["imported"], 1)
        self.old_index = self.index.read_bytes()
        self.old_record = next(p for p in json.loads(self.old_index)["packets"] if p["ticker"] == "GLW")
        self.target = self.index.parent / self.old_record["html_file"]
        self.run = self.producer / "_studio_motion_codex/production/scheduled_runs/replacement-test"
        self.run.mkdir(parents=True)
        self.review = self.run / "review.json"
        self.preserve = self.run / "preserved"
        for name in ("source-display-review.json", "queue-revision.json"):
            (self.run / name).write_text(json.dumps({"fixture": name, "approved": True}))
        self.revise()

    def revise(self, *, prose=False):
        hand = json.loads((self.source / "handoff.json").read_bytes())
        file = self.source / "packet" / hand["packet"]["file"]
        raw = self.old_html.replace(b"<text>", b'<text x="10">')
        if prose:
            raw = raw.replace(b"Original packet", b"Changed financial prose")
        file.write_bytes(raw)
        digest = hashlib.sha256(raw).hexdigest()
        hand["packet"]["sha256"] = digest
        hand["staged_at"] = "2026-09-08T00:01:00Z"
        (self.source / "handoff.json").write_text(json.dumps(hand))
        (self.source / "READY").write_text(hand["staged_at"] + "\n")
        meta = json.loads((self.source / "packet/packet_meta.json").read_bytes())
        meta.update(sha256=digest, bytes=len(raw))
        (self.source / "packet/packet_meta.json").write_text(json.dumps(meta))
        content = json.loads((self.source / "packet/packet.json").read_bytes())
        content["display_fixture"] = {"label_x": 10}
        (self.source / "packet/packet.json").write_text(json.dumps(content))
        self.new = sync.read_candidate(self.source)
        packet_hashes = lambda r: {
            "html": r["sha256"],
            "content": r["source_sha256"]["packet/packet.json"],
            "meta": r["source_sha256"]["packet/packet_meta.json"],
        }
        self.approval = {
            "schema_version": 1, "report_type": sync.REPLACEMENT_REPORT,
            "approved": True, "model": "gpt-6.1-terra", "reviewer": "Terra 6.1 /root/replacement_fixture",
            "episode_id": self.episode, "base_commit": self.git("rev-parse", "HEAD"),
            "expected_index_sha256": hashlib.sha256(self.old_index).hexdigest(),
            "old_source_sha256": self.old_record["source_sha256"],
            "old_packet_sha256": packet_hashes(self.old_record),
            "new_source_sha256": self.new["record"]["source_sha256"],
            "new_packet_sha256": packet_hashes(self.new["record"]),
            "preserve_directory": str(self.preserve),
            "evidence": [{"path": str(self.run / name),
                          "sha256": hashlib.sha256((self.run / name).read_bytes()).hexdigest()}
                         for name in ("source-display-review.json", "queue-revision.json")],
        }
        self.save_approval()

    def save_approval(self):
        self.review.write_text(json.dumps(self.approval, indent=2) + "\n")

    def replace(self, execute=False):
        return sync.replace_unpublished_candidate(self.queue, self.index, self.episode, self.review, execute=execute)

    def assert_original(self):
        self.assertEqual(self.index.read_bytes(), self.old_index)
        self.assertEqual(self.target.read_bytes(), self.old_html)

    def test_default_import_stays_immutable(self):
        result = sync.sync_packets(self.queue, self.index, episode=self.episode, execute=True)
        self.assertEqual(result["errors"], 1)
        self.assert_original()

    def test_plan_writes_no_packet_or_preservation_files(self):
        before = {str(p): p.read_bytes() for p in self.source.rglob("*") if p.is_file()}
        result = self.replace()
        self.assertEqual(result["status"], "ready_to_replace_unpublished")
        self.assert_original()
        self.assertFalse(self.preserve.exists())
        self.assertEqual(before, {str(p): p.read_bytes() for p in self.source.rglob("*") if p.is_file()})

    def test_execute_preserves_old_bytes_other_packets_and_queue_then_is_idempotent(self):
        before = {str(p): p.read_bytes() for p in self.source.rglob("*") if p.is_file()}
        old_other = [p for p in json.loads(self.old_index)["packets"] if p["ticker"] != "GLW"]
        other_html = {p["html_file"]: (self.index.parent / p["html_file"]).read_bytes() for p in old_other}
        result = self.replace(True)
        self.assertEqual(result["status"], "replaced_unpublished")
        self.assertEqual(self.target.read_bytes(), self.new["html_bytes"])
        records = json.loads(self.index.read_bytes())["packets"]
        self.assertEqual([p for p in records if p["ticker"] != "GLW"], old_other)
        for file, raw in other_html.items():
            self.assertEqual((self.index.parent / file).read_bytes(), raw)
        self.assertEqual((self.preserve / "old-index.json").read_bytes(), self.old_index)
        self.assertEqual((self.preserve / "old-packet.html").read_bytes(), self.old_html)
        self.assertEqual(before, {str(p): p.read_bytes() for p in self.source.rglob("*") if p.is_file()})
        self.assertEqual(self.replace(True)["status"], "already_replaced")

    def test_tracked_or_published_candidate_is_rejected(self):
        self.git("add", "data")
        self.git("commit", "-qm", "A published packet is immutable")
        self.git("push", "-q", "origin", "main")
        self.approval["base_commit"] = self.git("rev-parse", "HEAD")
        self.save_approval()
        with self.assertRaisesRegex(ValueError, "Tracked packet"):
            self.replace(True)
        self.assert_original()

    def test_removed_historical_packet_is_still_rejected(self):
        self.git("add", "data")
        self.git("commit", "-qm", "Packet once entered history")
        self.git("rm", "-q", "--", self.old_record["html_file"].replace("research_packets/", "data/research_packets/"))
        baseline = self.git("show", "HEAD^:data/research_packets.json") + "\n"
        self.index.write_text(baseline)
        self.git("add", "data")
        self.git("commit", "-qm", "Removed historical packet")
        self.git("push", "-q", "origin", "main")
        self.index.write_bytes(self.old_index)
        self.target.write_bytes(self.old_html)
        self.approval["base_commit"] = self.git("rev-parse", "HEAD")
        self.save_approval()
        with self.assertRaisesRegex(ValueError, "Git history"):
            self.replace(True)
        self.assert_original()

    def test_stale_git_base_is_rejected(self):
        (self.site / "README").write_text("New remote base")
        self.git("add", "README")
        self.git("commit", "-qm", "New base")
        self.git("push", "-q", "origin", "main")
        with self.assertRaisesRegex(ValueError, "Fresh origin/main"):
            self.replace(True)
        self.assert_original()

    def test_exact_review_and_all_source_hashes_are_required(self):
        for field, value in (("approved", False), ("model", "gpt-6.1-sol"),
                             ("expected_index_sha256", "0" * 64),
                             ("old_packet_sha256", {}), ("new_source_sha256", {})):
            original = copy.deepcopy(self.approval)
            self.approval[field] = value
            self.save_approval()
            with self.subTest(field=field), self.assertRaises(ValueError):
                self.replace(True)
            self.assert_original()
            self.approval = original
        self.save_approval()

    def test_completion_or_packet_marker_blocks_replacement(self):
        for marker in ("PACKET_DONE", "DONE", "SHORT_DONE", "LONGFORM_DONE"):
            path = self.source / marker
            path.write_text("Actual publication/completion marker fixture")
            with self.subTest(marker=marker), self.assertRaisesRegex(ValueError, "published/completed"):
                self.replace(True)
            self.assert_original()
            path.unlink()

    def test_non_svg_change_is_rejected_even_with_matching_new_hashes(self):
        self.revise(prose=True)
        with self.assertRaisesRegex(ValueError, "Only source-reviewed SVG"):
            self.replace(True)
        self.assert_original()

    def test_changed_index_or_review_evidence_is_rejected(self):
        self.index.write_bytes(self.old_index + b" ")
        with self.assertRaisesRegex(ValueError, "registry hash"):
            self.replace(True)
        self.index.write_bytes(self.old_index)
        (self.run / "source-display-review.json").write_text("Changed evidence")
        with self.assertRaisesRegex(ValueError, "review evidence changed"):
            self.replace(True)
        self.assert_original()

    def test_exact_partial_html_transaction_can_resume_without_losing_old_bytes(self):
        with patch.object(sync, "atomic_index", side_effect=OSError("Simulated stop after HTML")):
            with self.assertRaises(OSError):
                self.replace(True)
        self.assertEqual(self.target.read_bytes(), self.new["html_bytes"])
        self.assertEqual(self.index.read_bytes(), self.old_index)
        self.assertEqual((self.preserve / "old-packet.html").read_bytes(), self.old_html)
        self.assertEqual(self.replace()["status"], "ready_to_replace_unpublished")
        self.assertEqual(self.replace(True)["status"], "replaced_unpublished")

    def test_symlink_preservation_or_journal_is_rejected(self):
        self.preserve.symlink_to(self.run, target_is_directory=True)
        with self.assertRaisesRegex(ValueError, "Symlink"):
            self.replace(True)
        self.preserve.unlink()
        self.preserve.mkdir()
        (self.preserve / "journal.json").symlink_to(self.review)
        with self.assertRaisesRegex(ValueError, "Symlink"):
            self.replace(True)
        self.assert_original()

    def test_marker_appearing_during_git_fetch_blocks_before_any_candidate_write(self):
        actual_git = sync.replacement_git
        def raced_git(root, *args, **kwargs):
            result = actual_git(root, *args, **kwargs)
            if args[0] == "fetch":
                (self.source / "PACKET_DONE").write_text("Concurrent marker fixture")
            return result
        with patch.object(sync, "replacement_git", side_effect=raced_git):
            with self.assertRaisesRegex(ValueError, "published/completed"):
                self.replace(True)
        self.assert_original()
        self.assertFalse(self.preserve.exists())

    def test_git_base_changing_after_initial_checks_blocks_before_write(self):
        actual = sync.unchanged
        raced = []
        def changed_base(candidate):
            actual(candidate)
            if not raced:
                raced.append(True)
                (self.site / "README").write_text("Concurrent Git base fixture")
                self.git("add", "README")
                self.git("commit", "-qm", "Concurrent local base change")
        with patch.object(sync, "unchanged", side_effect=changed_base):
            with self.assertRaisesRegex(ValueError, "Git base changed"):
                self.replace(True)
        self.assert_original()
        self.assertFalse(self.preserve.exists())

    def test_held_queue_packet_lock_refuses_without_waiting_or_writing(self):
        with (self.source / ".packet-publication.lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.assertRaisesRegex(ValueError, "resource_busy"):
                self.replace(True)
        self.assert_original()
        self.assertFalse(self.preserve.exists())

    def test_symlink_queue_packet_lock_is_refused(self):
        lock = self.source / ".packet-publication.lock"
        lock.unlink()
        lock.symlink_to(self.review)
        with self.assertRaisesRegex(ValueError, "Symlink"):
            self.replace(True)
        self.assert_original()


if __name__ == "__main__":
    unittest.main()
