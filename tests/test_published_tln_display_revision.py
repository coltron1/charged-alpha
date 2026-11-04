"""Isolated source/contract/receipt and interrupted-write tests; no live inputs."""
import fcntl
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from scripts import revise_published_tln_display as revision


class PublishedDisplayRevisionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve(); self.site = self.root / 'site'
        self.queue = self.root / '_studio_queue' / revision.EPISODE
        self.producer = self.root / '_pipeline_v2/episodes' / revision.EPISODE / '_packet'
        self.old = ('<p>Research, source links and numbers unchanged</p><figure><figcaption><b>Three fiscal years of cash after investment</b></figcaption>'
                    '<div class="ch-d"><svg><text x="357.0" y="195.6">$256M</text><text x="381.0" y="200.2">$189M</text></svg></div>'
                    '<div class="ch-m"><svg><text x="197.0" y="195.6">$256M</text><text x="221.0" y="200.2">$189M</text></svg></div></figure>').encode()
        patcher = patch.object(revision, 'OLD_HTML', revision.digest(self.old)); patcher.start(); self.addCleanup(patcher.stop)
        paths = revision.targets(self.root, self.site)
        for path in paths: path.parent.mkdir(parents=True, exist_ok=True)
        for index in (0, 2, 6): paths[index].write_bytes(self.old)
        meta = {'sha256': revision.OLD_HTML, 'bytes': len(self.old), 'title': 'Immutable title', 'finalized': True}
        for index in (1, 3): paths[index].write_bytes(revision.encoded(meta))
        hand = {'episode': revision.EPISODE, 'packet': {'sha256': revision.OLD_HTML},
                'primary': {'youtube_long': 'samePrimary'}, 'short': {'file': 'sameSource.mp4'}}
        paths[4].write_bytes(revision.encoded(hand))
        receipt = {'episode': revision.EPISODE, 'source_sha256': {
            str(paths[i]): revision.digest(paths[i].read_bytes()) for i in (0, 1)},
            'staged_file_sha256': {name: revision.digest(paths[i].read_bytes()) for i, name in
                [(2, 'packet/' + revision.FILE), (3, 'packet/packet_meta.json'), (4, 'handoff.json')]}}
        paths[5].write_bytes(revision.encoded(receipt))
        record = {'slug': revision.SLUG, 'source_episode': revision.EPISODE, 'sha256': revision.OLD_HTML,
                  'title': 'Immutable title', 'source_sha256': dict(receipt['staged_file_sha256'])}
        paths[7].write_bytes(revision.encoded({'packets': [{'slug': 'other', 'untouched': True}, record]}))
        for path, content in [(self.queue / 'READY', b'same timestamp'), (self.queue / 'CLAIMED', b'same claim'),
                              (self.queue / 'packet/packet.json', b'{"values":[6240,7028]}'),
                              (self.producer / 'packet.json', b'{"values":[6240,7028]}')]: path.write_bytes(content)
        registry = json.loads(paths[7].read_bytes())
        registry['packets'][1]['source_sha256']['packet/packet.json'] = revision.digest((self.queue / 'packet/packet.json').read_bytes())
        paths[7].write_bytes(revision.encoded(registry))
        (self.queue / 'PACKET_DONE').write_bytes(revision.encoded({'passed': True, 'source_sha256': revision.OLD_HTML}))
        subprocess.check_call(['git', 'init', '-q', '-b', 'main', str(self.site)])
        self.git('add', '.'); self.git('-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.invalid', 'commit', '-qm', 'baseline')
        self.git('update-ref', 'refs/remotes/origin/main', self.git('rev-parse', 'HEAD'))
        self.directory = self.root / 'revision-plan'
        self.paths = paths; self.before = [p.read_bytes() for p in paths]

    def git(self, *args):
        return subprocess.check_output(['git', '-C', str(self.site), *args], text=True).strip()

    def prepare(self):
        self.plan = revision.prepare(self.root, self.site, self.directory)
        self.plan_path = self.directory / 'plan.json'
        self.review_path = self.root / 'review.json'
        self.review = {'schema': 'charged-alpha-published-tln-display-review/1', 'episode_id': revision.EPISODE,
                       'approved': True, 'model': 'gpt-6.1-terra', 'reasoning_effort': 'high', 'display_only': True,
                       'all_research_content_preserved': True, 'plan_sha256': revision.digest(self.plan_path.read_bytes()),
                       'implementation_sha256': revision.digest(Path(revision.__file__).read_bytes())}
        self.review_path.write_bytes(revision.encoded(self.review))

    def apply(self):
        return revision.apply(self.plan_path, self.review_path, self.root, self.site)

    def test_plan_is_offline_and_execution_preserves_content_claims_and_old_proof(self):
        self.prepare(); self.assertEqual([p.read_bytes() for p in self.paths], self.before)
        protected = {p: p.read_bytes() for p in [self.queue / 'READY', self.queue / 'CLAIMED',
                    self.queue / 'packet/packet.json', self.producer / 'packet.json', self.queue / 'PACKET_DONE']}
        self.apply(); self.apply()
        for path, raw in protected.items(): self.assertEqual(path.read_bytes(), raw)
        self.assertEqual(self.paths[0].read_bytes(), revision.corrected_html(self.old))
        self.assertEqual(self.paths[0].read_bytes(), self.paths[2].read_bytes())
        self.assertEqual(self.paths[2].read_bytes(), self.paths[6].read_bytes())
        self.assertEqual(self.paths[6].read_bytes().count(b'y="179.6">$256M'), 2)
        self.assertEqual(self.paths[6].read_bytes().count(b'y="200.2">$189M'), 2)
        self.assertEqual(json.loads(self.paths[7].read_bytes())['packets'][0], {'slug': 'other', 'untouched': True})
        for index, raw in enumerate(self.before): self.assertEqual((self.directory / f'{index}.before').read_bytes(), raw)

    def test_review_model_plan_and_preservation_are_blocking(self):
        self.prepare()
        for key, bad in [('model', 'gpt-6.1-sol'), ('episode_id', 'GLW-Q3-2026'), ('reasoning_effort', 'medium'), ('approved', False),
                         ('plan_sha256', '0' * 64), ('display_only', False), ('all_research_content_preserved', False)]:
            with self.subTest(key=key):
                self.review_path.write_bytes(revision.encoded({**self.review, key: bad}))
                with self.assertRaises(ValueError): self.apply()
                self.assertEqual([p.read_bytes() for p in self.paths], self.before)

    def test_partial_transaction_resumes_only_exact_reviewed_bytes(self):
        self.prepare(); self.paths[0].write_bytes((self.directory / '0.after').read_bytes())
        self.apply(); self.assertEqual(self.paths[6].read_bytes(), self.paths[0].read_bytes())

    def test_changed_target_or_candidate_refuses_before_any_other_write(self):
        self.prepare(); self.paths[6].write_bytes(b'Unexpected change')
        with self.assertRaises(ValueError): self.apply()
        self.assertEqual(self.paths[0].read_bytes(), self.old)
        self.paths[6].write_bytes(self.old); (self.directory / '0.after').write_bytes(b'Changed prose')
        with self.assertRaises(ValueError): self.apply()
        self.assertEqual(self.paths[0].read_bytes(), self.old)

    def test_protected_claim_halt_completion_and_busy_lock_refuse(self):
        self.prepare()
        for path in [self.queue / 'DONE', self.root / '_HALT_AUTOPRODUCE']:
            path.write_bytes(b'hold')
            with self.assertRaises(ValueError): self.apply()
            path.unlink()
        claim = self.queue / 'CLAIMED'; claim.write_bytes(b'Changed claim')
        with self.assertRaises(ValueError): self.apply()
        claim.write_bytes(b'same claim')
        with (self.queue / '.packet-publication.lock').open('a+') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.assertRaises(BlockingIOError): self.apply()
        self.assertEqual([p.read_bytes() for p in self.paths], self.before)

    def test_other_packet_and_symlink_are_refused(self):
        with self.assertRaises(ValueError): revision.corrected_html(self.old + b' changed')
        path = self.paths[0]; path.unlink(); path.symlink_to(self.paths[2])
        with self.assertRaises(ValueError): self.prepare()

    def test_git_base_change_and_altered_research_data_are_blocking(self):
        self.prepare(); (self.site / 'concurrent.txt').write_text('Other update')
        self.git('add', '.'); self.git('-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.invalid', 'commit', '-qm', 'concurrent')
        with self.assertRaises(ValueError): self.apply()
        self.assertEqual(self.paths[0].read_bytes(), self.old)

    def test_plan_refuses_financial_data_drift(self):
        (self.queue / 'packet/packet.json').write_bytes(b'{"values":[1,2]}')
        with self.assertRaises(ValueError): self.prepare()
        self.assertEqual(self.paths[0].read_bytes(), self.old)


if __name__ == '__main__': unittest.main()
