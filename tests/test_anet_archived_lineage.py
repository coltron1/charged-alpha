import copy
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve().parents[1]
SITE = Path('/Users/colton/Projects/charged-alpha-studio-research')
sys.path.insert(0, str(SITE))
spec = importlib.util.spec_from_file_location('candidate_importer', HERE / 'scripts/sync_research_packets.py')
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)

class ArchivedLineageTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.candidate = m.read_candidate(Path('/Users/colton/Projects/charged-alpha-production/_studio_queue/_done/ANET-Q3-2026'))
        cls.index = HERE / 'data/research_packets.json'
        cls.records = m.load_packets(cls.index)
        cls.registered = next(x for x in cls.records if x['key'] == 'ANET:Q3-2026')

    def test_exact_archived_revision_is_idempotent_with_real_html(self):
        self.assertEqual(m.classify(self.candidate, self.records, self.index), 'already_imported')
        self.assertEqual(m.classify(self.candidate, self.records, self.index), 'already_imported')

    def test_later_studio_video_association_is_permitted(self):
        rec = copy.deepcopy(self.registered)
        rec['youtube_studio_short'] = 'abcdefghijk'
        self.assertEqual(m.classify(self.candidate, [rec], self.index), 'already_imported')

    def test_refuses_forged_lineage_or_archived_source(self):
        for field in m.ANET_DISPLAY_LINEAGE:
            rec = copy.deepcopy(self.registered); rec['display_revision'][field] = '0' * 64
            with self.subTest(field=field), self.assertRaises(ValueError):
                m.classify(self.candidate, [rec], self.index)
        for field in m.ANET_ARCHIVE_SOURCES:
            cand = copy.deepcopy(self.candidate); cand['record']['source_sha256'][field] = '0' * 64
            with self.subTest(source=field), self.assertRaises(ValueError):
                m.classify(cand, [self.registered], self.index)

    def test_refuses_wrong_identity_location_or_predecessor(self):
        for mutation in ('active', 'other_archive', 'other_period', 'predecessor'):
            cand = copy.deepcopy(self.candidate)
            if mutation == 'active': cand['queue_dir'] = '/tmp/_studio_queue/ANET-Q3-2026'
            if mutation == 'other_archive': cand['queue_dir'] = '/tmp/_done/ANET-Q3-2026--studio-both-abcdefghijk'
            if mutation == 'other_period': cand['record']['period'] = 'Q2 2026'
            if mutation == 'predecessor': cand['record']['sha256'] = '0' * 64
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                m.classify(cand, [self.registered], self.index)

    def test_refuses_changed_successor_or_identity(self):
        for field, value in [('sha256', '0' * 64), ('title', 'altered'), ('source_episode', 'OTHER-Q3-2026'), ('figures', 16), ('source_staged_at', 'other')]:
            rec = copy.deepcopy(self.registered); rec[field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                m.classify(self.candidate, [rec], self.index)

    def test_unrelated_immutable_conflict_still_fails(self):
        cand = copy.deepcopy(self.candidate); rec = copy.deepcopy(self.registered)
        cand['record']['key'] = rec['key'] = 'OTHER:Q3-2026'
        with self.assertRaises(ValueError): m.classify(cand, [rec], self.index)

    def test_tampered_successor_html_refused(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); (root / 'research_packets').mkdir()
            index = root / 'research_packets.json'
            index.write_bytes(self.index.read_bytes())
            (root / 'research_packets/anet-q3-2026.html').write_bytes(b'tampered')
            with self.assertRaises(ValueError):
                m.classify(self.candidate, self.records, index)

    def test_live_html_bytes_are_verified(self):
        with patch.object(m, 'packet_html_path', side_effect=ValueError('bad HTML')):
            with self.assertRaises(ValueError): m.classify(self.candidate, self.records, self.index)

if __name__ == '__main__': unittest.main()
