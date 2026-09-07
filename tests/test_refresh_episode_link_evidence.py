import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from scripts import refresh_episode_link_evidence as refresh


class EvidenceRefreshTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name)
        self.catalog=self.root/'catalog.json';self.evidence=self.root/'episode_link_evidence.json';self.proposal=self.root/'proposal.json'
        self.primary={'youtube_url':'https://youtu.be/Primary1234','title':'EXM Stock: Q2 FY2027',
                      'ticker':'EXM','quarter':'Q2 FY2027','spotify_url':'https://open.spotify.com/episode/keep'}
        self.clip={'youtube_url':'https://youtube.com/shorts/Short123456','title':'unknown'}
        self.data={'episodes':[self.primary],'video_sections':[{'title':'Shorts and Clips','videos':[self.clip]}]}
        self.save_catalog()
        self.details={'videoId':'Short123456','channelId':refresh.CHANNEL_ID,'title':'unknown',
                      'shortDescription':'FULL BREAKDOWN: https://youtu.be/Primary1234\nNot financial advice.'}

    def tearDown(self):self.temp.cleanup()
    def save_catalog(self):self.catalog.write_text(json.dumps(self.data))
    def collect(self,details=None):
        with patch.object(refresh,'fetch_public_video',return_value=details or self.details):
            return refresh.collect(self.catalog,self.evidence,self.proposal)

    def test_default_plan_is_offline_and_does_not_write(self):
        before=self.catalog.read_bytes()
        with patch.object(refresh,'fetch_public_video',side_effect=AssertionError('network')):
            data,plan=refresh.inputs(self.catalog,self.evidence)
        self.assertFalse(plan['network_called']);self.assertEqual(plan['candidate_count'],1)
        self.assertEqual(self.catalog.read_bytes(),before);self.assertFalse(self.evidence.exists());self.assertFalse(self.proposal.exists())

    def test_collect_apply_and_resume_preserve_catalog_and_existing_evidence(self):
        before=self.catalog.read_bytes();result=self.collect();self.assertEqual(result['accepted'],1)
        self.assertFalse(self.evidence.exists());self.assertEqual(refresh.apply(self.proposal)['added'],1)
        contents=self.evidence.read_bytes()
        self.assertEqual(refresh.apply(self.proposal)['status'],'already_applied')
        self.assertEqual(self.catalog.read_bytes(),before);self.assertEqual(self.evidence.read_bytes(),contents)
        self.assertEqual(refresh.inputs(self.catalog,self.evidence)[1]['candidate_count'],0)
        self.assertEqual(json.loads(contents)['records']['Short123456']['target_video_id'],'Primary1234')

    def test_wrong_channel_title_and_unlabelled_link_are_not_proposed(self):
        for changes in [{'channelId':'wrong'},{'title':'Changed title'},{'shortDescription':'Other video: https://youtu.be/Primary1234'}]:
            with self.subTest(changes=changes):
                result=self.collect(dict(self.details,**changes));self.assertEqual(result['accepted'],0)
                self.assertEqual(refresh.apply(self.proposal)['status'],'unchanged')
                self.assertFalse(self.evidence.exists());self.proposal.unlink()

    def test_multiple_known_targets_remain_ambiguous(self):
        self.data['episodes'].append(dict(self.primary,youtube_url='https://youtu.be/Another1234'));self.save_catalog()
        details=dict(self.details,shortDescription=self.details['shortDescription']+'\nFULL BREAKDOWN: https://youtu.be/Another1234')
        self.assertEqual(self.collect(details)['accepted'],0)

    def test_catalog_or_evidence_changes_after_collect_block_apply(self):
        self.collect();self.data['episodes'][0]['quarter']='Q3 FY2027';self.save_catalog()
        with self.assertRaisesRegex(ValueError,'Catalog changed'):refresh.apply(self.proposal)
        self.assertFalse(self.evidence.exists())
        self.evidence.write_text(json.dumps({'schema_version':1,'records':{}}));before=self.evidence.read_bytes()
        with self.assertRaisesRegex(ValueError,'Evidence changed'):refresh.apply(self.proposal)
        self.assertEqual(before,self.evidence.read_bytes())

    def test_source_change_during_collection_creates_no_proposal(self):
        def fetch(video_id):
            self.data['episodes'][0]['quarter']='Q3 FY2027';self.save_catalog();return self.details
        with patch.object(refresh,'fetch_public_video',side_effect=fetch):
            with self.assertRaisesRegex(ValueError,'changed while collecting'):
                refresh.collect(self.catalog,self.evidence,self.proposal)
        self.assertFalse(self.proposal.exists());self.assertFalse(self.evidence.exists())

    def test_tampered_description_or_wrong_candidate_relation_is_refused(self):
        self.collect();original=self.proposal.read_bytes();plan=json.loads(original)
        plan['records']['Short123456']['description']='Changed';self.proposal.write_text(json.dumps(plan))
        with self.assertRaises(ValueError):refresh.apply(self.proposal)
        plan=json.loads(original);plan['candidates'][0]['relation']='studio_primary';self.proposal.write_text(json.dumps(plan))
        with self.assertRaisesRegex(ValueError,'selected candidate'):refresh.apply(self.proposal)
        self.assertFalse(self.evidence.exists())

    def test_studio_collection_binds_same_ticker_primary_and_never_replaces_rows(self):
        studio={'youtube_url':'https://youtu.be/Studio12345','title':'EXM Stock: Results — Animated Studio Edition','ticker':'EXM','quarter':'Current'}
        self.data['episodes'].append(studio);self.data['video_sections']=[];self.save_catalog();before=self.catalog.read_bytes()
        details={'videoId':'Studio12345','channelId':refresh.CHANNEL_ID,'title':studio['title'],
                 'shortDescription':'Watch the original presentation: https://youtu.be/Primary1234'}
        self.assertEqual(self.collect(details)['accepted'],1);refresh.apply(self.proposal)
        self.assertEqual(self.catalog.read_bytes(),before)
        self.assertEqual(json.loads(self.evidence.read_bytes())['records']['Studio12345']['relation'],'studio_primary')

    def test_new_collection_cannot_replace_existing_proposal(self):
        self.collect();before=self.proposal.read_bytes()
        with self.assertRaisesRegex(ValueError,'new distinct file'):self.collect()
        self.assertEqual(self.proposal.read_bytes(),before)

    def test_changed_existing_evidence_title_is_reported_for_review_not_overwritten(self):
        self.collect();refresh.apply(self.proposal);before=self.evidence.read_bytes()
        self.clip['title']='New public title';self.save_catalog()
        plan=refresh.inputs(self.catalog,self.evidence)[1]
        self.assertEqual(plan['existing_stale_evidence'],['Short123456']);self.assertEqual(plan['candidate_count'],0)
        self.assertEqual(self.evidence.read_bytes(),before)


if __name__=='__main__':unittest.main()
