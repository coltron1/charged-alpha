import copy
import unittest
from scripts import sync_research_packets as sync

class CRWVHighlightProjectionTests(unittest.TestCase):
    html = 'ad9db4c34fdd59a58310cbf3bc71db16e0981e93502f6f0e3390b82f5f097239'
    content = 'e04bc447ca6e045ab66f5174a41047b957a6a764e38ccf3c6b9f17f3b63779f2'
    old = 'Q3 statements are unaudited and unresolved material control weaknesses remain a live risk; no remediation is assumed.'
    new = 'Q3 statements are unaudited. The latest quarterly filing reviewed, for Q2, reported material control weaknesses; the Q3 release supplied no controls assessment.'
    def sample(self):
        return {'summary':'A summary.', 'items':[{'title':f'Item {n}', 'detail':'Detail.'} for n in range(4)] + [{'title':'The evidence has limits.', 'detail':self.old}]}
    def test_exact_projection_preserves_source_and_other_findings(self):
        source=self.sample(); before=copy.deepcopy(source); out=sync.reviewed_highlight_projection('CRWV-Q3-2026',self.html,self.content,source)
        self.assertEqual(source,before); self.assertEqual(out['items'][:4],before['items'][:4]); self.assertEqual(out['items'][4]['detail'],self.new)
        self.assertEqual(out,sync.reviewed_highlight_projection('CRWV-Q3-2026',self.html,self.content,self.sample()))
    def test_preimage_mismatch_fails_closed(self):
        for html,content,items in [('0'*64,self.content,self.sample()),(self.html,'0'*64,self.sample()),(self.html,self.content,{**self.sample(),'items':[]})]:
            with self.subTest(html=html,content=content,items=items),self.assertRaises(ValueError): sync.reviewed_highlight_projection('CRWV-Q3-2026',html,content,items)
        source=self.sample(); source['items'][4]['detail']='Changed source.'
        with self.assertRaises(ValueError): sync.reviewed_highlight_projection('CRWV-Q3-2026',self.html,self.content,source)
    def test_html_projection_is_exact_source_bound_and_reversible(self):
        from pathlib import Path
        published=(Path(__file__).resolve().parents[1]/'data/research_packets/crwv-q3-2026.html').read_bytes()
        raw=published.replace(self.new.encode(),self.old.encode(),1)
        revised=sync.reviewed_html_projection('CRWV-Q3-2026',self.html,self.content,raw)
        self.assertEqual(revised,published); self.assertEqual(revised.count(self.new.encode()),1)
        self.assertEqual(revised.replace(self.new.encode(),self.old.encode(),1),raw)
        with self.assertRaises(ValueError): sync.reviewed_html_projection('CRWV-Q3-2026',self.html,self.content,raw+b'changed')
        self.assertIs(sync.reviewed_html_projection('OTHER-Q3-2026','0'*64,'0'*64,raw),raw)
    def test_unrelated_episode_untouched(self):
        source=self.sample(); self.assertIs(sync.reviewed_highlight_projection('OTHER-Q3-2026','0'*64,'0'*64,source),source)

if __name__=='__main__': unittest.main()
