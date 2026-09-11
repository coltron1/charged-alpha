import copy
import os
import unittest
from unittest.mock import patch

os.environ.setdefault('DATABASE_URL', 'sqlite:///:memory:')
import app as site
from datetime import datetime, timezone
from stock_research import comparison
from research_ui import publication_date, research_listing, latest_episode_podcasts


class BrandRedesignTests(unittest.TestCase):
    def test_stale_fundamentals_cannot_enter_medians(self):
        now=datetime(2026,9,10,tzinfo=timezone.utc)
        profiles={ticker:{'ticker':ticker,'issuer':ticker,'company':ticker,'market_cap_usd':cap,'observed_at':'2026-09-10','quote_at':'2026-09-10','business_groups':['Convenience stores'],'period_end':period,'revenue_growth':growth,'forward_pe':20} for ticker,cap,period,growth in [('CASY',100,'2026-07-31',24),('MUSA',90,'2026-06-30',40),('ATD.TO',150,'2020-10-11',-22)]}
        original=copy.deepcopy(profiles)
        result=comparison('CASY',profiles,now=now)
        growth=next(r for r in result['rows'] if r['key']=='revenue_growth')
        quote=next(r for r in result['rows'] if r['key']=='market_cap_usd')
        self.assertIsNone(growth['median_value'])
        self.assertEqual(growth['median_count'],1)
        self.assertIsNotNone(quote['median_value'])
        self.assertEqual(profiles,original)

    def test_publication_dates_are_not_import_dates(self):
        self.assertEqual(publication_date('September 9, 2026'), '2026-09-09')
        self.assertEqual(publication_date('2026-09-09T15:23:00Z'), '2026-09-09')
        self.assertEqual(publication_date('not known'), '')
        self.assertEqual(publication_date('2026-99-23'), '')

    def test_library_uses_actual_dates_not_fiscal_year_or_link_count(self):
        stocks = [{'ticker': 'OLD', 'latest_video_published_at': '2026-08-10', 'latest_quarter': 'Q3 FY2027'}, {'ticker': 'NEW', 'latest_video_published_at': '2026-09-10', 'latest_quarter': 'Q2 FY2026'}]
        original=copy.deepcopy(stocks)
        result=research_listing(stocks, [])
        self.assertEqual([s['ticker'] for s in result], ['NEW', 'OLD'])
        self.assertEqual(stocks, original)

    def test_packet_date_never_replaces_episode_date(self):
        result=research_listing([{'ticker':'TEST','latest_video_published_at':''}], [{'ticker':'TEST','year':2026,'quarter':2,'source_published':'September 10, 2026','source_staged_at':'2099-01-01','slug':'test-q2-2026'}])[0]
        self.assertEqual(result['research_date'],'2026-09-10')
        self.assertEqual(result['latest_video_published_at'],'')

    def test_exact_verified_studio_primary_podcasts_only(self):
        studio={'youtube_url':'https://youtu.be/StudioVid01','studio_primary_youtube_url':'https://youtu.be/PrimaryVid1','studio_primary_link_evidence':'StudioVid01','quarter':'Q1 FY2027'}
        primary={'youtube_url':'https://youtu.be/PrimaryVid1','quarter':'Q1 FY2027','spotify_url':'https://open.spotify.com/episode/exact'}
        unrelated={'youtube_url':'https://youtu.be/AnotherVid1','quarter':'Q1 FY2027','apple_url':'https://podcasts.apple.com/wrong'}
        stock={'latest_youtube_url':studio['youtube_url'],'episodes':[studio, primary, unrelated]}
        original=copy.deepcopy(stock)
        links=latest_episode_podcasts(stock, site._youtube_video_id)
        self.assertEqual(links,[{'label':'Spotify','url':primary['spotify_url'],'edition':'Presentation audio'}])
        self.assertEqual(stock,original)
        studio['studio_primary_link_evidence']='OtherVideo1'
        self.assertEqual(latest_episode_podcasts(stock,site._youtube_video_id),[])
        studio['studio_primary_link_evidence']='StudioVid01'
        primary['quarter']='Q4 FY2026'
        self.assertEqual(latest_episode_podcasts(stock,site._youtube_video_id),[])

    def test_catalog_keeps_studio_evidence_for_podcast_resolution(self):
        episodes=[{'ticker':'TEST','quarter':'Q2 2026','title':'Studio','youtube_url':'https://youtu.be/StudioVid01','published_at':'2026-09-10','studio_primary_youtube_url':'https://youtu.be/PrimaryVid1','studio_primary_link_evidence':'StudioVid01'}, {'ticker':'TEST','quarter':'Q2 2026','title':'Presentation','youtube_url':'https://youtu.be/PrimaryVid1','published_at':'2026-09-09','spotify_url':'https://open.spotify.com/episode/exact'}]
        stock=site.build_show_library(episodes)['stocks'][0]
        links=latest_episode_podcasts(stock,site._youtube_video_id)
        self.assertEqual(links[0]['url'],'https://open.spotify.com/episode/exact')
    def test_home_is_server_rendered_and_research_precedes_promotion(self):
        body=site.app.test_client().get('/').get_data(as_text=True)
        self.assertIn('what the headline misses',body)
        self.assertIn('class="ca-stock-card"',body)
        self.assertLess(body.index('id="stockGrid"'),body.index('id="mobile-app"'))
        self.assertNotIn('episodes linked',body)
        self.assertNotIn('cdn.tailwindcss.com',body)
        self.assertIn('charged-alpha-logo.png',body)
        self.assertIn('<sup>TM</sup>',body)

    def test_following_private_device_scope_no_email_or_analytics(self):
        response=site.app.test_client().get('/following',base_url='https://chargedalpha.com')
        self.assertEqual(response.status_code,200)
        self.assertEqual(response.headers['Cache-Control'],'no-store')
        self.assertIn('noindex',response.headers['X-Robots-Tag'])
        body=response.get_data(as_text=True)
        self.assertIn('No account, email notifications, or cross-device syncing yet.',body)
        self.assertNotIn('googletagmanager.com',body)

    def test_preview_never_emits_google_analytics_tag(self):
        with patch.object(site,'GOOGLE_ANALYTICS_ID','G-TEST'):
            local=site.app.test_client().get('/',base_url='http://127.0.0.1:5055').get_data(as_text=True)
            public=site.app.test_client().get('/',base_url='https://chargedalpha.com').get_data(as_text=True)
        self.assertNotIn('googletagmanager.com',local)
        self.assertIn('googletagmanager.com',public)

    def test_shared_navigation_destinations(self):
        client=site.app.test_client()
        for path in ('/', '/shows/CASY', '/about', '/studio', '/app', '/games'):
            with self.subTest(path=path):
                body=client.get(path).get_data(as_text=True)
                self.assertEqual(body.count('id="ca-nav"'),1)
                header=body.split('<header class="ca-header">',1)[1].split('</header>',1)[0]
                for destination in ('/following','/screener/','/games','/studio','/about','/app'):
                    self.assertIn('href="'+destination+'"',header)

    def test_channel_preview_is_not_a_public_page(self):
        with patch.dict(os.environ, {'BRAND_PREVIEW':'0'}):
            self.assertEqual(site.app.test_client().get('/brand-preview').status_code,404)
        with patch.dict(os.environ, {'BRAND_PREVIEW':'1'}):
            response=site.app.test_client().get('/brand-preview')
            self.assertEqual(response.status_code,200)
            self.assertIn('noindex',response.headers['X-Robots-Tag'])

    def test_tool_mode_controls_survive_navigation_replacement(self):
        for path in ('/screener/','/etf/','/mutual-funds/','/options/','/bonds/','/reits/','/crypto/','/forex/','/commodities/','/earnings/','/gold/','/charts/'):
            with self.subTest(path=path):
                response=site.app.test_client().get(path)
                self.assertEqual(response.status_code,200)
                self.assertEqual(response.get_data(as_text=True).count('id="modeToggle"'),1)

if __name__=='__main__':
    unittest.main()
