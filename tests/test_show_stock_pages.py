import unittest
from unittest.mock import patch

from app import (
    app,
    _cached_show_stock_detail,
    _detail_cache,
    _hydrate_show_stock_identity,
    build_show_library,
)
from scripts.sync_shows_catalog import collect_stock_metadata, refresh_catalog_stock_metadata
from yf_utils import fetch_ticker_info, ticker_info_cache


class ShowLibraryTests(unittest.TestCase):
    def test_shorts_belong_to_exact_earnings_video_not_just_ticker(self):
        episodes = [
            {"ticker": "TEST", "quarter": "Q2 2026", "title": "Newest", "published_at": "2026-08-01", "youtube_url": "https://youtu.be/new"},
            {"ticker": "TEST", "quarter": "Q1 2026", "title": "Older", "published_at": "2026-05-01", "youtube_url": "https://youtu.be/old"},
        ]
        clip = {"title": "A quick earnings take", "youtube_url": "https://youtube.com/shorts/clip", "earnings_youtube_url": "https://youtube.com/watch?v=old", "published_at": "2026-05-01"}
        sections = [{"title": "Shorts and Clips", "videos": [clip, clip]}]
        stock = build_show_library(episodes, video_sections=sections)["stocks"][0]
        self.assertEqual(stock["latest_youtube_shorts"], [])
        self.assertEqual(len(stock["episodes"][1]["youtube_shorts"]), 1)
        self.assertIn("clip", stock["episodes"][1]["youtube_shorts"][0]["thumbnail_url"])

    def test_stock_page_renders_shorts_with_main_video_and_archive_only_when_available(self):
        episodes = [{"ticker": "TEST", "quarter": "Q2 2026", "title": "Full earnings video", "published_at": "2026-08-01", "youtube_url": "https://youtu.be/full"}]
        clip = {"title": "Quick <earnings> take", "youtube_url": "https://youtube.com/shorts/clip", "earnings_youtube_url": "https://youtu.be/full", "published_at": "2026-08-02", "tickers": ["TEST"]}
        for clips in [[clip], []]:
            sections = [{"title": "Shorts and Clips", "videos": clips}]
            context = {"shows_data": {"video_sections": sections}, "show_library": build_show_library(episodes, video_sections=sections)}
            with patch("app._shows_context", return_value=context), patch("app._cached_show_stock_detail", return_value={}), patch("app._pick_competitor_stocks", return_value=[]):
                response = app.test_client().get("/shows/test")
            self.assertEqual(response.status_code, 200)
            rendered = response.get_data(as_text=True)
            self.assertEqual(rendered.count('class="earnings-short-link"'), 2 if clips else 0)
            self.assertIn("Full video on YouTube", rendered)
            if clips:
                self.assertIn("Quick &lt;earnings&gt; take", rendered)
                self.assertIn('datetime="2026-08-02"', rendered)
                self.assertNotIn("Non-quarter videos featuring", rendered)

    def test_profile_and_valid_history_beat_newer_placeholders(self):
        episodes = [
            {
                "ticker": "FRVO",
                "company": "FRVO",
                "sector": "Unclassified",
                "quarter": "Q2 2026",
                "title": "Newest",
                "published_at": "2026-08-12T22:00:00+00:00",
                "youtube_url": "https://www.youtube.com/watch?v=newest",
            },
            {
                "ticker": "FRVO",
                "company": "Fervo Energy Company",
                "sector": "Utilities",
                "quarter": "Q1 2026",
                "title": "Older",
                "published_at": "2026-05-01T22:00:00+00:00",
                "youtube_url": "https://www.youtube.com/watch?v=older",
            },
        ]

        stock = build_show_library(episodes)["stocks"][0]
        self.assertEqual(stock["company"], "Fervo Energy Company")
        self.assertEqual(stock["sector"], "Utilities")

        profiled = build_show_library(
            episodes,
            {"FRVO": {"company": "Fervo Energy", "sector": "Utilities"}},
        )["stocks"][0]
        self.assertEqual(profiled["company"], "Fervo Energy")

    def test_latest_video_uses_publication_time_not_quarter_label(self):
        episodes = [
            {
                "ticker": "SONY",
                "company": "Sony Group Corporation",
                "sector": "Communication Services",
                "quarter": "Q4 FY2026",
                "title": "Older quarter label",
                "published_at": "2026-05-11T10:00:00+00:00",
                "youtube_url": "https://www.youtube.com/watch?v=oldvideo",
                "podbean_url": "https://example.com/old-podcast",
            },
            {
                "ticker": "SONY",
                "company": "SONY",
                "sector": "Unclassified",
                "quarter": "Q1 FY2026",
                "title": "Newest upload",
                "published_at": "2026-07-31T10:00:00+00:00",
                "youtube_url": "https://www.youtube.com/watch?v=newvideo",
                "spotify_url": "https://open.spotify.com/episode/new",
            },
        ]

        stock = build_show_library(episodes)["stocks"][0]
        self.assertEqual(stock["latest_video_title"], "Newest upload")
        self.assertEqual(stock["latest_youtube_url"], "https://www.youtube.com/watch?v=newvideo")
        self.assertEqual(stock["latest_spotify_url"], "https://open.spotify.com/episode/new")
        self.assertEqual(stock["latest_podcast_url"], "https://example.com/old-podcast")

    def test_runtime_detail_hydrates_only_placeholder_identity(self):
        hydrated = _hydrate_show_stock_identity(
            {"ticker": "FRVO", "company": "FRVO", "sector": "Unclassified"},
            {"name": "Fervo Energy Company", "sector": "Utilities"},
        )
        self.assertEqual(hydrated["company"], "Fervo Energy Company")
        self.assertEqual(hydrated["sector"], "Utilities")

    def test_explicit_ticker_name_profile_is_not_treated_as_a_placeholder(self):
        stock = build_show_library(
            [{"ticker": "RH", "company": "RH", "sector": "Unclassified"}],
            {"RH": {"company": "RH", "company_is_ticker": True, "sector": "Consumer Cyclical"}},
        )["stocks"][0]

        self.assertEqual(stock["company"], "RH")
        self.assertTrue(stock["company_is_ticker"])
        self.assertEqual(stock["sector"], "Consumer Cyclical")


class ShowDetailCacheTests(unittest.TestCase):
    def setUp(self):
        _detail_cache.clear()

    def tearDown(self):
        _detail_cache.clear()

    def test_empty_detail_is_not_cached(self):
        with patch("app._build_fast_show_stock_detail", return_value={}) as fast, patch(
            "app.get_stock_detail", return_value=None
        ), patch("app._build_show_quote_fallback", return_value={}):
            self.assertEqual(_cached_show_stock_detail("FRVO")["info"], {})
            self.assertEqual(_cached_show_stock_detail("FRVO")["info"], {})

        self.assertEqual(fast.call_count, 2)

    def test_usable_detail_is_cached(self):
        detail = {"symbol": "FRVO", "price": 20.8, "market_data_source": "live"}
        with patch("app._build_fast_show_stock_detail", return_value=detail) as fast:
            first = _cached_show_stock_detail("FRVO")
            second = _cached_show_stock_detail("FRVO")

        self.assertEqual(first["info"]["price"], 20.8)
        self.assertEqual(second["info"]["price"], 20.8)
        self.assertEqual(fast.call_count, 1)


class CatalogMetadataTests(unittest.TestCase):
    def test_collects_historical_identity_when_newest_episode_is_placeholder(self):
        catalog = {
            "episodes": [
                {
                    "ticker": "FRVO",
                    "company": "FRVO",
                    "sector": "Unclassified",
                    "published_at": "2026-08-12T00:00:00+00:00",
                },
                {
                    "ticker": "FRVO",
                    "company": "Fervo Energy Company",
                    "sector": "Utilities",
                    "published_at": "2026-05-12T00:00:00+00:00",
                },
            ]
        }
        companies, sectors = collect_stock_metadata(catalog)
        self.assertEqual(companies["FRVO"], "Fervo Energy Company")
        self.assertEqual(sectors["FRVO"], "Utilities")

    def test_collects_newest_valid_history_with_mixed_timezones(self):
        catalog = {
            "episodes": [
                {
                    "ticker": "FRVO",
                    "company": "Older Fervo Name",
                    "sector": "Energy",
                    "published_at": "2026-08-12T23:00:00+00:00",
                },
                {
                    "ticker": "FRVO",
                    "company": "Fervo Energy Company",
                    "sector": "Utilities",
                    "published_at": "2026-08-12T20:30:00-03:00",
                },
            ]
        }

        companies, sectors = collect_stock_metadata(catalog)

        self.assertEqual(companies["FRVO"], "Fervo Energy Company")
        self.assertEqual(sectors["FRVO"], "Utilities")

    def test_refresh_persists_verified_profile(self):
        catalog = {
            "episodes": [
                {
                    "ticker": "FRVO",
                    "company": "FRVO",
                    "sector": "Unclassified",
                    "published_at": "2026-08-12T00:00:00+00:00",
                }
            ]
        }
        with patch(
            "yf_utils.fetch_ticker_info",
            return_value=(None, {"longName": "Fervo Energy Company", "sector": "Utilities"}),
        ):
            summary = refresh_catalog_stock_metadata(catalog)

        self.assertEqual(summary["unresolved"], 0)
        self.assertEqual(catalog["stock_metadata"]["FRVO"], {
            "company": "Fervo Energy Company",
            "sector": "Utilities",
        })


class TickerInfoRetryTests(unittest.TestCase):
    def setUp(self):
        ticker_info_cache.clear()

    def tearDown(self):
        ticker_info_cache.clear()

    def test_empty_provider_response_retries_before_failing(self):
        class FakeTicker:
            def __init__(self, info):
                self._info = info

            def get_info(self):
                return self._info

            @property
            def info(self):
                return self._info

        with patch(
            "yf_utils.yf.Ticker",
            side_effect=[FakeTicker({}), FakeTicker({"longName": "Fervo Energy Company"})],
        ) as ticker, patch("yf_utils.time.sleep") as sleep:
            _ticker, info = fetch_ticker_info("FRVO", max_retries=2)

        self.assertEqual(info["longName"], "Fervo Energy Company")
        self.assertEqual(ticker.call_count, 2)
        sleep.assert_called_once()

    def test_shell_provider_response_retries_before_caching(self):
        class FakeTicker:
            def __init__(self, info):
                self._info = info

            def get_info(self):
                return self._info

            @property
            def info(self):
                return self._info

        with patch(
            "yf_utils.yf.Ticker",
            side_effect=[
                FakeTicker({"quoteType": "EQUITY"}),
                FakeTicker({"longName": "Fervo Energy Company", "marketCap": 6130000000}),
            ],
        ) as ticker, patch("yf_utils.time.sleep"):
            _ticker, info = fetch_ticker_info("FRVO", max_retries=2)

        self.assertEqual(info["longName"], "Fervo Energy Company")
        self.assertEqual(ticker.call_count, 2)


if __name__ == "__main__":
    unittest.main()
