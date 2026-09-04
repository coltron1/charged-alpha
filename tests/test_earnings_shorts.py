import copy
import contextlib
import io
import json
import tempfile
import unittest
from argparse import Namespace
from pathlib import Path
from unittest.mock import patch

from scripts.earnings_shorts import build_earnings_index, link_earnings_shorts, match_short, youtube_id
from scripts.sync_shows_catalog import VideoRow, newest_unlinked, recover_earnings_from_explainers, sync_catalog


def episode(**changes):
    return dict({
        "ticker": "GTLB", "company": "GitLab Inc.", "quarter": "Q2 FY2027",
        "title": "GitLab Stock: A record quarter (GTLB Q2 FY2027)",
        "published_at": "2026-09-03T01:37:39+00:00",
        "youtube_url": "https://www.youtube.com/watch?v=L0ouh6Wd4gE",
        "spotify_url": "https://open.spotify.com/episode/example",
    }, **changes)


def short(**changes):
    return dict({
        "title": "GTLB Stock: Up 10%, margin fell - Q2 FY2027",
        "published_at": "2026-09-02T18:37:04-07:00",
        "youtube_url": "https://www.youtube.com/shorts/qIAUqXrvTiw",
    }, **changes)


class EarningsShortMatchingTests(unittest.TestCase):
    def match(self, clip=None, episodes=None):
        return match_short(clip or short(), *build_earnings_index(episodes or [episode()]))

    def test_matches_ticker_period_and_timezone_aware_date(self):
        matched, reason = self.match()
        self.assertEqual(matched, episode())
        self.assertEqual(reason, "stock_period_date")

    def test_matches_brand_without_quarter(self):
        self.assertEqual(self.match(short(title="GitLab's margin fell again #shorts"))[0], episode())

    def test_company_brand_can_equal_ticker_or_contain_two_characters(self):
        for ticker, brand in [("AXON", "Axon"), ("MMM", "3M")]:
            ep = episode(ticker=ticker, company=ticker, title=f"{brand} ({ticker}): Earnings")
            self.assertEqual(self.match(short(title=f"{brand} beat earnings #shorts"), [ep])[0], ep)

    def test_hashtag_stock_is_not_confused_with_ai_topic(self):
        extra = episode(ticker="AI", company="C3.ai", youtube_url="https://youtu.be/another")
        self.assertEqual(self.match(short(title="Margins fell #GTLB #AI"), [episode(), extra])[0], episode())

    def test_single_letter_ticker_and_ordinary_word_hashtags(self):
        for ticker in ["F", "ON", "BE"]:
            ep = episode(ticker=ticker)
            self.assertEqual(self.match(short(title=f"The quarter beat expectations #{ticker}"), [ep])[0], ep)

    def test_a_numeric_headline_does_not_mean_agilent(self):
        self.assertEqual(self.match(short(title="A 62% margin #GTLB"), [episode(), episode(ticker="A")])[0], episode())

    def test_rejects_wrong_period_stale_date_or_missing_date(self):
        for changes in [
            {"title": "GTLB Stock: Q1 FY2027"},
            {"title": "GTLB Stock: H1 2027"},
            {"published_at": "2026-06-03T00:00:00Z"},
            {"published_at": ""},
        ]:
            self.assertIsNone(self.match(short(**changes))[0])

    def test_accepts_annual_report_with_q4_coverage(self):
        ep = episode(quarter="Q4 FY2027")
        self.assertEqual(self.match(short(title="GTLB Stock: FY2027 Earnings"), [ep])[0], ep)

    def test_uses_episode_quarter_not_a_future_date_in_title(self):
        ep = episode(title="GTLB Stock: Phase 3 slipped to H1 2028")
        self.assertEqual(self.match(episodes=[ep])[0], ep)

    def test_does_not_match_general_research_to_an_earnings_clip(self):
        ep = episode(quarter="Current", title="GTLB Stock: AI's hidden cost")
        self.assertIsNone(self.match(short(title="GTLB and AI #shorts"), [ep])[0])

    def test_general_education_and_different_topics_remain_unmatched(self):
        for title in ["GTLB Options Explained", "GTLB earnings implied move"]:
            self.assertIsNone(self.match(short(title=title))[0])
        clip = short(title="GTLB has a new product", published_at="2026-09-03T14:00:00Z")
        self.assertIsNone(self.match(clip)[0])

    def test_company_name_beats_a_bare_symbol_inside_its_name(self):
        aerospace = episode(ticker="GE", company="GE Aerospace")
        vernova = episode(ticker="GEV", company="GE Vernova", youtube_url="https://youtu.be/vernova")
        self.assertEqual(self.match(short(title="GE Vernova: Orders grew 88%"), [aerospace, vernova])[0], vernova)

    def test_more_specific_company_name_wins_and_metric_comparisons_are_allowed(self):
        ko = episode(ticker="KO", company="The Coca-Cola Company")
        kof = episode(ticker="KOF", company="Coca-Cola FEMSA", youtube_url="https://youtu.be/femsa")
        self.assertEqual(self.match(short(title="Coca-Cola FEMSA: EPS vs ADJ EPS"), [ko, kof])[0], kof)

    def test_does_not_guess_between_stocks_or_nearby_uploads(self):
        self.assertIsNone(self.match(short(title="GTLB vs CRDO"), [episode(), episode(ticker="CRDO")])[0])
        near = episode(youtube_url="https://youtu.be/another", published_at="2026-09-03T02:00:00Z")
        self.assertEqual(self.match(episodes=[episode(), near])[1], "ambiguous_episode")

    def test_pairs_same_time_upload_when_other_coverage_is_much_older(self):
        old = episode(youtube_url="https://youtu.be/old", published_at="2026-08-25T00:00:00Z")
        self.assertEqual(self.match(episodes=[episode(), old])[0], episode())

    def test_backfill_is_idempotent_and_preserves_platform_links(self):
        catalog = {"episodes": [episode()], "video_sections": [{"title": "Shorts and Clips", "videos": [short()]}]}
        original_episodes = copy.deepcopy(catalog["episodes"])
        self.assertEqual(link_earnings_shorts(catalog)["newly_linked"], 1)
        after = copy.deepcopy(catalog)
        self.assertEqual(link_earnings_shorts(catalog)["updated"], 0)
        self.assertEqual(catalog, after)
        self.assertEqual(catalog["episodes"], original_episodes)

    def test_clip_uploaded_first_is_linked_on_later_sync_and_stale_link_is_removed(self):
        clip = short()
        catalog = {"episodes": [], "video_sections": [{"title": "Shorts and Clips", "videos": [clip]}]}
        self.assertEqual(link_earnings_shorts(catalog)["unmatched"], 1)
        catalog["episodes"].append(episode())
        self.assertEqual(link_earnings_shorts(catalog)["newly_linked"], 1)
        clip["title"] = "GTLB Stock: Q1 FY2027"
        self.assertEqual(link_earnings_shorts(catalog)["updated"], 1)
        self.assertNotIn("earnings_youtube_url", clip)

    def test_normalizes_youtube_ids_and_recovers_gap_after_known_short(self):
        for url in ["https://youtu.be/known", "https://www.youtube.com/shorts/known", "https://youtube.com/watch?v=known&t=2"]:
            self.assertEqual(youtube_id(url), "known")
        self.assertEqual(youtube_id("https://notyoutube.com/watch?v=known"), "")
        rows = [VideoRow("known", "https://www.youtube.com/shorts/known", "Known"), VideoRow("missing", "https://www.youtube.com/shorts/missing", "Missing")]
        self.assertEqual(newest_unlinked(rows, {"https://youtu.be/known"}, True), rows[1:])

    def test_half_year_earnings_are_recovered_without_losing_podcasts(self):
        video = {"title": "Woodside (WDS) H1 2026: Earnings", "youtube_url": "https://youtu.be/halfyear", "apple_url": "https://podcasts.apple.com/episode"}
        explainer = {"title": "GTLB vs CRDO: Comparing stocks", "youtube_url": "https://youtu.be/compare"}
        catalog = {"episodes": [], "video_sections": [{"title": "Market and Sector Explainers", "videos": [video, explainer]}]}
        recovered = recover_earnings_from_explainers(catalog, {}, {"WDS": "Woodside"}, {})
        self.assertEqual(recovered[0]["quarter"], "H1 2026")
        self.assertEqual(recovered[0]["apple_url"], video["apple_url"])
        self.assertEqual(catalog["video_sections"][0]["videos"], [explainer])
        self.assertEqual(recover_earnings_from_explainers(catalog, {}, {}, {}), [])

    def test_sync_backfills_old_short_without_new_upload_and_then_is_noop(self):
        catalog = {"episodes": [episode()], "video_sections": [{"title": "Shorts and Clips", "videos": [short()]}]}
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "catalog.json"
            path.write_text(json.dumps(catalog))
            args = Namespace(catalog=str(path), youtube_channel="https://youtube.com/@ChargedAlpha", youtube_rss="rss", podbean_feed="podbean", spotify_show="spotify", apple_lookup="apple", scan_all=False, refresh_stock_metadata=False, dry_run=False)
            with patch("scripts.sync_shows_catalog.run_ytdlp_flat", return_value=[]), patch("scripts.sync_shows_catalog.fetch_text", side_effect=lambda url: {"rss": "<feed/>", "podbean": "<rss><channel/></rss>", "spotify": "", "apple": '{"results": []}'}[url]), contextlib.redirect_stdout(io.StringIO()):
                first = sync_catalog(args)
                contents = path.read_text()
                second = sync_catalog(args)
            self.assertTrue(first["catalog_changed"])
            self.assertEqual(first["earnings_shorts"]["newly_linked"], 1)
            self.assertFalse(second["catalog_changed"])
            self.assertEqual(path.read_text(), contents)


if __name__ == "__main__":
    unittest.main()
