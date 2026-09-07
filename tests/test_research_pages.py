import hashlib
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import app as site


def packet(period="Q2 2026", year=2026, quarter=2, fiscal=False, primary="LongVideo01"):
    slug = "bzun-" + period.lower().replace(" ", "-")
    return {
        "ticker": "BZUN", "company": "Baozun Inc.", "period": period,
        "year": year, "quarter": quarter, "fiscal": fiscal, "slug": slug,
        "canonical_url": "https://chargedalpha.com/research/" + slug,
        "title": "Baozun: research <with sources>", "page_title": "BZUN Research Packet",
        "description": "The episode's research, charts, and sources.",
        "primary_youtube_long": primary, "primary_youtube_short": "ShortClip01",
        "youtube_studio": "StudioVid01", "youtube_studio_short": None,
        "source_published": "", "sha256": "", "figures": 19, "tables": 14,
    }


class ResearchPageTests(unittest.TestCase):
    def setUp(self):
        self.client = site.app.test_client()
        site._shows_cache.clear()

    def tearDown(self):
        site._shows_cache.clear()

    def test_packet_response_is_exact_source_with_canonical_header(self):
        source = b'<!doctype html><html><head><title>Report</title></head><body>Exact source.</body></html>'
        row = packet()
        row["sha256"] = hashlib.sha256(source).hexdigest()
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "packet.html"
            path.write_bytes(source)
            with patch("app.load_packets", return_value=[row]), patch("app.packet_html_path", return_value=path):
                response = self.client.get("/research/" + row["slug"])
                conditional = self.client.get("/research/" + row["slug"], headers={"If-None-Match": response.headers["ETag"]})
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.data, source)
            self.assertEqual(response.headers["Link"], '<' + row["canonical_url"] + '>; rel="canonical"')
            self.assertEqual(conditional.status_code, 304)

    def test_unknown_or_unverified_document_is_never_served(self):
        with patch("app.load_packets", return_value=[]):
            self.assertEqual(self.client.get("/research/unknown").status_code, 404)
        row = packet()
        with patch("app.load_packets", return_value=[row]), patch("app.packet_html_path", side_effect=ValueError("changed")):
            self.assertEqual(self.client.get("/research/" + row["slug"]).status_code, 503)

    def test_quarters_are_grouped_newest_first_without_replacing_older_reports(self):
        rows = [packet("Q2 2026", 2026, 2), packet("Q4 2025", 2025, 4, primary="OldVideo001"), packet("Q1 FY2027", 2027, 1, True, "NewVideo001")]
        stock = {"slug": "BZUN", "episodes": [{"youtube_url": "https://youtu.be/LongVideo01"}, {"youtube_url": "https://youtu.be/OtherVid001"}]}
        result, groups, selected = site._stock_research_context(stock, rows)
        self.assertEqual([group["label"] for group in groups], ["FY2027", "2026", "2025"])
        self.assertEqual(len(selected), 3)
        self.assertEqual(result["episodes"][0]["research_packet"]["period"], "Q2 2026")
        self.assertIsNone(result["episodes"][1]["research_packet"])
        self.assertNotIn("research_packet", stock["episodes"][0])

    def test_packet_is_searchable_when_episode_feed_has_not_arrived(self):
        row = packet()
        row["source_published"] = "2026-09-07"
        with patch("app.load_shows_catalog", return_value={"episodes": []}), patch("app.load_packets", return_value=[row]):
            response = self.client.get("/api/shows/stocks")
        stock = response.json["stocks"][0]
        self.assertEqual(stock["ticker"], "BZUN")
        self.assertIn("Baozun", stock["company"])
        self.assertIn("Q2 2026", stock["search_text"])
        self.assertEqual(stock["latest_video_published_at"], "")

    def test_later_studio_edition_joins_packet_by_verified_primary_id(self):
        row = packet()
        row["youtube_studio"] = None
        stock = {"slug": "BZUN", "episodes": [
            {"youtube_url": "https://youtu.be/StudioVid01", "studio_primary_youtube_url": "https://youtu.be/LongVideo01"},
            {"youtube_url": "https://youtu.be/LongVideo01"},
            {"youtube_url": "https://youtu.be/OtherVid001", "studio_primary_youtube_url": "https://youtu.be/OtherVid002"},
        ]}
        result, _, selected = site._stock_research_context(stock, [row])
        self.assertEqual(selected[0]["youtube_studio"], "StudioVid01")
        self.assertEqual(result["episodes"][0]["research_packet"]["slug"], row["slug"])
        self.assertEqual(result["episodes"][1]["research_packet"]["slug"], row["slug"])
        self.assertIsNone(result["episodes"][2]["research_packet"])
        self.assertIsNone(row["youtube_studio"])

    def test_packet_does_not_replace_existing_episode_or_podcast(self):
        episode = {"ticker": "BZUN", "youtube_url": "https://youtu.be/LongVideo01", "podbean_url": "https://podbean.com/real-episode"}
        result = site._episodes_with_research_packets([episode], [packet()])
        self.assertEqual(result, [episode])

    def test_research_section_beside_episodes_and_archive_link_are_present(self):
        rows = [packet(), packet("Q1 2026", 2026, 1, primary="OldVideo001"), packet("Q4 2025", 2025, 4, primary="OldVideo002")]
        with patch("app.load_shows_catalog", return_value={"episodes": []}), patch("app.load_packets", return_value=rows), patch("app._cached_show_stock_detail", return_value={}), patch("app._pick_competitor_stocks", return_value=[]):
            response = self.client.get("/shows/bzun")
        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn('id="quarterly-research"', body)
        self.assertIn("research &lt;with sources&gt;", body)
        self.assertEqual(body.count('class="research-year" open'), 1)
        self.assertEqual(body.count('class="research-quarter"'), 3)
        self.assertGreaterEqual(body.count('/research/bzun-q2-2026'), 3)
        self.assertLess(body.index('id="quarterly-research"'), body.index("Current price"))

    def test_sitemap_includes_packet_and_stock(self):
        row = packet()
        with patch("app.load_shows_catalog", return_value={"episodes": []}), patch("app.load_packets", return_value=[row]):
            response = self.client.get("/sitemap.xml")
        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn(row["canonical_url"], body)
        self.assertIn("/shows/BZUN", body)


if __name__ == "__main__":
    unittest.main()
