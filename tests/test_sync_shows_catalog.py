import unittest
import json

from scripts.sync_shows_catalog import (
    PodcastItem,
    normalize_title,
    parse_apple_items,
    parse_stock_title,
    resolve_stock_key,
)


class StockTitleParsingTests(unittest.TestCase):
    def test_parses_ticker_first_title_with_quarter(self):
        self.assertEqual(
            parse_stock_title("ISRG Stock: Intuitive Surgical BEAT Q2 2026"),
            ("ISRG", "Q2 2026"),
        )

    def test_parses_company_first_title_with_parenthetical_ticker(self):
        self.assertEqual(
            parse_stock_title(
                "ManpowerGroup Stock (MAN): It Already Doubled | Q2 2026 Earnings"
            ),
            ("MAN", "Q2 2026"),
        )

    def test_uses_podcast_description_for_company_first_title(self):
        self.assertEqual(
            parse_stock_title(
                "Alcoa Stock: AA Posted a Record Quarter",
                "Alcoa Corporation (AA) Q2 2026 reported record revenue.",
            ),
            ("AA", "Q2 2026"),
        )

    def test_resolves_missing_period_from_normalized_podcast_title(self):
        youtube_title = "GE Stock: Here's the Earnings Result"
        podcast_title = "GE Stock: Here\u2019s the Earnings Result"
        podcasts = {
            normalize_title(podcast_title): PodcastItem(
                title=podcast_title,
                url="https://example.com/ge",
                published_at="2026-07-17T00:00:00+00:00",
                ticker="GE",
                quarter="Q2 2026",
            )
        }

        self.assertEqual(resolve_stock_key(youtube_title, podcasts), ("GE", "Q2 2026"))

    def test_resolves_truncated_youtube_title_from_podcast_prefix(self):
        youtube_title = "Knight-Swift Stock: The Freight Recession Looks Over"
        podcast_title = (
            "Knight-Swift Stock: The Freight Recession Looks Over - Why We Say HOLD "
            "(KNX Q2 2026)"
        )
        podcasts = {
            normalize_title(podcast_title): PodcastItem(
                title=podcast_title,
                url="https://example.com/knx",
                published_at="2026-07-23T00:00:00+00:00",
                ticker="KNX",
                quarter="Q2 2026",
            )
        }

        self.assertEqual(resolve_stock_key(youtube_title, podcasts), ("KNX", "Q2 2026"))

    def test_parses_full_year_period(self):
        self.assertEqual(
            parse_stock_title("FIZZ Stock: The LaCroix Company (FY2026)"),
            ("FIZZ", "FY2026"),
        )

    def test_indexes_apple_episode_by_rss_guid(self):
        guid_items, titled_items = parse_apple_items(json.dumps({
            "results": [{
                "kind": "podcast-episode",
                "trackName": "Example Stock: Earnings Review (EXM Q2 2026)",
                "trackViewUrl": "https://podcasts.apple.com/us/podcast/example/id1?i=2",
                "episodeGuid": "chargedalpha.podbean.com/example-guid",
                "releaseDate": "2026-07-28T12:00:00Z",
            }]
        }))

        episode = guid_items["chargedalpha.podbean.com/example-guid"]
        self.assertEqual(episode.url, "https://podcasts.apple.com/us/podcast/example/id1?i=2")
        self.assertEqual(titled_items[normalize_title(episode.title)], episode)


if __name__ == "__main__":
    unittest.main()
