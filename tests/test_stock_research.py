from copy import deepcopy
from datetime import datetime, timedelta, timezone
import unittest
from unittest.mock import patch, Mock

import pandas as pd

from stock_research import normalize_profile, select_peers, comparison, group_episode_archive, number, format_value
from stock_research_data import statement_rows, financial_status, _cache, _failed, _pending

NOW = datetime(2026, 9, 10, 4, tzinfo=timezone.utc)


def profile(symbol, cap=100, industry="specialty-retail", **overrides):
    info = {"longName":symbol, "currency":"USD", "financialCurrency":"USD", "marketCap":cap,
            "industryKey":industry, "industry":"Specialty Retail", "bookValue":10, "trailingEps":2,
            "forwardEps":3, "currentPrice":50, "regularMarketTime":NOW.timestamp() - 3600,
            "mostRecentQuarter":(NOW - timedelta(days=60)).timestamp(),
            "totalRevenue":cap, "debtToEquity":70.547, "returnOnEquity":.15}
    info.update(overrides)
    return normalize_profile(symbol, info, (NOW - timedelta(hours=1)).isoformat(), {"USD":1, "CAD":.75})


class MetricTests(unittest.TestCase):
    def test_units_and_currency_conversion(self):
        result = profile("CASY", debtToEquity=70.547, revenueGrowth=.125, dividendRate=2)
        self.assertAlmostEqual(result["debt_to_equity"], .70547)
        self.assertEqual(format_value(result["debt_to_equity"], "multiple"), "0.71x")
        self.assertEqual(result["revenue_growth"], 12.5)
        self.assertEqual(result["dividend_yield"], 4)
        self.assertEqual(profile("ATD.TO", 200, currency="CAD")["market_cap_usd"], 150)

    def test_negative_earnings_equity_and_inapplicable_ratios(self):
        result = profile("TEST", trailingEps=-1, forwardEps=-2, bookValue=-3, trailingPE=-10, forwardPE=-20, priceToBook=-5)
        for key in ("trailing_pe", "forward_pe", "price_to_book", "debt_to_equity", "return_on_equity"):
            self.assertIsNone(result[key])
        bank = profile("JPM", industry="banks-diversified", currentRatio=3, enterpriseToEbitda=10)
        self.assertIsNone(bank["current_ratio"])
        self.assertIsNone(bank["ev_ebitda"])

    def test_missing_and_nonfinite_not_zero(self):
        for value in (None, "", float("nan"), float("inf"), "bad"):
            self.assertIsNone(number(value))
            self.assertEqual(format_value(value), "\u2014")
        self.assertEqual(format_value(0, "percent"), "0.0%")

    def test_cross_currency_yields_not_calculated(self):
        result = profile("TEST", currency="USD", financialCurrency="JPY", freeCashflow=100, enterpriseToRevenue=8)
        self.assertIsNone(result["fcf_yield"])
        self.assertIsNone(result["ev_sales"])
        self.assertIsNone(result["revenue_usd"])


class PeerTests(unittest.TestCase):
    def setUp(self):
        self.profiles = {s:profile(s, cap) for s, cap in (("CASY",100),("MUSA",60),("ATD.TO",150),("F",90),("TJX",95))}

    def test_caseys_never_matches_ford_or_tjx(self):
        peers, benchmarks = select_peers("CASY", self.profiles, NOW)
        self.assertEqual({p["ticker"] for p in peers}, {"MUSA","ATD.TO"})
        self.assertEqual(benchmarks, [])

    def test_size_boundaries_and_median_excludes_benchmarks(self):
        self.profiles["MUSA"]["market_cap_usd"] = 50
        self.profiles["ATD.TO"]["market_cap_usd"] = 301
        peers, benchmarks = select_peers("CASY", self.profiles, NOW)
        self.assertEqual(peers[0]["size_band"], "similar size")
        self.assertEqual(benchmarks[0]["role"], "benchmark")
        data = comparison("CASY", self.profiles, now=NOW)
        self.assertFalse(data["has_median"])
        self.profiles["ATD.TO"]["market_cap_usd"] = 300
        data = comparison("CASY", self.profiles, now=NOW)
        row = next(r for r in data["rows"] if r["key"] == "market_cap_usd")
        self.assertEqual(row["median_value"], 175)
        self.assertEqual(row["median_count"], 2)

    def test_stale_unknown_zero_negative_and_broad_industry(self):
        for cap in (0, -1, None):
            with self.subTest(cap=cap):
                profiles = deepcopy(self.profiles)
                profiles["CASY"]["market_cap_usd"] = cap
                self.assertEqual(select_peers("CASY", profiles, NOW), ([],[]))
        profiles = deepcopy(self.profiles)
        profiles["CASY"]["observed_at"] = (NOW - timedelta(days=15)).isoformat()
        self.assertEqual(select_peers("CASY", profiles, NOW), ([],[]))
        profiles = {s:profile(s) for s in ("UNKNOWN1","UNKNOWN2")}
        self.assertEqual(select_peers("UNKNOWN1", profiles, NOW), ([],[]))
        self.assertEqual(select_peers("MISSING", profiles, NOW), ([],[]))

    def test_old_quote_and_different_snapshot_dates_excluded(self):
        self.profiles["MUSA"]["quote_at"] = (NOW - timedelta(days=30)).isoformat()
        self.profiles["ATD.TO"]["observed_at"] = (NOW - timedelta(days=10)).isoformat()
        self.assertEqual(select_peers("CASY", self.profiles, NOW), ([],[]))

    def test_share_classes_not_peers_and_custom_deduplicates(self):
        profiles = {s:profile(s) for s in ("GOOG","GOOGL","META")}
        peers, _ = select_peers("GOOGL", profiles, NOW)
        self.assertEqual([p["ticker"] for p in peers], ["META"])
        result = comparison("GOOGL", profiles, ["GOOG","META","META","BAD"], NOW)
        self.assertEqual([p["ticker"] for p in result["columns"]], ["GOOGL","META"])
        self.assertFalse(result["has_median"])


class ArchiveTests(unittest.TestCase):
    def test_editions_grouped_without_losing_links_or_mutating(self):
        clip = {"youtube_url":"https://youtube.com/shorts/abc", "title":"Short"}
        episodes = [{"quarter":"Q2 FY2026", "title":"Studio", "youtube_url":"studio", "studio_primary_youtube_url":"original", "youtube_shorts":[clip], "apple_url":"apple"},
                    {"quarter":"Q2 FY2026", "title":"Original", "youtube_url":"original", "youtube_shorts":[clip], "spotify_url":"spotify"},
                    {"quarter":"Q1 FY2026", "title":"Earlier", "youtube_url":"earlier", "podbean_url":"podbean"}]
        original = deepcopy(episodes)
        archive = group_episode_archive(episodes)
        self.assertEqual(len(archive), 2)
        self.assertEqual(len(archive[0]["editions"]), 2)
        self.assertEqual(len(archive[0]["shorts"]), 1)
        self.assertEqual({p["url"] for p in archive[0]["podcasts"]}, {"apple","spotify"})
        self.assertEqual(episodes, original)


class StatementTests(unittest.TestCase):
    def test_period_join_gaps_and_cashflow_signs(self):
        dates = pd.to_datetime(["2026-06-30","2026-03-31"])
        income = pd.DataFrame({dates[0]:{"Total Revenue":100,"Operating Income":20,"Diluted EPS":2},
                               dates[1]:{"Total Revenue":80,"Operating Income":-8,"Diluted EPS":-1}})
        cashflow = pd.DataFrame({dates[0]:{"Operating Cash Flow":30,"Capital Expenditure":-5}})
        balance = pd.DataFrame({dates[1]:{"Total Debt":50,"Cash And Cash Equivalents":20}})
        rows = statement_rows(income,cashflow,balance,8)
        self.assertEqual([r["date"] for r in rows], ["2026-03-31","2026-06-30"])
        self.assertEqual(rows[1]["free_cashflow"],25)
        self.assertIsNone(rows[0]["free_cashflow"])
        self.assertEqual(rows[0]["operating_margin"],-10)
        self.assertIsNone(rows[1]["debt"])
        self.assertEqual(rows[0]["eps"],-1)

    def test_background_singleflight_and_queue_limit(self):
        with patch("stock_research_data._pool") as pool:
            _cache.clear(); _failed.clear(); _pending.clear()
            try:
                self.assertEqual(financial_status("TEST")[1],202)
                self.assertEqual(financial_status("TEST")[1],202)
                self.assertEqual(pool.submit.call_count,1)
                for index in range(11):
                    financial_status("TEST"+str(index))
                self.assertEqual(financial_status("OVERFLOW")[1],503)
            finally:
                _pending.clear()


class PageTests(unittest.TestCase):
    def test_financial_comparison_fx_uses_only_fresh_valid_snapshot_rates(self):
        from app import _financial_comparison_fx
        now = datetime.now(timezone.utc)
        data = _financial_comparison_fx({
            "fx": {"USD": 1, "EUR": 1.16, "CHF": 1.23, "OLD": 2, "BAD": -1},
            "fx_observed_at_by_currency": {
                "EUR": now.isoformat(), "CHF": now.isoformat(),
                "OLD": (now - timedelta(days=15)).isoformat(),
            },
        })
        self.assertEqual(data["target_currency"], "USD")
        self.assertEqual(data["rates"], {"USD": 1.0, "EUR": 1.16, "CHF": 1.23})
        self.assertEqual(data["observed_at_by_currency"]["EUR"], now.isoformat())

    def test_no_provider_calls_and_unknown_financial_symbol(self):
        from app import app, build_show_library
        context = {"shows_data":{}, "show_library":build_show_library([{"ticker":"CASY","title":"Earnings","quarter":"Q1 FY2027","youtube_url":"https://youtu.be/CurrentVid1","published_at":"2026-09-09"}])}
        with patch("app._shows_context", return_value=context), patch("app.read_registry",return_value={"profiles":{"CASY":profile("CASY")}}), patch("app._cached_show_stock_detail", side_effect=AssertionError("Blocking data fetch")), patch("app.fetch_ticker_info", side_effect=AssertionError("Blocking data fetch")):
            client = app.test_client()
            response = client.get("/shows/casy")
            self.assertEqual(response.status_code,200)
            self.assertIn('href="/#searchInput" class="return-search" data-search-return', response.get_data(as_text=True))
            self.assertIn('Search Another Stock</a>', response.get_data(as_text=True))
            self.assertIn("0.71x",response.get_data(as_text=True))
            self.assertEqual(client.get("/api/research/UNKNOWN/financials").status_code,404)


if __name__ == "__main__":
    unittest.main()
