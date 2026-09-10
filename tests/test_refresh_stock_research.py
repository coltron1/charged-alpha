from copy import deepcopy
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts import refresh_stock_research as subject


OLD = "2026-09-09T03:19:48+00:00"
FIRST = "2026-09-10T08:50:16+00:00"
RETRY = "2026-09-10T08:52:01+00:00"


def info(symbol="CADCO", currency="CAD"):
    return {"longName": symbol, "industry": "Retail", "industryKey": "retail",
            "currency": currency, "financialCurrency": currency, "marketCap": 100,
            "totalRevenue": 80, "currentPrice": 10}


class FxRefreshTests(unittest.TestCase):
    def test_failed_retry_preserves_first_refresh_rates_and_dates(self):
        rates = {currency: index / 10 for index, currency in enumerate(subject.FX_CURRENCIES, 1)}
        def success(symbol, max_retries):
            return None, {"regularMarketPrice": rates[symbol.removesuffix("USD=X")]}
        registry = {"fx": {"USD": 1, "CAD": .7}, "fx_observed_at": OLD}
        with patch.object(subject, "fetch_ticker_info", side_effect=success):
            merged, fresh, dates, failed, global_date = subject.refresh_fx(registry, FIRST)
        self.assertEqual(failed, [])
        self.assertEqual(global_date, FIRST)
        self.assertEqual(set(fresh), {"USD", *subject.FX_CURRENCIES})
        first = {"fx": merged, "fx_observed_at": global_date,
                 "fx_observed_at_by_currency": dates}
        with patch.object(subject, "fetch_ticker_info", return_value=(None, None)):
            retried, retry_fresh, retry_dates, retry_failed, retry_global = subject.refresh_fx(first, RETRY)
        self.assertEqual(retried, merged)
        self.assertEqual(retry_dates, dates)
        self.assertEqual(retry_fresh, {"USD": 1})
        self.assertEqual(retry_failed, list(subject.FX_CURRENCIES))
        self.assertEqual(retry_global, FIRST)

    def test_legacy_global_date_is_preserved_per_retained_currency(self):
        registry = {"fx": {"USD": 1, "CAD": .75}, "fx_observed_at": OLD}
        with patch.object(subject, "fetch_ticker_info", return_value=(None, None)):
            merged, _, dates, failed, global_date = subject.refresh_fx(registry, RETRY)
        self.assertEqual(merged, {"USD": 1, "CAD": .75})
        self.assertEqual(dates, {"USD": OLD, "CAD": OLD})
        self.assertEqual(global_date, OLD)
        self.assertIn("CAD", failed)

    def test_partial_success_dates_only_fresh_currency_and_rejects_invalid_rates(self):
        registry = {"fx": {"USD": 1, "CAD": .75, "EUR": 1.1}, "fx_observed_at": OLD}
        values = {"CAD": .8, "EUR": float("nan"), "GBP": -1, "JPY": "bad"}
        def response(symbol, max_retries):
            value = values.get(symbol.removesuffix("USD=X"))
            return None, ({"regularMarketPrice": value} if value is not None else None)
        with patch.object(subject, "fetch_ticker_info", side_effect=response):
            merged, fresh, dates, failed, global_date = subject.refresh_fx(registry, RETRY)
        self.assertEqual(merged["CAD"], .8)
        self.assertEqual(dates["CAD"], RETRY)
        self.assertEqual(merged["EUR"], 1.1)
        self.assertEqual(dates["EUR"], OLD)
        self.assertEqual(fresh, {"USD": 1, "CAD": .8})
        self.assertTrue({"EUR", "GBP", "JPY"}.issubset(failed))
        self.assertEqual(global_date, OLD)

    def test_missing_fresh_fx_never_degrades_or_reuses_stale_conversion(self):
        source = info()
        previous = subject.normalize_profile("CADCO", source, OLD, {"USD": 1, "CAD": .75})
        previous_copy = deepcopy(previous)
        self.assertIsNone(subject.refreshed_profile("CADCO", source, RETRY, {"USD": 1}, previous))
        self.assertEqual(previous, previous_copy)
        new_profile = subject.refreshed_profile("NEWCAD", info("NEWCAD"), RETRY, {"USD": 1})
        self.assertIsNotNone(new_profile)
        self.assertIsNone(new_profile["market_cap_usd"])
        self.assertIsNone(new_profile["revenue_usd"])

    def test_fresh_usd_profile_can_update(self):
        previous = subject.normalize_profile("USCO", info("USCO", "USD"), OLD, {"USD": 1})
        updated_info = info("USCO", "USD")
        updated_info.update({"marketCap": 125, "totalRevenue": 90})
        updated = subject.refreshed_profile("USCO", updated_info, RETRY, {"USD": 1}, previous)
        self.assertEqual(updated["market_cap_usd"], 125)
        self.assertEqual(updated["revenue_usd"], 90)
        self.assertEqual(updated["observed_at"], RETRY)

    def test_main_failed_targeted_retry_retains_fx_dates_and_whole_nonusd_profile(self):
        previous = subject.normalize_profile("CADCO", info(), OLD, {"USD": 1, "CAD": .75})
        registry = {"schema_version": 1, "refreshed_at": OLD, "full_refreshed_at": OLD,
                    "fx": {"USD": 1, "CAD": .75}, "fx_observed_at": OLD,
                    "failed_symbols": ["CADCO"], "profiles": {"CADCO": previous}}
        def response(symbol, max_retries):
            return (None, info()) if symbol == "CADCO" else (None, None)
        with tempfile.TemporaryDirectory() as td:
            output = Path(td) / "snapshot.json"
            with patch.object(subject, "read_registry", return_value=deepcopy(registry)), \
                 patch.object(subject, "SNAPSHOT_PATH", output), \
                 patch.object(subject, "fetch_ticker_info", side_effect=response), \
                 patch.object(sys, "argv", ["refresh_stock_research.py", "--symbols", "CADCO"]):
                self.assertEqual(subject.main(), 1)
            result = json.loads(output.read_text())
        self.assertEqual(result["profiles"]["CADCO"], previous)
        self.assertEqual(result["failed_symbols"], ["CADCO"])
        self.assertEqual(result["fx"], {"USD": 1.0, "CAD": .75})
        self.assertEqual(result["fx_observed_at"], OLD)
        self.assertEqual(result["fx_observed_at_by_currency"], {"CAD": OLD, "USD": OLD})
        self.assertEqual(result["full_refreshed_at"], OLD)


if __name__ == "__main__":
    unittest.main()
