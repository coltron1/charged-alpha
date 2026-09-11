from datetime import date, datetime, timezone
from copy import deepcopy
import unittest
from unittest.mock import patch, Mock

import pandas as pd
from sec_financials import parse_company_facts
from stock_research_data import statement_rows, derived_metrics, fetch_financials
from stock_research import comparison, normalize_profile
from stock_comparison_data import search_stocks, custom_profile_status
import stock_comparison_data as lookup
from yf_utils import fetch_chart, chart_cache


def fact(value, end="2025-12-31", start="2025-01-01", filed="2026-02-20", **extra):
    return {"val": value, "end": end, "start": start, "filed": filed, "form": "10-K", "accn": "0000000001-26-000001", **extra}


def facts(tags):
    return {"cik": 1, "facts": {"us-gaap": {tag: {"units": {unit: items}} for tag, (unit, items) in tags.items()}}}


def profile(symbol):
    now = datetime.now(timezone.utc)
    return normalize_profile(symbol, {"longName": symbol+" Company", "marketCap": 1000000,
        "currency": "USD", "financialCurrency": "USD", "currentPrice": 10, "regularMarketTime": now.timestamp(),
        "mostRecentQuarter": now.timestamp(), "industryKey": "internet-retail"}, now.isoformat())


class ExtendedStatementTests(unittest.TestCase):
    def test_five_annual_periods_and_twenty_quarters_cap(self):
        annual = [fact(year, end=f"{year}-12-31", start=f"{year}-01-01") for year in range(2019, 2026)]
        payload = facts({"Revenues": ("USD", annual)})
        result = parse_company_facts(payload, today=date(2026,9,10))
        self.assertEqual([r["date"] for r in result["annual"]], [f"{year}-12-31" for year in range(2021,2026)])

    def test_cumulative_cashflows_are_not_quarters_or_eps_subtraction(self):
        payload = facts({
            "Revenues": ("USD", [fact(100), fact(25,end="2025-06-30",start="2025-04-01",form="10-Q")]),
            "NetCashProvidedByUsedInOperatingActivities": ("USD", [fact(40), fact(25,end="2025-06-30",start="2025-01-01",form="10-Q")]),
            "EarningsPerShareDiluted": ("USD/shares", [fact(2)]),
            "Assets": ("USD", [fact(500,start=None)]),
        })
        result=parse_company_facts(payload,today=date(2026,9,10))
        june=next(r for r in result["quarterly"] if r["date"]=="2025-06-30")
        q4=next(r for r in result["quarterly"] if r["date"]=="2025-12-31")
        self.assertNotIn("operating_cashflow",june)
        self.assertNotIn("eps",q4)
        self.assertEqual(q4["assets"],500)

    def test_latest_filing_exact_units_and_no_future_facts(self):
        payload=facts({"Revenues":("USD",[fact(100),fact(120,filed="2026-03-01"),fact(999,filed="2027-01-01")]),
                       "WeightedAverageNumberOfDilutedSharesOutstanding":("shares",[fact(50)]),
                       "EarningsPerShareDiluted":("USD/shares",[fact(2)])})
        result=parse_company_facts(payload,today=date(2026,9,10))["annual"][0]
        self.assertEqual((result["revenue"],result["shares"],result["eps"]),(120,50,2))
        self.assertIsNone(parse_company_facts(payload,currency="EUR",today=date(2026,9,10)))

    def test_same_end_different_duration_does_not_create_margin(self):
        payload=facts({"Revenues":("USD",[fact(100)]),"OperatingIncomeLoss":("USD",[fact(20,start="2024-12-20")])})
        result=parse_company_facts(payload,today=date(2026,9,10))["annual"][0]
        self.assertIsNone(derived_metrics(result)["operating_margin"])

    def test_zero_is_reported_and_missing_debt_component_not_zero(self):
        payload=facts({"Revenues":("USD",[fact(100)]),"LongTermDebtNoncurrent":("USD",[fact(0,start=None)])})
        result=parse_company_facts(payload,today=date(2026,9,10))["annual"][0]
        self.assertEqual(result["long_term_debt"],0)
        self.assertIsNone(result.get("debt"))

    def test_cashflow_and_balance_metrics(self):
        dt=pd.Timestamp("2025-12-31")
        income=pd.DataFrame({dt:{"Total Revenue":100,"Net Income":-10,"Gross Profit":40,"Operating Income":20,"Basic Average Shares":8,"Diluted Average Shares":10}})
        cash=pd.DataFrame({dt:{"Operating Cash Flow":30,"Capital Expenditure":-7,"Repurchase Of Capital Stock":-5,"Stock Based Compensation":3}})
        balance=pd.DataFrame({dt:{"Total Assets":200,"Total Liabilities Net Minority Interest":150,"Stockholders Equity":-5,"Total Debt":20,"Cash And Cash Equivalents":40,"Current Assets":80,"Current Liabilities":40}})
        row=statement_rows(income,cash,balance,20)[0]
        self.assertEqual((row["free_cashflow"],row["capex"],row["buybacks"]),(23,7,5))
        self.assertEqual((row["net_debt"],row["working_capital"],row["current_ratio"]),(-20,40,2))
        self.assertEqual((row["shares"],row["basic_shares"],row["net_margin"]),(10,8,-10))
        self.assertIsNone(row["debt_to_equity"])

    def test_sec_fallback_keeps_yahoo_source_separate(self):
        dt=pd.Timestamp("2025-12-31")
        frame=pd.DataFrame({dt:{"Total Revenue":100}})
        ticker=Mock(income_stmt=frame,cashflow=pd.DataFrame(),balance_sheet=pd.DataFrame(),
                    quarterly_income_stmt=frame,quarterly_cashflow=pd.DataFrame(),quarterly_balance_sheet=pd.DataFrame())
        with patch("stock_research_data.fetch_sec_financials",side_effect=ValueError("offline")),patch("stock_research_data.yf.Ticker",return_value=ticker):
            data=fetch_financials("TEST")
        self.assertEqual(data["source"],"Yahoo Finance")
        self.assertIn("limited",data["note"])

    def test_statement_response_retains_verified_reporting_currency_not_quote_currency(self):
        dt=pd.Timestamp("2025-12-31")
        frame=pd.DataFrame({dt:{"Total Revenue":100}})
        ticker=Mock(income_stmt=frame,cashflow=pd.DataFrame(),balance_sheet=pd.DataFrame(),
                    quarterly_income_stmt=frame,quarterly_cashflow=pd.DataFrame(),quarterly_balance_sheet=pd.DataFrame())
        with patch("stock_research_data.fetch_sec_financials",return_value=None),patch("stock_research_data.yf.Ticker",return_value=ticker):
            data=fetch_financials("FOREIGN","EUR")
        self.assertEqual(data["currency"],"EUR")

    def test_three_year_price_history_does_not_silently_return_one_year(self):
        chart_cache.clear()
        now=pd.Timestamp.now(tz="UTC")
        dates=pd.DatetimeIndex([now-pd.DateOffset(years=4),now-pd.DateOffset(years=2),now])
        history=pd.DataFrame({"Close":[10,20,30],"Volume":[100,200,300]},index=dates)
        ticker=Mock()
        ticker.history.return_value=history
        with patch("yf_utils.yf.Ticker",return_value=ticker):
            result=fetch_chart("TEST-3Y","3y")
        self.assertEqual(result["prices"],[20,30])
        self.assertEqual(result["volumes"],[200,300])
        ticker.history.assert_called_once_with(period="5y",interval="1wk",auto_adjust=True)

    def test_market_chart_omits_nonfinite_values(self):
        chart_cache.clear()
        history=pd.DataFrame({"Close":[float("inf"),20],"Volume":[-1,float("nan")]},index=pd.date_range("2026-01-01",periods=2))
        ticker=Mock()
        ticker.history.return_value=history
        with patch("yf_utils.yf.Ticker",return_value=ticker):
            result=fetch_chart("TEST-INVALID","1y")
        self.assertEqual(result["prices"],[None,20])
        self.assertEqual(result["volumes"],[None,None])


class ComparisonInteractionTests(unittest.TestCase):
    def setUp(self):
        from app import app, build_show_library
        self.client=app.test_client()
        self.profiles={s:profile(s) for s in ["TEST","PEER1","PEER2","PEER3","EXTRA"]}
        self.context={"shows_data":{},"show_library":build_show_library([{"ticker":"EXTRA","quarter":"Q1 FY2026","title":"Extra research","youtube_url":"https://youtube.com/watch?v=CurrentVid1","spotify_url":"https://open.spotify.com/episode/example"}]),
                      "research_packets":[{"ticker":"EXTRA","slug":"extra-q1-fy2026","period":"Q1 FY2026"}]}
        self.registry={"profiles":self.profiles}
        self.patches=[patch("app.read_registry",return_value=self.registry),patch("app._shows_context",return_value=self.context)]
        for item in self.patches:item.start()
        self.addCleanup(lambda:[item.stop() for item in self.patches])

    def test_two_default_columns_and_matching_medians(self):
        original=deepcopy(self.profiles)
        data=comparison("TEST",self.profiles,default_limit=2)
        self.assertEqual(len(data["columns"]),3)
        self.assertTrue(all(row["median_count"]<=2 for row in data["rows"]))
        self.assertEqual(self.profiles,original)

    def test_dynamic_comparison_updates_research_and_packet(self):
        response=self.client.get("/api/research/TEST/comparison?peers=EXTRA,PEER1")
        self.assertEqual(response.status_code,200)
        data=response.get_json()
        self.assertEqual([p["ticker"] for p in data["peers"]],["EXTRA","PEER1"])
        self.assertIn("/research/extra-q1-fy2026",data["html"])
        self.assertIn("CurrentVid1",data["html"])
        self.assertIn("open.spotify.com",data["html"])
        self.assertIn("Custom selections",data["html"])

    def test_invalid_duplicate_and_unknown_input(self):
        self.assertEqual(self.client.get("/api/research/TEST/comparison?peers=../../bad").status_code,400)
        self.assertEqual(self.client.get("/api/research/TEST/comparison?peers=PEER1,PEER1").status_code,400)
        self.assertEqual(self.client.get("/api/research/UNKNOWN/comparison").status_code,404)

    def test_loading_failure_and_empty_selection(self):
        with patch("app.custom_profile_status",return_value=(None,"loading")):
            self.assertEqual(self.client.get("/api/research/TEST/comparison?peers=NEW").status_code,202)
        with patch("app.custom_profile_status",return_value=(None,"unavailable")):
            self.assertEqual(self.client.get("/api/research/TEST/comparison?peers=NEW").status_code,503)
        self.assertEqual(self.client.get("/api/research/TEST/comparison?peers=").get_json()["peers"],[])

    def test_exact_ticker_search_first_and_remote_equities_only(self):
        self.assertEqual(search_stocks("PEER1",self.profiles)[0]["ticker"],"PEER1")
        lookup._search_cache.clear()
        with patch("stock_comparison_data.yf.Search",return_value=Mock(quotes=[{"symbol":"ABC","quoteType":"EQUITY","longname":"ABC Corp"},{"symbol":"FUND","quoteType":"ETF"}])):
            self.assertEqual([r["ticker"] for r in search_stocks("ABC",{},True)],["ABC"])

    def test_custom_data_singleflight(self):
        lookup._profiles.clear();lookup._failures.clear();lookup._pending.clear()
        with patch("stock_comparison_data._pool") as pool:
            self.assertEqual(custom_profile_status("NEW",self.registry)[1],"loading")
            self.assertEqual(custom_profile_status("NEW",self.registry)[1],"loading")
            self.assertEqual(pool.submit.call_count,1)
        lookup._pending.clear()

    def test_custom_hydration_unpacks_shared_quote_helper(self):
        lookup._profiles.clear();lookup._failures.clear();lookup._pending.clear()
        info={"quoteType":"EQUITY","longName":"New Company","currency":"USD","financialCurrency":"USD","marketCap":123000000}
        with patch("stock_comparison_data.fetch_ticker_info",return_value=(Mock(),info)),patch("stock_comparison_data._pool") as pool:
            pool.submit.side_effect=lambda fn:fn()
            custom_profile_status("NEW",self.registry)
            loaded,status=custom_profile_status("NEW",self.registry)
        self.assertEqual(status,"ready")
        self.assertEqual(loaded["market_cap_usd"],123000000)

    def test_financial_comparison_supports_verified_outside_catalog_stock(self):
        external={**profile("NEW"),"financial_currency":"EUR"}
        with patch("app.custom_profile_status",return_value=(external,"ready")),patch("app.financial_status",return_value=({"status":"ready","currency":"EUR","annual":[]},200)) as fetch:
            response=self.client.get("/api/research/NEW/financials?comparison=1")
        self.assertEqual(response.status_code,200)
        self.assertEqual(response.json["currency"],"EUR")
        fetch.assert_called_once_with("NEW","EUR")
        self.assertEqual(response.headers["Cache-Control"],"no-store")

    def test_comparison_financial_listing_loading_failure_and_validation(self):
        for listing_status,http_status in [("loading",202),("unavailable",503),("busy",503)]:
            with patch("app.custom_profile_status",return_value=(None,listing_status)),patch("app.financial_status") as fetch:
                response=self.client.get("/api/research/NEW/financials?comparison=1")
            self.assertEqual(response.status_code,http_status)
            fetch.assert_not_called()
        with patch("app.custom_profile_status") as lookup:
            self.assertEqual(self.client.get("/api/research/BAD%24/financials?comparison=1").status_code,400)
            self.assertEqual(self.client.get("/api/research/UNKNOWN/financials").status_code,404)
            lookup.assert_not_called()

    def test_channel_copy_keeps_frontier_ai_prominent_and_fits_channel_limit(self):
        import re
        with patch.dict("os.environ",{"BRAND_PREVIEW":"1"}):
            html=self.client.get("/brand-preview").get_data(as_text=True)
        copy=re.search(r'<div class="copy">(.*?)</div>',html,re.S).group(1)
        self.assertLessEqual(len(copy),1000)
        self.assertIn("Frontier AI stock research",copy[:80])


if __name__=="__main__":unittest.main()
