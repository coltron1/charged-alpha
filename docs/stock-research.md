# Stock research workspace

The stock route renders catalog media, immutable research packets and **dated
snapshots**, with no outbound market-data calls. Financial statements and price
history load independently as their sections enter the viewport. Research packet
HTML and publication hashes are never rewritten.

## Refresh and deployment

Run from a clean, current-main site checkout, preserving concurrent catalog work:

```sh
DATABASE_URL=sqlite:///:memory: python3 scripts/refresh_stock_research.py --max-age-hours 24
DATABASE_URL=sqlite:///:memory: python3 -m unittest discover -s tests -p test_stock_research.py
DATABASE_URL=sqlite:///:memory: python3 scripts/audit_show_stock_pages.py
DATABASE_URL=sqlite:///:memory: python3 scripts/audit_video_structured_data.py --strict-catalog-dates
git diff --check
```

The existing combined production/studio/site heartbeat owns this step. Do not
recreate the retired nightly sync. Include changed
`data/stock_research_snapshots.json` with the normal reviewed site commit.
The refresh covers catalog listings plus the reviewed business-group universe,
including companies without Charged Alpha episodes. A complete refresh runs at
most daily with the flag above; targeted repairs use `--symbols CASY,MUSA,ATD.TO`.
Failures retain their previous dated snapshot; failure symbols are recorded.
Check failure counts and unexpected coverage loss before committing. A partial
or failed refresh is never represented as freshly updated data for that company.
Run test modules separately to isolate the existing temporary SQLite fixtures.

## Peer rules

`stock_research.py` owns business groups and unit normalization. The rules are:

- Match reviewed operating groups first; otherwise use the same provider industry
  only if it is not in the broad/generic exclusion list. Never fall back to sector,
  episode count or popularity.
- Prefer USD market cap within 0.5-2x; permit an explicitly labeled extended range
  of one-third-3x. Rank using logarithmic cap and revenue differences.
- Outside that range, include at most two labeled industry benchmarks, only when
  there are fewer than three selected peers. Benchmarks never enter peer medians.
- Require positive cap, current quote and snapshot dates within 14 days, and
  snapshot observation dates within seven days of one another.
- Exclude other share classes of the same issuer. Custom comparisons are labeled,
  limited to four known symbols and never produce an automatic peer median.
- Require two valid matched peers per metric for a median; report its sample size.

Casey's, Murphy USA and Couche-Tard form a convenience/fuel group. Ford and TJX
cannot enter it. Business models still vary, especially prepared-food mix and
geography, which the page labels. Groups are reviewed product rules, not an
assertion of direct competition in every geography. Maintain groups with company
filings and operating segments, not just a matching sector label.

## Financial semantics

Profiles come from Yahoo Finance via the existing yfinance provider. Market cap
uses the quote currency, statements their financial currency; conversion rates
are recorded in the snapshot registry. Do not combine incompatible currencies.
ADR cash-flow yields and EV ratios are omitted when currencies differ. D/E raw
provider percentages are divided by 100 to show a multiple (70.547 becomes 0.71x).
Negative-earnings P/E, nonpositive-equity book metrics and irrelevant bank ratios
are omitted. Percentage ratios use percentage points; missing is not zero.
No row assigns a universal green/red investment winner.

Statement history joins income, cash flow and balance sheets by **period end**,
not column position. It shows available history, up to eight quarters/five years,
without filling gaps or mixing annual and quarterly amounts. Free cash flow
fallback is operating cash flow minus absolute capex. Diluted shares show share
counts, EPS currency/share, and margins percentages. The background queue is
bounded and single-flight per symbol; failures have a retry state and short
negative cache. Existing price history is provider-adjusted closing prices.

The earnings-multiple calculator is an explicitly illustrative sensitivity tool,
not an intrinsic-value target. The latest immutable research packet supplies the
source-backed thesis, risks, catalysts and industry-specific KPIs. Do not generate
consensus beats/misses, historic valuations, segment statistics or bull/bear
claims from incomplete provider snapshots.

## Archive and QA

Group editions by fiscal period; preserve both original and studio long-form
links, all verified episode podcast links and packet links. Deduplicate the same
Short within a period while retaining its exact episode association and date.
Latest media links must not inherit an older quarter's podcast.

Before release: all Python tests, full catalog rendering, strict video dates,
packet integrity tests, `scripts/qa_stock_research.cjs` on desktop/mobile, live
provider history checks, diff review and Railway verification. Browser QA needs
Playwright and installed Chrome, and accepts the server URL as its first argument.
