# Research Charts and Custom Comparisons

September 10, 2026. Local brand-preview branch only; not published.

## Financial History

- Annual/quarterly reporting, 1/3/5-year presets, bars/lines for the metric explorer, seven additional multi-series charts, expanded chart dialogs, and accessible reported-figure tables.
- Annual presets show up to 1/3/5 fiscal periods; quarterly presets show up to 4/12/20 quarter-end observations within the selected window. Dates remain fiscal dates, not relabeled calendar quarters.
- USD-reporting U.S. companies use the SEC Company Facts feed for longer directly reported US-GAAP histories. [SEC API reference](https://www.sec.gov/search-filings/edgar-application-programming-interfaces).
- Yahoo remains a separate-source fallback. It typically supplies four annual periods and five quarters; selecting 5Y does not invent the missing periods.
- Quarterly cumulative cash-flow totals are not mislabeled as standalone quarters. Annual EPS and weighted-average shares are not subtracted to manufacture Q4. Missing fields remain blank, including gaps from company-specific tags.
- Cash-flow and margin calculations require matching durations. Period-end shares remain distinct from weighted averages. Capital spending is a positive outflow; FCF subtracts it. Ratios with nonpositive denominators are unavailable, not misleading negative valuation signals.
- Debt is not inferred as zero from missing debt tags. SEC total debt requires both current and noncurrent debt; operating leases are shown separately. Metric definitions may differ across providers.
- SEC requests are server-side, bounded, cached and identified with the site's public contact; optional SEC_USER_AGENT overrides the contact. No API key or paid subscription was introduced.

## Market Charts

- Price, volume and drawdown views share 1Y/3Y/5Y presets.
- The 3Y adapter trims weekly history to three years instead of falling through to the old 1Y default.
- Prices are split/dividend-adjusted; volumes are daily or weekly according to source interval. Drawdown is from the running high inside the selected window, not an all-time drawdown.
- Null/nonfinite data is not rendered as zero. Loading or failed requests do not leave an older chart beneath a newly selected time label.

## Comparison Flow

- Two automatic comparisons by default, with medians calculated only from those selected eligible peers.
- Replace either stock, add comparisons (up to four), remove one, or restore suggested peers. Native dialogs support keyboard search and dismissal.
- Typing searches the local registry; explicit broader search uses Yahoo equity listings, including names outside the video catalog.
- Existing fresh profiles are reused; uncatalogued/stale custom selections are fetched in a bounded background pool and cached without modifying the production snapshot registry.
- The table, video thumbnails, podcasts, Shorts and available packet links are re-rendered together. No episode link is implied when none exists.
- Custom comparisons disable automatic peer medians and retain business/size context. Duplicate issuers are rejected. Failed lookups preserve the existing table.
- URL peer selections are retained; the metric-group filter survives dynamic updates.

## Verification

Desktop and 390-pixel phone browser checks exercised Chewy's five annual and 20 quarter-end periods, 1/3/5 presets, the expanded share/cash charts, and actual 3/5-year price requests. Replacing Wayfair with Casey's refreshed metrics and linked research. Remote search/hydration of Bowman Consulting succeeded and correctly showed no linked episode. Remove/reset succeeded. Chart canvases were visually inspected; no browser JavaScript errors were recorded.

Run the isolated Python suite with scripts/test_brand_preview.py and the pure chart-model tests with node --test tests/research-chart-model.test.cjs. Full catalog and VideoObject audits remain required before publishing. Real-reader review and production approval are still pending.

Final local results: 163 Python tests and 6 chart-model tests passed; all 1,118 stock pages rendered in the catalog audit; all 1,116 stock VideoObjects have uploadDate. JavaScript syntax and git diff whitespace checks passed. The new quote-helper unpacking regression is covered. No content catalogs or finalized packets changed.

## Financial Chart Comparisons

- One off-by-default switch applies comparison mode to all eight financial
  charts and their expanded views. The price-history chart stays separate.
- The first two selected table peers are preselected. If the table has more,
  checkboxes choose up to two for charts; the table's four-peer capacity remains.
  Peer replacement/removal updates the charts through `ca-comparison-update`.
- In comparison mode each advanced chart uses a measure selector so it compares
  the same metric across companies, rather than overlaying twelve unrelated
  series. Switching off restores the original multi-metric charts.
- Annual groups use the calendar year of each period end; quarterly groups use
  its calendar quarter. The subject stock defines the available window. Exact
  company period-end dates appear in tooltips and tables. This does not claim
  identical fiscal calendars. Missing periods stay missing; ambiguous groups
  with multiple reports are excluded rather than silently choosing one.
- Monetary/EPS comparisons require known, matching reporting currencies.
  Unknown or different currencies remain blank in those charts, with a visible
  explanation; share counts, margins and ratios can still compare. No currency
  conversion, interpolation, normalization or fabricated zero values.
- A bounded, abortable loader fetches at most two peer series at a time and
  reuses responses while the page is open. Failures leave the other series
  visible and have explicit retry. Stale requests cannot replace new selections.
- Verified outside-catalog equities use the existing listing lookup and bounded
  financial cache. Yahoo responses now include verified reporting currency,
  never a guessed quote currency. Default unknown-symbol API behavior remains
  unchanged; chart-peer requests opt in with `comparison=1`.

Current verification: 168 Python tests and 12 chart-model tests passed.
`node scripts/qa_financial_comparisons.cjs` covers all eight charts at 390, 521
and 1280px: peer caps, currency mismatch, annual/quarterly and 1/3/5-year controls,
expanded-chart updates, failure/retry, live peer-picker replacement, cached
toggle behavior and cancellation races. Actual AVAV/WMT SEC statements also
loaded together for 2022-2026; the expanded revenue view was visually checked.
Different company sizes use one honest absolute scale, so a much smaller
company's bar can be tiny. This is still an unpublished local preview.
