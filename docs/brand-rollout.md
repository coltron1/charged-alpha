# Charged Alpha Brand Review Build

September 10, 2026. Colton approved the website review with "both good proceed"
after the financial chart comparisons and research-packet navigation reviews.
The website is approved for release. Channel publishing, email delivery and
production-queue changes remain separate steps; this release changes only the site.

## Brand Source of Truth

- Master mark: `static/assets/charged-alpha-logo.png`. It is the existing Charged Alpha app icon, not newly generated artwork.
- Original app counterpart: `static/assets/mobile-app/charged-alpha-app-icon.jpg`.
- Use the same intact mark for the channel avatar, website navigation, favicon, future channel art, and brand-level materials. Preserve its proportions, black field, alpha glyph, and electricity artwork.
- Wordmark: Charged Alpha. A small TM may accompany it. Do not use the registered-trademark symbol or claim a registration/application number. Trademark filing remains separate from this design work.
- Approved promise: Understand what changed in the companies you follow, what the headline misses, and what to watch next.
- Core identity: Frontier AI stock research. Keep the use of frontier AI models explicit alongside the reader benefit, not only in a footer disclosure. No implication of guaranteed accuracy or investment performance.
- Research surfaces use charcoal, off-white, mint for actions and a restrained gold identity accent. Existing presenter artwork and finalized episode/packet files are not altered.
- Plotava and other Studio products retain their own product identities.

## Review Build

Run `/usr/bin/python3 scripts/preview_brand.py` from this checkout. It binds to `127.0.0.1:5055`, overrides the DB with an in-memory SQLite database, disables GA, and enables `/brand-preview`. It never deploys or publishes.

- `/`: research-first homepage, date-sorted cards, ticker/company/title search, reset, load more, browser Back state and app promotion after results.
- `/shows`: same research entry, preserving the old public URL.
- `/shows/CASY`: representative stock page, full analysis, verified presentation podcast links, Short, free packet, dated metrics, relevant peers, financials, scenario tool and unified reporting history.
- `/following`: a real browser-local list. Explicitly not email notifications or cross-device accounts. No email fields, fake confirmation or inactive signup controls.
- `/brand-preview`: local-only channel banner/avatar composition, channel description and primary link proposal.
- Shared navigation across the core research, app, Studio, About, Games and research-tool pages. Existing tools, games, app store URLs and product identity remain available.

The new homepage is server-rendered and does not need Tailwind's runtime or the YouTube subscription widget. Charts are served locally; embedded YouTube players load only when selected. Private-device Following and localhost previews do not emit the Google Analytics tag.

## Release Boundary

- [x] Stage the app-icon-based identity and channel copy for review.
- [x] Implement the website review build in a clean isolated branch.
- [x] Complete final desktop/mobile, regression, basic accessibility and link checks; record evidence below.
- [x] Colton reviews and approves the website.
- [x] Coordinate with the active episode publisher, fetch latest main and reconcile content-only changes before any release.
- [ ] Publish approved channel branding and copy; confirm public readback and device crops.
- [ ] Deploy the reviewed website source commit to Railway only after explicit approval.
- [ ] Configure an approved email provider, sender domain and verified stock-following service as a separate gated release. Migrate browser-local stocks only by explicit reader choice.
- [ ] Introduce reviewed structured change/watchpoint summaries upstream. Never invent comparison content from titles or edit finalized packets.
- [ ] Produce the new reusable video CTA after the advertised website/email capability is genuinely live.

Release preparation: fast-forwarded to `b9ca05c3b22060603eac5dbca90cf1877aa9f505`,
preserving the two newer ORCL packet/catalog commits. The episode publisher
confirmed no push in flight and is holding competing website pushes until the
release is verified. Deployment evidence is recorded in the shared website-sync
state under `brand_research_release`, not in finalized packet files.

The preview deliberately does not advertise email alerts, create accounts, touch production credentials or publish an ad. The free packets remain public. No paid newsletter is introduced.

## Validation

Follow-up review build: expanded financial/price charts, five-year SEC history where supported, dynamic two-peer-plus-custom comparisons, and prominent frontier AI identity. See [research-charts.md](research-charts.md) for data methodology and browser verification. This does not change the unpublished release boundary.

Packet navigation review: website packet links now open an enhanced reading view with wrapped desktop section links and a compact jump menu. Canonical originals and their publication checks remain intact. See [research-reader.md](research-reader.md).

Completed September 10, 2026 (America/Chicago):

- 146 tests passed across all 13 test modules using the isolated-module runner.
- Catalog audit: all 1,118 stock pages rendered; 1,116 have a primary YouTube episode.
- Video structured-data audit: 1,116 stock VideoObjects, all with uploadDate.
- Desktop screenshots reviewed at 1,440 pixels; phone layouts checked at 360, 390 and 433 pixels. Core homepage and stock views have no document-width overflow. Actual episode thumbnails and the app icon render.
- Tested exact-ticker search, no-results state, clearing, 24-card load more, return-to-search URL preservation, browser-local Following, verified podcast dropdown links, click-to-load video, older-period deep links, and the earnings-multiple scenario (23.45 x 20 = 469.00).
- Shared header and mode controls checked on all 12 research tools. Stock-screener mode switching, mobile Tools/More navigation, Studio navigation and Escape dismissal were exercised. The final isolated QA tab recorded no JavaScript errors. A missing legacy mode control found during QA was restored before these checks.
- Channel avatar/banner composition reviewed locally at `/brand-preview`; it has not been published or exported as final channel artwork.
- JavaScript syntax checks and `git diff --check` passed. No catalog, packet or production-pipeline data changed.

These are engineering checks, not a formal accessibility certification or an uncoached reader usability study. Real-reader feedback, email delivery and conversion measurement still need their own release gates. The local preview disables analytics and does not test live campaign attribution.

`/usr/bin/python3 scripts/test_brand_preview.py` runs each existing test module in a separate process because their database fixtures are not suite-global-safe. New checks cover date sorting, unchanged packet/video dates, exact studio/primary podcast association, data freshness, SSR ordering, no preview analytics and shared navigation.

Financial display guard: historical ratios with missing/future financial periods or a period older than 550 days are withheld from the comparison and its medians. Market cap, forward P/E, indicated dividend yield and beta are separate quote/estimate metrics; automatic peer selection retains existing quote/snapshot freshness rules. This is a conservative exclusion guard, not a claim that all retained statements share a reporting period.

No modifications to `data/shows_catalog.json`, `data/research_packets.json`, packet HTML, the producer, automation definitions or original worktrees are part of this preview.

## Approved Release Checks

- Latest-main regression run: 168 Python tests across 14 isolated modules and 12 chart-model tests passed.
- `node scripts/qa_following.cjs [base-url]`: real catalog, disposable browser profiles at 390, 785 and 1280 pixels. Covers stock-page and search-card saves, navigation, refresh persistence, reopening/back, cross-tab changes, search/reset, removal and empty states. Also tests denied storage, malformed/duplicate values, the 300-stock limit and a failed-library retry without losing saved tickers.
- `node scripts/qa_financial_comparisons.cjs`: eight charts at 390, 521 and 1280 pixels, two-peer maximum, exact fiscal dates, currency guards, failed-provider retry, expanded views, dynamic peer replacement and cancellation races. Financial fixtures remain test-only.
- `node scripts/qa_research_reader.cjs`: all 11 packets, 820 section jumps at 320, 390, 521, 785 and 1280 pixels; keyboard, deep links, browser Back and no-JavaScript fallback passed. The original SAIL packet's existing 5-pixel overflow at 320 pixels remains unchanged.
- Following is browser-local in the approved live release. No account, email delivery or cross-device synchronization is represented as connected.
- Final live verification and exact released commit belong in the shared `brand_research_release` receipt after deployment, not in a pre-release claim here.
