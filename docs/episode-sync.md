# Charged Alpha Episode Sync

Use this when new YouTube videos, Shorts, or podcast episodes need to be linked on the site.

## Nightly sync (9 PM America/Chicago)

The existing **Charged Alpha nightly episode sync** automation runs every night.
The same workflow can be run manually from a clean checkout of current `origin/main`:

```bash
python3 -m pip install --user -U yt-dlp
python3 scripts/sync_shows_catalog.py
python3 -m json.tool data/shows_catalog.json >/tmp/shows_catalog_validated.json
python3 -m py_compile app.py scripts/sync_shows_catalog.py scripts/earnings_shorts.py
python3 -m unittest discover -s tests -p 'test_earnings_shorts.py'
python3 -m unittest discover -s tests -p 'test_sync_shows_catalog.py'
python3 -m unittest discover -s tests -p 'test_show_stock_pages.py'
python3 scripts/audit_video_structured_data.py
git diff --check
git diff --stat
git add data/shows_catalog.json
git commit -m "Update stock episode catalog"
git push origin HEAD:main
```

Railway deploys after the push to `main`.
Do not commit or push if `catalog_changed` is false. Leave unrelated work alone;
use an isolated worktree based on `origin/main` if the working tree is dirty.

## Preview first

```bash
python3 scripts/sync_shows_catalog.py --dry-run
```

## When something older was missed

The normal long-video sync stops at the first already-linked upload. Shorts always
scan the entire tab, deduplicating by video ID, to recover older gaps. To also
recover older long-form uploads, run:

```bash
python3 scripts/sync_shows_catalog.py --scan-all
```

## What the sync does

- Adds new full stock earnings videos to `episodes`.
- Links matching Podbean episodes and uses their RSS GUIDs to add matching Apple Podcasts episode URLs.
- Adds Spotify episode URLs when they are published on the public Spotify show page.
- Keeps platform-show links available in the stock pages when Spotify, Apple Podcasts, or another service has not yet exposed a verifiable episode-specific URL.
- Adds non-standard full videos to `Market and Sector Explainers`.
- Adds new YouTube Shorts to `Shorts and Clips`.
- Associates Shorts with their full earnings video using `earnings_youtube_url`;
  `earnings_match` records the matching method. The Short's own title, date, and
  URL stay in the clips section rather than replacing long-form or podcast links.
- Rechecks all Shorts on every sync, including old clips and those uploaded before
  their full-length video. This backfill runs even when there are no new uploads.
- Recovers clearly identified earnings reports (including H1/H2 reports) that were
  previously filed as explainers, preserving all existing podcast links.
- Dedupes duplicate full uploads for the same ticker and quarter during each run.

## Earnings Shorts matching

`scripts/earnings_shorts.py` checks an explicit ticker or a company name present
in existing earnings titles. A named reporting period must match (annual/Q4
year-end coverage is compatible), and both publication dates must be within 14
days. Clips without a named period use a tighter three-day window and require
shared topic words when uploaded more than an hour apart. General education and
comparison clips are kept separate. With multiple candidates, only a clearly separated same-day upload is
accepted. Otherwise the Short remains in the general clips library, not on an
arbitrary earnings row. Multi-stock clips are not forced onto one stock.

The summary's `earnings_shorts` object reports linked, newly linked, updated, and
unmatched counts plus unmatched reasons. Investigate unresolved earnings Shorts
using their public title/description and the full report; do not invent a quarter
or guess a URL. Non-earnings comparisons can legitimately remain unmatched.

Stock pages show lightweight thumbnail links below the latest full video's title
and platform links, and alongside the exact earnings entry in the archive. They
do not load an additional player or add Shorts to the homepage stock payload.
Stocks without a matching Short show no empty Shorts block. The original primary
VideoObject SEO markup remains unchanged.
