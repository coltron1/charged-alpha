# Charged Alpha Episode Sync

Use this when new YouTube videos, Shorts, or podcast episodes need to be linked on the site.

## One-command weekly sync

```bash
python3 -m pip install --user -U yt-dlp
python3 scripts/sync_shows_catalog.py
python3 -m json.tool data/shows_catalog.json >/tmp/shows_catalog_validated.json
python3 -m py_compile app.py
git diff --stat
git add data/shows_catalog.json
git commit -m "Update stock episode catalog"
git push origin main
```

Railway deploys after the push to `main`.

## Preview first

```bash
python3 scripts/sync_shows_catalog.py --dry-run
```

## When something older was missed

The normal sync stops when it reaches the first already-linked YouTube upload. If an older upload needs to be recovered, run:

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
- Dedupes duplicate full uploads for the same ticker and quarter during each run.
