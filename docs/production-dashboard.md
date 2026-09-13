# Public episode tracker

The homepage shows the most recent three completed episode bundles and the next three anticipated earnings reports. `/production` expands into the seven-day earnings calendar and completed history. Earnings release timing is separate from the production plan; exact issuer-confirmed clock times use Central Time, while before/after-market and unknown times retain their actual level of certainty.

The combined Charged Alpha producer/studio/site heartbeat owns this data. After its normal episode/catalog/packet sync, refresh the private production snapshot and run:

```bash
/usr/bin/python3 scripts/sync_production_dashboard.py
```

Review counts and the proposed public fields, then run the same script with `--execute` through the production controller's website maintenance lane. Commit `data/production_dashboard.json` with the normal website data update and deploy through the existing Git/Railway process. Verify `/` and `/production` after deployment. Run this even when no new episode was buildable so the dated calendar is refreshed; do not claim that a failed provider request means there are no earnings.

The importer is local-only. The deployed website has no dependency on the producer's Mac, localhost server, filesystem paths, credentials, or models. Its explicit output allowlist contains company identity, periods, dates/timing, public video/packet/source links and freshness. Raw private snapshot fields—including claims, PIDs, automation status, internal scoring, blocked work and local provenance—are never copied into the site.

Canonical `DONE` evidence supplies completed bundles. A primary upload or render alone is not a completed bundle. Forecast entries are tentative and do not bypass the producer's filing, eligibility or quality checks. Dates older than the viewer's current day are removed from “Reporting next”; a source older than four hours is visibly marked as needing refresh.

Validation: `DATABASE_URL=sqlite:///:memory: /usr/bin/python3 -m unittest discover -s tests -p 'test_production_board.py'`, the normal website regression modules, and desktop/phone checks of the preview and expansion link.
