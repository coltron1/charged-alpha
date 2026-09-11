# Stock Email Alerts

Email subscriptions are separate from browser-local Following and the existing
mobile-app newsletter preferences. No existing subscriber is silently imported.
No paid email service tier or new Railway service is needed for the initial pilot.

## Rollout

The default is off. Configure only in Railway environment variables:

- `RESEND_API_KEY`: sending-only, restricted to chargedalpha.com.
- `RESEND_WEBHOOK_SECRET`: signing secret for `/api/alerts/webhook`.
- `STOCK_ALERTS_POSTAL_ADDRESS`: the owner's approved mailing address, not in Git.
- `STOCK_ALERTS_ENABLED=1`: enables the configured backend.
- `STOCK_ALERTS_WORKER=1`: starts the delivery loop in the existing web process.
- `STOCK_ALERTS_PUBLIC=0`: pilot only; set to 1 after real delivery verification.
- `STOCK_ALERTS_TEST_EMAIL`: owner-approved pilot inbox. Not exposed publicly.

Public signup also refuses to turn on unless the configured database is Postgres
and the sender worker is enabled. That prevents a configuration mistake from
collecting subscribers when no durable delivery path is available.

Subscribe from Following. Confirm by POST on the emailed link; GET requests from
mail scanners do not activate subscriptions. Confirmation links expire after 24
hours and are single-use. Private management links expire after 30 days, and
preference changes invalidate earlier management sessions. Unsubscribe works
without signing in, including RFC 8058 one-click POSTs. The browser bookmark list
does not automatically change the emailed list, or vice versa.

Enable only these Resend events: email.delivered, email.bounced, email.complained,
email.suppressed. Verify the raw signed body with the official Svix library.
Events only affect messages already known to this service. Bounce/complaint
suppression cannot be undone by an anonymous signup request.

## Delivery

The process checks durable Postgres work every 30 seconds. The current Gunicorn
configuration does not preload, and the database lease serializes replicas and
deployments. Keep Railway serverless sleeping disabled: this worker depends on
the existing web service remaining running. It does not depend on Colton's Mac
or a Codex automation being awake. A crashed process resumes queued work after
the lease expires.

After 9:10 p.m. America/Chicago, prepare at most one digest per subscriber per
local day, only if there is new coverage. A missed window is recovered at the
next evening's run, not by sending a morning backfill. The logical notification
identity is ticker + reporting period, so presentation, studio, Short and podcast
versions do not generate duplicate earnings alerts. Already-covered periods are
baselined on confirmation and when adding a stock. Undated, future-dated and
more-than-seven-day-old discoveries are excluded. Each digest includes up to 20
reports; remaining reports can follow in the next daily digest.

Every request has a durable outbox row and fixed provider idempotency key and
payload. Retry uncertain network responses only within 23 hours of the first
attempt, below Resend's 24-hour idempotency window. Pending, failed and uncertain
reports remain reserved across daily runs: never reissue them under another key.
An uncertain/failed delivery needs operator review, not a blind resend. Compare
the Resend log with the outbox provider ID before deciding on any recovery.

Defaults cap new sends at 80/day and 2,500/month for this service, leaving some
room for other projects in the shared Resend account. Confirmation requests have
an additional 20/day cap, per-email cooldown and hashed per-IP rate limit. These
are conservative application limits, not a guarantee of unused provider quota.
Provider 429 responses keep work queued. Do not upgrade billing automatically.

Only additive `stock_alert_*` tables are created. Unsubscribes cancel unsent work
but cannot recall in-flight provider requests. Capability URLs and email bodies
are cleared after sending, and old queued bodies expire; minimal consent,
opt-out and delivery identities remain to enforce suppression and deduplication.
Never put API keys, recipient lists, confirmation links or postal addresses in
commits or deployment logs. Private alert pages use no-store, noindex and
no-referrer, and do not emit Google Analytics.

## Validation

- `/usr/bin/python3 -m unittest discover -s tests -p test_email_alerts.py`
- `/usr/bin/python3 scripts/test_brand_preview.py`
- `/usr/bin/python3 scripts/preview_alerts.py --background`
- `node scripts/qa_email_alerts.cjs`

The preview binds only to loopback port 5056, uses an in-memory DB and fake keys,
and never starts a sending worker. Its confirmation fixture route exists only
in that standalone preview process, never in the deployed Flask application.
