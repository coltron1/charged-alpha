"""Confirmed, stock-specific alerts with durable delivery and explicit rollout gates."""
import datetime as dt
import hashlib
import hmac
import json
import os
import re
import secrets
import threading
import uuid
from urllib.parse import urlsplit, quote
from zoneinfo import ZoneInfo

import requests
from flask import Blueprint, abort, current_app, jsonify, redirect, render_template, request, session
from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer
from sqlalchemy import or_
from sqlalchemy.exc import IntegrityError

from auth import normalize_email_updates_address
from email_alert_models import (db, StockAlertSubscriber as Subscriber, StockAlertRequest as ConfirmRequest,
                               StockAlertSeen as Seen, StockAlertMessage as Message,
                               StockAlertThrottle as Throttle, StockAlertLease as Lease)
from research_ui import publication_date
from stock_research import group_episode_archive

bp = Blueprint('stock_alerts', __name__)
BASE = 'https://chargedalpha.com'
PERIOD = re.compile(r'^(?:Q[1-4]|H[12])\s+(?:FY)?\d{4}$|^FY\s?\d{4}$')
PENDING = ('queued', 'sending')
_wake = threading.Event()


def now():
    return dt.datetime.utcnow()


def utc_timestamp(value=None):
    """Return a stable epoch even when the runtime's local timezone is not UTC."""
    value = value or now()
    return value.replace(tzinfo=dt.timezone.utc).timestamp()


def configured():
    config = current_app.config
    return bool(config.get('STOCK_ALERTS_ENABLED') and config.get('RESEND_API_KEY')
                and config.get('STOCK_ALERTS_POSTAL_ADDRESS') and config.get('RESEND_WEBHOOK_SECRET'))


def public_enabled():
    # Public signups need both a durable database and a live sender. Tests use an
    # in-memory database intentionally, so they are the one safe exception.
    database_uri = str(current_app.config.get('SQLALCHEMY_DATABASE_URI', ''))
    durable_database = current_app.testing or database_uri.startswith('postgresql')
    return (configured() and current_app.config.get('STOCK_ALERTS_PUBLIC', False)
            and (current_app.testing or current_app.config.get('STOCK_ALERTS_WORKER', False))
            and durable_database)


def allowed_recipient(email):
    return configured() and (public_enabled() or email == current_app.config.get('STOCK_ALERTS_TEST_EMAIL'))


def csrf_token():
    if not session.get('stock_alert_csrf'):
        session['stock_alert_csrf'] = secrets.token_urlsafe(24)
    return session['stock_alert_csrf']


def csrf_check():
    expected = session.get('stock_alert_csrf', '')
    supplied = request.headers.get('X-CSRF-Token') or request.form.get('csrf', '')
    origin = request.headers.get('Origin')
    allowed = {BASE, 'https://www.chargedalpha.com'}
    if current_app.testing or os.environ.get('BRAND_PREVIEW') == '1':
        allowed.add(request.host_url.rstrip('/'))
    # The page deliberately uses no-referrer. Chromium consequently submits
    # native same-page forms with Origin: null. The session-bound, high-entropy
    # CSRF field is still required below, so accepting this form preserves
    # privacy without admitting a token-less cross-site request.
    if origin and origin != 'null' and origin not in allowed:
        abort(403)
    if not expected or not isinstance(supplied, str) or not hmac.compare_digest(expected, supplied):
        abort(403)


def signer(salt):
    return URLSafeTimedSerializer(current_app.secret_key, salt='stock-alerts-' + salt)


def unsubscribe_url(sub):
    return BASE + '/alerts/unsubscribe/' + signer('unsubscribe').dumps(sub.id)


def manage_url(sub):
    return BASE + '/alerts/access/' + signer('manage').dumps([sub.id, sub.version])


def current_subscriber():
    identity = session.get('stock_alert_identity')
    if (not isinstance(identity, list) or len(identity) != 3
            or not isinstance(identity[2], (int, float))
            or identity[2] < utc_timestamp() - 30 * 86400):
        return None
    sub = db.session.get(Subscriber, identity[0])
    return sub if sub and sub.version == identity[1] and sub.state == 'active' else None


def establish_session(sub):
    session['stock_alert_identity'] = [sub.id, sub.version, utc_timestamp()]


def masked_email(email):
    local, domain = email.split('@', 1)
    return local[:2] + '***@' + domain


def catalog():
    return current_app.extensions['stock_alert_catalog']()


def validate_tickers(value):
    if not isinstance(value, list) or not 1 <= len(value) <= 50 or any(not isinstance(t, str) for t in value):
        raise ValueError('Choose between 1 and 50 stocks.')
    valid = {s['ticker'] for s in catalog()['show_library']['stocks']}
    result = sorted(set(t.strip().upper() for t in value))
    if any(t not in valid for t in result):
        raise ValueError('One or more stocks are not in the research library.')
    return result


def requested_tickers():
    """Return only catalog stocks explicitly supplied by a Follow link."""
    requested = request.args.getlist('ticker')
    if not requested:
        return []
    try:
        return validate_tickers(requested)
    except ValueError:
        # A malformed private-link query must not become a usable subscription
        # selection, but it also should not reveal catalog details.
        return []


def safe_link(url):
    try:
        parsed = urlsplit(url or '')
        hosts = {'chargedalpha.com', 'www.youtube.com', 'youtube.com', 'youtu.be', 'open.spotify.com',
                 'podcasts.apple.com', 'chargedalpha.podbean.com', 'www.podbean.com'}
        return url if parsed.scheme == 'https' and parsed.hostname in hosts and not parsed.username else ''
    except ValueError:
        return ''


def editions():
    """One identity per company/reporting period, independent of platform or edition."""
    context, result = catalog(), {}
    for stock in context['show_library']['stocks']:
        ticker = stock['ticker']
        for group in group_episode_archive(stock.get('episodes', [])):
            period = group['quarter']
            if not PERIOD.fullmatch(period):
                continue
            dates = [publication_date(e.get('published_at')) for e in group['editions']]
            dates = [d for d in dates if d]
            date = min(dates) if dates else publication_date(group.get('published_at'))
            links = [{'label': 'YouTube', 'url': e['url']} for e in group['editions'][:1]] + group['podcasts']
            links = [link for link in links if safe_link(link['url'])]
            if not links or not date:
                continue
            key = ticker + ':' + re.sub(r'\s+', '', period.upper())
            result[key] = {'key': key, 'ticker': ticker, 'company': stock['company'], 'period': period,
                           'date': date, 'title': group['title'] or f'{ticker} {period} research', 'links': links,
                           'url': BASE + '/shows/' + quote(stock['slug'], safe='') + '?utm_source=stock_alert&utm_medium=email'}
    for packet in context.get('research_packets', []):
        period = packet['period']
        if not PERIOD.fullmatch(period):
            continue
        key = packet['ticker'] + ':' + re.sub(r'\s+', '', period.upper())
        link = {'label': 'Research packet', 'url': BASE + '/research/' + quote(packet['slug'], safe='') + '?view=reader'}
        if key in result:
            result[key]['links'].append(link)
        else:
            date = publication_date(packet.get('source_published'))
            if date:
                result[key] = {'key': key, 'ticker': packet['ticker'], 'company': packet['company'], 'period': period,
                               'date': date, 'title': packet['title'], 'links': [link],
                               'url': BASE + '/shows/' + quote(packet['ticker'], safe='') + '?utm_source=stock_alert&utm_medium=email'}
    return result


def mark_seen(sub, keys):
    existing = {r.edition_key for r in Seen.query.filter_by(subscriber_id=sub.id)}
    for key in set(keys) - existing:
        db.session.add(Seen(subscriber_id=sub.id, edition_key=key))


def baseline(sub, selected):
    mark_seen(sub, [key for key, entry in editions().items() if entry['ticker'] in selected])


def cancel_queued(sub):
    # Requests already handed to the provider cannot be recalled, but unsent work is cancelled.
    Message.query.filter(Message.subscriber_id == sub.id, Message.state.in_(PENDING)).update(
        {'state': 'cancelled', 'payload_json': '{}'}, synchronize_session=False)


def deactivate(sub, state='unsubscribed'):
    sub.state = state
    sub.version += 1
    sub.updated_at = now()
    sub.tickers_json = '[]'
    cancel_queued(sub)


def rate_hit(value, maximum, seconds):
    window = int(utc_timestamp()) // seconds
    key = hmac.new(current_app.secret_key.encode(), f'{value}:{window}'.encode(), hashlib.sha256).hexdigest()
    if not db.session.get(Throttle, key):
        db.session.add(Throttle(key=key, hits=0, expires_at=now() + dt.timedelta(seconds=seconds * 2)))
        try:
            db.session.commit()
        except IntegrityError:
            db.session.rollback()
    changed = Throttle.query.filter_by(key=key).filter(Throttle.hits < maximum).update({'hits': Throttle.hits + 1})
    db.session.commit()
    return bool(changed)


def email_payload(sub, kind, entries=None, confirm_url=None, selected=None):
    digest = kind == 'digest'
    subject = ('New research: ' + ', '.join(dict.fromkeys(e['ticker'] for e in entries))) if digest else (
        'Confirm your Charged Alpha stock alerts' if kind == 'confirm' else 'Manage your Charged Alpha stock alerts')
    context = {'subscriber': sub, 'kind': kind, 'entries': entries or [], 'confirm_url': confirm_url,
               'selected': selected or [], 'manage_url': manage_url(sub), 'unsubscribe_url': unsubscribe_url(sub),
               'postal_address': current_app.config['STOCK_ALERTS_POSTAL_ADDRESS']}
    return {'from': 'Charged Alpha <alerts@chargedalpha.com>', 'to': [sub.email], 'subject': subject[:200],
            'html': current_app.jinja_env.get_template('emails/stock_alert.html').render(**context),
            'text': current_app.jinja_env.get_template('emails/stock_alert.txt').render(**context),
            'headers': {'List-Unsubscribe': '<' + context['unsubscribe_url'] + '>',
                        'List-Unsubscribe-Post': 'List-Unsubscribe=One-Click'}}


@bp.after_request
def private_response(response):
    response.headers['Cache-Control'] = 'no-store'
    response.headers['X-Robots-Tag'] = 'noindex, nofollow'
    response.headers['Referrer-Policy'] = 'no-referrer'
    return response


@bp.get('/api/alerts/preferences')
def preferences():
    sub = current_subscriber()
    return jsonify(enabled=public_enabled(), pilot=configured() and not public_enabled(), csrf=csrf_token(),
                   email=masked_email(sub.email) if sub else None,
                   tickers=json.loads(sub.tickers_json) if sub else [])


@bp.post('/api/alerts/request')
def request_alerts():
    csrf_check()
    if not configured():
        return jsonify(error='Email alerts are not available yet.'), 503
    if request.content_length and request.content_length > 8192:
        abort(413)
    body = request.get_json(silent=True)
    if not isinstance(body, dict):
        abort(400)
    if body.get('website'):
        return jsonify(ok=True)
    email = normalize_email_updates_address(body.get('email'))
    if not email:
        return jsonify(error='Enter a valid email address.'), 400
    if not allowed_recipient(email):
        return jsonify(error='Public email alerts are not open yet.'), 503
    kind = 'access' if body.get('action') == 'access' else 'confirm'
    try:
        selected = validate_tickers(body.get('tickers')) if kind == 'confirm' else []
    except ValueError as exc:
        return jsonify(error=str(exc)), 400
    if kind == 'confirm' and body.get('consent') is not True:
        return jsonify(error='Please confirm you want these stock alerts.'), 400
    if not rate_hit('ip:' + (request.remote_addr or ''), 5, 900):
        return jsonify(error='Too many requests. Please try again in 15 minutes.'), 429
    if not rate_hit('email:' + email, 1, 900) or not rate_hit('confirmations', 20, 86400):
        return jsonify(ok=True)
    sub = Subscriber.query.filter_by(email=email).with_for_update().first()
    if (sub and sub.state == 'suppressed') or (kind == 'access' and (not sub or sub.state != 'active')):
        return jsonify(ok=True)
    if not sub:
        sub = Subscriber(email=email, created_at=now(), updated_at=now())
        db.session.add(sub)
        try:
            db.session.flush()
        except IntegrityError:
            db.session.rollback()
            return jsonify(ok=True)
    token = secrets.token_urlsafe(32)
    record = ConfirmRequest(token_hash=hashlib.sha256(token.encode()).hexdigest(), subscriber_id=sub.id,
                            version=sub.version, kind=kind, tickers_json=json.dumps(selected),
                            created_at=now(), expires_at=now() + dt.timedelta(hours=24))
    db.session.add(record)
    db.session.flush()
    payload = email_payload(sub, kind, confirm_url=BASE + '/alerts/confirm/' + token, selected=selected)
    db.session.add(Message(dedupe_key='request:' + record.id, subscriber_id=sub.id, version=sub.version,
                           kind=kind, payload_json=json.dumps(payload), created_at=now(), retry_at=now()))
    db.session.commit()
    _wake.set()
    return jsonify(ok=True)


@bp.route('/alerts/confirm/<token>', methods=['GET', 'POST'])
def confirm_alerts(token):
    record = ConfirmRequest.query.filter_by(token_hash=hashlib.sha256(token.encode()).hexdigest()).with_for_update().first()
    sub = db.session.get(Subscriber, record.subscriber_id) if record else None
    valid = record and sub and not record.used_at and record.expires_at > now() and record.version == sub.version and sub.state != 'suppressed'
    if not valid:
        return render_template('stock_alerts.html', mode='expired', csrf=csrf_token()), 400
    if request.method == 'POST':
        csrf_check()
        # Serialize confirmation, preference edits and unsubscribe against the same subscriber row.
        sub = Subscriber.query.filter_by(id=sub.id).with_for_update().populate_existing().one()
        if record.version != sub.version or sub.state == 'suppressed':
            abort(400)
        if record.kind == 'confirm':
            selected = json.loads(record.tickers_json)
            baseline(sub, set(selected) - set(json.loads(sub.tickers_json)))
            cancel_queued(sub)
            sub.tickers_json = record.tickers_json
            sub.state = 'active'
            sub.confirmed_at = now()
            sub.version += 1
            sub.updated_at = now()
        elif sub.state != 'active':
            abort(400)
        record.used_at = now()
        establish_session(sub)
        db.session.commit()
        return redirect('/alerts')
    return render_template('stock_alerts.html', mode='confirm', csrf=csrf_token(),
                           selected=json.loads(record.tickers_json), access=record.kind == 'access', email=masked_email(sub.email))


@bp.route('/alerts/access/<token>', methods=['GET', 'POST'])
def access_alerts(token):
    try:
        identity = signer('manage').loads(token, max_age=30 * 86400)
        sub = db.session.get(Subscriber, identity[0])
        if not sub or sub.state != 'active' or sub.version != identity[1]:
            raise BadSignature('Invalid session')
    except (BadSignature, SignatureExpired, TypeError, IndexError):
        return render_template('stock_alerts.html', mode='expired', csrf=csrf_token()), 400
    if request.method == 'POST':
        csrf_check()
        establish_session(sub)
        return redirect('/alerts')
    return render_template('stock_alerts.html', mode='confirm', access=True, selected=[], email=masked_email(sub.email), csrf=csrf_token())


@bp.route('/alerts/unsubscribe/<token>', methods=['GET', 'POST'])
def unsubscribe_alerts(token):
    try:
        sub_id = signer('unsubscribe').loads(token)
        sub = Subscriber.query.filter_by(id=sub_id).with_for_update().first()
        if not sub:
            raise BadSignature('Unknown subscription')
    except (BadSignature, TypeError):
        abort(400)
    if request.method == 'POST':
        # RFC 8058 mail-client POSTs do not carry a browser session or CSRF token.
        if sub.state not in ('unsubscribed', 'suppressed'):
            deactivate(sub)
            db.session.commit()
        session.pop('stock_alert_identity', None)
        return render_template('stock_alerts.html', mode='unsubscribed', csrf=csrf_token())
    return render_template('stock_alerts.html', mode='unsubscribe', csrf=csrf_token())


@bp.route('/alerts', methods=['GET'])
def portal():
    requested = requested_tickers()
    sub = current_subscriber()
    if sub:
        selected = json.loads(sub.tickers_json)
        mode = 'manage'
        suggested = [ticker for ticker in requested if ticker not in selected]
    elif requested and public_enabled():
        selected = requested
        mode = 'signup'
        suggested = []
    elif requested:
        selected = []
        mode = 'unavailable'
        suggested = []
    else:
        selected = []
        mode = 'login'
        suggested = []
    return render_template('stock_alerts.html', mode=mode, csrf=csrf_token(),
                           email=masked_email(sub.email) if sub else '', selected=selected,
                           suggested=suggested)


@bp.post('/api/alerts/preferences')
def save_preferences():
    csrf_check()
    sub = current_subscriber()
    if not sub:
        abort(401)
    sub = Subscriber.query.filter_by(id=sub.id).with_for_update().populate_existing().one()
    if sub.state != 'active' or sub.version != session['stock_alert_identity'][1]:
        abort(401)
    body = request.get_json(silent=True)
    if not isinstance(body, dict):
        abort(400)
    if body.get('action') == 'unsubscribe':
        deactivate(sub)
        session.pop('stock_alert_identity', None)
    else:
        try:
            selected = validate_tickers(body.get('tickers'))
        except ValueError as exc:
            return jsonify(error=str(exc)), 400
        baseline(sub, set(selected) - set(json.loads(sub.tickers_json)))
        cancel_queued(sub)
        sub.tickers_json = json.dumps(selected)
        sub.version += 1
        sub.updated_at = now()
        establish_session(sub)
    db.session.commit()
    return jsonify(ok=True)


@bp.post('/api/alerts/logout')
def logout_alerts():
    csrf_check()
    session.pop('stock_alert_identity', None)
    return jsonify(ok=True)


@bp.post('/api/alerts/webhook')
def webhook():
    from svix.webhooks import Webhook, WebhookVerificationError
    secret = current_app.config.get('RESEND_WEBHOOK_SECRET')
    if not secret:
        abort(503)
    if request.content_length and request.content_length > 65536:
        abort(413)
    try:
        raw = request.get_data()
        Webhook(secret).verify(raw, dict(request.headers))
        event = json.loads(raw)
    except (WebhookVerificationError, ValueError):
        abort(400)
    if not isinstance(event, dict):
        abort(400)
    details = event.get('data')
    provider_id = details.get('email_id') if isinstance(details, dict) else None
    if not isinstance(provider_id, str) or not provider_id:
        return jsonify(ok=True)
    message = Message.query.filter_by(provider_id=provider_id).first()
    if message and event.get('type') in ('email.bounced', 'email.complained', 'email.suppressed'):
        sub = Subscriber.query.filter_by(id=message.subscriber_id).with_for_update().first()
        if sub and sub.state != 'suppressed':
            deactivate(sub, 'suppressed')
        message.outcome = event['type']
        db.session.commit()
    elif message and event.get('type') == 'email.delivered' and message.outcome not in ('email.bounced', 'email.complained', 'email.suppressed'):
        message.outcome = 'delivered'
        db.session.commit()
    return jsonify(ok=True)


def queue_digests(at=None):
    at = at or now()
    local = at.replace(tzinfo=dt.timezone.utc).astimezone(ZoneInfo('America/Chicago'))
    if (local.hour, local.minute) < (21, 10):
        return 0
    reports = editions()
    cutoff, today = (at - dt.timedelta(days=7)).date().isoformat(), at.date().isoformat()
    queued = 0
    for sub in Subscriber.query.filter_by(state='active').all():
        if not allowed_recipient(sub.email):
            continue
        key = f'digest:{sub.id}:{local.date().isoformat()}'
        if Message.query.filter_by(dedupe_key=key).first():
            continue
        selected, seen = set(json.loads(sub.tickers_json)), {row.edition_key for row in Seen.query.filter_by(subscriber_id=sub.id)}
        # A pending or uncertain send must never reappear tomorrow under a new key.
        for reserved in Message.query.filter(Message.subscriber_id == sub.id, Message.kind == 'digest', Message.state != 'cancelled'):
            seen.update(json.loads(reserved.editions_json))
        fresh = [entry for entry in reports.values() if entry['ticker'] in selected and entry['key'] not in seen and cutoff <= entry['date'] <= today]
        fresh.sort(key=lambda entry: (entry['date'], entry['ticker']))
        if fresh:
            fresh = fresh[:20]
            db.session.add(Message(dedupe_key=key, subscriber_id=sub.id, version=sub.version, kind='digest',
                                   payload_json=json.dumps(email_payload(sub, 'digest', entries=fresh)),
                                   editions_json=json.dumps([e['key'] for e in fresh]), created_at=now(), retry_at=now()))
            queued += 1
    db.session.commit()
    return queued


def take_lease(owner):
    if not db.session.get(Lease, 'sender'):
        db.session.add(Lease(id='sender', owner='', expires_at=now()))
        try:
            db.session.commit()
        except IntegrityError:
            db.session.rollback()
    changed = Lease.query.filter_by(id='sender').filter(or_(Lease.expires_at <= now(), Lease.owner == owner)).update(
        {'owner': owner, 'expires_at': now() + dt.timedelta(minutes=3)})
    db.session.commit()
    return bool(changed)


def send_pending(owner):
    day = now().replace(hour=0, minute=0, second=0, microsecond=0)
    month = day.replace(day=1)
    daily = Message.query.filter(Message.first_attempt_at >= day).count()
    monthly = Message.query.filter(Message.first_attempt_at >= month).count()
    rows = Message.query.filter(Message.state.in_(PENDING), Message.retry_at <= now()).order_by(Message.created_at).limit(10).all()
    for message in rows:
        if not take_lease(owner):
            break
        sub = db.session.get(Subscriber, message.subscriber_id)
        if not sub or sub.version != message.version or sub.state == 'suppressed' or (message.kind in ('digest', 'access') and sub.state != 'active'):
            message.state, message.payload_json = 'cancelled', '{}'
            db.session.commit()
            continue
        if not allowed_recipient(sub.email):
            continue
        if message.first_attempt_at and message.first_attempt_at < now() - dt.timedelta(hours=23):
            message.state, message.outcome = 'uncertain', 'retry_window_expired'
            db.session.commit()
            continue
        if not message.first_attempt_at:
            if daily >= current_app.config['STOCK_ALERTS_DAILY_LIMIT'] or monthly >= current_app.config['STOCK_ALERTS_MONTHLY_LIMIT']:
                break
            if message.kind != 'digest' and message.created_at < now() - dt.timedelta(hours=23):
                message.state, message.payload_json = 'cancelled', '{}'
                db.session.commit()
                continue
            message.first_attempt_at = now()
            daily += 1
            monthly += 1
        message.state = 'sending'
        message.attempts += 1
        message.retry_at = now() + dt.timedelta(minutes=min(30, 2 ** min(message.attempts, 5)))
        db.session.commit()
        try:
            response = requests.post('https://api.resend.com/emails',
                                     headers={'Authorization': 'Bearer ' + current_app.config['RESEND_API_KEY'],
                                              'Idempotency-Key': 'stock-alert/' + message.id},
                                     json=json.loads(message.payload_json), timeout=(3, 10))
            status = response.status_code
            body = response.json() if 200 <= status < 300 else {}
            if 200 <= status < 300 and isinstance(body.get('id'), str):
                message.state, message.provider_id, message.sent_at = 'sent', body['id'], now()
                message.outcome = 'accepted'
                if message.kind == 'digest':
                    mark_seen(sub, json.loads(message.editions_json))
                message.payload_json = '{}'
            elif status == 429 or status >= 500:
                message.state, message.outcome = 'queued', f'provider_{status}'
            else:
                message.state, message.outcome = 'failed', f'provider_{status}'
                message.payload_json = '{}'
        except (requests.RequestException, ValueError, TypeError):
            message.state, message.outcome = 'queued', 'uncertain_network_response'
        db.session.commit()


def run_cycle():
    if not configured():
        return
    owner = uuid.uuid4().hex
    if not take_lease(owner):
        return
    try:
        queue_digests()
        send_pending(owner)
        ConfirmRequest.query.filter(ConfirmRequest.expires_at < now() - dt.timedelta(days=7)).delete()
        Throttle.query.filter(Throttle.expires_at < now()).delete()
        # Retain delivery identities and opt-out state, not old email bodies/capability URLs.
        old = Message.query.filter(Message.created_at < now() - dt.timedelta(days=2))
        old.filter(Message.state.in_(PENDING)).update({'state': 'expired'})
        old.update({'payload_json': '{}'})
        db.session.commit()
    finally:
        db.session.rollback()
        Lease.query.filter_by(id='sender', owner=owner).update({'expires_at': now()})
        db.session.commit()


def start_worker(app):
    if not app.config.get('STOCK_ALERTS_WORKER'):
        return
    def loop():
        while True:
            with app.app_context():
                try:
                    run_cycle()
                except Exception:
                    db.session.rollback()
                    # Do not log tokens, addresses, payloads, provider responses or credentials.
                    app.logger.error('Stock alert cycle failed; queued work retained for retry.')
            _wake.wait(30)
            _wake.clear()
    threading.Thread(target=loop, name='stock-alert-delivery', daemon=True).start()


def init_alerts(app, provider):
    for key in ('RESEND_API_KEY', 'RESEND_WEBHOOK_SECRET', 'STOCK_ALERTS_POSTAL_ADDRESS', 'STOCK_ALERTS_TEST_EMAIL'):
        app.config[key] = os.environ.get(key, '').strip()
    for key in ('STOCK_ALERTS_ENABLED', 'STOCK_ALERTS_PUBLIC', 'STOCK_ALERTS_WORKER'):
        app.config[key] = os.environ.get(key) == '1'
    app.config['STOCK_ALERTS_DAILY_LIMIT'] = min(80, max(1, int(os.environ.get('STOCK_ALERTS_DAILY_LIMIT', '80'))))
    app.config['STOCK_ALERTS_MONTHLY_LIMIT'] = min(2500, max(1, int(os.environ.get('STOCK_ALERTS_MONTHLY_LIMIT', '2500'))))
    app.extensions['stock_alert_catalog'] = provider
    app.register_blueprint(bp)
    app.context_processor(lambda: {'stock_alerts_public': public_enabled()})
