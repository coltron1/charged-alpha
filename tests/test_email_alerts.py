import base64
import copy
import datetime as dt
import json
import os
import re
import unittest
from unittest.mock import Mock, patch

os.environ['DATABASE_URL'] = 'sqlite:///:memory:'
os.environ['STOCK_ALERTS_WORKER'] = '0'
import app as site
import email_alerts as alerts
from email_alert_models import (db, StockAlertSubscriber as Subscriber, StockAlertRequest as Request,
                                StockAlertMessage as Message, StockAlertSeen as Seen, StockAlertEvent as Event)
from svix.webhooks import Webhook

FIXED = dt.datetime(2026, 9, 11, 3, 0)
SECRET = 'whsec_' + base64.b64encode(b'a-test-secret-with-no-production-use').decode()


class StockAlertsTests(unittest.TestCase):
    def setUp(self):
        site.app.config.update(TESTING=True, STOCK_ALERTS_ENABLED=True, STOCK_ALERTS_PUBLIC=True,
                               STOCK_ALERTS_WORKER=False, RESEND_API_KEY='not-a-real-key',
                               RESEND_WEBHOOK_SECRET=SECRET, STOCK_ALERTS_POSTAL_ADDRESS='Test Business Address',
                               STOCK_ALERTS_DAILY_LIMIT=80, STOCK_ALERTS_MONTHLY_LIMIT=2500,
                               STOCK_ALERTS_ADMIN_TOKEN='test-admin-token-1234567890-long')
        self.context = site.app.app_context()
        self.context.push()
        db.drop_all()
        db.create_all()
        self.data = {'show_library': {'stocks': [
            {'ticker': ticker, 'slug': ticker, 'company': ticker + ' Company', 'episodes': [
                {'quarter': 'Q1 FY2026', 'title': ticker + ' Q1', 'published_at': '2026-05-01',
                 'youtube_url': 'https://www.youtube.com/watch?v=old' + ticker}
            ]} for ticker in ('ADBE', 'AVAV')]}, 'research_packets': []}
        site.app.extensions['stock_alert_catalog'] = lambda: self.data
        self.clock = patch('email_alerts.now', return_value=FIXED)
        self.clock.start()
        self.client = site.app.test_client()
        self.csrf = self.client.get('/api/alerts/preferences').json['csrf']

    def tearDown(self):
        db.session.remove()
        self.context.pop()
        self.clock.stop()

    def post(self, path, data, client=None):
        return (client or self.client).post(path, json=data, headers={'X-CSRF-Token': self.csrf, 'Origin': alerts.BASE})

    def signup(self, email='reader@example.com', stocks=None, source='direct'):
        return self.post('/api/alerts/request', {
            'email': email,
            'tickers': stocks or ['ADBE'],
            'consent': True,
            'source': source,
        })

    def token(self):
        payload = json.loads(Message.query.filter_by(kind='confirm').order_by(Message.created_at.desc()).first().payload_json)
        return re.search(r'/alerts/confirm/([^\s<"]+)', payload['text']).group(1)

    def active(self, source='direct'):
        self.assertEqual(self.signup(source=source).status_code, 200)
        token = self.token()
        response = self.client.post('/alerts/confirm/' + token, data={'csrf': self.csrf})
        self.assertEqual(response.status_code, 302)
        return Subscriber.query.one()

    def admin_csrf(self, client=None):
        response = (client or self.client).get('/alerts/admin')
        match = re.search(r'name="csrf" value="([^"]+)"', response.text)
        self.assertIsNotNone(match)
        return match.group(1)

    def admin_login(self, client=None, token=None):
        client = client or self.client
        return client.post('/alerts/admin', data={
            'csrf': self.admin_csrf(client),
            'token': token or site.app.config['STOCK_ALERTS_ADMIN_TOKEN'],
        })

    def add_report(self):
        self.data['show_library']['stocks'][0]['episodes'].insert(0, {
            'quarter': 'Q2 FY2026', 'title': 'ADBE: What changed', 'published_at': '2026-09-10',
            'youtube_url': 'https://www.youtube.com/watch?v=new', 'spotify_url': 'https://open.spotify.com/episode/test'})

    def successful_provider(self):
        return patch('email_alerts.requests.post', side_effect=lambda *a, **kw: Mock(status_code=200, json=lambda: {'id': kw['headers']['Idempotency-Key']}))

    def test_rollout_gates_and_private_pages(self):
        site.app.config['STOCK_ALERTS_PUBLIC'] = False
        self.assertNotIn('id="stockAlertSignup"', self.client.get('/following').text)
        self.assertEqual(self.signup().status_code, 503)
        site.app.config['STOCK_ALERTS_TEST_EMAIL'] = 'reader@example.com'
        self.assertEqual(self.signup().status_code, 200)
        for path in ('/alerts', '/api/alerts/preferences'):
            response = self.client.get(path)
            self.assertEqual(response.headers['Cache-Control'], 'no-store')
            self.assertEqual(response.headers['Referrer-Policy'], 'no-referrer')
            self.assertIn('noindex', response.headers['X-Robots-Tag'])
            self.assertNotIn('googletagmanager.com', response.text)

    def test_follow_link_preselects_a_confirmed_email_signup(self):
        response = self.client.get('/alerts?ticker=ADBE&source=stock_follow')
        self.assertEqual(response.status_code, 200)
        self.assertIn('Get updates for ADBE', response.text)
        self.assertIn('id="stockAlertSignup" data-alert-selection="requested"', response.text)
        self.assertIn('data-alert-source="stock_follow"', response.text)
        self.assertIn('Selected email alert: ADBE.', response.text)
        self.assertEqual(response.headers['Cache-Control'], 'no-store')
        self.assertIn('noindex', response.headers['X-Robots-Tag'])
        self.assertNotIn('googletagmanager.com', response.text)

        invalid = self.client.get('/alerts?ticker=NOTREAL')
        self.assertIn('Manage your email alerts.', invalid.text)
        self.assertNotIn('data-alert-selection="requested"', invalid.text)

        sub = self.active()
        managed = self.client.get('/alerts?ticker=AVAV&source=stock_follow')
        self.assertIn('data-alert-add-stock="AVAV"', managed.text)
        self.assertIn('data-alert-add-source="stock_follow"', managed.text)
        self.assertEqual(json.loads(sub.tickers_json), ['ADBE'])

        site.app.config['STOCK_ALERTS_PUBLIC'] = False
        unavailable = site.app.test_client().get('/alerts?ticker=ADBE')
        self.assertIn('Email alerts are not available yet.', unavailable.text)
        self.assertNotIn('id="stockAlertSignup"', unavailable.text)

    def test_disabled_delivery_does_nothing(self):
        site.app.config['RESEND_WEBHOOK_SECRET'] = ''
        self.assertEqual(self.signup().status_code, 503)
        with patch('email_alerts.requests.post') as provider:
            alerts.run_cycle()
            provider.assert_not_called()

    def test_csrf_and_foreign_origins_rejected(self):
        self.assertEqual(self.client.post('/api/alerts/request', json={}).status_code, 403)
        self.assertEqual(self.client.post('/api/alerts/request', json={}, headers={'X-CSRF-Token': self.csrf, 'Origin': 'https://evil.test'}).status_code, 403)

    def test_native_confirmation_form_allows_no_referrer_origin_with_csrf(self):
        self.signup()
        response = self.client.post('/alerts/confirm/' + self.token(), data={'csrf': self.csrf}, headers={'Origin': 'null'})
        self.assertEqual(response.status_code, 302)
        self.assertEqual(Subscriber.query.one().state, 'active')

    def test_requires_explicit_consent_and_valid_catalog_stocks(self):
        for data in ({'email':'reader@example.com','tickers':['ADBE']}, {'email':'reader@example.com','tickers':['NOTREAL'],'consent':True}, {'email':'bad\r\nemail','tickers':['ADBE'],'consent':True}, {'email':'reader@example.com','tickers':[],'consent':True}):
            self.assertEqual(self.post('/api/alerts/request', data).status_code, 400)
        self.assertEqual(Subscriber.query.count(), 0)

    def test_signup_never_activates_until_explicit_post(self):
        self.signup()
        token = self.token()
        sub = Subscriber.query.one()
        self.assertEqual(sub.state, 'pending')
        self.assertEqual(self.client.get('/alerts/confirm/' + token).status_code, 200)
        self.assertEqual(sub.state, 'pending')
        self.assertNotIn(token, Request.query.one().token_hash)
        self.assertEqual(self.client.post('/alerts/confirm/' + token, data={'csrf': self.csrf}).status_code, 302)
        self.assertEqual(sub.state, 'active')
        self.assertEqual(self.client.post('/alerts/confirm/' + token, data={'csrf': self.csrf}).status_code, 400)

    def test_records_signup_and_preference_attribution(self):
        self.assertEqual(self.signup(source='stock_follow').status_code, 200)
        request_record = Request.query.one()
        self.assertEqual(Event.query.filter_by(event_type='signup_requested').count(), 1)
        requested = Event.query.filter_by(event_type='signup_requested').one()
        self.assertEqual((requested.request_id, requested.ticker, requested.source),
                         (request_record.id, 'ADBE', 'stock_follow'))
        self.assertEqual(self.client.post('/alerts/confirm/' + self.token(), data={'csrf': self.csrf}).status_code, 302)
        confirmed = Event.query.filter_by(event_type='signup_confirmed').one()
        self.assertEqual((confirmed.request_id, confirmed.ticker, confirmed.source),
                         (request_record.id, 'ADBE', 'stock_follow'))
        self.assertEqual(self.post('/api/alerts/preferences', {
            'tickers': ['ADBE', 'AVAV'],
            'ticker_sources': {'AVAV': 'stock_follow'},
        }).status_code, 200)
        added = Event.query.filter_by(event_type='preference_added', ticker='AVAV').one()
        self.assertEqual(added.source, 'stock_follow')

    def test_admin_dashboard_is_private_and_csv_requires_authenticated_session(self):
        self.active(source='stock_follow')
        locked = self.client.get('/alerts/admin')
        self.assertEqual(locked.status_code, 200)
        self.assertIn('Dashboard passphrase', locked.text)
        self.assertNotIn('reader@example.com', locked.text)
        rejected = self.admin_login(token='wrong-admin-token-1234567890')
        self.assertEqual(rejected.status_code, 401)
        self.assertEqual(self.admin_login().status_code, 302)
        dashboard = self.client.get('/alerts/admin')
        self.assertEqual(dashboard.status_code, 200)
        self.assertIn('Confirmed subscribers', dashboard.text)
        self.assertIn('ADBE', dashboard.text)
        self.assertIn('re***@example.com', dashboard.text)
        self.assertNotIn('reader@example.com', dashboard.text)
        self.assertEqual(dashboard.headers['Cache-Control'], 'no-store')
        self.assertEqual(dashboard.headers['X-Frame-Options'], 'DENY')
        self.assertIn('frame-ancestors', dashboard.headers['Content-Security-Policy'])
        self.assertNotIn('googletagmanager.com', dashboard.text)
        csv_response = self.client.post('/alerts/admin/export.csv', data={'csrf': self.admin_csrf()})
        self.assertEqual(csv_response.status_code, 200)
        self.assertIn('text/csv', csv_response.content_type)
        self.assertIn('reader@example.com', csv_response.text)
        self.assertIn('stock_follow', csv_response.text)
        self.assertEqual(self.client.post('/alerts/admin/signout', data={'csrf': self.admin_csrf()}).status_code, 302)
        self.assertIn('Dashboard passphrase', self.client.get('/alerts/admin').text)

    def test_admin_dashboard_is_unavailable_without_a_configured_secret(self):
        site.app.config['STOCK_ALERTS_ADMIN_TOKEN'] = ''
        self.assertEqual(self.client.get('/alerts/admin').status_code, 404)
        self.assertEqual(self.client.post('/alerts/admin/export.csv').status_code, 404)

    def test_expired_and_tampered_confirmation_links_fail(self):
        self.signup()
        token = self.token()
        self.assertEqual(self.client.get('/alerts/confirm/' + token + 'bad').status_code, 400)
        Request.query.one().expires_at = FIXED - dt.timedelta(seconds=1)
        db.session.commit()
        self.assertEqual(self.client.get('/alerts/confirm/' + token).status_code, 400)

    def test_baseline_does_not_send_historical_reports(self):
        sub = self.active()
        self.assertEqual(Seen.query.filter_by(subscriber_id=sub.id).count(), 1)
        self.assertEqual(alerts.queue_digests(), 0)
        self.add_report()
        self.assertEqual(alerts.queue_digests(), 1)
        self.assertEqual(alerts.queue_digests(), 0)
        payload = json.loads(Message.query.filter_by(kind='digest').one().payload_json)
        self.assertIn('What changed', payload['text'])
        self.assertIn('Spotify', payload['text'])
        self.assertIn('List-Unsubscribe-Post', payload['headers'])

    def test_separate_platform_editions_are_one_earnings_alert(self):
        sub = self.active()
        self.add_report()
        episode = copy.deepcopy(self.data['show_library']['stocks'][0]['episodes'][0])
        episode['youtube_url'] = 'https://www.youtube.com/watch?v=studio'
        episode['studio_primary_youtube_url'] = 'https://www.youtube.com/watch?v=new'
        self.data['show_library']['stocks'][0]['episodes'].append(episode)
        self.assertEqual(len(alerts.editions()), 3)
        with self.successful_provider():
            alerts.run_cycle()
        digest = Message.query.filter_by(kind='digest').one()
        self.assertEqual(digest.state, 'sent')
        self.assertEqual(len(json.loads(digest.editions_json)), 1)
        with patch('email_alerts.now', return_value=FIXED + dt.timedelta(days=1)):
            self.assertEqual(alerts.queue_digests(), 0)

    def test_no_digest_before_daily_window(self):
        self.active()
        self.add_report()
        self.assertEqual(alerts.queue_digests(FIXED.replace(hour=15)), 0)

    def test_pending_or_uncertain_digest_never_gets_a_new_daily_key(self):
        self.active()
        self.add_report()
        self.assertEqual(alerts.queue_digests(), 1)
        digest = Message.query.filter_by(kind='digest').one()
        for state in ('queued', 'sending', 'uncertain', 'failed'):
            digest.state = state
            db.session.commit()
            with patch('email_alerts.now', return_value=FIXED + dt.timedelta(days=1)):
                self.assertEqual(alerts.queue_digests(), 0)

    def test_unknown_dates_future_dates_and_old_backfill_skipped(self):
        self.active()
        for date in ('unknown', '2099-01-01', '2020-01-01'):
            self.add_report()
            self.data['show_library']['stocks'][0]['episodes'][0]['published_at'] = date
            self.assertEqual(alerts.queue_digests(), 0)
            self.data['show_library']['stocks'][0]['episodes'].pop(0)

    def test_confirmation_messages_and_digests_use_same_durable_sender(self):
        self.signup()
        with self.successful_provider() as provider:
            alerts.run_cycle()
        message = Message.query.one()
        self.assertEqual(message.state, 'sent')
        self.assertEqual(message.payload_json, '{}')
        self.assertEqual(provider.call_args.kwargs['headers']['Idempotency-Key'], 'stock-alert/' + message.id)

    def test_rate_limits_do_not_modify_existing_preferences(self):
        sub = self.active()
        for _ in range(3):
            self.signup(stocks=['AVAV'])
        self.assertEqual(Request.query.count(), 1)
        self.assertEqual(json.loads(sub.tickers_json), ['ADBE'])

    def test_preference_edit_baselines_added_stocks(self):
        sub = self.active()
        self.add_report()
        alerts.queue_digests()
        self.assertEqual(self.post('/api/alerts/preferences', {'tickers':['AVAV']}).status_code, 200)
        self.assertEqual(Message.query.filter_by(kind='digest').one().state, 'cancelled')
        self.assertEqual(json.loads(sub.tickers_json), ['AVAV'])
        self.assertEqual(alerts.queue_digests(), 0)
        self.assertEqual(self.post('/api/alerts/preferences', []).status_code, 400)

    def test_unsubscribe_prefetch_safe_and_one_click_without_cookie(self):
        sub = self.active()
        self.add_report()
        alerts.queue_digests()
        path = alerts.unsubscribe_url(sub).replace(alerts.BASE, '')
        stranger = site.app.test_client()
        self.assertEqual(stranger.get(path).status_code, 200)
        self.assertEqual(sub.state, 'active')
        self.assertEqual(stranger.post(path, data={'List-Unsubscribe':'One-Click'}).status_code, 200)
        self.assertEqual(sub.state, 'unsubscribed')
        self.assertEqual(Message.query.filter_by(kind='digest').one().state, 'cancelled')
        with self.successful_provider() as provider:
            alerts.run_cycle()
            provider.assert_not_called()

    def test_resubscription_requires_a_new_confirmation(self):
        sub = self.active()
        self.client.post(alerts.unsubscribe_url(sub).replace(alerts.BASE, ''))
        with patch('email_alerts.now', return_value=FIXED + dt.timedelta(minutes=16)):
            self.assertEqual(self.signup(stocks=['AVAV']).status_code, 200)
            self.assertEqual(sub.state, 'unsubscribed')
            with self.successful_provider() as provider:
                alerts.run_cycle()
                provider.assert_called_once()

    def test_stale_confirmation_cannot_undo_an_unsubscribe(self):
        sub = self.active()
        with patch('email_alerts.now', return_value=FIXED + dt.timedelta(minutes=16)):
            self.signup(stocks=['AVAV'])
        message = Message.query.filter_by(state='queued').one()
        token = re.search(r'/alerts/confirm/([^\s<"]+)', json.loads(message.payload_json)['text']).group(1)
        self.client.post(alerts.unsubscribe_url(sub).replace(alerts.BASE, ''))
        self.assertEqual(self.client.post('/alerts/confirm/' + token, data={'csrf': self.csrf}).status_code, 400)

    def test_manage_link_is_scoped_and_revoked_on_change(self):
        sub = self.active()
        link = alerts.manage_url(sub).replace(alerts.BASE, '')
        other = site.app.test_client()
        csrf = other.get('/api/alerts/preferences').json['csrf']
        self.assertEqual(other.get(link).status_code, 200)
        self.assertIsNone(other.get('/api/alerts/preferences').json['email'])
        self.assertEqual(other.post(link, data={'csrf':csrf}).status_code, 302)
        self.assertTrue(other.get('/api/alerts/preferences').json['email'])
        self.post('/api/alerts/preferences', {'tickers':['AVAV']})
        self.assertEqual(other.get(link).status_code, 400)
        self.assertIsNone(other.get('/api/alerts/preferences').json['email'])

    def test_network_uncertainty_reuses_payload_and_idempotency_key(self):
        self.signup()
        with patch('email_alerts.requests.post', side_effect=alerts.requests.Timeout) as provider:
            alerts.run_cycle()
        message = Message.query.one()
        payload = message.payload_json
        message.retry_at = FIXED
        db.session.commit()
        with self.successful_provider() as retry:
            alerts.run_cycle()
        self.assertEqual(retry.call_args.kwargs['json'], json.loads(payload))
        self.assertEqual(retry.call_args.kwargs['headers']['Idempotency-Key'], provider.call_args.kwargs['headers']['Idempotency-Key'])
        self.assertEqual(message.state, 'sent')

    def test_never_retries_after_provider_deduplication_window(self):
        self.signup()
        message = Message.query.one()
        message.first_attempt_at = FIXED - dt.timedelta(hours=24)
        db.session.commit()
        with self.successful_provider() as provider:
            alerts.run_cycle()
            provider.assert_not_called()
        self.assertEqual(message.state, 'uncertain')

    def test_quota_and_worker_lease_prevent_unbounded_sending(self):
        self.signup()
        site.app.config['STOCK_ALERTS_DAILY_LIMIT'] = 0
        with self.successful_provider() as provider:
            alerts.run_cycle()
            provider.assert_not_called()
        self.assertTrue(alerts.take_lease('first'))
        self.assertFalse(alerts.take_lease('second'))

    def test_verified_bounce_suppresses_future_mail(self):
        sub = self.active()
        self.add_report()
        with self.successful_provider():
            alerts.run_cycle()
        message = Message.query.filter_by(kind='digest').one()
        data = json.dumps({'type':'email.bounced','data':{'email_id':message.provider_id}})
        stamp = dt.datetime.now(dt.timezone.utc)
        signature = Webhook(SECRET).sign('evt_test', stamp, data)
        headers = {'svix-id':'evt_test','svix-timestamp':str(int(stamp.timestamp())),'svix-signature':signature,'Content-Type':'application/json'}
        self.assertEqual(self.client.post('/api/alerts/webhook', data=data, headers=headers).status_code, 200)
        self.assertEqual(sub.state, 'suppressed')
        self.assertEqual(self.client.post('/api/alerts/webhook', data=data, headers=headers).status_code, 200)
        self.assertEqual(self.client.post('/api/alerts/webhook', data=data + ' ', headers=headers).status_code, 400)

    def test_external_catalog_text_is_escaped_in_email(self):
        sub = self.active()
        payload = alerts.email_payload(sub, 'digest', entries=[{'ticker':'ADBE','period':'Q2','date':'2026-09-10','title':'<script>bad()</script>','url':alerts.BASE+'/shows/ADBE','links':[]}])
        self.assertNotIn('<script>', payload['html'])
        self.assertIn('&lt;script&gt;', payload['html'])
        self.assertFalse(alerts.safe_link('javascript:alert(1)'))
        self.assertFalse(alerts.safe_link('https://chargedalpha.com.evil.test/'))


if __name__ == '__main__':
    unittest.main()
