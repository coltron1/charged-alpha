import os
import tempfile
import unittest


_test_database_dir = tempfile.TemporaryDirectory()
os.environ["DATABASE_URL"] = (
    "sqlite:///" + os.path.join(_test_database_dir.name, "newsletter-api.db")
)
os.environ["SITE_URL"] = "https://chargedalpha.com"
os.environ["ENABLE_PUBLIC_AUTH"] = "0"

from app import (  # noqa: E402
    NEWSLETTER_API_RATE_LIMIT,
    _newsletter_rate_hits,
    _newsletter_rate_lock,
    app,
)
from models import EmailSubscriber, User, db  # noqa: E402


ALLOWED_ORIGIN = "capacitor://localhost"


def app_request(email="learner@example.com", **overrides):
    body = {
        "email": email,
        "source": "app-ios",
        "platform": "ios",
        "app": "charged-alpha",
    }
    body.update(overrides)
    if "platform" in overrides and "source" not in overrides:
        body["source"] = f"app-{body['platform']}"
    return body


class NewsletterApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        app.config.update(TESTING=True)
        cls.client = app.test_client()

    @classmethod
    def tearDownClass(cls):
        with app.app_context():
            db.session.remove()
            db.drop_all()
        _test_database_dir.cleanup()

    def setUp(self):
        with app.app_context():
            EmailSubscriber.query.delete()
            User.query.delete()
            db.session.commit()
        with _newsletter_rate_lock:
            _newsletter_rate_hits.clear()

    def post(self, path, body, origin=ALLOWED_ORIGIN):
        headers = {"Origin": origin} if origin is not None else {}
        return self.client.post(path, json=body, headers=headers)

    def test_allowed_native_and_local_origins_receive_preflight_headers(self):
        for origin in (
            "capacitor://localhost",
            "https://localhost",
            "http://localhost",
            "http://127.0.0.1:5175",
            "https://chargedalpha.com",
            "https://www.chargedalpha.com",
        ):
            with self.subTest(origin=origin):
                response = self.client.options(
                    "/api/newsletter/subscribe",
                    headers={
                        "Origin": origin,
                        "Access-Control-Request-Method": "POST",
                        "Access-Control-Request-Headers": "Content-Type",
                    },
                )
                self.assertEqual(response.status_code, 204)
                self.assertEqual(response.headers["Access-Control-Allow-Origin"], origin)
                self.assertIn("POST", response.headers["Access-Control-Allow-Methods"])
                self.assertIn("Content-Type", response.headers["Access-Control-Allow-Headers"])
                self.assertEqual(response.headers["Cache-Control"], "no-store")
                self.assertIn("Origin", response.headers.get("Vary", ""))

    def test_unknown_or_missing_origin_is_rejected_without_cors_permission(self):
        for origin in ("https://example.com", None):
            with self.subTest(origin=origin):
                response = self.post(
                    "/api/newsletter/subscribe",
                    app_request(),
                    origin=origin,
                )
                self.assertEqual(response.status_code, 403)
                self.assertEqual(response.get_json(), {"ok": False, "error": "Invalid request"})
                self.assertNotIn("Access-Control-Allow-Origin", response.headers)

    def test_subscribe_normalizes_and_idempotently_reactivates_one_row(self):
        first = self.post(
            "/api/newsletter/subscribe",
            app_request("  Learner@Example.COM  ", platform="android"),
        )
        second = self.post(
            "/api/newsletter/subscribe",
            app_request("learner@example.com", platform="android"),
        )

        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 200)
        self.assertEqual(first.get_json(), {"ok": True})
        self.assertEqual(second.get_json(), {"ok": True})
        with app.app_context():
            rows = EmailSubscriber.query.all()
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0].email, "learner@example.com")
            self.assertTrue(rows[0].subscribed)
            self.assertEqual(rows[0].consent_source, "app-android")
            self.assertIsNotNone(rows[0].subscribed_at)
            self.assertIsNone(rows[0].unsubscribed_at)

    def test_unsubscribe_is_generic_and_idempotent_for_existing_or_new_address(self):
        self.post("/api/newsletter/subscribe", app_request("known@example.com"))

        known = self.post(
            "/api/newsletter/unsubscribe",
            app_request("known@example.com"),
        )
        unknown = self.post(
            "/api/newsletter/unsubscribe",
            app_request("unknown@example.com"),
        )
        unknown_again = self.post(
            "/api/newsletter/unsubscribe",
            app_request("unknown@example.com"),
        )

        self.assertEqual(known.get_json(), {"ok": True})
        self.assertEqual(unknown.get_json(), {"ok": True})
        self.assertEqual(unknown_again.get_json(), {"ok": True})
        with app.app_context():
            rows = {
                row.email: row
                for row in EmailSubscriber.query.order_by(EmailSubscriber.email).all()
            }
            self.assertEqual(set(rows), {"known@example.com", "unknown@example.com"})
            self.assertFalse(rows["known@example.com"].subscribed)
            self.assertFalse(rows["unknown@example.com"].subscribed)
            self.assertIsNotNone(rows["known@example.com"].unsubscribed_at)
            self.assertIsNotNone(rows["unknown@example.com"].unsubscribed_at)
            self.assertIsNone(rows["unknown@example.com"].subscribed_at)

    def test_anonymous_app_request_preserves_existing_account_linkage(self):
        with app.app_context():
            user = User(email="account@example.com", name="Alex", provider="local")
            db.session.add(user)
            db.session.flush()
            subscriber = EmailSubscriber(
                user_id=user.id,
                email=user.email,
                name="Alex",
                subscribed=False,
                consent_source="account",
            )
            db.session.add(subscriber)
            db.session.commit()
            user_id = user.id

        response = self.post(
            "/api/newsletter/subscribe",
            app_request("ACCOUNT@example.com"),
        )

        self.assertEqual(response.get_json(), {"ok": True})
        with app.app_context():
            subscriber = EmailSubscriber.query.one()
            self.assertEqual(subscriber.user_id, user_id)
            self.assertEqual(subscriber.name, "Alex")
            self.assertTrue(subscriber.subscribed)
            self.assertEqual(subscriber.consent_source, "app-ios")

    def test_invalid_email_or_app_metadata_is_rejected_without_writing(self):
        invalid_bodies = (
            app_request("not-an-email"),
            app_request("a" * 245 + "@example.com"),
            app_request(source="website"),
            app_request(source="app-android"),
            app_request(platform="windows"),
            app_request(platform=["ios"]),
            app_request(app="another-app"),
        )
        for body in invalid_bodies:
            with self.subTest(body=body):
                response = self.post("/api/newsletter/subscribe", body)
                self.assertEqual(response.status_code, 400)
                self.assertEqual(response.get_json(), {"ok": False, "error": "Invalid request"})

        with app.app_context():
            self.assertEqual(EmailSubscriber.query.count(), 0)

    def test_non_json_and_oversized_requests_are_rejected(self):
        non_json = self.client.post(
            "/api/newsletter/subscribe",
            data="email=learner@example.com",
            headers={"Origin": ALLOWED_ORIGIN},
        )
        oversized = self.post(
            "/api/newsletter/subscribe",
            {**app_request(), "padding": "x" * 4096},
        )

        self.assertEqual(non_json.status_code, 400)
        self.assertEqual(oversized.status_code, 400)
        self.assertEqual(non_json.get_json(), {"ok": False, "error": "Invalid request"})
        self.assertEqual(oversized.get_json(), {"ok": False, "error": "Invalid request"})

    def test_honeypot_drops_request_with_generic_success(self):
        response = self.post(
            "/api/newsletter/subscribe",
            app_request("bot@example.com", website="https://spam.example"),
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json(), {"ok": True})
        with app.app_context():
            self.assertEqual(EmailSubscriber.query.count(), 0)

    def test_rate_limit_uses_generic_error_and_does_not_add_rows(self):
        for index in range(NEWSLETTER_API_RATE_LIMIT):
            response = self.post(
                "/api/newsletter/subscribe",
                app_request(f"learner-{index}@example.com"),
            )
            self.assertEqual(response.status_code, 200)

        limited = self.post(
            "/api/newsletter/subscribe",
            app_request("one-too-many@example.com"),
        )
        self.assertEqual(limited.status_code, 429)
        self.assertEqual(limited.get_json(), {"ok": False, "error": "Try again later"})
        with app.app_context():
            self.assertEqual(EmailSubscriber.query.count(), NEWSLETTER_API_RATE_LIMIT)

    def test_subscribe_rate_limit_cannot_block_an_unsubscribe(self):
        for index in range(NEWSLETTER_API_RATE_LIMIT):
            response = self.post(
                "/api/newsletter/subscribe",
                app_request(f"learner-{index}@example.com"),
            )
            self.assertEqual(response.status_code, 200)

        unsubscribe = self.post(
            "/api/newsletter/unsubscribe",
            app_request("stop@example.com"),
        )
        self.assertEqual(unsubscribe.status_code, 200)
        self.assertEqual(unsubscribe.get_json(), {"ok": True})


if __name__ == "__main__":
    unittest.main()
