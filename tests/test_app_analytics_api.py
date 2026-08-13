import datetime
import os
import tempfile
import unittest
import uuid


_test_database_dir = tempfile.TemporaryDirectory()
os.environ["DATABASE_URL"] = (
    "sqlite:///" + os.path.join(_test_database_dir.name, "app-analytics-api.db")
)
os.environ["SITE_URL"] = "https://chargedalpha.com"
os.environ["ENABLE_PUBLIC_AUTH"] = "0"

from app import (  # noqa: E402
    _app_analytics_rate_hits,
    _app_analytics_rate_lock,
    app,
)
from models import AppAnalyticsEvent, db  # noqa: E402


ALLOWED_ORIGIN = "capacitor://localhost"


def analytics_event(**overrides):
    event = {
        "event_id": str(uuid.uuid4()),
        "install_id": str(uuid.uuid4()),
        "session_id": str(uuid.uuid4()),
        "name": "paywall_view",
        "app": "charged-alpha",
        "platform": "ios",
        "app_version": "1.0.3",
        "app_build": "2026081301",
        "occurred_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "schema_version": 1,
        "properties": {
            "source": "map",
            "page_id": "2.1",
        },
    }
    event.update(overrides)
    return event


class AppAnalyticsApiTests(unittest.TestCase):
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
            AppAnalyticsEvent.query.delete()
            db.session.commit()
        with _app_analytics_rate_lock:
            _app_analytics_rate_hits.clear()

    def post(self, events, origin=ALLOWED_ORIGIN):
        headers = {"Origin": origin} if origin is not None else {}
        return self.client.post(
            "/api/app-analytics/events",
            json={"events": events},
            headers=headers,
        )

    def test_native_preflight_and_valid_batch(self):
        preflight = self.client.options(
            "/api/app-analytics/events",
            headers={
                "Origin": ALLOWED_ORIGIN,
                "Access-Control-Request-Method": "POST",
                "Access-Control-Request-Headers": "Content-Type",
            },
        )
        self.assertEqual(preflight.status_code, 204)
        self.assertEqual(preflight.headers["Access-Control-Allow-Origin"], ALLOWED_ORIGIN)
        self.assertEqual(preflight.headers["Cache-Control"], "no-store")

        event = analytics_event(
            name="store_result",
            platform="android",
            properties={
                "source": "settings",
                "product_id": "com.chargedalpha.academy.premium.annual",
                "package_type": "annual",
                "result": "success",
                "duration_ms": 2345,
                "has_entitlement": True,
                "trial_eligible": True,
                "trial_days": 7,
            },
        )
        response = self.post([event])

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json(), {"ok": True, "accepted": 1, "duplicates": 0})
        with app.app_context():
            row = AppAnalyticsEvent.query.one()
            self.assertEqual(row.event_id, event["event_id"])
            self.assertEqual(row.install_id, event["install_id"])
            self.assertEqual(row.event_name, "store_result")
            self.assertEqual(row.platform, "android")
            self.assertNotIn("remote", row.__table__.columns)
            self.assertNotIn("user_agent", row.__table__.columns)

    def test_retry_and_same_batch_duplicates_are_idempotent(self):
        event = analytics_event()
        first = self.post([event, event])
        retry = self.post([event])

        self.assertEqual(first.get_json(), {"ok": True, "accepted": 1, "duplicates": 1})
        self.assertEqual(retry.get_json(), {"ok": True, "accepted": 0, "duplicates": 1})
        with app.app_context():
            self.assertEqual(AppAnalyticsEvent.query.count(), 1)

    def test_unknown_or_personal_properties_are_rejected_without_writing(self):
        invalid_properties = (
            {"email": "learner@example.com"},
            {"query": "how much should I invest"},
            {"portfolio_value": 100000},
            {"page_id": "raw words are not a page"},
            {"error_code": "Store failed for learner@example.com"},
            {"error_code": "learner-example.com"},
        )
        for properties in invalid_properties:
            with self.subTest(properties=properties):
                response = self.post([analytics_event(properties=properties)])
                self.assertEqual(response.status_code, 400)
                self.assertEqual(response.get_json(), {"ok": False, "error": "Invalid request"})
        with app.app_context():
            self.assertEqual(AppAnalyticsEvent.query.count(), 0)

    def test_invalid_metadata_timestamp_schema_and_extra_fields_are_rejected(self):
        stale = (
            datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=32)
        ).isoformat()
        cases = (
            analytics_event(occurred_at=stale),
            analytics_event(app_version="version with spaces"),
            analytics_event(schema_version=2),
            analytics_event(name="typed_search"),
            {**analytics_event(), "email": "learner@example.com"},
        )
        for event in cases:
            with self.subTest(event=event):
                self.assertEqual(self.post([event]).status_code, 400)
        with app.app_context():
            self.assertEqual(AppAnalyticsEvent.query.count(), 0)

    def test_unknown_origin_non_json_and_empty_or_oversized_batches_are_rejected(self):
        self.assertEqual(self.post([analytics_event()], origin="https://example.com").status_code, 403)
        self.assertEqual(self.post([], origin=ALLOWED_ORIGIN).status_code, 400)
        self.assertEqual(
            self.post([analytics_event() for _ in range(26)], origin=ALLOWED_ORIGIN).status_code,
            400,
        )
        non_json = self.client.post(
            "/api/app-analytics/events",
            data="events=none",
            headers={"Origin": ALLOWED_ORIGIN},
        )
        self.assertEqual(non_json.status_code, 400)


if __name__ == "__main__":
    unittest.main()
