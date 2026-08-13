import os
import unittest
from unittest.mock import patch


from app import DEFAULT_DEV_SECRET_KEY, _resolve_secret_key


class ProductionConfigurationTests(unittest.TestCase):
    def test_local_development_keeps_the_explicit_dev_fallback(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(_resolve_secret_key(), DEFAULT_DEV_SECRET_KEY)

    def test_current_railway_runtime_refuses_a_missing_secret(self):
        with patch.dict(
            os.environ,
            {"RAILWAY_ENVIRONMENT_ID": "production-environment"},
            clear=True,
        ):
            with self.assertRaisesRegex(RuntimeError, "SECRET_KEY must be set"):
                _resolve_secret_key()

    def test_production_refuses_the_known_dev_secret_even_with_whitespace(self):
        with patch.dict(
            os.environ,
            {
                "FLASK_ENV": "production",
                "SECRET_KEY": f"  {DEFAULT_DEV_SECRET_KEY}  ",
            },
            clear=True,
        ):
            with self.assertRaisesRegex(RuntimeError, "SECRET_KEY must be set"):
                _resolve_secret_key()

    def test_railway_accepts_a_non_default_secret_without_rewriting_it(self):
        configured = "  production-secret-with-intentional-spaces  "
        with patch.dict(
            os.environ,
            {
                "RAILWAY_DEPLOYMENT_ID": "deployment-id",
                "SECRET_KEY": configured,
            },
            clear=True,
        ):
            self.assertEqual(_resolve_secret_key(), configured)


if __name__ == "__main__":
    unittest.main()
