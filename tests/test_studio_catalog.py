import unittest

from app import _load_studio_catalog, app


class StudioCatalogTests(unittest.TestCase):
    def setUp(self):
        self.client = app.test_client()

    def test_catalog_has_unique_products_and_supported_links(self):
        catalog = _load_studio_catalog()
        apps = catalog["apps"]
        slugs = [studio_app["slug"] for studio_app in apps]

        self.assertEqual(catalog["schema_version"], 1)
        self.assertEqual(len(apps), 4)
        self.assertEqual(len(slugs), len(set(slugs)))
        self.assertIn("plotava", slugs)
        self.assertEqual(
            next(studio_app for studio_app in apps if studio_app["slug"] == "plotava")["product_url"],
            "https://plotava.com/",
        )

    def test_public_catalog_feed_is_cacheable_and_available_to_plotava(self):
        response = self.client.get("/api/studio/apps")
        payload = response.get_json()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["Access-Control-Allow-Origin"], "https://plotava.com")
        self.assertIn("max-age=900", response.headers["Cache-Control"])
        self.assertEqual(payload["publisher"]["name"], "Charged Alpha Studio")
        self.assertTrue(payload["apps"][0]["image_url"].startswith("https://chargedalpha.com/"))

    def test_studio_page_links_to_plotava_product_site(self):
        response = self.client.get("/studio")
        html = response.get_data(as_text=True)

        self.assertEqual(response.status_code, 200)
        self.assertIn("https://plotava.com/", html)
        self.assertIn("studio_crosslink", html)
        self.assertIn("data-studio-product", html)
        self.assertIn("Charged Alpha Studio", html)


if __name__ == "__main__":
    unittest.main()
