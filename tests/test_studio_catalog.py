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
        self.assertEqual(len(apps), 5)
        self.assertEqual(len(slugs), len(set(slugs)))
        self.assertIn("whirlytwig", slugs)
        self.assertIn("plotava", slugs)
        self.assertEqual(slugs[:2], ["plotava", "today-was"])
        whirlytwig = next(studio_app for studio_app in apps if studio_app["slug"] == "whirlytwig")
        today_was = next(studio_app for studio_app in apps if studio_app["slug"] == "today-was")
        plotava = next(studio_app for studio_app in apps if studio_app["slug"] == "plotava")
        self.assertIn("ios", whirlytwig["stores"])
        self.assertNotIn("android", whirlytwig["stores"])
        self.assertEqual(whirlytwig["coming_soon"], ["android"])
        self.assertEqual(len(whirlytwig["gallery"]), 2)
        self.assertEqual(today_was["operating_system"], "iOS, Android")
        self.assertEqual(
            today_was["stores"]["android"],
            "https://play.google.com/store/apps/details?id=com.chargedalpha.daymoire",
        )
        self.assertEqual(
            plotava["product_url"],
            "https://plotava.com/",
        )
        self.assertEqual(plotava["operating_system"], "iOS, Android")
        self.assertEqual(
            plotava["stores"]["ios"],
            "https://apps.apple.com/us/app/plotava/id6800150073",
        )
        self.assertNotIn("ios", plotava.get("coming_soon", []))

    def test_public_catalog_feed_is_cacheable_and_available_to_plotava(self):
        response = self.client.get("/api/studio/apps")
        payload = response.get_json()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["Access-Control-Allow-Origin"], "https://plotava.com")
        self.assertIn("max-age=900", response.headers["Cache-Control"])
        self.assertEqual(payload["publisher"]["name"], "Charged Alpha Studio")
        self.assertTrue(payload["apps"][0]["image_url"].startswith("https://chargedalpha.com/"))
        whirlytwig = next(studio_app for studio_app in payload["apps"] if studio_app["slug"] == "whirlytwig")
        self.assertTrue(whirlytwig["gallery"][0]["image_url"].startswith("https://chargedalpha.com/"))

    def test_studio_page_links_to_plotava_product_site(self):
        response = self.client.get("/studio")
        html = response.get_data(as_text=True)

        self.assertEqual(response.status_code, 200)
        self.assertIn("https://plotava.com/", html)
        self.assertIn("studio_crosslink", html)
        self.assertIn("data-studio-product", html)
        self.assertIn("Charged Alpha Studio", html)
        self.assertIn("Whirlytwig", html)
        self.assertIn("App Store gameplay", html)
        self.assertIn("Google Play preview", html)
        self.assertIn("com.chargedalpha.daymoire", html)
        self.assertLess(html.index('id="plotava"'), html.index('id="today-was"'))
        self.assertLess(html.index('id="today-was"'), html.index('id="charged-alpha"'))

    def test_about_page_links_plotava_on_both_app_stores(self):
        response = self.client.get("/about")
        html = response.get_data(as_text=True)

        self.assertEqual(response.status_code, 200)
        self.assertIn("https://apps.apple.com/us/app/plotava/id6800150073", html)
        self.assertIn("https://play.google.com/store/apps/details?id=com.plotava.app", html)
        self.assertIn("Available now on iPhone &amp; Android", html)
        self.assertNotIn("Plotava is coming soon to the App Store", html)


if __name__ == "__main__":
    unittest.main()
