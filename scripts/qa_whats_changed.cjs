/* Visual and responsive regression checks for the stock-page report highlights. */
const { chromium } = require('playwright');
const assert = require('node:assert/strict');

const base = process.argv[2] || 'http://127.0.0.1:5055';

(async () => {
  const browser = await chromium.launch({ channel: 'chrome', headless: true });
  try {
    for (const width of [390, 1280]) {
      const page = await browser.newPage({ viewport: { width, height: 900 } });
      const errors = [];
      page.on('pageerror', error => errors.push(error.message));
      await page.route('**/api/research/*/financials*', route => route.abort());
      await page.route('**/screener/api/stock/*/chart?*', route => route.abort());

      const response = await page.goto(`${base}/shows/AVAV#whats-changed`, {
        waitUntil: 'domcontentloaded',
      });
      assert.equal(response.status(), 200);
      const section = page.locator('#whats-changed');
      await section.scrollIntoViewIfNeeded();
      assert.equal(await section.locator('h2').innerText(), "What’s changed for AVAV");
      assert.equal(await section.locator('.change-summary-list > li').count(), 5);
      assert.match(await section.innerText(), /Research published September 9, 2026/);
      assert.match(await section.innerText(), /See the evidence in the full report/);
      assert.equal(await page.locator('a[href="#whats-changed"]').count(), 1);

      const dimensions = await page.evaluate(() => ({
        viewport: document.documentElement.clientWidth,
        scroll: document.documentElement.scrollWidth,
      }));
      assert.ok(dimensions.scroll <= dimensions.viewport, `Page overflows at ${width}px`);
      const box = await section.boundingBox();
      assert.ok(box && box.width <= width, `Highlight section exceeds ${width}px viewport`);
      assert.deepEqual(errors, []);

      await section.screenshot({ path: `/tmp/charged-alpha-whats-changed-${width}.png` });
      console.log(`What's Changed passed at ${width}px`);
      await page.close();
    }
  } finally {
    await browser.close();
  }
})().catch(error => {
  console.error(error);
  process.exit(1);
});
