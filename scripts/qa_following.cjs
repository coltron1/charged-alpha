/* Real catalog and browser storage. Each run uses isolated, disposable profiles. */
const {chromium} = require('playwright');
const assert = require('node:assert/strict');
const base = process.argv[2] || 'http://127.0.0.1:5055';
const key = 'chargedalpha.following.v1';

async function ready(page) {
  await page.waitForFunction(() => !!window.CAFollowing);
}
async function libraryReady(page) {
  await page.waitForFunction(() => /compan(y|ies)/.test(document.querySelector('#searchStatus')?.textContent || ''));
}
async function followingNav(page) {
  const menu = page.getByRole('button', {name: 'Open navigation', exact: true});
  if (await menu.isVisible()) await menu.click();
  await page.locator('#ca-nav a[href="/following"]').click();
  await libraryReady(page);
}

(async () => {
  const browser = await chromium.launch({channel: 'chrome', headless: true});
  try {
    for (const width of [390, 785, 1280]) {
      const context = await browser.newContext({viewport: {width, height: 900}});
      await context.route(/(googletagmanager|google-analytics)\.com/, route => route.abort());
      const page = await context.newPage(), errors = [];
      context.on('page', p => p.on('pageerror', e => errors.push(e.message)));
      page.on('pageerror', e => errors.push(e.message));
      await page.goto(base + '/shows/ADBE');
      await ready(page);
      const follow = page.locator('main [data-follow-stock="ADBE"]');
      assert.equal(await follow.getAttribute('aria-pressed'), 'false');
      await follow.click();
      assert.equal(await follow.getAttribute('aria-pressed'), 'true');
      await followingNav(page);
      assert.equal(await page.locator('[data-stock="ADBE"]').count(), 1);
      assert.match(await page.locator('.ca-storage-note').textContent(), /No account, email notifications, or cross-device syncing yet/);
      await page.reload();
      await libraryReady(page);
      assert.equal(await page.locator('[data-stock="ADBE"]').count(), 1);
      await page.locator('[data-stock="ADBE"] h3 a').click();
      await ready(page);
      assert.equal(await page.locator('main [data-follow-stock="ADBE"]').getAttribute('aria-pressed'), 'true');
      await page.goBack();
      await libraryReady(page);

      const other = await context.newPage();
      await other.goto(base + '/?q=AVAV');
      await libraryReady(other);
      await other.locator('[data-follow-stock="AVAV"]').click();
      await page.locator('[data-stock="AVAV"]').waitFor();
      assert.equal(await page.locator('.ca-follow-count').first().textContent(), '2');
      await page.locator('#searchInput').fill('not-a-saved-company');
      assert.equal(await page.locator('#stockGrid article').count(), 0);
      assert.match(await page.locator('#emptyState h2').textContent(), /No followed stocks match/);
      await page.locator('#clearSearch').click();
      assert.equal(await page.locator('#stockGrid article').count(), 2);
      await page.screenshot({path: `/tmp/charged-alpha-following-${width}.png`, fullPage: true});
      assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth));
      await page.locator('[data-follow-stock="ADBE"]').click();
      assert.equal(await page.locator('[data-stock="ADBE"]').count(), 0);
      await page.reload();
      await libraryReady(page);
      assert.equal(await page.locator('#stockGrid article').count(), 1);
      await other.evaluate(key => localStorage.removeItem(key), key);
      await page.waitForFunction(() => document.querySelectorAll('#stockGrid article').length === 0);
      assert.match(await page.locator('#emptyState h2').textContent(), /A place for the stocks/);
      assert.equal(await page.locator('.ca-follow-count').first().isVisible(), false);
      assert.deepEqual(errors, []);
      await context.close();
      console.log(`${width}px: save on detail/home, Following navigation, refresh, reopen/back, cross-tab updates, search/clear, remove, empty state passed`);
    }

    const context = await browser.newContext();
    await context.route(/(googletagmanager|google-analytics)\.com/, route => route.abort());
    const page = await context.newPage();
    await page.goto(base + '/');
    await libraryReady(page);
    await page.evaluate(key => localStorage.setItem(key, '["ADBE","ADBE",null,"bad symbol"]'), key);
    assert.deepEqual(await page.evaluate(() => CAFollowing.read()), ['ADBE']);
    await page.evaluate(key => localStorage.setItem(key, 'malformed'), key);
    assert.deepEqual(await page.evaluate(() => CAFollowing.read()), []);
    await page.evaluate(key => localStorage.setItem(key, '["ADBE"]'), key);
    await page.route('**/api/shows/stocks', route => route.fulfill({status: 503, json: {error: 'QA temporary outage'}}));
    await page.goto(base + '/following');
    await page.locator('#retryLibrary').waitFor();
    assert.match(await page.locator('#emptyState p').textContent(), /saved stocks are still/);
    assert.deepEqual(await page.evaluate(() => CAFollowing.read()), ['ADBE']);
    await page.unroute('**/api/shows/stocks');
    await page.locator('#retryLibrary').click();
    await libraryReady(page);
    assert.equal(await page.locator('[data-stock="ADBE"]').count(), 1);
    await page.goto(base + '/?q=AVAV');
    await libraryReady(page);
    await page.evaluate(key => localStorage.setItem(key, JSON.stringify(Array.from({length: 300}, (_, i) => 'QA' + i))), key);
    await page.locator('[data-follow-stock="AVAV"]').click();
    assert.match(await page.locator('#ca-toast').textContent(), /300 stocks/);
    assert.equal(await page.evaluate(() => CAFollowing.read().includes('AVAV')), false);
    await context.close();

    const denied = await browser.newContext();
    await denied.route(/(googletagmanager|google-analytics)\.com/, route => route.abort());
    await denied.addInitScript(() => {
      const original = Storage.prototype.setItem;
      Storage.prototype.setItem = function (key, value) {
        if (key === 'chargedalpha.following.v1') throw new DOMException('QA storage denied', 'SecurityError');
        return original.call(this, key, value);
      };
    });
    const blocked = await denied.newPage();
    await blocked.goto(base + '/?q=AVAV');
    await libraryReady(blocked);
    await blocked.locator('[data-follow-stock="AVAV"]').click();
    assert.match(await blocked.locator('#ca-toast').textContent(), /could not save/);
    assert.equal(await blocked.locator('[data-follow-stock="AVAV"]').getAttribute('aria-pressed'), 'false');
    await denied.close();
    console.log('Malformed/duplicate storage, library outage/retry, full-list guard, denied storage passed');
  } finally {
    await browser.close();
  }
})().catch(error => { console.error(error); process.exitCode = 1; });
