/* Local browser checks for packet navigation. No external account access. */
const {chromium} = require('playwright');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const base = process.argv[2] || 'http://127.0.0.1:5055';
const packets = JSON.parse(fs.readFileSync(path.join(__dirname, '../data/research_packets.json'))).packets;

(async () => {
  const browser = await chromium.launch({channel:'chrome', headless:true});
  let jumps = 0;
  try {
    for (const width of [320, 390, 521, 785, 1280]) {
      const page = await browser.newPage({viewport:{width, height:741}});
      const errors = [];
      const inheritedOverflow = [];
      page.on('pageerror', e => errors.push(e.message));
      for (const packet of packets) {
        await page.goto(`${base}/research/${packet.slug}`, {waitUntil:'load'});
        const originalWidth = await page.evaluate(() => document.documentElement.scrollWidth);
        if (originalWidth > width) inheritedOverflow.push(`${packet.slug}: ${originalWidth - width}px`);
        const response = await page.goto(`${base}/research/${packet.slug}?view=reader`, {waitUntil:'load'});
        assert.equal(response.status(), 200);
        await page.locator('nav.toc[data-reader-ready]').waitFor();
        const compact = width < 960;
        assert.equal(await page.locator('nav.toc summary').count(), compact ? 1 : 0);
        const hrefs = await page.locator('nav.toc a').evaluateAll(links => links.map(link => link.getAttribute('href')));
        for (const href of hrefs) {
          if (compact) await page.locator('nav.toc summary').click();
          await page.locator(`nav.toc a[href="${href}"]`).click();
          const state = await page.evaluate(href => {
            const nav = document.querySelector('nav.toc');
            const section = document.getElementById(decodeURIComponent(href.slice(1)));
            const links = nav.querySelector('div');
            return {top:section.getBoundingClientRect().top, bottom:nav.getBoundingClientRect().bottom,
              width:innerWidth, docWidth:document.documentElement.scrollWidth,
              navOverflow:links.scrollWidth > links.clientWidth + 1,
              open:!!nav.querySelector('details[open]'), focused:document.activeElement === section};
          }, href);
          assert.ok(state.top >= state.bottom - 1, `${packet.slug} ${width} ${href}: heading hidden ${JSON.stringify(state)}`);
          assert.equal(state.navOverflow, false, `${packet.slug} ${width}: horizontal navigation overflow`);
          assert.ok(state.docWidth <= Math.max(state.width, originalWidth), `${packet.slug} ${width}: new document overflow`);
          assert.equal(state.open, false);
          assert.equal(state.focused, true);
          jumps++;
        }
        if (compact) {
          await page.locator('nav.toc summary').press('Enter');
          assert.equal(await page.locator('nav.toc details[open]').count(), 1);
          await page.locator('nav.toc summary').press('Escape');
          assert.equal(await page.locator('nav.toc details[open]').count(), 0);
        }
        if (packet.slug === 'avav-q1-fy2027') {
          if (compact) await page.locator('nav.toc summary').click();
          await page.locator('nav.toc a[href="#read"]').click();
          await page.screenshot({path:`/tmp/charged-alpha-reader-${width}.png`});
          if (width === 521) {
            await page.locator('nav.toc summary').click();
            await page.screenshot({path:'/tmp/charged-alpha-reader-menu.png'});
            await page.locator('nav.toc a[href="#valuation"]').click();
          }
          await page.reload({waitUntil:'load'});
          assert.equal(await page.locator('nav.toc a[aria-current="location"]').getAttribute('href'), new URL(page.url()).hash);
          await page.goBack();
          assert.equal(await page.locator('nav.toc a[aria-current="location"]').getAttribute('href'), new URL(page.url()).hash);
        }
      }
      assert.deepEqual(errors, []);
      console.log(`${width}px: ${packets.length} packets passed`);
      if (inheritedOverflow.length) console.log('  Existing report overflow (unchanged): ' + inheritedOverflow.join(', '));
      await page.close();
    }
    const noJS = await browser.newPage({viewport:{width:390, height:741}, javaScriptEnabled:false});
    await noJS.goto(`${base}/research/avav-q1-fy2027?view=reader`);
    assert.equal(await noJS.locator('nav.toc a:visible').count(), 15);
    assert.equal(await noJS.locator('nav.toc').evaluate(nav => getComputedStyle(nav).position), 'static');
    await noJS.locator('nav.toc a[href="#cash"]').click();
    assert.equal(new URL(noJS.url()).hash, '#cash');
    await noJS.close();
    console.log(`PASS: ${jumps} section jumps; keyboard, deep links, back navigation, no-JS fallback; screenshots in /tmp/charged-alpha-reader-*.png`);
  } finally { await browser.close(); }
})().catch(error => { console.error(error); process.exitCode = 1; });
