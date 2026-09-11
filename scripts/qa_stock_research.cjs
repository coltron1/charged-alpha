/* Deterministic browser regression checks; provider fixtures never ship to users. */
const {chromium} = require('playwright');
const assert = require('node:assert/strict');
const base = process.argv[2] || 'http://127.0.0.1:5063';
const figures = {status:'ready', fetched_at:'2026-09-10T00:00:00Z', currency:'USD', source:'Test statement source', source_url:'https://example.com/statements',
  quarterly:[{date:'2026-03-31', revenue:100, net_income:10, eps:1, free_cashflow:null, operating_margin:10},
    {date:'2026-06-30', revenue:120, net_income:14, eps:1.4, free_cashflow:12, operating_margin:12}],
  annual:[{date:'2025-12-31', revenue:400, net_income:35, eps:3.5, free_cashflow:30, operating_margin:9}]};
const crossCurrencyFigures = {
  BIRK:{...figures,currency:'EUR',annual:[{date:'2025-09-30',revenue:100,eps:2,shares:10,operating_margin:9}]},
  ONON:{...figures,currency:'CHF',annual:[{date:'2025-12-31',revenue:100,eps:2,shares:11,operating_margin:10}]},
  DECK:{...figures,currency:'USD',annual:[{date:'2025-03-31',revenue:100,eps:2,shares:12,operating_margin:11}]},
};
const financialFixture = request => {
  const ticker = decodeURIComponent(new URL(request.url()).pathname.split('/')[3] || '').toUpperCase();
  return crossCurrencyFigures[ticker] || figures;
};
const displayedAmount = value => String(Math.round((value + Number.EPSILON) * 100) / 100);
const comparisonTableSnapshot = page => page.locator('#financialChartTable table').evaluate(table => ({
  caption:table.caption.textContent.trim(),
  headers:[...table.tHead.rows[0].cells].map(cell=>cell.textContent.trim()),
  rows:[...table.tBodies[0].rows].map(row=>({
    period:row.cells[0].textContent.trim(),
    companies:[...row.cells].slice(1).map(cell=>({
      value:[...cell.childNodes].filter(node=>node.nodeType===Node.TEXT_NODE).map(node=>node.textContent).join('').trim(),
      periodEnd:cell.querySelector('.chart-period-date')?.textContent.trim() || '',
    })),
  })),
}));
async function assertComparisonTable(page, expected) {
  assert.deepEqual(await comparisonTableSnapshot(page),expected);
}

(async () => {
  const browser = await chromium.launch({channel:'chrome', headless:true});
  let checks = 0;
  try {
    for (const width of [320,390,768,1440]) {
      const page = await browser.newPage({viewport:{width,height:900}});
      const errors = [];
      page.on('pageerror', e => errors.push(e.message));
      await page.route('**/api/research/*/financials*', route => route.fulfill({json:financialFixture(route.request())}));
      await page.route('**/screener/api/stock/*/chart?*', route => route.fulfill({json:{labels:['2026-08-01','2026-09-01'],prices:[100,110]}}));
      for (const symbol of ['CASY','NVDA','JPM','SFM','SWBI']) {
        const start = Date.now();
        const response = await page.goto(base + '/shows/' + symbol, {waitUntil:'domcontentloaded'});
        assert.equal(response.status(),200);
        await page.locator('#metricGroup').selectOption('overview');
        assert.equal(await page.locator('h1').count(),1);
        assert.match(await page.title(),/Charged Alpha/);
        assert.equal(await page.locator('link[rel="canonical"]').count(),1);
        const dimensions = await page.evaluate(() => ({width:innerWidth,scroll:document.documentElement.scrollWidth}));
        assert.ok(dimensions.scroll <= dimensions.width, symbol + ' overflows at ' + width);
        assert.ok(await page.locator('.comparison-table tbody tr:visible').count() <= 9);
        await page.locator('#metricGroup').selectOption('all');
        assert.ok(await page.locator('.comparison-table tbody tr:visible').count() > 9);
        await page.locator('#metricGroup').selectOption('overview');
        if (symbol === 'CASY') {
          const columns = await page.locator('.comparison-table thead').innerText();
          assert.doesNotMatch(await page.locator('#scenarioResult').innerText(),/Enter positive/);
          assert.match(columns,/MUSA/); assert.match(columns,/ATD.TO/); assert.doesNotMatch(columns,/Ford|TJX/);
          const debt = page.locator('.comparison-table tbody tr').filter({has:page.locator('th', {hasText:'Debt / equity'})});
          assert.match(await debt.innerText(),/0.71x/);
          await page.locator('#financials').scrollIntoViewIfNeeded();
          const annual = page.locator('input[name="statementPeriod"][value="annual"]');
          const quarterly = page.locator('input[name="statementPeriod"][value="quarterly"]');
          await page.waitForFunction(() => document.getElementById('financialStatus').textContent.includes('1 available annual period'));
          assert.equal(await annual.isChecked(),true); assert.equal(await quarterly.isChecked(),false);
          await page.locator('#statementMetric').selectOption('eps');
          assert.match(await page.locator('#financialStatus').innerText(),/1 available annual period/);
          await page.locator('#financialChartTableWrap').evaluate(el => el.open = true);
          assert.match(await page.locator('#financialChartTable').innerText(),/3.5/);
          await quarterly.check();
          await page.waitForFunction(() => document.getElementById('financialStatus').textContent.includes('2 available quarterly periods'));
          assert.equal(await annual.isChecked(),false); assert.equal(await quarterly.isChecked(),true);
          assert.match(await page.locator('#financialStatus').innerText(),/2 available quarterly periods/);
          assert.match(await page.locator('#financialChartTable').innerText(),/1\.4/);
          await annual.check();
          await page.waitForFunction(() => document.getElementById('financialStatus').textContent.includes('1 available annual period'));
          assert.equal(await annual.isChecked(),true); assert.equal(await quarterly.isChecked(),false);
          assert.match(await page.locator('#financialChartTable').innerText(),/3.5/);
          await page.locator('#valuation').scrollIntoViewIfNeeded();
          await page.waitForFunction(() => document.getElementById('priceStatus').textContent.includes('Split- and dividend-adjusted close'));
          await page.locator('#scenarioEPS').fill('10');
          await page.locator('#scenarioPE').fill('20');
          assert.equal(await page.locator('#scenarioResult').innerText(),'USD 200.00');
          await page.locator('#scenarioPE').fill('-1');
          assert.match(await page.locator('#scenarioResult').innerText(),/Enter positive/);
          const chartPixels = await page.locator('#financialChart').evaluate(el => {
            const data = el.getContext('2d').getImageData(0,0,el.width,el.height).data;
            let visible = 0; for (let i=3;i<data.length;i+=4) if(data[i]) visible++;
            return visible;
          });
          assert.ok(chartPixels > 100,'Financial chart must not be blank');
          const groups = page.locator('.archive-period');
          assert.ok(await groups.count() > 0);
          if (await groups.count() > 1) {
            await groups.nth(1).locator('summary').click();
            assert.ok(await groups.nth(1).evaluate(el=>el.open));
          }
          if (width < 640) {
            const menu = page.locator('.ca-menu-button');
            await menu.click();
            assert.equal(await menu.getAttribute('aria-expanded'),'true');
            await page.locator('#ca-nav details').filter({has:page.locator('a[href="/games"]')}).locator('summary').click();
            assert.ok(await page.locator('#ca-nav a[href="/games"]').isVisible());
            await menu.click();
            assert.equal(await menu.getAttribute('aria-expanded'),'false');
          }
          await page.locator('#peers').screenshot({path:'/tmp/research-peers-' + width + '.png'});
          await page.evaluate(()=>scrollTo(0,0));
          await page.screenshot({path:'/tmp/research-overview-' + width + '.png'});
        }
        console.log(symbol, width, 'passed', Date.now()-start+'ms (browser + controls)');
        checks++;
      }
      await page.goto(base + '/shows/CASY?peers=MUSA,ATD.TO#peers');
      assert.match(await page.locator('#peers').innerText(),/Custom selections/);
      assert.doesNotMatch(await page.locator('.comparison-table thead').innerText(),/Peer median/);
      assert.deepEqual(errors,[]);
      await page.close();
    }
    const comparisonPage = await browser.newPage({viewport:{width:1280,height:900}});
    const comparisonErrors = [];
    comparisonPage.on('pageerror', error => comparisonErrors.push(error.message));
    await comparisonPage.route('**/api/research/*/financials*', route => route.fulfill({json:financialFixture(route.request())}));
    await comparisonPage.route('**/screener/api/stock/*/chart?*', route => route.fulfill({json:{labels:['2026-08-01','2026-09-01'],prices:[100,110]}}));
    const comparisonResponse = await comparisonPage.goto(base + '/shows/BIRK?peers=ONON,DECK#financials', {waitUntil:'domcontentloaded'});
    assert.equal(comparisonResponse.status(),200);
    await comparisonPage.locator('#financials').scrollIntoViewIfNeeded();
    await comparisonPage.waitForFunction(() => document.getElementById('financialStatus').textContent.includes('1 available annual period'));
    const currentFx = await comparisonPage.locator('#financialComparisonFx').evaluate(node => JSON.parse(node.textContent));
    assert.equal(currentFx.target_currency,'USD');
    assert.equal(currentFx.rates?.USD,1);
    const expectedCompanies = ['BIRK','ONON','DECK'].map(ticker => {
      const statement = crossCurrencyFigures[ticker], currency = statement.currency, rate = Number(currentFx.rates?.[currency]);
      assert.ok(Number.isFinite(rate) && rate > 0,`${currency} comparison FX must be finite and positive`);
      if(currency !== 'USD')assert.ok(Number.isFinite(Date.parse(currentFx.observed_at_by_currency?.[currency])),`${currency} comparison FX observation date must be valid`);
      assert.ok(Number.isFinite(statement.annual[0].revenue),`${ticker} annual revenue fixture must be finite`);
      return {ticker,currency,periodEnd:statement.annual[0].date,value:displayedAmount(statement.annual[0].revenue*rate)};
    });
    const expectedComparisonTable = {
      caption:'Revenue (USD)',
      headers:['Year of period end',...expectedCompanies.map(company=>company.ticker)],
      rows:[{period:'2025',companies:expectedCompanies.map(company=>({value:company.value,periodEnd:'Ended '+company.periodEnd}))}],
    };
    await comparisonPage.locator('#compareFinancials').check();
    await comparisonPage.waitForFunction(() => document.getElementById('financialCompareStatus').textContent.includes('normalized to USD'));
    const wrongFxTable = structuredClone(expectedComparisonTable);
    wrongFxTable.rows[0].companies[0].value=displayedAmount(Number(expectedCompanies[0].value)+0.01);
    await assert.rejects(assertComparisonTable(comparisonPage,wrongFxTable),/Expected values to be strictly deep-equal/);
    await assertComparisonTable(comparisonPage,expectedComparisonTable);
    assert.match(await comparisonPage.locator('#financialCompareStatus').innerText(),/normalized to USD/);
    assert.match(await comparisonPage.locator('#financialCompareSources').innerText(),/CHF to USD/);
    await comparisonPage.locator('#financialChartTableWrap').evaluate(element => element.open=true);
    await comparisonPage.locator('#financials').screenshot({path:'/tmp/research-financial-comparison.png'});
    console.log('Cross-currency table passed with server FX:',expectedCompanies.map(company=>`${company.ticker} ${company.currency} ${company.value} USD`).join('; '),'; one-cent wrong conversion rejected.');
    assert.deepEqual(comparisonErrors,[]);
    await comparisonPage.close();
    const page = await browser.newPage();
    await page.route('**/api/research/*/financials*', route => route.fulfill({status:503,json:{status:'unavailable',message:'Test provider unavailable'}}));
    await page.goto(base + '/shows/CASY#financials');
    await page.waitForFunction(()=>document.getElementById('financialStatus').textContent.includes('Test provider unavailable'));
    assert.equal(await page.locator('#loadFinancials').isEnabled(),true);
    console.log(checks + ' stock/viewport combinations, custom comparison, history controls, chart pixels and failure recovery passed.');
  } finally { await browser.close(); }
})().catch(error => { console.error(error); process.exit(1); });
