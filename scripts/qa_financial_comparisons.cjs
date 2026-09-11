/* Deterministic integration tests; financial fixtures never enter the catalog. */
const {chromium} = require('playwright');
const assert = require('node:assert/strict');
const base=process.argv[2] || 'http://127.0.0.1:5055';
const {metrics}=require('../static/research-chart-model.js');
const chartIds=['financialChart','cashflowChart','sharesChart','balanceChart','debtChart','marginsChart','capitalChart','liquidityChart'];
function statement(ticker) {
  const factor={AVAV:1,WMT:3,CASY:2,AAPL:4,IBM:5}[ticker] || 1;
  const row=date=>({date,...Object.fromEntries(Object.keys(metrics).map((key,i)=>[key,(i+1)*100*factor]))});
  return {status:'ready',currency:ticker==='AAPL'?'EUR':'USD',source:'QA statement fixture',source_url:'https://www.sec.gov/',fetched_at:'2026-09-11T00:00:00Z',
    annual:[2022,2023,2024,2025,2026].map(y=>row(`${y}-${ticker==='AVAV'?'04-30':'01-31'}`)),
    quarterly:['2025-10-31','2026-01-31','2026-04-30','2026-07-31'].map(row)};
}
(async()=>{
  const browser=await chromium.launch({channel:'chrome',headless:true});
  try {
    for(const width of [390,521,1280]) {
      const page=await browser.newPage({viewport:{width,height:900}}), errors=[], calls={};
      let failCASY=true, delayWMT=true;
      page.on('pageerror',e=>errors.push(e.message));
      await page.route('**/api/research/*/financials*',async route=>{
        const ticker=new URL(route.request().url()).pathname.split('/')[3];calls[ticker]=(calls[ticker]||0)+1;
        if(ticker==='CASY'&&failCASY)return route.fulfill({status:503,json:{status:'unavailable',message:'Fixture provider unavailable.'}});
        if(ticker==='WMT'&&delayWMT){await new Promise(resolve=>setTimeout(resolve,700));delayWMT=false;}
        return route.fulfill({json:statement(ticker)});
      });
      await page.route('**/screener/api/stock/*/chart?*',route=>route.fulfill({json:{labels:['2025-01-01','2026-01-01'],prices:[10,15]}}));
      await page.goto(base+'/shows/AVAV?peers=WMT,CASY,AAPL#financials',{waitUntil:'load'});
      await page.waitForFunction(()=>!!Chart.getChart('financialChart'));
      assert.equal(await page.locator('#compareFinancials').isChecked(),false);
      assert.equal(calls.WMT || 0,0);
      await page.locator('#compareFinancials').check();
      await page.locator('[data-expand-chart="financialChart"]').click();
      await page.waitForFunction(()=>Chart.getChart('expandedChart')?.data.datasets.find(ds=>ds.label==='WMT')?.data[0]===300);
      assert.equal(await page.locator('#expandedChartTable th').allTextContents().then(a=>a.includes('WMT')),true);
      await page.getByRole('button',{name:'Close expanded chart',exact:true}).click();
      await page.locator('#retryFinancialPeers').waitFor({state:'visible'});
      assert.match(await page.locator('#financialCompareSources').textContent(),/Fixture provider unavailable/);
      failCASY=false;
      await page.locator('#retryFinancialPeers').click();
      await page.waitForFunction(()=>Chart.getChart('financialChart')?.data.datasets.find(ds=>ds.label==='CASY')?.data[0]===200);
      for(const id of chartIds)assert.equal(await page.evaluate(id=>Chart.getChart(id).data.datasets.length,id),3,id);
      assert.equal(await page.locator('#financialComparePeers input[value="AAPL"]').isDisabled(),true);
      assert.match(await page.locator('#financialChartTable').textContent(),/Ended 2026-04-30/);
      assert.match(await page.locator('#financialChartTable').textContent(),/Ended 2026-01-31/);
      await page.locator('#cashflowCompareMetric').selectOption('operating_cashflow');
      assert.equal(await page.evaluate(()=>Chart.getChart('cashflowChart').data.datasets[0].data[0]),600);
      await page.locator('[name="financialRange"][value="1"]').check();
      assert.equal(await page.evaluate(()=>Chart.getChart('financialChart').data.labels.length),1);
      await page.locator('[name="financialRange"][value="3"]').check();
      assert.equal(await page.evaluate(()=>Chart.getChart('financialChart').data.labels.length),3);
      await page.locator('[name="statementPeriod"][value="quarterly"]').check();
      assert.deepEqual(await page.evaluate(()=>Chart.getChart('financialChart').data.labels),['2025 Q4','2026 Q1','2026 Q2','2026 Q3']);
      await page.locator('[name="statementPeriod"][value="annual"]').check();
      await page.locator('[name="financialRange"][value="5"]').check();
      await page.locator('[name="financialStyle"][value="line"]').check();
      assert.equal(await page.evaluate(()=>Chart.getChart('financialChart').config.type),'line');
      await page.locator('[name="financialStyle"][value="bar"]').check();
      await page.locator('#compareFinancials').scrollIntoViewIfNeeded();
      await page.screenshot({path:`/tmp/charged-alpha-financial-compare-${width}.png`});
      await page.locator('#financialComparePeers input[value="CASY"]').uncheck();
      await page.locator('#financialComparePeers input[value="AAPL"]').check();
      await page.waitForFunction(()=>document.querySelector('#financialCompareSources').textContent.includes('EUR'));
      assert.ok((await page.evaluate(()=>Chart.getChart('financialChart').data.datasets.find(ds=>ds.label==='AAPL').data)).every(v=>v===null));
      assert.ok((await page.evaluate(()=>Chart.getChart('sharesChart').data.datasets.find(ds=>ds.label==='AAPL').data)).some(v=>v>0));
      assert.match(await page.locator('#financialCompareSources').textContent(),/Currency amounts excluded/);
      await page.locator('#financialComparePeers input[value="AAPL"]').uncheck();
      await page.locator('#financialComparePeers input[value="CASY"]').check();
      const previousCalls={...calls};
      await page.locator('#compareFinancials').uncheck();
      assert.equal(await page.locator('#financialCompareOptions').isVisible(),false);
      assert.equal(await page.evaluate(()=>Chart.getChart('financialChart').data.datasets.length),1);
      assert.equal(await page.evaluate(()=>Chart.getChart('cashflowChart').data.datasets.length),3);
      assert.deepEqual(await page.evaluate(()=>Chart.getChart('financialChart').data.labels),statement('AVAV').annual.map(r=>r.date));
      await page.locator('#compareFinancials').check();
      assert.deepEqual(calls,previousCalls,'Cached toggling must not fetch again');
      // Exercise the actual peer picker/event connection with deterministic search.
      await page.route('**/api/research/search?*',route=>route.fulfill({json:{results:[{ticker:'IBM',company:'International Business Machines',exchange:'NYSE'}]}}));
      await page.getByRole('button',{name:'Replace WMT comparison stock',exact:true}).click();
      await page.locator('#peerSearchInput').fill('IBM');
      await page.locator('#peerSearchResults button').first().click();
      await page.waitForFunction(()=>Chart.getChart('financialChart').data.datasets.some(ds=>ds.label==='IBM'&&ds.data[0]===500));
      assert.equal(await page.evaluate(()=>Chart.getChart('financialChart').data.datasets.some(ds=>ds.label==='WMT')),false);
      assert.equal(await page.locator('#peerSlots [data-ticker="IBM"]').count(),1);
      assert.equal(await page.locator('#financialComparePeers input:checked').count(),2);
      const dimensions=await page.evaluate(()=>({width:innerWidth,scroll:document.documentElement.scrollWidth}));
      assert.ok(dimensions.scroll<=dimensions.width);
      assert.deepEqual(errors,[]);
      await page.close();
      console.log(`${width}px: all eight charts, two-peer limit, periods, currency guards, failure/retry, expansion, peer replacement, cached toggles passed`);
    }
    const page=await browser.newPage({viewport:{width:521,height:900}});
    let releaseOld, firstWMT=true, secondStarted=false;
    await page.route('**/api/research/*/financials*',async route=>{
      const ticker=new URL(route.request().url()).pathname.split('/')[3];
      if(ticker==='WMT'&&firstWMT) {
        firstWMT=false;
        await new Promise(resolve=>{releaseOld=resolve;});
        const stale=statement('WMT');stale.annual.forEach(row=>row.revenue=999999);
        return route.fulfill({json:stale}).catch(()=>{});
      }
      if(ticker==='WMT')secondStarted=true;
      return route.fulfill({json:statement(ticker)});
    });
    await page.route('**/screener/api/stock/*/chart?*',route=>route.fulfill({json:{labels:[],prices:[]}}));
    await page.goto(base+'/shows/AVAV?peers=WMT#financials');
    await page.waitForFunction(()=>!!Chart.getChart('financialChart'));
    await page.locator('#compareFinancials').check();
    while(!releaseOld)await new Promise(resolve=>setTimeout(resolve,10));
    await page.locator('#compareFinancials').uncheck();
    await page.locator('#compareFinancials').check();
    await page.waitForFunction(()=>Chart.getChart('financialChart').data.datasets.find(ds=>ds.label==='WMT')?.data[0]===300);
    assert.equal(secondStarted,true);
    releaseOld();
    await page.waitForTimeout(100);
    assert.equal(await page.evaluate(()=>Chart.getChart('financialChart').data.datasets.find(ds=>ds.label==='WMT').data[0]),300);
    await page.locator('#financialComparePeers input[value="WMT"]').uncheck();
    assert.equal(await page.evaluate(()=>Chart.getChart('financialChart').data.datasets.length),1);
    assert.match(await page.locator('#financialCompareStatus').textContent(),/No chart comparison stocks selected/);
    await page.close();
    console.log('Cancellation/re-enable race and empty chart selection passed');
  } finally {await browser.close();}
})().catch(error=>{console.error(error);process.exitCode=1;});
