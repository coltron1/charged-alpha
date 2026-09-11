const test = require('node:test');
const assert = require('node:assert/strict');
const model = require('../static/research-chart-model.js');

test('annual presets retain exactly one, three, and five fiscal periods', () => {
  const rows = ['2022-01-30','2023-01-29','2024-01-28','2025-02-02','2026-02-01'].map(date=>({date}));
  for (const years of [1,3,5]) assert.equal(model.windowRows(rows,years,'annual').length,years);
  assert.equal(rows[0].date,'2022-01-30');
});
test('quarterly windows are date bounded, not just a slice of ancient records', () => {
  const rows = [{date:'2020-03-31'},{date:'2025-06-30'},{date:'2025-09-30'},{date:'2025-12-31'},{date:'2026-03-31'}];
  assert.equal(model.windowRows(rows,1).length,4);
  assert.equal(model.windowRows(rows,5).length,4);
});
test('missing numbers remain null while real zero and losses are retained', () => {
  assert.deepEqual(model.values([{x:0},{x:null},{x:NaN},{x:-2},{}],'x'),[0,null,null,-2,null]);
});
test('drawdown uses running peak within the selected data, never a future peak', () => {
  const points=model.priceSeries({labels:['a','b','c','d'],prices:[100,80,120,90]},'drawdown');
  assert.ok(Math.abs(points[1].value+20)<1e-8);
  assert.deepEqual(points.filter((_,i)=>i!==1).map(p=>p.value),[0,0,-25]);
});
test('volume is not silently substituted with price', () => {
  assert.deepEqual(model.priceSeries({labels:['a','b'],prices:[100,200],volumes:[0,null]},'volume').map(p=>p.value),[0,null]);
});
test('every advanced chart series has defined labels and consistent units', () => {
  for (const chart of model.charts) {
    const units=chart.keys.map(key=>model.metrics[key][1]);
    assert.equal(new Set(units).size,1);
  }
});

const company = (ticker, currency, annual, quarterly=[]) => ({ticker,data:{currency,annual,quarterly}});
test('comparison aligns ending years while retaining exact dates and missing periods', () => {
  const subject=company('AVAV','USD',[{date:'2024-04-30',revenue:100},{date:'2025-04-30',revenue:120},{date:'2026-04-30',revenue:150}]);
  const peer=company('WMT','USD',[{date:'2024-01-31',revenue:300},{date:'2026-01-31',revenue:400},{date:'2027-01-31',revenue:999}]);
  const before=JSON.stringify([subject,peer]);
  const view=model.compareStatements(subject,[peer],'revenue','annual',3);
  assert.deepEqual(view.rows.map(row=>row.date),['2024','2025','2026']);
  assert.deepEqual(model.values(view.rows,'company1'),[300,null,400]);
  assert.equal(view.rows[2].periodEnds.company0,'2026-04-30');
  assert.equal(view.rows[2].periodEnds.company1,'2026-01-31');
  assert.equal(JSON.stringify([subject,peer]),before);
});
test('quarterly comparison groups calendar quarter ends, never slides missing reports', () => {
  const subject=company('AVAV','USD',[],[{date:'2026-04-30',revenue:100},{date:'2026-08-01',revenue:150}]);
  const peer=company('WMT','USD',[],[{date:'2026-01-31',revenue:999},{date:'2026-07-31',revenue:400}]);
  const view=model.compareStatements(subject,[peer],'revenue','quarterly',1);
  assert.deepEqual(view.rows.map(row=>row.date),['2026 Q2','2026 Q3']);
  assert.deepEqual(model.values(view.rows,'company1'),[null,400]);
});
test('currency comparisons never mix money or EPS but allow ratios and shares', () => {
  const rows=[{date:'2026-01-31',revenue:100,eps:2,shares:10,net_margin:-4}];
  const subject=company('BASE','USD',rows),peer=company('EURO','EUR',rows);
  for(const metric of ['revenue','eps']) {
    const view=model.compareStatements(subject,[peer],metric,'annual',1);
    assert.equal(view.rows[0].company1,null);assert.deepEqual(view.excluded,['EURO']);
  }
  assert.equal(model.compareStatements(subject,[peer],'shares','annual',1).rows[0].company1,10);
  assert.equal(model.compareStatements(subject,[peer],'net_margin','annual',1).rows[0].company1,-4);
  assert.equal(model.compareStatements(company('BASE',null,rows),[company('OTHER',null,rows)],'revenue','annual',1).rows[0].company1,null);
});
test('comparison enforces two distinct peers and preserves actual zeros and losses', () => {
  const rows=[{date:'2026-01-31',revenue:0,net_income:-1}];
  const subject=company('BASE','USD',rows),peers=['BASE','ONE','ONE','TWO','THREE'].map(ticker=>company(ticker,'USD',rows));
  const view=model.compareStatements(subject,peers,'revenue','annual',5);
  assert.deepEqual(view.labels,['BASE','ONE','TWO']);
  assert.equal(view.rows[0].company1,0);
  assert.equal(model.compareStatements(subject,peers,'net_income','annual',5).rows[0].company2,-1);
});
test('multiple reports ending in the same period are excluded, not silently selected', () => {
  const subject=company('BASE','USD',[{date:'2026-12-31',revenue:10}]);
  const peer=company('CHANGED','USD',[{date:'2026-01-31',revenue:20},{date:'2026-12-31',revenue:30}]);
  const view=model.compareStatements(subject,[peer],'revenue','annual',1);
  assert.equal(view.rows[0].company1,null);assert.deepEqual(view.ambiguous,['CHANGED 2026']);
});
test('chart choices default to two and reconcile replacements without losing manual limits', () => {
  const peers=['A','B','C','D'].map(ticker=>({ticker}));
  assert.deepEqual(model.comparisonPeers(peers),['A','B']);
  assert.deepEqual(model.comparisonPeers(peers,['B','OLD']),['B','A']);
  assert.deepEqual(model.comparisonPeers(peers,['C']),['C']);
  assert.deepEqual(model.comparisonPeers(peers,[]),[]);
  assert.deepEqual(model.comparisonPeers(peers,null,'A'),['B','C']);
});
