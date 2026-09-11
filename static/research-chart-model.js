(function (root, factory) {
  const model = factory();
  if (typeof module === 'object' && module.exports) module.exports = model;
  else root.CAChartModel = model;
})(typeof globalThis === 'object' ? globalThis : this, function () {
  'use strict';
  const metrics = {
    revenue: ['Revenue', 'money'], gross_profit: ['Gross profit', 'money'],
    operating_income: ['Operating income', 'money'], net_income: ['Net income', 'money'],
    eps: ['Diluted EPS', 'per-share'], operating_cashflow: ['Operating cash flow', 'money'],
    capex: ['Capital spending', 'money'], free_cashflow: ['Free cash flow', 'money'],
    assets: ['Total assets', 'money'], liabilities: ['Total liabilities', 'money'], equity: ['Stockholders\u2019 equity', 'money'],
    cash: ['Cash & equivalents', 'money'], debt: ['Total debt', 'money'], long_term_debt: ['Noncurrent debt', 'money'],
    lease_liabilities: ['Operating lease liabilities', 'money'], net_debt: ['Net debt', 'money'],
    current_assets: ['Current assets', 'money'], current_liabilities: ['Current liabilities', 'money'],
    working_capital: ['Working capital', 'money'], shares: ['Diluted weighted-average shares', 'shares'],
    basic_shares: ['Basic weighted-average shares', 'shares'], shares_outstanding: ['Period-end common shares', 'shares'],
    stock_compensation: ['Stock-based compensation', 'money'], buybacks: ['Common-stock repurchases', 'money'],
    dividends: ['Common dividends paid', 'money'], gross_margin: ['Gross margin', 'percent'],
    operating_margin: ['Operating margin', 'percent'], net_margin: ['Net margin', 'percent'],
    fcf_margin: ['Free cash flow margin', 'percent'], current_ratio: ['Current ratio', 'ratio'],
    debt_to_equity: ['Debt / equity', 'ratio'],
  };
  const charts = [
    {id:'cashflow', title:'Cash generation', type:'bar', keys:['operating_cashflow','capex','free_cashflow'], note:'Free cash flow = operating cash flow less capital spending. Capital spending is shown as a positive outflow.'},
    {id:'shares', title:'Share count & dilution', type:'line', keys:['basic_shares','shares','shares_outstanding'], note:'Weighted-average shares measure a period. Period-end common shares are a balance-sheet snapshot. Splits and share-class changes can affect comparability.'},
    {id:'balance', title:'Assets, liabilities & equity', type:'bar', keys:['assets','liabilities','equity'], note:'Period-end balances. Minority interests and temporary equity may prevent these series from reconciling exactly.'},
    {id:'debt', title:'Cash, debt & leases', type:'bar', keys:['cash','debt','long_term_debt','lease_liabilities'], note:'Noncurrent debt is part of total debt, not an additional amount. Operating leases are separately reported obligations; debt definitions vary.'},
    {id:'margins', title:'Profitability & cash margins', type:'line', keys:['gross_margin','operating_margin','net_margin','fcf_margin'], note:'Each margin divides its profit or cash-flow measure by revenue for the same reporting period.'},
    {id:'capital', title:'Shareholder capital allocation', type:'bar', keys:['buybacks','dividends','stock_compensation'], note:'Repurchases and dividends are cash outflows; stock-based compensation is a noncash expense, not a cash payment.'},
    {id:'liquidity', title:'Liquidity & leverage', type:'line', keys:['current_ratio','debt_to_equity'], note:'Current assets / current liabilities; total debt / positive stockholders\u2019 equity. These ratios may not be meaningful for banks or insurers.'},
  ];
  const finite = value => typeof value === 'number' && Number.isFinite(value);
  function windowRows(rows, years, period = 'quarterly') {
    const sorted = rows.filter(r=>/^\d{4}-\d{2}-\d{2}$/.test(r.date)).slice().sort((a,b)=>a.date.localeCompare(b.date));
    if (!sorted.length) return [];
    if (period === 'annual') return sorted.slice(-years);
    const end = new Date(sorted[sorted.length-1].date + 'T00:00:00Z');
    const cutoff = new Date(end); cutoff.setUTCFullYear(end.getUTCFullYear() - years);
    return sorted.filter(r=>new Date(r.date+'T00:00:00Z') > cutoff).slice(-years*4);
  }
  function values(rows, key) { return rows.map(r=>finite(r[key])?r[key]:null); }
  function comparisonPeers(peers, previous = null, subject = '') {
    const unique = peers.filter((peer, index) => peer.ticker !== subject && peers.findIndex(p=>p.ticker===peer.ticker)===index);
    const count = previous === null ? 2 : Math.min(previous.length, 2);
    const retained = previous === null ? [] : previous.filter(ticker=>unique.some(peer=>peer.ticker===ticker));
    return [...new Set([...retained, ...unique.map(peer=>peer.ticker)])].slice(0, count);
  }
  function periodGroup(date, period) {
    if (!/^\d{4}-\d{2}-\d{2}$/.test(date || '')) return null;
    return period === 'annual' ? date.slice(0,4) : date.slice(0,4)+' Q'+Math.ceil(Number(date.slice(5,7))/3);
  }
  function compareStatements(subject, peers, metric, period, years) {
    const kind = metrics[metric][1], currency = subject.data?.currency || null;
    const monetary = kind === 'money' || kind === 'per-share';
    const companies = [subject, ...peers.filter((p,i)=>p.ticker!==subject.ticker && peers.findIndex(other=>other.ticker===p.ticker)===i).slice(0,2)];
    const selectedRows = windowRows(subject.data?.[period] || [], years, period);
    const groups = [...new Set(selectedRows.map(row=>periodGroup(row.date,period)).filter(Boolean))];
    const keys = companies.map((_,i)=>'company'+i), excluded = [], ambiguous = [];
    const indexes = companies.map((company,i)=>{
      const map = new Map();
      for (const row of (i===0 ? selectedRows : company.data?.[period] || [])) {
        const group = periodGroup(row.date,period);
        if (!groups.includes(group)) continue;
        if (map.has(group)) { map.set(group,null); ambiguous.push(company.ticker+' '+group); }
        else map.set(group,row);
      }
      if (i > 0 && monetary && (!currency || !company.data?.currency || company.data.currency !== currency)) {
        excluded.push(company.ticker);
        return new Map();
      }
      return map;
    });
    const rows = groups.map(group=>{
      const row = {date:group, periodEnds:{}};
      keys.forEach((key,i)=>{
        const reported = indexes[i].get(group);
        row[key] = finite(reported?.[metric]) ? reported[metric] : null;
        row.periodEnds[key] = reported?.date || null;
      });
      return row;
    });
    return {rows,keys,labels:companies.map(company=>company.ticker),comparison:true,excluded,ambiguous,
      periodLabel:period==='annual'?'Year of period end':'Calendar quarter of period end',
      unit:({money:currency || 'Reporting currency','per-share':(currency || 'Reporting currency')+'/share',shares:'shares',percent:'%',ratio:'x'})[kind]};
  }
  function priceSeries(data, mode) {
    let peak = null;
    return (data.labels || []).map((date,index)=>{
      const price = data.prices?.[index];
      let value = finite(price) ? price : null;
      if (mode === 'volume') value = finite(data.volumes?.[index]) ? data.volumes[index] : null;
      if (mode === 'drawdown') {
        if (finite(price) && price > 0) { peak = Math.max(peak || price, price); value = (price/peak-1)*100; }
        else value = null;
      }
      return {date,value};
    });
  }
  return {metrics,charts,finite,windowRows,values,priceSeries,comparisonPeers,compareStatements};
});
