(() => {
  'use strict';
  const root = document.querySelector('main[data-symbol]');
  if (!root) return;
  const $ = id => document.getElementById(id);
  const symbol = encodeURIComponent(root.dataset.symbol);
  const finite = value => typeof value === 'number' && Number.isFinite(value);
  const compact = value => finite(value) ? new Intl.NumberFormat('en-US', {notation:'compact', maximumFractionDigits:2}).format(value) : '\u2014';
  const filterMetrics = () => document.querySelectorAll('[data-metric-group]').forEach(row => {
    row.hidden = $('metricGroup').value !== 'all' && row.dataset.metricGroup !== $('metricGroup').value;
  });
  $('metricGroup').addEventListener('change', filterMetrics);
  filterMetrics();

  const scenario = () => {
    const eps = $('scenarioEPS'), pe = $('scenarioPE');
    const value = Number(eps.value) * Number(pe.value);
    $('scenarioResult').textContent = eps.value && pe.value && eps.checkValidity() && pe.checkValidity() && Number.isFinite(value) && value > 0
      ? root.dataset.currency + ' ' + value.toLocaleString('en-US', {maximumFractionDigits:2, minimumFractionDigits:2})
      : 'Enter positive EPS and P/E assumptions.';
  };
  ['scenarioEPS', 'scenarioPE'].forEach(id => $(id).addEventListener('input', scenario));
  scenario();

  async function request(url) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 15000);
    try {
      const response = await fetch(url, {signal:controller.signal});
      const data = await response.json();
      if (!response.ok) throw new Error(data.message || 'Data is temporarily unavailable. Please try again.');
      return data;
    } finally { clearTimeout(timer); }
  }
  function draw(id, labels, values, label, color) {
    if (!window.Chart) return null;
    return new Chart($(id), {
      type:'line',
      data:{labels, datasets:[{label, data:values, borderColor:color, backgroundColor:color, borderWidth:2, pointRadius:values.length > 50 ? 0 : 3, spanGaps:false, tension:0}]},
      options:{responsive:true, maintainAspectRatio:false, animation:false,
        plugins:{legend:{display:false}, tooltip:{callbacks:{label:context => label + ': ' + compact(context.raw)}}},
        scales:{x:{ticks:{color:'#aab7ba', maxTicksLimit:8}, grid:{color:'#283235'}},
          y:{ticks:{color:'#aab7ba', callback:value => compact(value)}, grid:{color:'#283235'}}}}
    });
  }
  let statements, financialChart, financialLoading = false;
  function renderStatements() {
    if (!statements) return;
    const rows = statements[$('statementPeriod').value] || [];
    const metric = $('statementMetric').value;
    const label = $('statementMetric').selectedOptions[0].textContent;
    const unit = metric === 'shares' ? 'shares' : metric === 'operating_margin' ? '%' : root.dataset.financialCurrency;
    if (financialChart) financialChart.destroy();
    $('financialChartFrame').hidden = !window.Chart;
    financialChart = draw('financialChart', rows.map(r => r.date), rows.map(r => r[metric]), label + ' (' + unit + ')', '#70ddcf');
    $('financialStatus').textContent = rows.length + ' available ' + ($('statementPeriod').value === 'annual' ? 'annual' : 'quarterly') +
      ' periods. ' + label + ' (' + unit + '). Yahoo Finance, fetched ' + statements.fetched_at.slice(0, 10) + '.' +
      (!window.Chart ? ' Chart unavailable; reported figures are below.' : '');
    const table = document.createElement('table');
    const caption = table.createCaption();
    caption.textContent = label + ' (' + unit + ')';
    const head = table.createTHead().insertRow();
    ['Period ended', label].forEach(text => { const cell = document.createElement('th'); cell.scope = 'col'; cell.textContent = text; head.append(cell); });
    const body = table.createTBody();
    rows.forEach(row => {
      const tr = body.insertRow();
      tr.insertCell().textContent = row.date;
      tr.insertCell().textContent = finite(row[metric]) ? row[metric].toLocaleString('en-US', {maximumFractionDigits:2}) : '\u2014';
    });
    $('financialTable').replaceChildren(table);
    $('financialTableWrap').hidden = false;
  }
  async function loadFinancials() {
    if (financialLoading) return;
    financialLoading = true;
    $('loadFinancials').disabled = true;
    $('financialStatus').textContent = 'Loading reported financial statements...';
    try {
      const deadline = Date.now() + 35000;
      while (Date.now() < deadline) {
        const data = await request('/api/research/' + symbol + '/financials');
        if (data.status === 'ready') {
          statements = data; renderStatements(); $('loadFinancials').hidden = true; return;
        }
        await new Promise(resolve => setTimeout(resolve, 1500));
      }
      throw new Error('The data provider is taking longer than expected. Try again later.');
    } catch (error) {
      $('financialStatus').textContent = error.name === 'AbortError' ? 'The request timed out. Please try again.' : error.message;
      $('loadFinancials').textContent = 'Retry financial trends';
    } finally { financialLoading = false; $('loadFinancials').disabled = false; }
  }
  $('loadFinancials').addEventListener('click', loadFinancials);
  ['statementPeriod', 'statementMetric'].forEach(id => $(id).addEventListener('change', renderStatements));
  let priceChart, priceGeneration = 0;
  async function loadPrice() {
    const generation = ++priceGeneration;
    $('loadPrice').disabled = true;
    $('priceStatus').textContent = 'Loading price history...';
    try {
      const data = await request('/screener/api/stock/' + symbol + '/chart?range=' + $('priceRange').value);
      if (generation !== priceGeneration) return;
      const points = (data.labels || []).map((date, index) => ({date, price:(data.prices || [])[index]})).filter(p => finite(p.price));
      if (!points.length) throw new Error('Price history is unavailable for this listing.');
      if (!window.Chart) throw new Error('The chart library could not load. Please reload the page.');
      if (priceChart) priceChart.destroy();
      priceChart = draw('priceChart', points.map(p => p.date), points.map(p => p.price), root.dataset.currency + ' adjusted close', '#f2c96d');
      $('priceStatus').textContent = 'Yahoo Finance adjusted closing prices (' + root.dataset.currency + '), ' + points[0].date + ' to ' + points[points.length - 1].date + '. Not a live quote.';
      $('loadPrice').hidden = true;
    } catch (error) {
      if (generation !== priceGeneration) return;
      $('priceStatus').textContent = error.name === 'AbortError' ? 'Price history timed out. Please try again.' : error.message;
      $('loadPrice').hidden = false;
      $('loadPrice').textContent = 'Retry price history';
    } finally { if (generation === priceGeneration) $('loadPrice').disabled = false; }
  }
  $('loadPrice').addEventListener('click', loadPrice);
  $('priceRange').addEventListener('change', loadPrice);
  if ('IntersectionObserver' in window) {
    const observer = new IntersectionObserver(entries => entries.forEach(entry => {
      if (!entry.isIntersecting) return;
      observer.unobserve(entry.target);
      if (entry.target.id === 'financials') loadFinancials(); else loadPrice();
    }), {rootMargin:'100px'});
    observer.observe($('financials'));
    observer.observe($('valuation'));
  }
})();
