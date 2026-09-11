(() => {
  'use strict';
  const root = document.querySelector('main[data-symbol]'), model = window.CAChartModel;
  if (!root || !model) return;
  const $ = id => document.getElementById(id), {metrics, finite} = model;
  const symbol = encodeURIComponent(root.dataset.symbol);
  const colors = ['#9edcca','#eccc7e','#82b8ed','#ef9ea9'];
  const charts = new Map(), views = new Map();
  const peerData = new Map(), peerJobs = new Map(), peerErrors = new Map();
  let availablePeers = [...$('peerSlots').querySelectorAll('[data-ticker]')].map(button=>({ticker:button.dataset.ticker,company:button.dataset.company}));
  let chartPeers = model.comparisonPeers(availablePeers,null,root.dataset.symbol);
  let expandedId;
  let statements, financialLoading = false, priceData, priceGeneration = 0, expandedChart;
  const comparing = () => $('compareFinancials').checked;
  const selected = name => document.querySelector(`input[name="${name}"]:checked`).value;
  const currency = () => statements?.currency || root.dataset.financialCurrency || 'Reporting currency';
  const unit = key => ({money:currency(), 'per-share':currency()+'/share', shares:'shares', percent:'%', ratio:'x'})[metrics[key][1]];
  const format = (value, units, compact = true) => {
    if (!finite(value)) return '\u2014';
    const text = value.toLocaleString('en-US', {notation:compact?'compact':'standard',maximumFractionDigits:2});
    return units === '%' || units === 'x' ? text+units : text;
  };
  async function request(url, signal) {
    const controller = new AbortController(), timer = setTimeout(()=>controller.abort(),15000);
    const abort = () => controller.abort();
    if (signal?.aborted) controller.abort();
    signal?.addEventListener('abort',abort,{once:true});
    try {
      const response = await fetch(url,{signal:controller.signal}), data = await response.json();
      if (!response.ok) throw new Error(data.message || data.error || 'Data is temporarily unavailable.');
      return data;
    } finally { clearTimeout(timer); signal?.removeEventListener('abort',abort); }
  }
  function draw(canvas, view) {
    if (!window.Chart) return null;
    return new Chart(canvas, {
      type:view.type,
      data:{labels:view.rows.map(r=>r.date),datasets:view.keys.map((key,index)=>({
        label:view.labels[index],data:model.values(view.rows,key),borderColor:colors[index%colors.length],
        backgroundColor:colors[index%colors.length]+(view.type==='bar'?'c4':'26'),
        borderWidth:view.type==='bar'?1:2,pointRadius:view.rows.length>40?0:3,
        spanGaps:false,tension:0,maxBarThickness:50,
      }))},
      options:{responsive:true,maintainAspectRatio:false,animation:false,
        interaction:{mode:'index',intersect:false},
        plugins:{legend:{display:view.keys.length>1,position:'bottom',labels:{color:'#c7d2cc',boxWidth:10,boxHeight:10,padding:16,font:{size:11}}},
          tooltip:{callbacks:{label:ctx=>{
            const date=view.rows[ctx.dataIndex]?.periodEnds?.[view.keys[ctx.datasetIndex]];
            return `${ctx.dataset.label}: ${format(ctx.raw,view.unit,false)} ${view.unit==='%'||view.unit==='x'?'':view.unit}${date?' | ended '+date:''}`;
          }}}},
        scales:{x:{title:{display:!!view.comparison,text:view.periodLabel,color:'#aab7ba'},ticks:{color:'#aab7ba',maxTicksLimit:7,maxRotation:45},grid:{display:false}},
          y:{beginAtZero:view.type==='bar',title:{display:true,text:view.unit,color:'#aab7ba'},ticks:{color:'#aab7ba',callback:v=>format(v,view.unit)},grid:{color:'#303635'}}}},
    });
  }
  function table(container, view) {
    const element = document.createElement('table');
    element.createCaption().textContent = view.title + ' (' + view.unit + ')';
    const head = element.createTHead().insertRow();
    [view.periodLabel || 'Period ended',...view.labels].forEach(text=>{const th=document.createElement('th');th.scope='col';th.textContent=text;head.append(th);});
    const body=element.createTBody();
    view.rows.forEach(row=>{const tr=body.insertRow();const th=document.createElement('th');th.scope='row';th.textContent=row.date;tr.append(th);view.keys.forEach(key=>{
      const cell=tr.insertCell();cell.textContent=format(row[key],view.unit,false);
      if(row.periodEnds?.[key]) {const date=document.createElement('small');date.className='chart-period-date';date.textContent='Ended '+row.periodEnds[key];cell.append(date);}
    });});
    container.replaceChildren(element);
  }
  function renderView(id, view) {
    charts.get(id)?.destroy(); charts.delete(id);
    const hasData=view.rows.some(r=>view.keys.some(key=>finite(r[key])));
    views.set(id,view);
    const frame=$(id+'Frame'), empty=$(id+'Empty'), expand=document.querySelector(`[data-expand-chart="${id}"]`);
    frame.hidden=!hasData || !window.Chart;
    empty.hidden=hasData && !!window.Chart;
    empty.textContent=!hasData?'No reported values for this view. Missing data is not zero.':'Chart unavailable. Reported figures are available below.';
    if (expand) expand.disabled=!hasData;
    if(hasData) { if(window.Chart)charts.set(id,draw($(id),view)); table($(id+'Table'),view); }
    else $(id+'Table').replaceChildren();
    $(id+'TableWrap').hidden=!hasData;
    $(id)?.setAttribute('aria-label',`${view.title}. ${view.unit}. ${view.rows[0]?.date || ''} to ${view.rows.at(-1)?.date || ''}. Reported figures below.`);
    if(expandedId===id && $('expandedChartDialog').open) refreshExpanded(view);
  }
  function financialView(config, rows) {
    if(comparing()) {
      const metric=config.id ? $(config.id+'CompareMetric').value : config.keys[0];
      return {...config,title:config.id?config.title+': '+metrics[metric][0]:config.title,
        ...model.compareStatements({ticker:root.dataset.symbol,data:statements},chartPeers.map(ticker=>({ticker,data:peerData.get(ticker)})),metric,selected('statementPeriod'),Number(selected('financialRange')))};
    }
    return {...config,rows,labels:config.keys.map(key=>metrics[key][0]),unit:unit(config.keys[0])};
  }
  function renderFinancials() {
    if (!statements) return;
    const period=selected('statementPeriod'), years=Number(selected('financialRange'));
    const rows=model.windowRows(statements[period]||[],years,period), metric=$('statementMetric').value;
    $('financialResults').hidden=false;
    $('financialStatus').textContent=`${rows.length} available ${period} period${rows.length===1?'':'s'} in the ${years}-year window${rows.length?`, ${rows[0].date} to ${rows.at(-1).date}`:''}. Fetched ${statements.fetched_at.slice(0,10)}. Missing values stay blank.`;
    $('financialSource').textContent=statements.source || 'Yahoo Finance';
    $('financialSource').href=statements.source_url;
    $('financialSourceLine').hidden=false;
    $('financialMethodology').textContent=statements.note || '';
    $('financialChartTitle').textContent=metrics[metric][0];
    renderView('financialChart',financialView({title:metrics[metric][0],keys:[metric],type:selected('financialStyle')},rows));
    model.charts.forEach(config=>{
      $(config.id+'CompareControl').hidden=!comparing();
      renderView(config.id+'Chart',financialView(config,rows));
    });
    comparisonStatus();
  }
  function comparisonStatus() {
    $('financialCompareOptions').hidden=!comparing();
    if(!comparing())return;
    const loading=chartPeers.filter(ticker=>peerJobs.has(ticker));
    const failed=chartPeers.filter(ticker=>peerErrors.has(ticker));
    $('financialCompareStatus').textContent=!chartPeers.length?'No chart comparison stocks selected.':
      loading.length?'Loading '+loading.join(', ')+' financial statements...':
      failed.length?'Some comparison data is unavailable. Available series remain visible.':
      'Comparing '+[root.dataset.symbol,...chartPeers].join(', ')+'.';
    $('retryFinancialPeers').hidden=!failed.length;
    const list=$('financialCompareSources');list.replaceChildren();
    for(const ticker of chartPeers) {
      const data=peerData.get(ticker),li=document.createElement('li');
      if(data) {
        const note=document.createElement('span');note.textContent=ticker+': '+(data.currency || 'Unknown reporting currency')+'; fetched '+(data.fetched_at || '').slice(0,10)+'. ';
        li.append(note);
        const source=document.createElement('a');source.textContent=data.source || 'Statement source';
        if(/^https:\/\//.test(data.source_url || '')){source.href=data.source_url;source.target='_blank';source.rel='noopener noreferrer';}
        li.append(source);
        if(!data.currency || !statements?.currency || data.currency!==statements.currency)li.append(' Currency amounts excluded: reporting currencies differ or are unknown. Ratios and share counts remain available.');
        const period=selected('statementPeriod'),years=Number(selected('financialRange'));
        const check=model.compareStatements({ticker:root.dataset.symbol,data:statements},[{ticker,data}],'shares',period,years);
        if(!check.rows.some(row=>row.periodEnds.company1))li.append(' No matching reported periods in this window.');
        if(data.note){const methodology=document.createElement('details'),summary=document.createElement('summary'),text=document.createElement('p');summary.textContent=ticker+' data notes';text.textContent=data.note;methodology.append(summary,text);li.append(methodology);}
      } else li.textContent=ticker+': '+(peerErrors.get(ticker) || 'Loading financial statements...');
      list.append(li);
    }
    const ambiguous=new Set([...views.values()].filter(view=>view.comparison).flatMap(view=>view.ambiguous));
    if(ambiguous.size){const li=document.createElement('li');li.textContent='Excluded ambiguous period groups (multiple reports): '+[...ambiguous].join(', ')+'.';list.append(li);}
  }
  function comparisonChoices(focusTicker) {
    const container=$('financialComparePeers');container.replaceChildren();
    for(const peer of availablePeers) {
      const label=document.createElement('label'),input=document.createElement('input'),text=document.createElement('span');
      input.type='checkbox';input.value=peer.ticker;input.checked=chartPeers.includes(peer.ticker);input.disabled=!input.checked&&chartPeers.length>=2;
      text.textContent=peer.ticker;label.title=peer.company;label.append(input,text);container.append(label);
      input.addEventListener('change',()=>{chartPeers=input.checked?[...chartPeers,peer.ticker].slice(0,2):chartPeers.filter(ticker=>ticker!==peer.ticker);comparisonChoices(peer.ticker);syncPeerFinancials();});
      if(peer.ticker===focusTicker)input.focus();
    }
  }
  async function loadPeerFinancials(ticker) {
    const controller=new AbortController();peerJobs.set(ticker,controller);comparisonStatus();
    try {
      const deadline=Date.now()+40000;
      while(Date.now()<deadline && !controller.signal.aborted) {
        const data=await request('/api/research/'+encodeURIComponent(ticker)+'/financials?comparison=1',controller.signal);
        if(controller.signal.aborted || peerJobs.get(ticker)!==controller)return;
        if(data.status==='ready') {peerData.set(ticker,data);return;}
        await new Promise(resolve=>setTimeout(resolve,1500));
      }
      if(!controller.signal.aborted)throw new Error('The provider is taking longer than expected. Try again later.');
    } catch(error) {
      if(!controller.signal.aborted && peerJobs.get(ticker)===controller)peerErrors.set(ticker,error.name==='AbortError'?'Request timed out. Please retry.':error.message);
    } finally {
      if(peerJobs.get(ticker)===controller){peerJobs.delete(ticker);renderFinancials();comparisonStatus();}
    }
  }
  function syncPeerFinancials() {
    for(const [ticker,controller] of peerJobs) {
      if(!comparing() || !chartPeers.includes(ticker)){controller.abort();peerJobs.delete(ticker);}
    }
    if(comparing())for(const ticker of chartPeers) {
      if(!peerData.has(ticker)&&!peerJobs.has(ticker)&&!peerErrors.has(ticker))loadPeerFinancials(ticker);
    }
    if(statements)renderFinancials();
    comparisonStatus();
  }
  $('compareFinancials').addEventListener('change',()=>{syncPeerFinancials();if(!statements&&!financialLoading)loadFinancials();});
  $('retryFinancialPeers').addEventListener('click',()=>{chartPeers.forEach(ticker=>peerErrors.delete(ticker));syncPeerFinancials();});
  window.addEventListener('ca-comparison-update',event=>{
    const next=event.detail?.peers;if(!Array.isArray(next))return;
    chartPeers=model.comparisonPeers(next,availablePeers.length?chartPeers:null,root.dataset.symbol);
    availablePeers=next;comparisonChoices();syncPeerFinancials();
  });
  comparisonChoices();
  for(const [key,[label]] of Object.entries(metrics)) {
    const option=document.createElement('option');option.value=key;option.textContent=label;$('statementMetric').append(option);
  }
  for(const config of model.charts) {
    const section=document.createElement('section');section.className='advanced-chart';section.setAttribute('aria-labelledby',config.id+'Title');
    section.innerHTML=`<div class="chart-title-row"><h3 id="${config.id}Title"></h3><button type="button" class="chart-icon-button" data-expand-chart="${config.id}Chart" disabled><i data-lucide="maximize-2" aria-hidden="true"></i></button></div><p class="chart-explanation"></p><div class="chart-frame advanced-frame" id="${config.id}ChartFrame"><canvas id="${config.id}Chart" role="img"></canvas></div><p id="${config.id}ChartEmpty" class="chart-empty" hidden></p><details id="${config.id}ChartTableWrap"><summary>Reported figures</summary><div id="${config.id}ChartTable" class="table-scroll" tabindex="0"></div></details>`;
    section.querySelector('h3').textContent=config.title;
    section.querySelector('.chart-explanation').textContent=config.note;
    const control=document.createElement('label');control.className='chart-compare-metric';control.id=config.id+'CompareControl';control.hidden=true;control.textContent='Measure';
    const select=document.createElement('select');select.id=config.id+'CompareMetric';select.setAttribute('aria-label',config.title+' comparison measure');
    config.keys.forEach(key=>{const option=document.createElement('option');option.value=key;option.textContent=metrics[key][0];select.append(option);});
    if(config.id==='cashflow')select.value='free_cashflow';
    select.addEventListener('change',renderFinancials);control.append(select);section.querySelector('.chart-explanation').after(control);
    const button=section.querySelector('button');button.title='Expand '+config.title;button.setAttribute('aria-label',button.title);
    $('advancedCharts').append(section);
  }
  window.lucide?.createIcons();
  async function loadFinancials() {
    if(financialLoading)return;
    financialLoading=true;$('loadFinancials').disabled=true;$('financials').setAttribute('aria-busy','true');
    $('financialStatus').textContent='Loading reported financial statements...';
    try {
      const deadline=Date.now()+40000;
      while(Date.now()<deadline) {
        const data=await request('/api/research/'+symbol+'/financials');
        if(data.status==='ready') {statements={...data,currency:data.currency || root.dataset.financialCurrency || null};renderFinancials();$('loadFinancials').hidden=true;return;}
        await new Promise(resolve=>setTimeout(resolve,1500));
      }
      throw new Error('The provider is taking longer than expected. Try again later.');
    } catch(error) {$('financialStatus').textContent=error.name==='AbortError'?'Financial data timed out. Please try again.':error.message;$('loadFinancials').textContent='Retry financial trends';}
    finally {financialLoading=false;$('loadFinancials').disabled=false;$('financials').setAttribute('aria-busy','false');}
  }
  $('loadFinancials').addEventListener('click',loadFinancials);
  $('statementMetric').addEventListener('change',()=>statements?renderFinancials():loadFinancials());
  document.querySelectorAll('[name="statementPeriod"],[name="financialRange"],[name="financialStyle"]').forEach(input=>input.addEventListener('change',()=>statements?renderFinancials():loadFinancials()));
  function renderPrice() {
    if(!priceData)return;
    const mode=$('priceMetric').value, points=model.priceSeries(priceData,mode);
    const volumeUnit=priceData.interval==='1wk'?'shares/week':'shares/day';
    const label={price:'Adjusted closing price',volume:'Trading volume',drawdown:'Drawdown within selected window'}[mode];
    const units=mode==='volume'?volumeUnit:mode==='drawdown'?'%':root.dataset.currency;
    renderView('priceChart',{title:label,keys:['value'],labels:[label],unit:units,rows:points,type:mode==='volume'?'bar':'line'});
    $('priceChartTitle').textContent=label;
    const valid=points.filter(p=>finite(p.value));
    $('priceStatus').textContent=valid.length?`Yahoo Finance, ${valid[0].date} to ${valid.at(-1).date}. ${mode==='drawdown'?'Decline from the running high within this window, not the all-time high.':mode==='volume'?'Volume aggregated per '+(priceData.interval==='1wk'?'week.':'day.'):'Split- and dividend-adjusted close. Not a live quote.'}`:'No reported values for this price view.';
  }
  async function loadPrice() {
    const generation=++priceGeneration;
    priceData=null;charts.get('priceChart')?.destroy();charts.delete('priceChart');views.delete('priceChart');
    $('priceChartFrame').hidden=true;$('priceChartTableWrap').hidden=true;$('expandPrice').disabled=true;$('priceChartEmpty').hidden=true;
    $('loadPrice').hidden=false;$('loadPrice').disabled=true;$('priceStatus').textContent='Loading price history...';
    try {
      const data=await request('/screener/api/stock/'+symbol+'/chart?range='+selected('priceRange'));
      if(generation!==priceGeneration)return;
      if(!data.labels?.length)throw new Error('Price history is unavailable for this listing.');
      priceData=data;renderPrice();$('loadPrice').hidden=true;
    } catch(error) {if(generation!==priceGeneration)return;$('priceStatus').textContent=error.name==='AbortError'?'Price history timed out. Please try again.':error.message;$('loadPrice').textContent='Retry price history';}
    finally {if(generation===priceGeneration)$('loadPrice').disabled=false;}
  }
  $('loadPrice').addEventListener('click',loadPrice);
  document.querySelectorAll('[name="priceRange"]').forEach(input=>input.addEventListener('change',loadPrice));
  $('priceMetric').addEventListener('change',()=>priceData?renderPrice():loadPrice());
  const dialog=$('expandedChartDialog');
  function refreshExpanded(view) {
    expandedChart?.destroy();
    $('expandedChartTitle').textContent=view.title;
    $('expandedChartPeriod').textContent=`${view.rows[0]?.date || ''} to ${view.rows.at(-1)?.date || ''} | ${view.unit}${view.comparison?' | '+view.labels.join(', '):''}`;
    expandedChart=draw($('expandedChart'),view);table($('expandedChartTable'),view);
  }
  document.addEventListener('click',event=>{
    const button=event.target.closest('[data-expand-chart]');
    if(!button)return;
    const view=views.get(button.dataset.expandChart);if(!view)return;
    expandedId=button.dataset.expandChart;dialog.showModal();refreshExpanded(view);
  });
  dialog.addEventListener('close',()=>{expandedChart?.destroy();expandedChart=null;expandedId=null;});
  if('IntersectionObserver' in window) {
    const observer=new IntersectionObserver(entries=>entries.forEach(entry=>{
      if(!entry.isIntersecting)return;observer.unobserve(entry.target);
      if(entry.target.id==='financials')loadFinancials();else loadPrice();
    }),{rootMargin:'100px'});
    observer.observe($('financials'));observer.observe($('valuation'));
  }
})();
