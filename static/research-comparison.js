(() => {
  'use strict';
  const root=document.querySelector('main[data-symbol]');if(!root)return;
  const $=id=>document.getElementById(id), dialog=$('peerSearchDialog');
  let peers=[...$('peerSlots').querySelectorAll('[data-ticker]')].map(b=>({ticker:b.dataset.ticker,company:b.dataset.company}));
  let slot=0, timer, searchGeneration=0, comparisonGeneration=0, searching;
  const symbol=root.dataset.symbol;
  function slots() {
    $('peerSlots').replaceChildren();
    for(let index=0;index<Math.min(peers.length+1,4);index++) {
      const peer=peers[index],button=document.createElement('button');button.type='button';button.className='peer-slot'+(peer?'':' peer-slot-add');button.dataset.peerSlot=index;
      if(peer) {
        button.dataset.ticker=peer.ticker;button.dataset.company=peer.company;
        const label=document.createElement('span');label.className='peer-slot-label';label.textContent='Comparison '+(index+1);
        const ticker=document.createElement('strong');ticker.textContent=peer.ticker;
        const company=document.createElement('span');company.textContent=peer.company;
        const change=document.createElement('span');change.className='peer-slot-change';change.textContent='Change stock';button.append(label,ticker,company,change);
        button.setAttribute('aria-label','Replace '+peer.ticker+' comparison stock');
      } else {button.textContent='+ Add stock';}
      $('peerSlots').append(button);
    }
  }
  async function request(url,signal) {
    const response=await fetch(url,{signal}),data=await response.json();
    if(!response.ok)throw new Error(data.message||'Market data could not load. Please try again.');
    return data;
  }
  async function update(tickers) {
    const generation=++comparisonGeneration,controller=new AbortController();
    const timeout=setTimeout(()=>controller.abort(),40000);
    $('comparisonContents').setAttribute('aria-busy','true');$('comparisonStatus').textContent='Updating comparison and linked research...';
    $('peerSlots').querySelectorAll('button').forEach(b=>b.disabled=true);$('resetPeers').disabled=true;
    try {
      const url='/api/research/'+encodeURIComponent(symbol)+'/comparison'+(tickers===null?'':'?peers='+encodeURIComponent(tickers.join(',')));
      let data;
      do {
        data=await request(url,controller.signal);
        if(generation!==comparisonGeneration)return;
        if(data.status==='loading') {$('comparisonStatus').textContent=data.message;await new Promise(resolve=>setTimeout(resolve,1500));}
      } while(data.status==='loading'&&!controller.signal.aborted);
      if(data.status!=='ready')throw new Error('Data is taking longer than expected. Please try again.');
      $('comparisonContents').innerHTML=data.html;peers=data.peers;slots();
      window.dispatchEvent(new CustomEvent('ca-comparison-update',{detail:{peers:peers.map(peer=>({...peer}))}}));window.lucide?.createIcons();
      const urlState=new URL(location.href);if(tickers===null)urlState.searchParams.delete('peers');else urlState.searchParams.set('peers',peers.map(p=>p.ticker).join(','));
      history.replaceState(history.state,'',urlState.pathname+urlState.search+urlState.hash);
      $('comparisonStatus').textContent='Comparison updated. '+peers.map(p=>p.ticker).join(', ')+(peers.length?'.':' No comparison stocks selected.');
    } catch(error) {
      if(generation!==comparisonGeneration)return;
      $('comparisonStatus').textContent=(error.name==='AbortError'?'The market-data request timed out.':error.message)+' Your previous comparison is unchanged.';
    } finally {
      clearTimeout(timeout);if(generation===comparisonGeneration){$('comparisonContents').setAttribute('aria-busy','false');$('peerSlots').querySelectorAll('button').forEach(b=>b.disabled=false);$('resetPeers').disabled=false;}
    }
  }
  async function search(remote=false) {
    const query=$('peerSearchInput').value.trim(),generation=++searchGeneration;
    searching?.abort();searching=new AbortController();const controller=searching;
    $('peerSearchResults').replaceChildren();
    if(!query){$('peerSearchStatus').textContent='Search by ticker or company name.';return;}
    $('peerSearchStatus').textContent=remote?'Searching listed stocks...':'Searching covered companies...';
    const timeout=setTimeout(()=>controller.abort(),12000);
    try {
      const data=await request('/api/research/search?q='+encodeURIComponent(query)+(remote?'&remote=1':''),controller.signal);
      if(generation!==searchGeneration)return;
      const results=data.results.filter(p=>p.ticker!==symbol&&!peers.some((peer,index)=>index!==slot&&peer.ticker===p.ticker));
      for(const result of results) {
        const button=document.createElement('button');button.type='button';const strong=document.createElement('strong');strong.textContent=result.ticker;
        const name=document.createElement('span');name.textContent=result.company;
        const exchange=document.createElement('small');exchange.textContent=result.exchange;button.append(strong,name,exchange);
        button.addEventListener('click',()=>{const next=peers.map(p=>p.ticker);next[slot]=result.ticker;dialog.close();update(next);});
        $('peerSearchResults').append(button);
      }
      $('peerSearchStatus').textContent=results.length?results.length+' matching stocks.':remote?'No matching stock listings found. Try the exchange ticker.':'No covered companies found. Search all listed stocks for more results.';
    } catch(error) {
      if(generation===searchGeneration&&!controller.signal.aborted)$('peerSearchStatus').textContent=error.message;
      else if(generation===searchGeneration&&dialog.open)$('peerSearchStatus').textContent='Search timed out. Please try again.';
    } finally {clearTimeout(timeout);}
  }
  $('peerSlots').addEventListener('click',event=>{
    const button=event.target.closest('[data-peer-slot]');if(!button)return;
    slot=Number(button.dataset.peerSlot);$('peerSearchTitle').textContent=peers[slot]?'Replace '+peers[slot].ticker:'Add a comparison stock';
    $('peerSearchInput').value='';$('peerSearchResults').replaceChildren();$('peerSearchStatus').textContent='Search covered companies, or search all listed stocks.';
    $('removePeer').hidden=!peers[slot];dialog.showModal();$('peerSearchInput').focus();
  });
  $('peerSearchInput').addEventListener('input',()=>{clearTimeout(timer);++searchGeneration;searching?.abort();timer=setTimeout(()=>search(false),250);});
  $('peerSearchForm').addEventListener('submit',event=>{event.preventDefault();clearTimeout(timer);search(true);});
  $('removePeer').addEventListener('click',()=>{const next=peers.filter((_,index)=>index!==slot).map(p=>p.ticker);dialog.close();update(next);});
  $('resetPeers').addEventListener('click',()=>update(null));
  dialog.addEventListener('close',()=>{clearTimeout(timer);++searchGeneration;searching?.abort();});
  slots();
  const query=new URLSearchParams(location.search).get('peers');
  if(query!==null&&query.toUpperCase()!==peers.map(p=>p.ticker).join(','))update(query.split(',').map(s=>s.trim().toUpperCase()).filter(Boolean));
})();
