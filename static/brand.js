(() => {
  'use strict';
  const key = 'chargedalpha.following.v1';
  const read = () => { try { const v = JSON.parse(localStorage.getItem(key) || '[]'); return Array.isArray(v) ? [...new Set(v.filter(s => typeof s === 'string' && /^[A-Z0-9.-]{1,24}$/.test(s)))].slice(0,300) : []; } catch { return []; } };
  function toast(message) { let node = document.getElementById('ca-toast'); if (!node) { node = document.createElement('div'); node.id = 'ca-toast'; node.className = 'ca-toast'; node.setAttribute('role','status'); document.body.append(node); } node.textContent=message; node.hidden=false; clearTimeout(window.caToastTimer); window.caToastTimer=setTimeout(()=>{node.hidden=true;},4500); }
  function update() { const saved=read(); document.querySelectorAll('[data-follow-stock]').forEach(b=>{const active=saved.includes(b.dataset.followStock);b.setAttribute('aria-pressed',String(active));b.setAttribute('aria-label',`${active?'Unfollow':'Follow'} ${b.dataset.followStock}`);b.title=`${active?'Unfollow':'Follow'} ${b.dataset.followStock}`;const text=b.querySelector('[data-follow-label]');if(text)text.textContent=active?'Following':'Follow This Stock';});document.querySelectorAll('.ca-follow-count').forEach(n=>{n.textContent=saved.length;n.hidden=!saved.length;}); window.lucide?.createIcons(); }
  window.CAFollowing={read,update,toast};
  document.addEventListener('click',e=>{const b=e.target.closest('[data-follow-stock]');if(!b)return;const ticker=b.dataset.followStock;if(!/^[A-Z0-9.-]{1,24}$/.test(ticker))return;const saved=read(),active=saved.includes(ticker);if(!active&&saved.length>=300){toast('Your Following list has 300 stocks. Remove one before adding another.');return;}try{localStorage.setItem(key,JSON.stringify(active?saved.filter(s=>s!==ticker):[...saved,ticker]));}catch{toast('This browser could not save your list. Check its storage settings.');return;}update();window.dispatchEvent(new Event('ca-follow-change'));toast(active?`${ticker} removed from Following.`:`${ticker} saved to Following on this browser. No emails are sent.`);});
  function refreshFollowing(){update();window.dispatchEvent(new Event('ca-follow-change'));}
  window.addEventListener('storage',e=>{if(e.key===key||e.key===null)refreshFollowing();});
  window.addEventListener('pageshow',refreshFollowing);
  const button=document.querySelector('.ca-menu-button'), nav=document.getElementById('ca-nav');
  function closeMenu(){nav?.classList.remove('is-open');button?.setAttribute('aria-expanded','false');button?.setAttribute('aria-label','Open navigation');document.querySelectorAll('.ca-dropdown[open]').forEach(d=>d.open=false);}
  button?.addEventListener('click',()=>{const open=nav.classList.toggle('is-open');button.setAttribute('aria-expanded',String(open));button.setAttribute('aria-label',open?'Close navigation':'Open navigation');});
  document.addEventListener('click',e=>{if(!e.target.closest('.ca-header'))closeMenu();document.querySelectorAll('.ca-dropdown[open]').forEach(d=>{if(!d.contains(e.target))d.open=false;});});
  document.addEventListener('keydown',e=>{if(e.key==='Escape'){const focus=nav?.contains(document.activeElement);closeMenu();if(focus)button?.focus();}});
  document.querySelectorAll('.ca-dropdown').forEach(d=>d.addEventListener('toggle',()=>{if(d.open)document.querySelectorAll('.ca-dropdown').forEach(other=>{if(other!==d)other.open=false;});}));
  update();
})();
