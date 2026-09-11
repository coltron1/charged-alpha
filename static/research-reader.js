(() => {
  'use strict';
  const nav = document.querySelector('nav.toc');
  const list = nav?.querySelector('div');
  if (!nav || !list) return;
  const entries = Array.from(list.querySelectorAll('a[href^="#"]')).flatMap(link => {
    let id;
    try { id = decodeURIComponent(link.hash.slice(1)); } catch { return []; }
    const target = document.getElementById(id);
    if (!target) return [];
    target.classList.add('packet-section-target');
    if (!target.hasAttribute('tabindex')) target.tabIndex = -1;
    return [{link, target}];
  });
  if (!entries.length) return;
  nav.setAttribute('aria-label', 'Research packet sections');
  const details = document.createElement('details');
  const summary = document.createElement('summary');
  const label = document.createElement('span');
  const current = document.createElement('span');
  label.textContent = 'Jump to section';
  current.className = 'packet-toc-current';
  summary.append(label, current);
  details.append(summary);
  const compact = window.matchMedia('(max-width: 959px)');
  let frame = 0;

  function offset() {
    // The expanded menu closes before a jump; measure its collapsed height.
    return (compact.matches ? summary.getBoundingClientRect().height : nav.getBoundingClientRect().height) + 16;
  }
  function update() {
    frame = 0;
    const top = offset();
    document.documentElement.style.setProperty('--packet-toc-offset', `${top}px`);
    let selected = entries[0];
    for (const entry of entries) {
      if (entry.target.getBoundingClientRect().top <= top + 24) selected = entry;
    }
    for (const entry of entries) {
      if (entry === selected) entry.link.setAttribute('aria-current', 'location');
      else entry.link.removeAttribute('aria-current');
    }
    current.textContent = selected.link.textContent;
  }
  function schedule() {
    if (!frame) frame = requestAnimationFrame(update);
  }
  function layout() {
    details.open = false;
    if (compact.matches) {
      details.append(list);
      nav.append(details);
    } else {
      nav.append(list);
      details.remove();
    }
    nav.dataset.readerReady = 'true';
    update();
  }
  nav.addEventListener('click', event => {
    const entry = entries.find(item => item.link === event.target.closest('a'));
    if (!entry || event.defaultPrevented || event.button !== 0 || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
    event.preventDefault();
    details.open = false;
    if (location.hash !== entry.link.hash) history.pushState(null, '', entry.link.hash);
    update();
    entry.target.focus({preventScroll: true});
    entry.target.scrollIntoView({block: 'start', behavior: 'instant'});
    schedule();
  });
  nav.addEventListener('keydown', event => {
    if (event.key === 'Escape' && details.open) {
      details.open = false;
      summary.focus();
    }
  });
  document.addEventListener('click', event => {
    if (!nav.contains(event.target)) details.open = false;
  });
  function restoreAnchor() {
    const entry = entries.find(item => item.link.hash === location.hash);
    if (entry) {
      details.open = false;
      update();
      entry.target.scrollIntoView({block: 'start', behavior: 'instant'});
    }
    schedule();
  }
  compact.addEventListener('change', layout);
  window.addEventListener('scroll', schedule, {passive: true});
  window.addEventListener('resize', schedule);
  window.addEventListener('hashchange', restoreAnchor);
  window.addEventListener('pageshow', restoreAnchor);
  if ('ResizeObserver' in window) new ResizeObserver(schedule).observe(nav);
  layout();
  restoreAnchor();
})();
