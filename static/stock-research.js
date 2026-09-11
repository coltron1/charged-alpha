(() => {
  'use strict';
  const root = document.querySelector('main[data-symbol]');
  if (!root) return;
  document.querySelector('[data-youtube-embed]')?.addEventListener('click', function () {
    const frame = document.createElement('iframe');
    frame.src = this.dataset.youtubeEmbed + '?autoplay=1';
    frame.title = this.getAttribute('aria-label');
    frame.allow = 'autoplay; encrypted-media; picture-in-picture; fullscreen';
    frame.allowFullscreen = true;
    frame.referrerPolicy = 'strict-origin-when-cross-origin';
    this.replaceWith(frame);
  });
  const returnLink = document.querySelector('[data-search-return]');
  try {
    const previous = sessionStorage.getItem('ca.last-search');
    if (previous && /^\/(?:shows)?\?(?:[^#]*)#searchInput$/.test(previous)) returnLink.href = previous;
  } catch {}
  const $ = id => document.getElementById(id);
  function revealPeriod() {
    let id;
    try { id = decodeURIComponent(location.hash.slice(1)); } catch { return; }
    const period = document.getElementById(id);
    if (period?.matches('details.archive-period')) period.open = true;
  }
  revealPeriod();
  window.addEventListener('hashchange', revealPeriod);
  const filterMetrics = () => document.querySelectorAll('[data-metric-group]').forEach(row => {
    row.hidden = $('metricGroup').value !== 'all' && row.dataset.metricGroup !== $('metricGroup').value;
  });
  $('metricGroup').addEventListener('change', filterMetrics);
  window.addEventListener('ca-comparison-update', filterMetrics);
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

})();
