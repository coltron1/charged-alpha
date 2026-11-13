(() => {
  'use strict';
  const id = document.currentScript?.dataset.id;
  if (!id) return;
  window.dataLayer = window.dataLayer || [];
  window.gtag = window.gtag || function () { window.dataLayer.push(arguments); };
  window.gtag('js', new Date());
  // Reading-view selectors, campaign strings and fragment text are not sent.
  window.gtag('config', id, {
    page_location: location.origin + location.pathname,
    page_referrer: '',
  });
})();
