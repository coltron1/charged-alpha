(() => {
  'use strict';
  const id = document.currentScript?.dataset.id;
  if (!id) return;
  window.dataLayer = window.dataLayer || [];
  window.gtag = window.gtag || function () { window.dataLayer.push(arguments); };
  window.gtag('js', new Date());
  // Neither the confirmation URL nor the prior page is sent to Google.
  const cleanPage = 'https://chargedalpha.com/confirmation/alerts';
  window.gtag('config', id, {send_page_view: false, page_location: cleanPage, page_referrer: ''});
  window.gtag('event', 'email_alert_signup_confirmed', {page_location: cleanPage, page_referrer: ''});
})();
