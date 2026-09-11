(() => {
  'use strict';

  const $ = id => document.getElementById(id);
  let preferences;
  let csrf;

  async function loadPreferences() {
    const response = await fetch('/api/alerts/preferences', {cache: 'no-store'});
    if (!response.ok) throw new Error('Email preferences are temporarily unavailable. Please retry.');
    preferences = await response.json();
    csrf = preferences.csrf;
    return preferences;
  }

  const ready = loadPreferences();
  ready.catch(() => {});

  function initialStocks() {
    const node = $('alert-initial-stocks');
    try {
      const stocks = JSON.parse(node?.textContent || '[]');
      return Array.isArray(stocks) ? stocks.filter(ticker => typeof ticker === 'string') : [];
    } catch {
      return [];
    }
  }

  async function post(path, body) {
    await ready;
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 15000);
    try {
      const response = await fetch(path, {
        method: 'POST',
        headers: {'Content-Type': 'application/json', 'X-CSRF-Token': csrf},
        body: JSON.stringify(body),
        signal: controller.signal,
      });
      const data = await response.json().catch(() => ({}));
      if (!response.ok) {
        throw new Error(data.error || (response.status === 401
          ? 'Your session expired. Request a new email link.'
          : 'Could not save this change. Refresh and try again.'));
      }
      return data;
    } finally {
      clearTimeout(timer);
    }
  }

  async function submit(form, status, action) {
    const buttons = [...form.querySelectorAll('button')];
    buttons.forEach(button => { button.disabled = true; });
    status.classList.remove('alert-error');
    status.textContent = 'Working...';
    try {
      await action();
    } catch (error) {
      status.classList.add('alert-error');
      status.textContent = error.name === 'AbortError'
        ? 'The request timed out. Please try again.'
        : error.message;
    } finally {
      buttons.forEach(button => { button.disabled = false; });
    }
  }

  if ($('stockAlertSignup')) {
    const form = $('stockAlertSignup');
    const status = $('alertSignupStatus');
    const source = form.dataset.alertSelection || 'following';
    const alertSource = form.dataset.alertSource || 'direct';
    const selectedStocks = () => source === 'requested'
      ? initialStocks()
      : (window.CAFollowing?.read() || []);

    function selection() {
      const stocks = selectedStocks();
      const selection = $('alertSelection');
      const requestButton = $('requestStockAlerts');
      if (source === 'requested') {
        selection.textContent = stocks.length
          ? `Selected email alert${stocks.length === 1 ? '' : 's'}: ${stocks.join(', ')}.`
          : 'Choose a stock before requesting email alerts.';
      } else {
        selection.textContent = stocks.length
          ? `Email stocks: ${stocks.join(', ')}`
          : 'No stocks selected. Save a stock to Following first.';
      }
      requestButton.disabled = !stocks.length || stocks.length > 50;
      if (stocks.length > 50) {
        selection.textContent = 'Email alerts support up to 50 stocks. Manage a smaller email list from Email Alerts.';
      }
    }

    selection();
    if (source === 'following') window.addEventListener('ca-follow-change', selection);
    form.addEventListener('submit', event => {
      event.preventDefault();
      submit(form, status, async () => {
        const tickers = selectedStocks();
        if (!tickers.length || tickers.length > 50) throw new Error('Choose between 1 and 50 stocks.');
        await post('/api/alerts/request', {
          email: $('alertEmail').value,
          tickers,
          consent: $('alertConsent').checked,
          website: form.elements.website?.value || '',
          source: alertSource,
        });
        status.textContent = 'Check your inbox for a confirmation link. Your email selection changes only after you confirm.';
      });
    });
  }

  if ($('alertAccessForm')) {
    const form = $('alertAccessForm');
    const status = $('alertAccessStatus');
    form.addEventListener('submit', event => {
      event.preventDefault();
      submit(form, status, async () => {
        await post('/api/alerts/request', {email: $('accessEmail').value, action: 'access'});
        status.textContent = 'If this address has confirmed stock alerts, a sign-in link will arrive shortly. Check your spam folder too.';
      });
    });
  }

  if ($('alertManageForm')) {
    const form = $('alertManageForm');
    const status = $('alertManageStatus');
    let selected = initialStocks();
    const selectionSources = Object.fromEntries(selected.map(ticker => [ticker, 'manage']));
    let stocks = [];
    const suggestedButtons = [...document.querySelectorAll('[data-alert-add-stock]')];

    function addStock(ticker, source = 'manage') {
      if (!ticker || selected.includes(ticker) || selected.length >= 50) return;
      selected.push(ticker);
      selectionSources[ticker] = source;
      selected.sort();
      draw();
      search();
    }

    function draw() {
      const holder = $('alertSelectedStocks');
      holder.replaceChildren();
      for (const ticker of selected) {
        const button = document.createElement('button');
        button.type = 'button';
        button.setAttribute('aria-label', `Remove ${ticker} from emailed stocks`);
        button.textContent = ticker;
        const icon = document.createElement('i');
        icon.dataset.lucide = 'x';
        icon.setAttribute('aria-hidden', 'true');
        button.append(icon);
        button.addEventListener('click', () => {
          selected = selected.filter(value => value !== ticker);
          delete selectionSources[ticker];
          draw();
          search();
        });
        holder.append(button);
      }
      $('alertSelectedCount').textContent = `${selected.length} of 50 stocks selected`;
      suggestedButtons.forEach(button => {
        const ticker = button.dataset.alertAddStock;
        const added = selected.includes(ticker);
        button.disabled = added || selected.length >= 50;
        button.textContent = added ? `${ticker} added - save preferences` : `Add ${ticker} to emailed stocks`;
      });
      window.lucide?.createIcons();
    }

    function search() {
      const query = $('alertStockSearch').value.trim().toLowerCase();
      const holder = $('alertStockResults');
      holder.replaceChildren();
      if (!query) return;
      const matches = stocks.filter(stock => !selected.includes(stock.ticker)
        && (stock.ticker.toLowerCase().includes(query) || stock.company.toLowerCase().includes(query)))
        .slice(0, 8);
      if (!matches.length) {
        holder.textContent = 'No matching stocks in the research library.';
        return;
      }
      for (const stock of matches) {
        const button = document.createElement('button');
        button.type = 'button';
        button.textContent = `Add ${stock.ticker} - ${stock.company}`;
        button.disabled = selected.length >= 50;
        button.addEventListener('click', () => addStock(stock.ticker));
        holder.append(button);
      }
    }

    suggestedButtons.forEach(button => {
      button.addEventListener('click', () => addStock(button.dataset.alertAddStock, button.dataset.alertAddSource || 'manage'));
    });
    draw();
    fetch('/api/shows/stocks')
      .then(response => {
        if (!response.ok) throw new Error();
        return response.json();
      })
      .then(data => {
        stocks = data.stocks;
        search();
      })
      .catch(() => {
        status.textContent = 'Stock search could not load. Refresh to retry. Your saved selection is unchanged.';
        status.classList.add('alert-error');
      });
    $('alertStockSearch').addEventListener('input', search);
    form.addEventListener('submit', event => {
      event.preventDefault();
      submit(form, status, async () => {
        await post('/api/alerts/preferences', {tickers: selected, ticker_sources: selectionSources});
        status.textContent = 'Email preferences saved. Browser bookmarks are unchanged.';
      });
    });
    $('alertUnsubscribe').addEventListener('click', () => submit(form, status, async () => {
      await post('/api/alerts/preferences', {action: 'unsubscribe'});
      form.hidden = true;
      status.textContent = 'Unsubscribed. No further stock alerts will be sent; an email already being delivered may still arrive.';
    }));
    $('alertSignOut').addEventListener('click', () => submit(form, status, async () => {
      await post('/api/alerts/logout', {});
      location.assign('/alerts');
    }));
  }
})();
