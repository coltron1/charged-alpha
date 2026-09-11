(() => {
  'use strict';

  const key = 'chargedalpha.following.v1';
  const tickerPattern = /^[A-Z0-9.-]{1,24}$/;
  const read = () => {
    try {
      const value = JSON.parse(localStorage.getItem(key) || '[]');
      return Array.isArray(value)
        ? [...new Set(value.filter(ticker => typeof ticker === 'string' && tickerPattern.test(ticker)))].slice(0, 300)
        : [];
    } catch {
      return [];
    }
  };

  function toast(message) {
    let node = document.getElementById('ca-toast');
    if (!node) {
      node = document.createElement('div');
      node.id = 'ca-toast';
      node.className = 'ca-toast';
      node.setAttribute('role', 'status');
      document.body.append(node);
    }
    node.textContent = message;
    node.hidden = false;
    clearTimeout(window.caToastTimer);
    window.caToastTimer = setTimeout(() => { node.hidden = true; }, 4500);
  }

  function update() {
    const saved = read();
    document.querySelectorAll('[data-follow-stock]').forEach(button => {
      const ticker = button.dataset.followStock;
      const active = saved.includes(ticker);
      const opensSignup = button.dataset.followEmailSignup === 'true';
      const label = button.dataset.followLabel || 'Follow This Stock';
      button.setAttribute('aria-pressed', String(active));
      button.setAttribute('aria-label', active
        ? `Unfollow ${ticker}`
        : (opensSignup ? `Follow ${ticker} and get email alerts` : `Follow ${ticker}`));
      button.title = active
        ? `Unfollow ${ticker}`
        : (opensSignup ? `Follow ${ticker} and get email alerts` : `Follow ${ticker}`);
      const text = button.querySelector('[data-follow-label]');
      if (text) text.textContent = active ? 'Following' : label;
    });
    document.querySelectorAll('.ca-follow-count').forEach(node => {
      node.textContent = saved.length;
      node.hidden = !saved.length;
    });
    window.lucide?.createIcons();
  }

  function openAlertSignup(ticker) {
    location.assign(`/alerts?ticker=${encodeURIComponent(ticker)}`);
  }

  window.CAFollowing = {read, update, toast};
  document.addEventListener('click', event => {
    const button = event.target.closest('[data-follow-stock]');
    if (!button) return;
    const ticker = button.dataset.followStock;
    if (!tickerPattern.test(ticker)) return;

    const saved = read();
    const active = saved.includes(ticker);
    const opensSignup = button.dataset.followEmailSignup === 'true';
    if (!active && saved.length >= 300) {
      if (opensSignup) openAlertSignup(ticker);
      else toast('Your Following list has 300 stocks. Remove one before adding another.');
      return;
    }

    try {
      localStorage.setItem(key, JSON.stringify(active ? saved.filter(value => value !== ticker) : [...saved, ticker]));
    } catch {
      if (!active && opensSignup) openAlertSignup(ticker);
      else toast('This browser could not save your list. Check its storage settings.');
      return;
    }

    update();
    window.dispatchEvent(new Event('ca-follow-change'));
    if (!active && opensSignup) {
      openAlertSignup(ticker);
      return;
    }
    toast(active ? `${ticker} removed from Following.` : `${ticker} saved to Following on this browser.`);
  });

  function refreshFollowing() {
    update();
    window.dispatchEvent(new Event('ca-follow-change'));
  }

  window.addEventListener('storage', event => {
    if (event.key === key || event.key === null) refreshFollowing();
  });
  window.addEventListener('pageshow', refreshFollowing);

  const button = document.querySelector('.ca-menu-button');
  const nav = document.getElementById('ca-nav');
  function closeMenu() {
    nav?.classList.remove('is-open');
    button?.setAttribute('aria-expanded', 'false');
    button?.setAttribute('aria-label', 'Open navigation');
    document.querySelectorAll('.ca-dropdown[open]').forEach(dropdown => { dropdown.open = false; });
  }
  button?.addEventListener('click', () => {
    const open = nav.classList.toggle('is-open');
    button.setAttribute('aria-expanded', String(open));
    button.setAttribute('aria-label', open ? 'Close navigation' : 'Open navigation');
  });
  document.addEventListener('click', event => {
    if (!event.target.closest('.ca-header')) closeMenu();
    document.querySelectorAll('.ca-dropdown[open]').forEach(dropdown => {
      if (!dropdown.contains(event.target)) dropdown.open = false;
    });
  });
  document.addEventListener('keydown', event => {
    if (event.key === 'Escape') {
      const focus = nav?.contains(document.activeElement);
      closeMenu();
      if (focus) button?.focus();
    }
  });
  document.querySelectorAll('.ca-dropdown').forEach(dropdown => {
    dropdown.addEventListener('toggle', () => {
      if (dropdown.open) {
        document.querySelectorAll('.ca-dropdown').forEach(other => {
          if (other !== dropdown) other.open = false;
        });
      }
    });
  });
  update();
})();
