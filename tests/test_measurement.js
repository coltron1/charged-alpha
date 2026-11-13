const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const source = fs.readFileSync('static/brand.js', 'utf8');
function setup({failStorage = false, analytics = true} = {}) {
  const listeners = {};
  const events = [];
  const storage = {};
  const document = {
    addEventListener: (name, fn) => (listeners[name] ||= []).push(fn),
    querySelectorAll: () => [], querySelector: () => null,
    getElementById: () => null,
    createElement: () => ({setAttribute() {}, append() {}}),
    body: {append() {}},
  };
  const window = {
    addEventListener() {}, dispatchEvent() {}, setTimeout() {},
    ...(analytics ? {gtag: (...args) => events.push(args)} : {}),
  };
  const localStorage = {
    getItem: key => storage[key] || null,
    setItem(key, value) { if (failStorage) throw Error('blocked'); storage[key] = value; },
  };
  const location = {href: 'https://chargedalpha.com/shows/CRWV', origin: 'https://chargedalpha.com', pathname: '/shows/CRWV', assign() {}};
  vm.runInNewContext(source, {document, window, localStorage, location, URL, Event: class {}, clearTimeout() {}, setTimeout() {}});
  return {listeners, events, storage};
}
function click(env, target) { for (const fn of env.listeners.click) fn({target}); }
function followButton() { return {dataset: {followStock: 'CRWV'}, setAttribute() {}, querySelector: () => null}; }
function targetForButton(button) { return {closest: selector => selector === '[data-follow-stock]' ? button : null}; }
function targetForLink(href) { return {closest: selector => selector === 'a[href]' ? {href} : null}; }
const ok = setup();
click(ok, targetForButton(followButton()));
assert.equal(ok.events.length, 1);
assert.equal(ok.events[0][1], 'stock_follow');
assert.equal('ticker' in ok.events[0][2], false);
click(ok, targetForButton(followButton()));
assert.equal(ok.events.length, 1, 'unfollow is not a follow event');
const blocked = setup({failStorage: true});
click(blocked, targetForButton(followButton()));
assert.equal(blocked.events.length, 0, 'failed storage is not a follow');
click(ok, targetForLink('https://www.youtube.com/watch?v=123&email=private@example.com'));
click(ok, targetForLink('https://open.spotify.com/episode/123'));
click(ok, targetForLink('https://youtube.com.evil.example/watch'));
assert.deepEqual(ok.events.slice(1).map(event => event[1]), ['media_outbound_click', 'media_outbound_click']);
assert.equal(ok.events[1][2].platform, 'youtube');
assert.equal(ok.events[2][2].platform, 'spotify');
assert.equal(JSON.stringify(ok.events).includes('private@example.com'), false);
const noAnalytics = setup({analytics: false});
click(noAnalytics, targetForLink('https://youtube.com/watch?v=123'));
assert.equal(noAnalytics.events.length, 0);
console.log('Measurement behavior passed');
const confirmationSource = fs.readFileSync('static/alert-confirmation-measurement.js', 'utf8');
const confirmationWindow = {};
vm.runInNewContext(confirmationSource, {window: confirmationWindow, document: {currentScript: {dataset: {id: 'G-TEST123'}}}, Date});
const confirmationEvents = confirmationWindow.dataLayer.map(args => Array.from(args));
assert.equal(confirmationEvents[2][1], 'email_alert_signup_confirmed');
assert.equal(confirmationEvents[1][2].send_page_view, false);
assert.equal(confirmationEvents[2][2].page_location, 'https://chargedalpha.com/confirmation/alerts');
assert.equal(confirmationEvents[2][2].page_referrer, '');
console.log('Confirmation measurement behavior passed');
const readerSource = fs.readFileSync('static/research-measurement.js', 'utf8');
const readerWindow = {};
vm.runInNewContext(readerSource, {window: readerWindow, document: {currentScript: {dataset: {id: 'G-TEST123'}}}, Date,
  location: {origin: 'https://chargedalpha.com', pathname: '/research/crwv-q3-2026', href: 'https://chargedalpha.com/research/crwv-q3-2026?view=reader&email=private@example.com#private'}});
const readerEvents = readerWindow.dataLayer.map(args => Array.from(args));
assert.equal(readerEvents[1][0], 'config');
assert.equal(readerEvents[1][2].page_location, 'https://chargedalpha.com/research/crwv-q3-2026');
assert.equal(readerEvents[1][2].page_referrer, '');
assert.equal(JSON.stringify(readerEvents).includes('private'), false);
console.log('Reader measurement behavior passed');
const readerNavSource = fs.readFileSync('static/research-reader.js', 'utf8');
function readerLinks(enabled) {
  const body = {children: [], prepend(node) { this.children.unshift(node); }};
  function element(tag) { return {tag, dataset: {}, children: [], setAttribute() {}, append(node) { this.children.push(node); }}; }
  const document = {
    body, createElement: element,
    querySelector(selector) {
      if (selector.startsWith('script[')) return {dataset: {ticker: 'CRWV', youtubeId: 'Primary1234', alertsEnabled: String(enabled)}};
      return null;
    },
  };
  vm.runInNewContext(readerNavSource, {document, encodeURIComponent});
  return body.children[0].children;
}
const disabledLinks = readerLinks(false), enabledLinks = readerLinks(true);
assert.equal(disabledLinks.find(x => x.tag === 'button').dataset.followEmailSignup, undefined);
assert.equal(disabledLinks.some(x => x.href && x.href.startsWith('/alerts')), false);
assert.equal(enabledLinks.find(x => x.tag === 'button').dataset.followEmailSignup, 'true');
assert.equal(enabledLinks.find(x => x.tag === 'button').dataset.followSource, 'research_reader');
assert.ok(enabledLinks.some(x => x.href && x.href.includes('source=research_reader')));
console.log('Reader alert availability behavior passed');
