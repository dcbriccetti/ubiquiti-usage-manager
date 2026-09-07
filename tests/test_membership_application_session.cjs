const assert = require('node:assert/strict');
const { readFileSync } = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { test } = require('node:test');

const source = readFileSync(path.join(__dirname, '../src/club_admin/static/membership-application-session.js'), 'utf8');
const flush = () => new Promise(setImmediate);

function element(properties = {}) {
  const listeners = new Map();
  return {
    ...properties,
    addEventListener(name, fn) {
      if (!listeners.has(name)) listeners.set(name, []);
      listeners.get(name).push(fn);
    },
    emit(name, event = {}) {
      for (const fn of listeners.get(name) || []) fn(event);
    },
  };
}

function kiosk({ remaining = 300, fail = false, status = 200, publicForm = true } = {}) {
  let now = 1000000;
  const requests = [];
  const navigations = [];
  const timers = [];
  const main = { hidden: false };
  const warning = element({
    open: false,
    showModal() { this.open = true; },
    close() { this.open = false; },
  });
  const countdown = { value: '' };
  const continueButton = element();
  const form = element({
    action: { toString: () => '[object RadioNodeList]' },
    getAttribute: name => name === 'action' ? '/users/membership-application' : null,
    dataset: { idleSeconds: '300', remainingSeconds: String(remaining), exitUrl: '/users/self-checkin' },
    elements: { namedItem: () => ({ value: 'example-session-token' }) },
    resets: 0,
    reset() { this.resets += 1; },
  });
  const document = element({ querySelector(selector) {
    return {
      '[data-application-session]': publicForm ? form : null,
      '[data-application-idle-warning]': warning,
      '[data-application-idle-countdown]': countdown,
      '[data-application-continue]': continueButton,
      main,
    }[selector];
  } });
  const window = element({
    location: { replace: url => navigations.push(url) },
    setInterval: fn => timers.push(fn),
  });
  vm.runInNewContext(source, {
    document, window, URLSearchParams, AbortController,
    Date: class extends Date { static now() { return now; } },
    fetch: async (url, options) => {
      requests.push({ url, options });
      if (fail) throw new Error('Offline');
      return { ok: status === 200, redirected: false, json: async () => ({ idle_seconds: 300 }) };
    },
  });
  return {
    document, window, form, warning, countdown, continueButton, main, requests, navigations, timers,
    advance(seconds, runTimers = true) {
      now += seconds * 1000;
      if (runTimers) timers.forEach(fn => fn());
    },
  };
}

test('unattended form warns, then hides personal details and exits without renewal', () => {
  const k = kiosk();
  k.advance(240);
  assert.equal(k.warning.open, true);
  assert.equal(k.countdown.value, '60');
  k.advance(60);
  assert.equal(k.main.hidden, true);
  assert.equal(k.form.resets, 1);
  assert.deepEqual(k.navigations, ['/users/self-checkin']);
  assert.equal(k.requests.length, 0);
});

test('typing renews with the form token and preserves entered values', async () => {
  const k = kiosk();
  k.advance(200);
  k.document.emit('input');
  await flush();
  assert.equal(k.requests.length, 1);
  assert.equal(k.requests[0].url, '/users/membership-application');
  assert.equal(k.requests[0].options.body.get('application_token'), 'example-session-token');
  assert.equal(k.requests[0].options.body.get('action'), 'activity');
  k.advance(101);
  assert.equal(k.main.hidden, false);
  assert.equal(k.form.resets, 0);
  k.advance(199);
  assert.equal(k.main.hidden, true);
});

test('Keep Working dismisses the warning and renews the session', async () => {
  const k = kiosk();
  k.advance(241);
  assert.equal(k.warning.open, true);
  k.continueButton.emit('click');
  await flush();
  assert.equal(k.warning.open, false);
  k.advance(60);
  assert.equal(k.main.hidden, false);
});

test('offline activity cannot extend the original deadline', async () => {
  const k = kiosk({ fail: true });
  k.advance(250);
  k.document.emit('input');
  await flush();
  k.advance(50);
  assert.equal(k.main.hidden, true);
});

test('server rejection hides a stale form immediately', async () => {
  const k = kiosk({ status: 401 });
  k.document.emit('input');
  await flush();
  assert.equal(k.main.hidden, true);
});

test('reload uses the remaining lease instead of starting five more minutes', () => {
  const k = kiosk({ remaining: 20 });
  assert.equal(k.warning.open, true);
  k.advance(20);
  assert.equal(k.main.hidden, true);
});

test('browser back restoration cannot reveal the old applicant', () => {
  const k = kiosk();
  k.window.emit('pagehide');
  assert.equal(k.main.hidden, true);
  k.window.emit('pageshow', { persisted: true });
  assert.deepEqual(k.navigations, ['/users/self-checkin']);
});

test('expired forms cannot be submitted even if timers were throttled', () => {
  const k = kiosk();
  k.advance(301, false);
  let prevented = false;
  k.form.emit('submit', { preventDefault() { prevented = true; } });
  assert.equal(prevented, true);
});

test('admin forms do not acquire a kiosk timer', () => {
  const k = kiosk({ publicForm: false });
  assert.equal(k.timers.length, 0);
  assert.equal(k.requests.length, 0);
});
