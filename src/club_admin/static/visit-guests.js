/* Clear shared-kiosk identity after inactivity; typing keeps an active list open. */
(() => {
  const page = document.querySelector('[data-guest-session]');
  if (!page) return;
  let timer;
  let lastPing = Date.now();
  const reset = () => window.location.replace(page.dataset.resetUrl);
  const touch = () => {
    clearTimeout(timer);
    timer = setTimeout(reset, Number(page.dataset.idleSeconds) * 1000);
    if (Date.now() - lastPing < 30000) return;
    lastPing = Date.now();
    const body = new URLSearchParams({action: 'activity', token: page.dataset.token});
    fetch(page.dataset.sessionUrl, {method: 'POST', body, credentials: 'same-origin'})
      .then(response => { if (!response.ok || response.redirected) reset(); })
      .catch(() => {});
  };
  ['pointerdown', 'keydown', 'input'].forEach(event => page.addEventListener(event, touch, {passive: true}));
  window.addEventListener('pageshow', event => { if (event.persisted) reset(); });
  touch();
})();
