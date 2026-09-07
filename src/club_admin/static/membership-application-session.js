(() => {
  const form = document.querySelector("[data-application-session]");
  if (!form) return;

  const warning = document.querySelector("[data-application-idle-warning]");
  const countdown = document.querySelector("[data-application-idle-countdown]");
  const continueButton = document.querySelector("[data-application-continue]");
  const token = form.elements.namedItem("application_token").value;
  // Controls named "action" shadow the form.action property in browsers.
  const activityUrl = form.getAttribute("action");
  const idleMs = Number(form.dataset.idleSeconds) * 1000;
  let deadline = Date.now() + Number(form.dataset.remainingSeconds) * 1000;
  let lastActivity = 0;
  let acknowledgedActivity = 0;
  let lastAttempt = 0;
  let pending = false;
  let ended = false;
  let renewalController = null;

  const exit = () => {
    if (ended) return;
    ended = true;
    renewalController?.abort();
    // Remove personal details even if the network is unavailable during navigation.
    form.reset();
    document.querySelector("main").hidden = true;
    warning.close();
    window.location.replace(form.dataset.exitUrl);
  };

  const renew = async () => {
    if (pending || ended) return;
    const startedAt = Date.now();
    if (startedAt >= deadline) {
      exit();
      return;
    }
    pending = true;
    lastAttempt = startedAt;
    const activity = lastActivity;
    renewalController = new AbortController();
    try {
      const response = await fetch(activityUrl, {
        method: "POST",
        credentials: "same-origin",
        cache: "no-store",
        signal: renewalController.signal,
        body: new URLSearchParams({ action: "activity", application_token: token }),
      });
      if (ended) return;
      if (!response.ok || response.redirected) {
        exit();
        return;
      }
      const result = await response.json();
      if (result.idle_seconds * 1000 !== idleMs) {
        exit();
        return;
      }
      // Start at request time so network delays never extend the server's lease.
      deadline = startedAt + idleMs;
      acknowledgedActivity = activity;
      warning.close();
    } catch {
      // Keep the original deadline on network failure; a timer still hides the form.
    } finally {
      pending = false;
    }
  };

  const tick = () => {
    if (ended) return;
    const remaining = deadline - Date.now();
    if (remaining <= 0) {
      exit();
      return;
    }
    if (remaining <= 60000) {
      countdown.value = String(Math.ceil(remaining / 1000));
      if (!warning.open) warning.showModal();
    }
    // Only human activity renews the session; an unattended tab cannot keep it alive.
    if (lastActivity > acknowledgedActivity && Date.now() - lastAttempt >= 30000) {
      void renew();
    }
  };

  ["input", "keydown", "pointerdown", "touchstart", "wheel"].forEach((eventName) => {
    document.addEventListener(eventName, (event) => {
      if (event.target?.closest?.("a, button[type='submit']")) return;
      lastActivity = Date.now();
      tick();
    }, { passive: true });
  });
  continueButton.addEventListener("click", () => {
    lastActivity = Date.now();
    void renew();
  });
  warning.addEventListener("cancel", (event) => {
    event.preventDefault();
    lastActivity = Date.now();
    void renew();
  });
  form.addEventListener("submit", (event) => {
    if (ended || Date.now() >= deadline) {
      event.preventDefault();
      exit();
    }
  });
  window.addEventListener("pagehide", () => {
    renewalController?.abort();
    form.reset();
    document.querySelector("main").hidden = true;
  });
  window.addEventListener("pageshow", (event) => {
    if (event.persisted) exit();
    else tick();
  });
  document.addEventListener("visibilitychange", tick);
  window.setInterval(tick, 1000);
  tick();
})();
