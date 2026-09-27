(() => {
  const report = document.querySelector('.guest-host-report');
  if (!report) return;
  report.addEventListener('click', event => {
    const button = event.target.closest('button[aria-controls]');
    if (!button || !report.contains(button)) return;
    const id = button.getAttribute('aria-controls');
    const details = document.getElementById(id);
    if (!details) return;
    const expanded = details.hidden;
    details.hidden = !expanded;
    report.querySelectorAll('button[aria-controls]').forEach(control => {
      if (control.getAttribute('aria-controls') !== id) return;
      control.setAttribute('aria-expanded', String(expanded));
      control.setAttribute('aria-label', control.getAttribute('aria-label').replace(
        /; (show|hide) visit details$/, `; ${expanded ? 'hide' : 'show'} visit details`));
    });
  });
})();
