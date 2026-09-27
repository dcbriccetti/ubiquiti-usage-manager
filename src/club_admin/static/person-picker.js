(() => {
  const normalize = text => text.toLocaleLowerCase().replace(/[^\p{L}\p{N}]/gu, '');
  const matchesSearch = (text, query) => {
    const haystack = normalize(text);
    return haystack.includes(normalize(query)) || query.trim().split(/\s+/).every(word => haystack.includes(normalize(word)));
  };
  document.querySelectorAll('[data-person-picker]').forEach(picker => {
    const search = picker.querySelector('[data-person-search]');
    const select = picker.querySelector('[data-person-select]');
    const status = picker.querySelector('[data-person-selection]');
    const options = [...select.options];
    const refresh = () => {
      const selected = select.value;
      const query = search.value;
      const matches = options.filter(option => option.value && matchesSearch(option.dataset.search || '', query));
      // Keep the current selection when filtering so typing never silently changes the person.
      select.replaceChildren(...options.filter(option => !option.value || option.value === selected || matches.includes(option)));
      select.value = selected;
      status.textContent = selected ? `Selected: ${select.selectedOptions[0].textContent}` : 'No person selected.';
      if (!matches.length) status.textContent += ' No matches for this search.';
    };
    search.addEventListener('input', refresh);
    select.addEventListener('change', refresh);
    refresh();
  });
})();
