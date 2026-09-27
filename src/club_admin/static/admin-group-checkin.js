(() => {
  const form = document.querySelector('[data-group-checkin]');
  if (!form) return;
  const search = form.querySelector('[data-group-search]');
  const options = [...form.querySelectorAll('[data-guest-option]')];
  const selectedList = form.querySelector('[data-selected-guests]');
  const normalize = text => text.toLocaleLowerCase().replace(/[^\p{L}\p{N}]/gu, '');
  const matchesSearch = (text, query) => {
    const haystack = normalize(text);
    return haystack.includes(normalize(query)) || query.trim().split(/\s+/).every(word => haystack.includes(normalize(word)));
  };
  const update = () => {
    const query = search.value;
    const canSearch = normalize(query).length >= 2;
    let matches = 0;
    let selectedCount = 0;
    selectedList.replaceChildren();
    for (const option of options) {
      const checkbox = option.querySelector('input');
      const selected = checkbox.checked;
      const matchesQuery = !selected && canSearch && matchesSearch(option.dataset.search, query);
      option.hidden = !matchesQuery || ++matches > 10;
      if (!selected) continue;
      selectedCount++;
      const item = document.createElement('li');
      const name = option.querySelector('[data-guest-name]').textContent;
      const details = option.querySelector('span').cloneNode(true);
      const remove = document.createElement('button');
      remove.type = 'button';
      remove.textContent = 'Remove';
      remove.setAttribute('aria-label', `Remove ${name}`);
      remove.addEventListener('click', () => {
        checkbox.checked = false;
        update();
        search.focus();
      });
      item.append(details, remove);
      selectedList.append(item);
    }
    form.querySelector('[data-group-results]').hidden = matches === 0;
    form.querySelector('[data-search-status]').textContent = !canSearch
      ? 'Search by name or phone. Enter at least two characters.'
      : matches === 0 ? 'No unselected guests match this search.'
      : matches > 10 ? 'Showing 10 matches. Refine your search to find more.'
      : `${matches} ${matches === 1 ? 'match' : 'matches'}. Select guests to add them.`;
    form.querySelector('[data-group-selection]').textContent = selectedCount
      ? `${selectedCount} selected` : 'No guests selected.';
  };
  form.addEventListener('input', update);
  form.addEventListener('change', update);
  update();
})();
