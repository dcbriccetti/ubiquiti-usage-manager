(() => {
    const container = document.getElementById('wan-detail-panels');
    if (!container) {
        return;
    }

    const detailsUrl = container.dataset.wanDetailsUrl || '';
    if (!detailsUrl) {
        return;
    }

    const renderError = () => {
        container.innerHTML = `
            <article class="panel">
                <div class="panel-body">
                    <h2>WAN Details</h2>
                    <p class="muted">WAN details are not available right now.</p>
                </div>
            </article>
        `;
    };

    fetch(detailsUrl, { cache: 'no-store' })
        .then((response) => {
            if (!response.ok) {
                throw new Error(`WAN detail request failed: ${response.status}`);
            }
            return response.text();
        })
        .then((html) => {
            container.innerHTML = html;
        })
        .catch(renderError);
})();
