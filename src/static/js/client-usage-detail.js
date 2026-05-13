(() => {
    const container = document.getElementById('wan-detail-panels');
    if (!container) {
        return;
    }

    const detailsUrl = container.dataset.wanDetailsUrl || '';
    if (!detailsUrl) {
        return;
    }
    const reverseDnsUrl = container.dataset.reverseDnsUrl || '';

    const renderError = () => {
        container.innerHTML = `
            <article class="panel">
                <div class="panel-body">
                    <h2>Internet Details</h2>
                    <p class="muted">Internet details are not available right now.</p>
                </div>
            </article>
        `;
    };

    fetch(detailsUrl, { cache: 'no-store' })
        .then((response) => {
            if (!response.ok) {
                throw new Error(`Internet detail request failed: ${response.status}`);
            }
            return response.text();
        })
        .then((html) => {
            container.innerHTML = html;
            if (typeof window.renderUsageCharts === 'function') {
                window.renderUsageCharts();
            }
            refreshReverseDnsLabels();
        })
        .catch(renderError);

    const renderReverseDnsLabel = (element, label) => {
        const ipAddress = element.dataset.rdnsIp || '';
        if (!ipAddress || !label) {
            return;
        }

        if (element.dataset.rdnsMode === 'summary') {
            const extraCount = Number.parseInt(element.dataset.rdnsExtraCount || '0', 10);
            element.textContent = extraCount > 0 ? `${label} +${extraCount}` : label;
            const title = element.getAttribute('title') || '';
            if (title.startsWith(`${ipAddress}:`)) {
                element.setAttribute('title', `${label} (${ipAddress})${title.slice(ipAddress.length)}`);
            }
            return;
        }

        element.textContent = `${label} (${ipAddress})`;
    };

    const collectReverseDnsIps = () => {
        const elements = Array.from(container.querySelectorAll('[data-rdns-ip]'));
        const ipAddresses = new Set();
        elements.forEach((element) => {
            const ipAddress = element.dataset.rdnsIp || '';
            if (ipAddress) {
                ipAddresses.add(ipAddress);
            }
        });
        return { elements, ipAddresses: Array.from(ipAddresses) };
    };

    const requestReverseDnsLabels = async () => {
        if (!reverseDnsUrl) {
            return false;
        }

        const { elements, ipAddresses } = collectReverseDnsIps();
        if (!ipAddresses.length) {
            return false;
        }

        const params = new URLSearchParams();
        ipAddresses.forEach((ipAddress) => params.append('ip', ipAddress));
        const response = await fetch(`${reverseDnsUrl}?${params.toString()}`, { cache: 'no-store' });
        if (!response.ok) {
            return false;
        }

        const payload = await response.json();
        const labels = payload.labels || {};
        let updated = false;
        elements.forEach((element) => {
            const ipAddress = element.dataset.rdnsIp || '';
            const label = labels[ipAddress];
            if (label) {
                renderReverseDnsLabel(element, label);
                updated = true;
            }
        });
        return updated;
    };

    const refreshReverseDnsLabels = () => {
        [500, 1500, 3500, 7000, 12000].forEach((delay) => {
            window.setTimeout(() => {
                requestReverseDnsLabels().catch(() => {});
            }, delay);
        });
    };
})();
