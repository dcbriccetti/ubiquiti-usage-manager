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
    const flowActivityUrl = container.dataset.flowActivityUrl || '';
    const loadButton = container.querySelector('[data-load-wan-details]');
    let detailsLoaded = false;

    const renderError = () => {
        container.innerHTML = `
            <article class="panel">
                <div class="panel-body">
                    <h2>Internet Details</h2>
                    <p class="muted">Internet details are not available right now.</p>
                    <button type="button" data-load-wan-details>Try Again</button>
                </div>
            </article>
        `;
        container.querySelector('[data-load-wan-details]')?.addEventListener('click', loadDetails);
    };

    const renderLoading = () => {
        container.setAttribute('aria-busy', 'true');
        const button = container.querySelector('[data-load-wan-details]');
        if (button) {
            button.disabled = true;
            button.textContent = 'Loading Internet Details...';
        }
        const prompt = container.querySelector('[data-wan-details-prompt] .muted');
        if (prompt) {
            prompt.textContent = 'Gathering recent activity and host details.';
        }
    };

    async function loadDetails() {
        if (detailsLoaded) {
            return;
        }
        detailsLoaded = true;
        renderLoading();
        try {
            const response = await fetch(detailsUrl, { cache: 'no-store' });
            if (!response.ok) {
                throw new Error(`Internet detail request failed: ${response.status}`);
            }
            const html = await response.text();
            container.innerHTML = html;
            container.removeAttribute('aria-busy');
            if (typeof window.renderUsageCharts === 'function') {
                window.renderUsageCharts();
            }
            initializePaginatedTables();
            initializeFlowActivityRangeControls();
            refreshReverseDnsLabels();
        } catch (_error) {
            detailsLoaded = false;
            container.removeAttribute('aria-busy');
            renderError();
        }
    }

    const initializeFlowActivityRangeControls = () => {
        container.querySelectorAll('[data-flow-activity-panel]').forEach((panel) => {
            if (panel.dataset.rangeControlReady === 'true') {
                return;
            }

            const rangeSelect = panel.querySelector('[data-flow-activity-range]');
            const panelUrl = panel.dataset.flowActivityUrl || flowActivityUrl;
            if (!rangeSelect || !panelUrl) {
                return;
            }

            panel.dataset.rangeControlReady = 'true';
            rangeSelect.addEventListener('change', async () => {
                const selectedRange = rangeSelect.value || 'this_month';
                rangeSelect.disabled = true;
                panel.classList.add('panel-loading');

                try {
                    const params = new URLSearchParams({ flow_activity_range: selectedRange });
                    const response = await fetch(`${panelUrl}?${params.toString()}`, { cache: 'no-store' });
                    if (!response.ok) {
                        throw new Error(`Internet activity request failed: ${response.status}`);
                    }

                    const html = await response.text();
                    const template = document.createElement('template');
                    template.innerHTML = html.trim();
                    const nextPanel = template.content.querySelector('[data-flow-activity-panel]');
                    if (!nextPanel) {
                        throw new Error('Internet activity response was missing the panel');
                    }

                    panel.replaceWith(nextPanel);
                    initializeFlowActivityRangeControls();
                    refreshReverseDnsLabels();
                } catch (_error) {
                    rangeSelect.disabled = false;
                    panel.classList.remove('panel-loading');
                }
            });
        });
    };

    const initializePaginatedTables = () => {
        container.querySelectorAll('[data-paginated-table]').forEach((table) => {
            const tableId = table.dataset.paginatedTable || '';
            const pager = tableId ? container.querySelector(`[data-table-pager="${tableId}"]`) : null;
            const rows = Array.from(table.querySelectorAll('tbody tr[data-paginated-row]'));
            if (!tableId || !pager || rows.length <= 10 || table.dataset.paginationReady === 'true') {
                return;
            }

            table.dataset.paginationReady = 'true';
            const pageStatus = pager.querySelector('[data-page-status]');
            const pageSizeSelect = pager.querySelector('[data-page-size]');
            const prevButton = pager.querySelector('[data-page-prev]');
            const nextButton = pager.querySelector('[data-page-next]');
            let page = 0;

            const selectedPageSize = () => {
                if (pageSizeSelect?.value === 'all') {
                    return rows.length;
                }
                const value = Number.parseInt(pageSizeSelect?.value || '10', 10);
                return Number.isFinite(value) && value > 0 ? value : 10;
            };

            const renderPage = () => {
                const pageSize = selectedPageSize();
                const pageCount = Math.max(1, Math.ceil(rows.length / pageSize));
                page = Math.max(0, Math.min(page, pageCount - 1));
                const start = page * pageSize;
                const end = Math.min(rows.length, start + pageSize);

                rows.forEach((row, index) => {
                    row.hidden = index < start || index >= end;
                });

                if (pageStatus) {
                    pageStatus.innerHTML = `${start + 1}<span class="range-dash">–</span>${end} of ${rows.length}`;
                }
                if (prevButton) {
                    prevButton.disabled = page === 0;
                }
                if (nextButton) {
                    nextButton.disabled = page >= pageCount - 1;
                }
                pager.hidden = false;
            };

            pageSizeSelect?.addEventListener('change', () => {
                page = 0;
                renderPage();
            });
            prevButton?.addEventListener('click', () => {
                page -= 1;
                renderPage();
            });
            nextButton?.addEventListener('click', () => {
                page += 1;
                renderPage();
            });
            renderPage();
        });
    };

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

    loadButton?.addEventListener('click', loadDetails);
})();
