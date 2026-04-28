// @ts-nocheck
(() => {
    const bootstrapScript = document.getElementById('dashboard-bootstrap');
    if (!bootstrapScript) {
        return;
    }

    let bootstrap;
    try {
        bootstrap = JSON.parse(bootstrapScript.textContent || '{}');
    } catch (_err) {
        return;
    }

    const clientsTable = document.getElementById('clients-table');
    const preUsageGroupHeader = document.getElementById('pre-usage-group-header');
    const usageGroupHeader = document.getElementById('usage-group-header');
    const connectedBody = document.getElementById('connected-clients-body');
    const windowSelect = document.getElementById('window-select');
    const activitySpanSelect = document.getElementById('activity-span-select');
    const statUsageToday = document.getElementById('stat-usage-today');
    const statUsage7Days = document.getElementById('stat-usage-7-days');
    const statUsageThisMonth = document.getElementById('stat-usage-this-month');
    const statUsageMonthLabel = document.getElementById('stat-usage-month-label');
    const usageMonthHeader = document.getElementById('usage-month-header');
    const usageCostHeader = document.getElementById('usage-cost-header');
    const costGroupHeader = document.getElementById('cost-group-header');
    const topCurrentConsumersCanvas = document.getElementById('top-current-consumers-chart');
    const topCurrentConsumersLegend = document.getElementById('top-current-consumers-legend');
    const topCurrentConsumersEmpty = document.getElementById('top-current-consumers-empty');
    const ipPrefixHeader = document.getElementById('ip-prefix-header');

    if (
        !clientsTable || !preUsageGroupHeader || !usageGroupHeader || !connectedBody ||
        !windowSelect || !activitySpanSelect || !statUsageToday || !statUsage7Days ||
        !statUsageThisMonth || !statUsageMonthLabel || !usageMonthHeader || !usageCostHeader ||
        !costGroupHeader ||
        !topCurrentConsumersCanvas || !topCurrentConsumersLegend || !topCurrentConsumersEmpty ||
        !ipPrefixHeader
    ) {
        return;
    }

    const detailPattern = String(bootstrap.detailPattern || '');
    const streamBaseUrl = String(bootstrap.streamBaseUrl || '');
    const snapshotBaseUrl = String(bootstrap.snapshotBaseUrl || '');
    const defaultIpHeader = String(bootstrap.defaultIpHeader || 'IP');
    const initialPayload = bootstrap.initialPayload || {};

    if (!detailPattern || !streamBaseUrl || !snapshotBaseUrl) {
        return;
    }

    let fallbackTimer = null;
    let stream = null;
    let selectedWindow = windowSelect.value;
    let selectedActivitySpan = activitySpanSelect.value;
    let topCurrentConsumersChart = null;
    const activityScaleQuantile = 0.95;
    const activityScaleFloorMb = 0.05;
    const activityScaleShrinkFactor = 0.90;
    const activityScaleByView = new Map();
    const realtimeWindows = new Set(['active_now', 'online_now']);
    const windowFocusClassByWindow = {
        active_now: 'focus-minute-total',
        online_now: 'focus-minute-total',
        today: 'focus-today',
        last_7_days: 'focus-7-days',
        this_month: 'focus-month'
    };
    const costHeaderByWindow = {
        active_now: 'Cost',
        online_now: 'Cost',
        today: 'Today Cost',
        last_7_days: '7-Day Cost',
        this_month: 'Month Cost'
    };

    const applyWindowColumnVisibility = () => {
        const isRealtime = realtimeWindows.has(selectedWindow);
        clientsTable.classList.toggle('realtime-window', isRealtime);
        clientsTable.classList.toggle('non-realtime-window', !isRealtime);
        clientsTable.classList.toggle('hide-cost-column', isRealtime);
        preUsageGroupHeader.colSpan = isRealtime ? 10 : 7;
        usageGroupHeader.colSpan = isRealtime ? 6 : 3;
        clientsTable.classList.remove('focus-minute-total', 'focus-today', 'focus-7-days', 'focus-month');
        if (windowFocusClassByWindow[selectedWindow]) {
            clientsTable.classList.add(windowFocusClassByWindow[selectedWindow]);
        }
        usageCostHeader.textContent = costHeaderByWindow[selectedWindow] || 'Cost';
    };

    const formatInt = (value) => Math.round(value).toLocaleString();
    const formatMinute = (value) => {
        if (!value || value <= 0) return '';
        return value.toLocaleString(undefined, {
            minimumFractionDigits: 3,
            maximumFractionDigits: 3
        });
    };
    const formatAvgMbps = (intervalMb) => {
        if (!intervalMb || intervalMb <= 0) return '';
        const avgMbps = (intervalMb * 8) / 60;
        return avgMbps.toLocaleString(undefined, {
            minimumFractionDigits: 3,
            maximumFractionDigits: 3
        });
    };
    const formatWhole = (value) => {
        if (!value) return '';
        const rounded = Math.round(value);
        return rounded > 0 ? rounded.toLocaleString() : '';
    };
    const formatCost = (costCents) => {
        if (!costCents || costCents < 0.5) return '';
        const dollars = Number(costCents) / 100;
        return dollars.toLocaleString(undefined, {
            style: 'currency',
            currency: 'USD',
            minimumFractionDigits: 2,
            maximumFractionDigits: 2
        });
    };
    const costCentsForSelectedWindow = (client) => {
        if (selectedWindow === 'today') {
            return client.day_cost_cents;
        }
        if (selectedWindow === 'last_7_days') {
            return client.last_7_days_cost_cents;
        }
        if (selectedWindow === 'this_month') {
            return client.month_cost_cents;
        }
        return null;
    };
    const normalizeApName = (value) => {
        const text = String(value || '');
        return text.endsWith(' AP') ? text.slice(0, -3) : text;
    };
    const formatMacShort = (value) => {
        const text = String(value || '');
        if (!text) return '';
        return text.slice(-5);
    };
    const renderAccessPointCell = (client) => {
        const primary = normalizeApName(client.ap_name || '');
        const apCount = Number(client.ap_count) || 0;
        const extraCount = Math.max(0, apCount - 1);
        const breakdown = client.ap_breakdown || '';
        const titleText = breakdown || primary;
        const extraSuffix = extraCount > 0 ? ` +${extraCount}` : '';
        return `<span title="${escapeHtml(titleText)}">${escapeHtml(primary)}${escapeHtml(extraSuffix)}</span>`;
    };

    const activityScaleViewKey = () => `${selectedWindow}:${selectedActivitySpan}`;

    const quantile = (sortedValues, q) => {
        if (!sortedValues.length) return 0;
        const clampedQ = Math.max(0, Math.min(1, Number(q) || 0));
        const pos = (sortedValues.length - 1) * clampedQ;
        const base = Math.floor(pos);
        const rest = pos - base;
        if (base + 1 >= sortedValues.length) {
            return sortedValues[base];
        }
        return sortedValues[base] + (rest * (sortedValues[base + 1] - sortedValues[base]));
    };

    const computeActivityScaleTarget = (clients) => {
        const values = [];
        for (const client of (Array.isArray(clients) ? clients : [])) {
            const series = (client && Array.isArray(client.recent_activity)) ? client.recent_activity : [];
            for (const value of series) {
                const numeric = Number(value) || 0;
                if (numeric > 0) {
                    values.push(numeric);
                }
            }
        }
        if (!values.length) {
            return activityScaleFloorMb;
        }
        values.sort((left, right) => left - right);
        return Math.max(activityScaleFloorMb, quantile(values, activityScaleQuantile));
    };

    const getCurrentActivityScale = () => {
        return activityScaleByView.get(activityScaleViewKey()) || activityScaleFloorMb;
    };

    const updateActivityScale = (clients) => {
        const viewKey = activityScaleViewKey();
        const previousScale = activityScaleByView.get(viewKey) || activityScaleFloorMb;
        const targetScale = computeActivityScaleTarget(clients);
        const nextScale = targetScale > previousScale
            ? targetScale
            : Math.max(targetScale, previousScale * activityScaleShrinkFactor);
        activityScaleByView.set(viewKey, nextScale);
    };

    const renderRecentActivity = (client) => {
        const values = (client && Array.isArray(client.recent_activity)) ? client.recent_activity : [];
        if (!values.length) return '';
        const sharedScale = getCurrentActivityScale();
        const bucketLabel = selectedActivitySpan === '12d' ? 'MB/day' : (selectedActivitySpan === '12h' ? 'MB/hour' : 'MB/min');
        const bars = values.map((value) => {
            const numeric = Number(value) || 0;
            const isCapped = numeric > sharedScale;
            const height = numeric > 0 ? Math.min(100, (numeric / sharedScale) * 100) : 14;
            const klass = numeric > 0 ? `bar${isCapped ? ' capped' : ''}` : 'bar zero';
            const tip = `${numeric.toFixed(3)} ${bucketLabel}${isCapped ? ` (capped at ${sharedScale.toFixed(3)})` : ''}`;
            return `<span class="${klass}" style="height:${height.toFixed(1)}%" title="${tip}"></span>`;
        }).join('');
        return `<div class="sparkline" title="Recent activity (${bucketLabel}, shared scale ${sharedScale.toFixed(3)})">${bars}</div>`;
    };
    const escapeHtml = (value) =>
        String(value)
            .replaceAll('&', '&amp;')
            .replaceAll('<', '&lt;')
            .replaceAll('>', '&gt;')
            .replaceAll('"', '&quot;')
            .replaceAll('\'', '&#39;');
    const emptyWindowMessage = () => {
        if (selectedWindow === 'active_now') {
            return 'No clients are actively using data right now.';
        }
        return 'No clients found for this view yet.';
    };

    const renderConnectedClients = (clients) => {
        const ipPrefixes = new Set(
            clients
                .map((client) => String(client.ip_prefix || ''))
                .filter(Boolean)
        );
        ipPrefixHeader.textContent = ipPrefixes.size === 1 ? `${[...ipPrefixes][0]}.` : defaultIpHeader;

        if (!clients.length) {
            connectedBody.innerHTML = `<tr><td colspan="20" class="muted">${escapeHtml(emptyWindowMessage())}</td></tr>`;
            return;
        }

        connectedBody.innerHTML = clients.map((client) => {
            const signal = client.signal === null ? '' : client.signal;
            const detailHref = detailPattern.replace('__MAC__', encodeURIComponent(client.mac));

            return `
                <tr data-client-mac="${escapeHtml(client.mac)}">
                    <td class="nowrap-col">${escapeHtml(client.user_id)}</td>
                    <td class="client-name-col"><a class="mac-link" href="${detailHref}" title="Usage details">${escapeHtml(client.name)}</a></td>
                    <td class="mono mac-cell">${escapeHtml(formatMacShort(client.mac))}</td>
                    <td class="nowrap-col ip-col">${escapeHtml(client.ip_half || '')}</td>
                    <td>${escapeHtml(client.vlan_name)}</td>
                    <td class="ap-col">${renderAccessPointCell(client)}</td>
                    <td class="sig-col">${escapeHtml(signal)}</td>
                    <td class="activity-col">${renderRecentActivity(client)}</td>
                    <td class="nowrap-col">${escapeHtml(client.connection_duration || '')}</td>
                    <td class="num nowrap-col mbps-col">${formatAvgMbps(client.interval_mb)}</td>
                    <td class="num usage-col usage-first minute-col">${formatMinute(client.minute_tx_mb)}</td>
                    <td class="num usage-col minute-col">${formatMinute(client.minute_rx_mb)}</td>
                    <td class="num usage-col minute-col minute-total-col">${formatMinute(client.interval_mb)}</td>
                    <td class="num usage-col today-col">${formatWhole(client.day_total_mb)}</td>
                    <td class="num usage-col seven-days-col">${formatWhole(client.last_7_days_total_mb)}</td>
                    <td class="num usage-col month-col">${formatWhole(client.calendar_month_total_mb)}</td>
                    <td class="num usage-col usage-last">${formatCost(costCentsForSelectedWindow(client))}</td>
                    <td class="nowrap-col speed-col speed-first">${escapeHtml(client.speed_limit_name || '')}</td>
                    <td class="num nowrap-col speed-col">${Number.isFinite(client.speed_limit_up_kbps) ? Math.round(client.speed_limit_up_kbps).toLocaleString() : ''}</td>
                    <td class="num nowrap-col speed-col">${Number.isFinite(client.speed_limit_down_kbps) ? Math.round(client.speed_limit_down_kbps).toLocaleString() : ''}</td>
                </tr>
            `;
        }).join('');
    };

    const topConsumerColors = [
        '#0f766e',
        '#c2410c',
        '#2563eb',
        '#7c3aed',
        '#ca8a04',
        '#475569'
    ];

    const formatPieMb = (value) => {
        const numeric = Number(value) || 0;
        if (numeric >= 10) {
            return `${Math.round(numeric).toLocaleString()} MB`;
        }
        return `${numeric.toLocaleString(undefined, {
            minimumFractionDigits: 3,
            maximumFractionDigits: 3
        })} MB`;
    };

    const renderTopCurrentConsumersLegend = (slices, colors) => {
        topCurrentConsumersLegend.innerHTML = slices.map((consumer, index) => {
            const value = Number(consumer.intervalMb) || 0;
            const mbps = (value * 8) / 60;
            const title = `${consumer.label}: ${formatPieMb(value)} (${mbps.toFixed(3)} Mbps)`;
            return `
                <div class="top-consumers-legend-item" title="${escapeHtml(title)}">
                    <span class="top-consumers-legend-swatch" style="background:${escapeHtml(colors[index])}"></span>
                    <span class="top-consumers-legend-label">${escapeHtml(consumer.label)}</span>
                </div>
            `;
        }).join('');
    };

    const renderTopCurrentConsumers = (consumers) => {
        if (typeof Chart === 'undefined') {
            topCurrentConsumersCanvas.hidden = true;
            topCurrentConsumersLegend.hidden = true;
            topCurrentConsumersEmpty.hidden = false;
            topCurrentConsumersEmpty.textContent = 'Chart unavailable.';
            return;
        }

        const slices = (Array.isArray(consumers) ? consumers : [])
            .map((consumer) => ({
                label: String(consumer.label || consumer.mac || 'Unknown'),
                intervalMb: Number(consumer.interval_mb) || 0
            }))
            .filter((consumer) => consumer.intervalMb > 0);

        if (!slices.length) {
            topCurrentConsumersCanvas.hidden = true;
            topCurrentConsumersLegend.hidden = true;
            topCurrentConsumersEmpty.hidden = false;
            topCurrentConsumersEmpty.textContent = 'No current usage.';
            if (topCurrentConsumersChart) {
                topCurrentConsumersChart.destroy();
                topCurrentConsumersChart = null;
            }
            return;
        }

        topCurrentConsumersCanvas.hidden = false;
        topCurrentConsumersLegend.hidden = false;
        topCurrentConsumersEmpty.hidden = true;
        const labels = slices.map((consumer) => consumer.label);
        const values = slices.map((consumer) => consumer.intervalMb);
        const colors = values.map((_value, index) => topConsumerColors[index % topConsumerColors.length]);
        renderTopCurrentConsumersLegend(slices, colors);

        if (!topCurrentConsumersChart) {
            topCurrentConsumersChart = new Chart(topCurrentConsumersCanvas, {
                type: 'pie',
                data: {
                    labels,
                    datasets: [{
                        data: values,
                        backgroundColor: colors,
                        borderColor: 'rgba(255, 251, 245, 0.95)',
                        borderWidth: 2
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    layout: {
                        padding: 0
                    },
                    plugins: {
                        legend: {
                            display: false
                        },
                        tooltip: {
                            callbacks: {
                                label: (context) => {
                                    const value = Number(context.raw) || 0;
                                    const mbps = (value * 8) / 60;
                                    return `${context.label}: ${formatPieMb(value)} (${mbps.toFixed(3)} Mbps)`;
                                }
                            }
                        }
                    }
                }
            });
            return;
        }

        topCurrentConsumersChart.data.labels = labels;
        topCurrentConsumersChart.data.datasets[0].data = values;
        topCurrentConsumersChart.data.datasets[0].backgroundColor = colors;
        topCurrentConsumersChart.update('none');
    };

    const getStreamUrl = () => `${streamBaseUrl}?window=${encodeURIComponent(selectedWindow)}&activity_span=${encodeURIComponent(selectedActivitySpan)}`;
    const getSnapshotUrl = () => `${snapshotBaseUrl}?window=${encodeURIComponent(selectedWindow)}&activity_span=${encodeURIComponent(selectedActivitySpan)}`;

    const applyPayload = (data) => {
        statUsageToday.textContent = `${formatInt(data.total_today_mb)} MB`;
        statUsage7Days.textContent = `${formatInt(data.total_last_7_days_mb)} MB`;
        statUsageThisMonth.textContent = `${formatInt(data.total_calendar_month_mb)} MB`;
        if (data.current_month_label) {
            statUsageMonthLabel.textContent = `Usage ${data.current_month_label}`;
            usageMonthHeader.textContent = data.current_month_label;
            const monthOption = windowSelect.querySelector('option[value="this_month"]');
            if (monthOption) {
                monthOption.textContent = data.current_month_label;
            }
        }

        if (data.selected_window && data.selected_window !== selectedWindow) {
            selectedWindow = data.selected_window;
            windowSelect.value = selectedWindow;
        }
        if (data.selected_activity_span && data.selected_activity_span !== selectedActivitySpan) {
            selectedActivitySpan = data.selected_activity_span;
            activitySpanSelect.value = selectedActivitySpan;
        }

        updateActivityScale(data.clients);
        applyWindowColumnVisibility();
        renderTopCurrentConsumers(data.top_current_consumers);
        renderConnectedClients(data.clients);
    };

    const fetchSnapshot = async () => {
        try {
            const response = await fetch(getSnapshotUrl(), { cache: 'no-store' });
            if (!response.ok) return;
            const data = await response.json();
            applyPayload(data);
        } catch (_err) {
            // Keep current values on transient fetch issues.
        }
    };

    const connectStream = () => {
        stream = new EventSource(getStreamUrl());

        stream.onmessage = (event) => {
            try {
                const data = JSON.parse(event.data);
                applyPayload(data);
                if (fallbackTimer) {
                    clearInterval(fallbackTimer);
                    fallbackTimer = null;
                }
            } catch (_err) {
                // Ignore malformed messages and keep current display.
            }
        };

        stream.onopen = () => {
            if (fallbackTimer) {
                clearInterval(fallbackTimer);
                fallbackTimer = null;
            }
        };

        stream.onerror = () => {
            if (!fallbackTimer) {
                fetchSnapshot();
                fallbackTimer = window.setInterval(fetchSnapshot, (initialPayload.live_update_seconds || 60) * 1000);
            }
        };
    };

    const reconnectForNewSelection = () => {
        selectedWindow = windowSelect.value;
        selectedActivitySpan = activitySpanSelect.value;
        if (stream) {
            stream.close();
        }
        if (fallbackTimer) {
            clearInterval(fallbackTimer);
            fallbackTimer = null;
        }
        fetchSnapshot();
        connectStream();
    };

    windowSelect.addEventListener('change', reconnectForNewSelection);
    activitySpanSelect.addEventListener('change', reconnectForNewSelection);

    applyPayload(initialPayload);
    connectStream();
})();
