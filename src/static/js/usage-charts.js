// @ts-nocheck
/* global Chart */
(() => {
    const renderedConfigIds = new Set();
    const commonBarStyle = {
        barPercentage: 0.72,
        categoryPercentage: 0.72,
        maxBarThickness: 20
    };
    const stackedDevicePalette = [
        "rgba(15, 118, 110, 0.82)",
        "rgba(37, 99, 235, 0.82)",
        "rgba(194, 65, 12, 0.82)",
        "rgba(79, 70, 229, 0.82)",
        "rgba(8, 145, 178, 0.82)",
        "rgba(21, 128, 61, 0.82)",
        "rgba(100, 116, 139, 0.82)"
    ];
    const throttleColors = [
        "rgba(194, 65, 12, 0.82)",
        "rgba(15, 118, 110, 0.72)",
        "rgba(59, 130, 246, 0.76)",
        "rgba(161, 98, 7, 0.78)",
        "rgba(2, 132, 199, 0.78)",
        "rgba(190, 24, 93, 0.78)",
        "rgba(22, 163, 74, 0.78)",
        "rgba(107, 114, 128, 0.78)"
    ];
    const accessPointPalette = [
        "rgba(0, 114, 178, 0.9)",
        "rgba(204, 121, 167, 0.9)",
        "rgba(0, 158, 115, 0.9)",
        "rgba(213, 94, 0, 0.9)",
        "rgba(86, 180, 233, 0.9)",
        "rgba(117, 112, 179, 0.9)",
        "rgba(181, 101, 29, 0.9)",
        "rgba(52, 64, 84, 0.9)",
        "rgba(0, 128, 128, 0.9)",
        "rgba(230, 159, 0, 0.9)",
        "rgba(228, 26, 28, 0.9)",
        "rgba(77, 77, 77, 0.9)"
    ];

    const parseConfig = (script) => {
        try {
            return JSON.parse(script.textContent || "{}");
        } catch (error) {
            console.warn("Skipping invalid usage chart config", error);
            return null;
        }
    };

    const xAxis = (title, offset = true) => ({
        offset,
        title: {
            display: true,
            text: title
        },
        grid: {
            display: false
        },
        ticks: {
            maxRotation: 0,
            autoSkip: true,
            maxTicksLimit: 12
        }
    });

    const legendLabels = {
        boxWidth: 20,
        boxHeight: 10,
        useBorderRadius: true,
        borderRadius: 3
    };

    const titleFor = (fullLabels) => (tooltipItems) => {
        const idx = tooltipItems[0]?.dataIndex ?? 0;
        return fullLabels[idx] || "";
    };

    const totalFromTooltip = (tooltipItems) => (
        tooltipItems.reduce((sum, item) => sum + (item.parsed?.y || 0), 0)
    );

    const deviceDatasets = (labels, mbSeries, rawSeries, fallbackLabel) => {
        const stackedMode = Array.isArray(rawSeries) && rawSeries.length > 0;
        const hasNamedSeries = stackedMode && rawSeries.some((series) => {
            const label = (series && typeof series.label === "string") ? series.label.trim() : "";
            return label.length > 0;
        });
        const datasets = stackedMode
            ? rawSeries.map((series, idx) => {
                const label = (series && typeof series.label === "string") ? series.label : "";
                return {
                    label,
                    data: labels.map((_, bucketIdx) => {
                        const points = Array.isArray(series?.data) ? series.data : [];
                        return Number(points[bucketIdx] || 0);
                    }),
                    backgroundColor: stackedDevicePalette[idx % stackedDevicePalette.length],
                    borderRadius: 2,
                    stack: "devices",
                    ...commonBarStyle
                };
            })
            : [
                {
                    label: fallbackLabel,
                    data: mbSeries,
                    backgroundColor: "rgba(15, 118, 110, 0.62)",
                    borderRadius: 3,
                    ...commonBarStyle
                }
            ];
        return { datasets, stackedMode, hasNamedSeries };
    };

    const throttleDatasets = (labels, throttleLabels, rawDatasets) => {
        const labelToIndex = new Map(
            (throttleLabels || []).map((bucketLabel, idx) => [Number(bucketLabel), idx])
        );
        return (rawDatasets || []).map((series, idx) => ({
            label: series.label,
            data: labels.map((bucketLabel) => {
                const mappedIndex = labelToIndex.get(Number(bucketLabel));
                if (mappedIndex === undefined) {
                    return 0;
                }
                const seriesData = Array.isArray(series.data) ? series.data : [];
                return Number(seriesData[mappedIndex] || 0);
            }),
            backgroundColor: series.label === "Default"
                ? "rgba(107, 114, 128, 0.38)"
                : throttleColors[idx % throttleColors.length],
            borderRadius: 2,
            stack: "throttle",
            ...commonBarStyle
        }));
    };

    const renderMbChart = (config) => {
        const canvas = document.getElementById(config.canvasId);
        if (!canvas) {
            return;
        }
        const labels = config.labels || [];
        const fullLabels = config.fullLabels || [];
        const { datasets, stackedMode, hasNamedSeries } = deviceDatasets(
            labels,
            config.mbSeries || [],
            config.deviceSeries || [],
            config.fallbackLabel || "Usage MB/day"
        );
        new Chart(canvas, {
            type: "bar",
            data: {
                labels,
                datasets
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                interaction: {
                    mode: "index",
                    intersect: false
                },
                plugins: {
                    legend: {
                        display: hasNamedSeries,
                        position: "top",
                        align: "start",
                        labels: legendLabels
                    },
                    tooltip: {
                        callbacks: {
                            title: titleFor(fullLabels),
                            afterBody: (tooltipItems) => {
                                if (!stackedMode) return "";
                                return `${config.totalLabel || "Total MB/day"}: ${totalFromTooltip(tooltipItems).toFixed(3)}`;
                            },
                        }
                    }
                },
                scales: {
                    x: {
                        stacked: stackedMode,
                        ...xAxis(config.xAxisTitle || "Day of month")
                    },
                    y: {
                        stacked: stackedMode,
                        beginAtZero: true,
                        title: {
                            display: true,
                            text: config.yAxisTitle || "MB/day"
                        },
                        grid: {
                            color: "rgba(31, 41, 51, 0.20)"
                        }
                    }
                }
            }
        });
    };

    const renderThrottleChart = (config) => {
        const canvas = document.getElementById(config.canvasId);
        if (!canvas) {
            return;
        }
        const labels = config.labels || [];
        const fullLabels = config.fullLabels || [];
        const datasets = config.alignToLabels === false
            ? (config.rawDatasets || []).map((series, idx) => ({
                label: series.label,
                data: series.data,
                backgroundColor: series.label === "Default"
                    ? "rgba(107, 114, 128, 0.38)"
                    : throttleColors[idx % throttleColors.length],
                borderRadius: 2,
                ...commonBarStyle,
                stack: "throttle"
            }))
            : throttleDatasets(labels, config.throttleLabels || labels, config.rawDatasets || []);

        new Chart(canvas, {
            type: "bar",
            data: { labels, datasets },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                layout: config.padRight ? { padding: { right: config.padRight } } : undefined,
                interaction: {
                    mode: "index",
                    intersect: false
                },
                plugins: {
                    legend: {
                        position: "top",
                        align: "start",
                        labels: legendLabels
                    },
                    tooltip: {
                        filter: (tooltipItem) => (tooltipItem.parsed?.y || 0) > 0,
                        callbacks: {
                            title: titleFor(fullLabels),
                            afterBody: (tooltipItems) => `Total active minutes: ${totalFromTooltip(tooltipItems)}`,
                        }
                    }
                },
                scales: {
                    x: {
                        stacked: true,
                        ...xAxis(config.xAxisTitle || "Day of month", config.xOffset !== false)
                    },
                    y: {
                        stacked: true,
                        beginAtZero: true,
                        title: {
                            display: true,
                            text: config.yAxisTitle || "Active minutes/day"
                        },
                        grid: {
                            color: "rgba(31, 41, 51, 0.20)"
                        }
                    }
                }
            }
        });
    };

    const renderAccessPointPie = (canvasId, apLabels, values, valueLabel) => {
        const canvas = document.getElementById(canvasId);
        if (!canvas) {
            return;
        }
        const emptyMessage = document.querySelector(`[data-empty-for="${canvas.id}"]`);
        const series = (apLabels || [])
            .map((apLabel, idx) => ({
                label: apLabel,
                value: Number(values?.[idx] || 0),
            }))
            .filter((row) => row.value > 0);

        if (!series.length) {
            canvas.hidden = true;
            if (emptyMessage) {
                emptyMessage.hidden = false;
            }
            return;
        }

        if (emptyMessage) {
            emptyMessage.hidden = true;
        }

        const total = series.reduce((sum, row) => sum + row.value, 0);
        new Chart(canvas, {
            type: "pie",
            data: {
                labels: series.map((row) => row.label),
                datasets: [
                    {
                        data: series.map((row) => row.value),
                        backgroundColor: series.map((_, idx) => accessPointPalette[idx % accessPointPalette.length]),
                        borderColor: "rgba(255, 255, 255, 0.85)",
                        borderWidth: 1,
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        position: "bottom",
                        labels: {
                            boxWidth: 12,
                            boxHeight: 12,
                            usePointStyle: true,
                            pointStyle: "circle",
                        }
                    },
                    tooltip: {
                        callbacks: {
                            label: (tooltipItem) => {
                                const value = Number(tooltipItem.parsed || 0);
                                const pct = total > 0 ? (value * 100.0) / total : 0;
                                const decimals = valueLabel === "MB" ? 1 : 0;
                                return `${tooltipItem.label}: ${value.toFixed(decimals)} ${valueLabel} (${pct.toFixed(1)}%)`;
                            }
                        }
                    }
                }
            }
        });
    };

    const renderUsageScale = (config) => {
        renderMbChart({
            canvasId: config.mbCanvasId,
            labels: config.labels,
            fullLabels: config.fullLabels,
            mbSeries: config.mbSeries,
            deviceSeries: config.deviceSeries,
            xAxisTitle: config.xAxisTitle,
            yAxisTitle: config.mbAxisTitle,
            fallbackLabel: "Usage",
            totalLabel: "Total"
        });
        renderThrottleChart({
            canvasId: config.minutesCanvasId,
            labels: config.labels,
            fullLabels: config.fullLabels,
            throttleLabels: config.throttleLabels,
            rawDatasets: config.throttleDatasets,
            xAxisTitle: config.xAxisTitle,
            yAxisTitle: config.minutesAxisTitle
        });
        renderAccessPointPie(config.mbApPieCanvasId, config.apLabels, config.apMbValues, "MB");
        renderAccessPointPie(config.minutesApPieCanvasId, config.apLabels, config.apMinutesValues, "minutes");
    };

    const renderInsights = (config) => {
        const activeClientsCanvas = document.getElementById("global-daily-active-clients-chart");
        if (activeClientsCanvas) {
            new Chart(activeClientsCanvas, {
                type: "bar",
                data: {
                    labels: config.activeUsersXLabels,
                    datasets: [
                        {
                            label: "Active Clients",
                            data: config.activeUsersCounts,
                            backgroundColor: "rgba(59, 130, 246, 0.78)"
                        }
                    ]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: { display: false },
                        tooltip: {
                            callbacks: {
                                title: titleFor(config.activeUsersFullLabels || []),
                            }
                        }
                    },
                    scales: {
                        x: xAxis("Day of month"),
                        y: {
                            beginAtZero: true,
                            title: { display: true, text: "Active clients/day" },
                            grid: { color: "rgba(31, 41, 51, 0.20)" }
                        }
                    }
                }
            });
        }

        const sharedStackedOptions = {
            responsive: true,
            maintainAspectRatio: false,
            interaction: {
                mode: "index",
                intersect: false
            },
            plugins: {
                legend: {
                    position: "top",
                    align: "start"
                },
                tooltip: {
                    callbacks: {
                        title: titleFor(config.fullLabels || []),
                    }
                }
            },
            scales: {
                x: {
                    stacked: true,
                    ...xAxis("Day of month")
                },
                y: {
                    stacked: true,
                    beginAtZero: true,
                    grid: {
                        color: "rgba(31, 41, 51, 0.20)"
                    }
                }
            }
        };

        const mbCanvas = document.getElementById("global-daily-mb-chart");
        if (mbCanvas) {
            new Chart(mbCanvas, {
                type: "bar",
                data: {
                    labels: config.xLabels,
                    datasets: [
                        { label: "Basic", data: config.basicMb, backgroundColor: "rgba(15, 118, 110, 0.72)" },
                        { label: "Plus", data: config.plusMb, backgroundColor: "rgba(194, 65, 12, 0.82)" }
                    ]
                },
                options: {
                    ...sharedStackedOptions,
                    scales: {
                        ...sharedStackedOptions.scales,
                        y: { ...sharedStackedOptions.scales.y, title: { display: true, text: "MB/day" } }
                    }
                }
            });
        }

        const minutesCanvas = document.getElementById("global-daily-minutes-chart");
        if (minutesCanvas) {
            new Chart(minutesCanvas, {
                type: "bar",
                data: {
                    labels: config.xLabels,
                    datasets: [
                        { label: "Basic", data: config.basicMinutes, backgroundColor: "rgba(15, 118, 110, 0.72)" },
                        { label: "Plus", data: config.plusMinutes, backgroundColor: "rgba(194, 65, 12, 0.82)" }
                    ]
                },
                options: {
                    ...sharedStackedOptions,
                    scales: {
                        ...sharedStackedOptions.scales,
                        y: { ...sharedStackedOptions.scales.y, title: { display: true, text: "Active minutes/day" } }
                    }
                }
            });
        }

        const peakConcurrencyCanvas = document.getElementById("global-peak-concurrency-chart");
        if (peakConcurrencyCanvas) {
            new Chart(peakConcurrencyCanvas, {
                type: "bar",
                data: {
                    labels: config.peakConcurrencyXLabels,
                    datasets: [{ label: "Peak simultaneous users", data: config.peakConcurrencyCounts, backgroundColor: "rgba(2, 132, 199, 0.76)" }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: { display: false },
                        tooltip: {
                            callbacks: {
                                title: titleFor(config.peakConcurrencyFullLabels || []),
                                afterBody: (tooltipItems) => {
                                    const idx = tooltipItems[0]?.dataIndex ?? 0;
                                    const peakTime = config.peakConcurrencyTimeLabels?.[idx] || "n/a";
                                    return `Peak time: ${peakTime}`;
                                },
                            }
                        }
                    },
                    scales: {
                        x: xAxis("Day of month"),
                        y: {
                            beginAtZero: true,
                            title: { display: true, text: "Peak concurrent users" },
                            grid: { color: "rgba(31, 41, 51, 0.20)" }
                        }
                    }
                }
            });
        }

        const profileMinutesCanvas = document.getElementById("global-throttling-profile-minutes-chart");
        if (profileMinutesCanvas && config.throttlingProfileLabels?.length) {
            new Chart(profileMinutesCanvas, {
                type: "bar",
                data: {
                    labels: config.throttlingProfileLabels,
                    datasets: [{ label: "Minutes", data: config.throttlingProfileMinutes, backgroundColor: "rgba(194, 65, 12, 0.72)" }]
                },
                options: {
                    indexAxis: "y",
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: { legend: { display: false } },
                    scales: {
                        x: {
                            beginAtZero: true,
                            title: { display: true, text: "Active minutes this month" },
                            grid: { color: "rgba(31, 41, 51, 0.20)" }
                        },
                        y: { grid: { display: false } }
                    }
                }
            });
        }

        const heatmapContainer = document.getElementById("global-concurrency-heatmap");
        if (heatmapContainer) {
            const heatmapValues = config.heatmapValues || [];
            const heatmapSampleCounts = config.heatmapSampleCounts || [];
            const heatmapDayLabels = config.heatmapDayLabels || [];
            const heatmapHourLabels = config.heatmapHourLabels || [];
            const flatValues = heatmapValues.flat();
            const maxValue = Math.max(...flatValues, 0);
            const minValue = Math.min(...flatValues, 0);
            const colorForValue = (value) => {
                if (maxValue <= minValue) return "rgba(148, 163, 184, 0.24)";
                const ratio = (value - minValue) / (maxValue - minValue);
                const alpha = 0.12 + (ratio * 0.78);
                return `rgba(14, 116, 144, ${alpha.toFixed(3)})`;
            };
            let html = '<div class="cell header"></div>';
            for (const hourLabel of heatmapHourLabels) {
                html += `<div class="cell header hour">${hourLabel.slice(0, 2)}</div>`;
            }
            for (let dayIndex = 0; dayIndex < heatmapDayLabels.length; dayIndex += 1) {
                html += `<div class="cell header day">${heatmapDayLabels[dayIndex]}</div>`;
                const row = heatmapValues[dayIndex] || [];
                const sampleRow = heatmapSampleCounts[dayIndex] || [];
                for (let hourIndex = 0; hourIndex < heatmapHourLabels.length; hourIndex += 1) {
                    const value = Number(row[hourIndex] || 0);
                    const sampleCount = Number(sampleRow[hourIndex] || 0);
                    const coveragePct = Math.min(100, (sampleCount / 60) * 100);
                    const title = `${heatmapDayLabels[dayIndex]} ${heatmapHourLabels[hourIndex]} | total user-minutes: ${Math.round(value)} | active minutes observed: ${sampleCount} | hour coverage: ${coveragePct.toFixed(1)}%`;
                    const cellText = Math.round(value).toLocaleString();
                    html += `<div class="cell value" style="background:${colorForValue(value)}" title="${title}">${cellText}</div>`;
                }
            }
            heatmapContainer.innerHTML = html;
        }
    };

    const renderConfig = (config) => {
        if (typeof Chart === "undefined") {
            return;
        }
        if (config.type === "month-mb") {
            renderMbChart(config);
        } else if (config.type === "month-throttle") {
            renderThrottleChart(config);
        } else if (config.type === "usage-scale") {
            renderUsageScale(config);
        } else if (config.type === "insights") {
            renderInsights(config);
        }
    };

    const renderAll = () => {
        document.querySelectorAll('script[data-usage-chart-config]').forEach((script) => {
            const configId = script.id || script.dataset.usageChartConfig;
            if (renderedConfigIds.has(configId)) {
                return;
            }
            const config = parseConfig(script);
            if (!config) {
                return;
            }
            renderedConfigIds.add(configId);
            renderConfig(config);
        });
    };

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", renderAll);
    } else {
        renderAll();
    }
})();
