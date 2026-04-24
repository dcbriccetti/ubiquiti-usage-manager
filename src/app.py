'''Flask application entry module.

This module keeps the web entrypoint intentionally small:
- creates the Flask app
- registers HTTP routes
- delegates dashboard data shaping to dashboard_service
- delegates SSE frame generation to dashboard_stream

Keeping route glue here and business/view-model logic in helper modules reduces
merge conflicts and makes testing easier because each module has a tighter scope.
'''
from datetime import datetime
import csv
import io
import os
import re
import zipfile
from typing import Any, TypedDict

from flask import Flask, Response, abort, jsonify, redirect, render_template, request, send_file, stream_with_context, url_for

import config as cfg
import database as db
import unifi_api as api
from database import UsageRecord
from dashboard_service import (
    build_insights_data,
    build_live_dashboard_payload,
    normalize_activity_span,
    normalize_window,
)
from dashboard_stream import event_stream
from lan_identity import find_client_mac_for_ip, get_request_ip
from logging_config import configure_logging
from monitor import get_connected_clients
from speedlimit import SpeedLimit

SpeedLimitsByName = dict[str, SpeedLimit]


class DailyUsagePoint(TypedDict):
    'One day point for month usage chart.'
    day_label: str
    day_of_month: int
    total_mb: float
    active_minutes: int


class ThrottleChartDataset(TypedDict):
    'One stacked-bar series for monthly throttling chart.'
    label: str
    data: list[int]


class ClientUsageContext(TypedDict):
    'Template context for client-detail and my-usage pages.'
    mac: str
    latest_record: UsageRecord
    usage_history: list[UsageRecord]
    daily_total_mb: float
    last_7_days_total_mb: float
    calendar_month_total_mb: float
    month_cost_cents: float
    month_daily_usage: list[DailyUsagePoint]
    month_usage_device_series: list[dict[str, object]]
    month_throttle_day_labels: list[str]
    month_throttle_x_labels: list[int]
    month_throttle_datasets: list[ThrottleChartDataset]
    current_month_label: str
    speed_limits_by_name: SpeedLimitsByName


def create_app() -> Flask:
    'Create and configure the Flask web application.'
    configure_logging()
    flask_app = Flask(__name__)
    live_update_seconds = 60
    live_update_boundary_offset_seconds = 3

    def render_month_label(now: datetime) -> str:
        'Return full month name unless it is long, then use abbreviation.'
        full_label = now.strftime('%B')
        if len(full_label) > 5:
            return now.strftime('%b')
        return full_label

    def calculate_month_cost_cents(calendar_month_total_mb: float) -> float:
        'Return month cost in cents using configured rate.'
        return (calendar_month_total_mb / 1000.0) * float(cfg.COST_IN_CENTS_PER_GB)

    def get_organization_title() -> str:
        'Return organization title for reports.'
        raw_title = getattr(cfg, 'ORGANIZATION_TITLE', '')
        return raw_title.strip() if isinstance(raw_title, str) else ''

    def get_plus_report_title_prefix() -> str:
        'Return optional prefix shown before "Network Usage Report".'
        raw_title = getattr(cfg, 'PLUS_REPORT_TITLE', '')
        return raw_title.strip() if isinstance(raw_title, str) else ''

    def get_plus_network_report_title() -> str:
        'Return full Plus report title label.'
        if prefix := get_plus_report_title_prefix():
            return f'{prefix} Network Usage Report'
        return 'Network Usage Report'

    def render_report_period_label(now: datetime) -> str:
        'Return report period label with month and year.'
        return now.strftime('%B %Y')

    def safe_file_stem(raw_value: str) -> str:
        'Return a filesystem-safe lowercase stem suitable for export filenames.'
        normalized = re.sub(r'[^a-zA-Z0-9._-]+', '-', raw_value.strip()).strip('-').lower()
        return normalized or 'unknown-user'

    def build_plus_user_chart_context(summary: db.PlusUserInvoiceSummary) -> dict[str, object]:
        'Build daily-usage chart dataset for Plus-user invoice summary pages/exports.'
        month_daily_usage: list[DailyUsagePoint] = [
            {
                'day_label': f'{usage_day.strftime("%b")} {usage_day.day}',
                'day_of_month': usage_day.day,
                'total_mb': total_mb,
                'active_minutes': active_minutes,
            }
            for usage_day, total_mb, active_minutes in summary.daily_usage
        ]
        stacked_day_labels, stacked_device_series = db.get_plus_user_daily_device_usage_current_month(summary.user_id)
        month_usage_device_series = [
            {
                'label': device_label,
                'data': series,
            }
            for device_label, series in stacked_device_series
        ]
        return {
            'month_daily_usage': month_daily_usage,
            'month_usage_device_series': month_usage_device_series,
            'month_usage_day_labels': [usage_day.day for usage_day in stacked_day_labels],
            'month_usage_full_labels': [f'{usage_day.strftime("%b")} {usage_day.day}' for usage_day in stacked_day_labels],
        }

    def build_plus_user_invoice_pdf(
        summary: db.PlusUserInvoiceSummary,
        report_period_label: str,
        generated_at: datetime,
    ) -> bytes:
        'Render one invoice-ready PDF summary for a Plus user.'
        from reportlab.graphics import renderPDF
        from reportlab.graphics.charts.barcharts import VerticalBarChart
        from reportlab.graphics.shapes import Drawing, Rect, String
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import letter
        from reportlab.pdfgen import canvas

        chart_context = build_plus_user_chart_context(summary)
        month_daily_usage = chart_context['month_daily_usage']
        day_labels = [str(point['day_of_month']) for point in month_daily_usage]
        usage_mb_series = [float(point['total_mb']) for point in month_daily_usage]
        stacked_day_labels, stacked_device_series = db.get_plus_user_daily_device_usage_current_month(summary.user_id)

        pdf_buffer = io.BytesIO()
        pdf = canvas.Canvas(pdf_buffer, pagesize=letter)
        page_width, page_height = letter

        margin_x = 42
        content_width = page_width - (margin_x * 2)
        y_cursor = page_height - 46
        text_primary = colors.HexColor('#0f172a')
        text_muted = colors.HexColor('#475569')
        accent = colors.HexColor('#0f766e')
        border_soft = colors.HexColor('#d9dee7')
        panel_fill = colors.HexColor('#fafcff')
        stat_fill = colors.HexColor('#f8fafc')
        header_fill = colors.HexColor('#eef2f7')
        identity_fill = colors.HexColor('#e6f4f1')

        report_title = get_plus_network_report_title()
        pdf.setTitle(f"{report_title} - {summary.user_id}")
        title_font_size = 17.0
        max_title_width = content_width
        while title_font_size > 13.0 and pdf.stringWidth(report_title, "Helvetica-Bold", title_font_size) > max_title_width:
            title_font_size -= 0.5
        pdf.setFillColor(text_primary)
        pdf.setFont("Helvetica-Bold", title_font_size)
        pdf.drawString(margin_x, y_cursor, report_title)
        pdf.setFillColor(text_muted)
        pdf.setFont("Helvetica", 9.5)
        organization_title = get_organization_title()
        header_meta_y = y_cursor - 18
        if organization_title:
            pdf.drawString(margin_x, header_meta_y, organization_title)
            header_meta_y -= 13
            pdf.drawString(margin_x, header_meta_y, f"Period: {report_period_label}")
            pdf.drawString(margin_x + 180, header_meta_y, f"Generated: {generated_at.strftime('%Y-%m-%d %H:%M:%S')}")
        else:
            pdf.drawString(margin_x, header_meta_y, f"Period: {report_period_label}")
            pdf.drawString(margin_x + 140, header_meta_y, f"Generated: {generated_at.strftime('%Y-%m-%d %H:%M:%S')}")

        divider_y = header_meta_y - 16
        identity_box_height = 38
        identity_box_x = margin_x
        identity_box_y = divider_y - 10 - identity_box_height
        identity_box_width = content_width
        pdf.setFillColor(identity_fill)
        pdf.setStrokeColor(colors.HexColor('#bddfd8'))
        pdf.roundRect(identity_box_x, identity_box_y, identity_box_width, identity_box_height, 8, stroke=1, fill=1)
        pdf.setFillColor(accent)
        pdf.setFont("Helvetica-Bold", 8.2)
        pdf.drawString(identity_box_x + 10, identity_box_y + identity_box_height - 13, "USER ID")
        pdf.setFillColor(text_primary)
        pdf.setFont("Helvetica-Bold", 14.2)
        pdf.drawString(identity_box_x + 10, identity_box_y + 10, summary.user_id)

        pdf.setStrokeColor(border_soft)
        pdf.setLineWidth(0.8)
        pdf.line(margin_x, divider_y, page_width - margin_x, divider_y)

        card_top = identity_box_y - 14
        card_gap = 10
        card_count = 4
        card_width = (content_width - (card_gap * (card_count - 1))) / card_count
        card_height = 66
        card_specs = [
            (f"Usage {report_period_label}", f"{summary.total_mb:,.0f} MB"),
            ("Active Minutes", f"{summary.active_minutes:,}"),
            ("Devices", f"{summary.device_count:,}"),
            ("Cost", f"${(calculate_month_cost_cents(summary.total_mb) / 100.0):,.2f}"),
        ]
        for idx, (label, value) in enumerate(card_specs):
            card_x = margin_x + (idx * (card_width + card_gap))
            card_y = card_top - card_height
            pdf.setFillColor(stat_fill)
            pdf.setStrokeColor(border_soft)
            pdf.roundRect(card_x, card_y, card_width, card_height, 8, stroke=1, fill=1)
            pdf.setFillColor(text_muted)
            pdf.setFont("Helvetica-Bold", 8.1)
            pdf.drawString(card_x + 10, card_y + card_height - 18, label.upper())
            pdf.setFillColor(text_primary)
            pdf.setFont("Helvetica-Bold", 15)
            pdf.drawString(card_x + 10, card_y + 18, value)

        chart_panel_top = card_top - card_height - 16
        chart_panel_height = 242
        chart_panel_y = chart_panel_top - chart_panel_height
        pdf.setFillColor(panel_fill)
        pdf.setStrokeColor(border_soft)
        pdf.roundRect(margin_x, chart_panel_y, content_width, chart_panel_height, 10, stroke=1, fill=1)
        pdf.setFillColor(text_primary)
        pdf.setFont("Helvetica-Bold", 11.5)
        pdf.drawString(margin_x + 12, chart_panel_top - 18, f"{report_period_label} Usage")

        chart_body_width = int(content_width - 24)
        chart_body_height = 194
        usage_chart = Drawing(chart_body_width, chart_body_height)
        usage_chart.add(
            Rect(
                0,
                0,
                chart_body_width,
                chart_body_height - 4,
                fillColor=colors.HexColor('#fbfdff'),
                strokeColor=border_soft,
                strokeWidth=0.7,
            )
        )
        usage_chart.add(
            String(
                12,
                172,
                f"{report_period_label} Usage (MB per day)",
                fontName="Helvetica-Bold",
                fontSize=10,
                fillColor=text_primary,
            )
        )
        usage_bar = VerticalBarChart()
        usage_bar.x = 42
        usage_bar.y = 28
        plot_width = max(260, chart_body_width - 182)
        usage_bar.width = plot_width
        usage_bar.height = 130
        if stacked_day_labels and stacked_device_series:
            usage_bar.data = [series for _, series in stacked_device_series]
            usage_bar.categoryAxis.style = "stacked"
            usage_bar.categoryAxis.categoryNames = [str(usage_day.day) for usage_day in stacked_day_labels]
            category_count = len(stacked_day_labels)
        else:
            usage_bar.data = [usage_mb_series or [0.0]]
            usage_bar.categoryAxis.categoryNames = day_labels or [""]
            category_count = len(day_labels)
        usage_bar.valueAxis.valueMin = 0
        usage_bar.valueAxis.visibleGrid = 1
        usage_bar.valueAxis.gridStrokeColor = colors.HexColor('#e2e8f0')
        usage_bar.valueAxis.gridStrokeWidth = 0.6
        usage_bar.valueAxis.strokeColor = colors.HexColor('#94a3b8')
        usage_bar.valueAxis.labels.fontSize = 6.5
        usage_bar.valueAxis.labels.fillColor = text_muted
        usage_bar.valueAxis.labelTextFormat = '%0.0f'
        usage_bar.categoryAxis.strokeColor = colors.HexColor('#94a3b8')
        usage_bar.categoryAxis.labels.fontSize = 6.5
        usage_bar.categoryAxis.labels.fillColor = text_muted
        usage_bar.barWidth = max(2, min(11, int(320 / max(1, category_count))))
        usage_bar.groupSpacing = 2
        usage_bar.barSpacing = 1
        palette = [
            colors.HexColor('#0f766e'),
            colors.HexColor('#2563eb'),
            colors.HexColor('#c2410c'),
            colors.HexColor('#4f46e5'),
            colors.HexColor('#0891b2'),
            colors.HexColor('#15803d'),
            colors.HexColor('#64748b'),
        ]
        for idx in range(len(usage_bar.data)):
            usage_bar.bars[idx].fillColor = palette[idx % len(palette)]
            usage_bar.bars[idx].strokeColor = colors.HexColor('#f8fafc')
            usage_bar.bars[idx].strokeWidth = 0.2

        usage_chart.add(usage_bar)
        legend_swatch_x = usage_bar.x + usage_bar.width + 16
        legend_swatch_size = 7
        legend_title_y = 160
        legend_first_row_center_y = 146
        legend_row_spacing = 13
        if stacked_device_series:
            usage_chart.add(String(legend_swatch_x, legend_title_y, "Devices", fontName="Helvetica-Bold", fontSize=7.4, fillColor=text_primary))
            for idx, (device_label, _) in enumerate(stacked_device_series):
                row_center_y = legend_first_row_center_y - (idx * legend_row_spacing)
                if row_center_y < 18:
                    break
                label_text = device_label
                if len(label_text) > 18:
                    label_text = f"{label_text[:15]}..."
                usage_chart.add(
                    Rect(
                        legend_swatch_x,
                        row_center_y - (legend_swatch_size / 2),
                        legend_swatch_size,
                        legend_swatch_size,
                        fillColor=palette[idx % len(palette)],
                        strokeColor=colors.white,
                        strokeWidth=0.3,
                    )
                )
                usage_chart.add(
                    String(
                        legend_swatch_x + 12,
                        row_center_y - 2.2,
                        label_text,
                        fontName="Helvetica",
                        fontSize=6.8,
                        fillColor=text_muted,
                    )
                )
        else:
            usage_chart.add(
                Rect(
                    legend_swatch_x,
                    legend_first_row_center_y - (legend_swatch_size / 2),
                    legend_swatch_size,
                    legend_swatch_size,
                    fillColor=palette[0],
                    strokeColor=colors.white,
                    strokeWidth=0.3,
                )
            )
            usage_chart.add(
                String(
                    legend_swatch_x + 12,
                    legend_first_row_center_y - 2.2,
                    "Total",
                    fontName="Helvetica",
                    fontSize=6.8,
                    fillColor=text_muted,
                )
            )

        renderPDF.draw(usage_chart, pdf, margin_x + 12, chart_panel_y + 24)

        devices_panel_top = chart_panel_y - 14
        devices_panel_y = 52
        devices_panel_height = max(0, devices_panel_top - devices_panel_y)
        pdf.setFillColor(panel_fill)
        pdf.setStrokeColor(border_soft)
        pdf.roundRect(margin_x, devices_panel_y, content_width, devices_panel_height, 10, stroke=1, fill=1)
        pdf.setFillColor(text_primary)
        pdf.setFont("Helvetica-Bold", 11.5)
        pdf.drawString(margin_x + 12, devices_panel_top - 18, "Devices")

        table_left = margin_x + 12
        table_column_widths = [145.0, 130.0, 55.0, 55.0]
        table_width = sum(table_column_widths)
        max_table_width = content_width - 24
        if table_width > max_table_width:
            shrink_ratio = max_table_width / table_width
            table_column_widths = [width * shrink_ratio for width in table_column_widths]
            table_width = sum(table_column_widths)
        table_right = table_left + table_width
        table_top = devices_panel_top - 30
        header_height = 18
        row_height = 16
        col_lefts: list[float] = [table_left]
        cursor_x = table_left
        for width in table_column_widths[:-1]:
            cursor_x += width
            col_lefts.append(cursor_x)
        col_rights = col_lefts[1:] + [table_right]

        pdf.setFillColor(header_fill)
        pdf.setStrokeColor(border_soft)
        pdf.rect(table_left, table_top - header_height, table_width, header_height, stroke=1, fill=1)
        pdf.setFillColor(text_muted)
        pdf.setFont("Helvetica-Bold", 8.6)
        header_labels = ["Name", "MAC", "MB", "Minutes"]
        for idx, header_label in enumerate(header_labels):
            if idx < 2:
                pdf.drawString(col_lefts[idx] + 6, table_top - 12, header_label)
            else:
                text = header_label
                text_width = pdf.stringWidth(text, "Helvetica-Bold", 8.6)
                pdf.drawString(col_rights[idx] - text_width - 6, table_top - 12, text)

        max_rows = max(0, int((table_top - header_height - (devices_panel_y + 10)) // row_height))
        visible_devices = summary.devices[:max_rows]
        omitted_count = max(0, len(summary.devices) - len(visible_devices))

        row_top = table_top - header_height
        pdf.setFont("Helvetica", 8.8)
        for row in visible_devices:
            row_bottom = row_top - row_height
            pdf.setStrokeColor(border_soft)
            pdf.line(table_left, row_bottom, table_right, row_bottom)
            pdf.setFillColor(text_primary)
            name_text = row.name if len(row.name) <= 30 else f'{row.name[:27]}...'
            pdf.drawString(col_lefts[0] + 6, row_top - 11.5, name_text)
            pdf.setFont("Helvetica", 8.2)
            pdf.drawString(col_lefts[1] + 6, row_top - 11.3, row.mac)
            pdf.setFont("Helvetica", 8.8)
            mb_text = f'{row.total_mb:,.0f}'
            minutes_text = f'{row.active_minutes:,.0f}'
            pdf.drawString(col_rights[2] - pdf.stringWidth(mb_text, "Helvetica", 8.8) - 6, row_top - 11.5, mb_text)
            pdf.drawString(col_rights[3] - pdf.stringWidth(minutes_text, "Helvetica", 8.8) - 6, row_top - 11.5, minutes_text)
            row_top = row_bottom

        if not visible_devices:
            pdf.setFillColor(text_muted)
            pdf.setFont("Helvetica", 9)
            pdf.drawString(table_left + 6, row_top - 13, "No device activity found for this month.")
        elif omitted_count > 0:
            pdf.setFillColor(text_muted)
            pdf.setFont("Helvetica", 8.5)
            pdf.drawString(table_left + 6, max(devices_panel_y + 8, row_top - 13), f"+ {omitted_count} additional devices not shown")

        pdf.showPage()
        pdf.save()
        pdf_buffer.seek(0)
        return pdf_buffer.getvalue()

    def get_speed_limits_by_name() -> SpeedLimitsByName:
        'Return mapping of speed-limit profile name to SpeedLimit object.'
        return {limit.name: limit for limit in api.get_speed_limits()}

    def get_live_client_record_by_mac(mac: str) -> dict[str, Any] | None:
        'Return live UniFi station payload for one MAC, if currently connected.'
        target_mac = mac.lower()
        for client in api.get_api_data("stat/sta"):
            raw_mac = client.get("mac")
            if isinstance(raw_mac, str) and raw_mac.lower() == target_mac:
                return client
        return None

    def is_plus_network(vlan_name: str | None) -> bool:
        'Return True when the VLAN/network label represents the Plus network.'
        return bool(vlan_name and vlan_name.strip().lower() == "plus")

    def is_plus_admin_user(user_id: str | None, vlan_name: str | None) -> bool:
        'Return True when requester is a Plus user whose RADIUS username is in admin allowlist.'
        if not is_plus_network(vlan_name) or not user_id:
            return False
        return user_id.strip().lower() in cfg.PLUS_ADMINS

    def resolve_request_ip() -> str | None:
        'Return request IP, allowing DEV_REQUEST_IP override for local/remote testing.'
        if dev_request_ip := os.getenv("DEV_REQUEST_IP", "").strip():
            return dev_request_ip
        return get_request_ip(request)

    def resolve_my_usage_mac(request_ip: str | None) -> tuple[str | None, str]:
        'Resolve MAC for my-usage routes, allowing MY_USAGE_OVERRIDE_MAC env override.'
        override_mac_raw = os.getenv("MY_USAGE_OVERRIDE_MAC", "").strip()
        if override_mac_raw:
            override_mac_normalized = override_mac_raw.lower().replace('-', ':')
            if re.fullmatch(r'(?:[0-9a-f]{2}:){5}[0-9a-f]{2}', override_mac_normalized):
                return override_mac_normalized, ''
            return None, "MY_USAGE_OVERRIDE_MAC is set but is not a valid MAC address."

        if not request_ip:
            return None, "Could not determine your client IP address from this request."

        if detected_mac := find_client_mac_for_ip(request_ip):
            return detected_mac, ''

        return None, (
            "Could not map your IP to a UniFi client right now. "
            "Try again in a moment after generating some network activity."
        )

    def dev_force_plus_admin_enabled() -> bool:
        'Return True when DEV_FORCE_PLUS_ADMIN requests admin-access bypass for testing.'
        return os.getenv("DEV_FORCE_PLUS_ADMIN", "").strip().lower() in {"1", "true", "yes", "on"}

    def speed_limit_option_label(limit: SpeedLimit) -> str:
        'Build select-option label for one speed-limit profile.'
        rendered = str(limit)
        if rendered:
            return rendered
        return f'{limit.name} (Unlimited)'

    def profile_display_label(profile_key: str, speed_limits_by_name: SpeedLimitsByName) -> str:
        'Render chart/display label for one stored profile name key.'
        if not profile_key:
            return 'Default'
        if matched_limit := speed_limits_by_name.get(profile_key):
            return speed_limit_option_label(matched_limit)
        return profile_key

    def profile_throttling_impact(profile_key: str, speed_limits_by_name: SpeedLimitsByName) -> float:
        'Return throttling-impact score where larger means more restrictive.'
        if not profile_key:
            return -1.0

        matched_limit = speed_limits_by_name.get(profile_key)
        if not matched_limit:
            return -0.5

        caps: list[int] = []
        for cap in (matched_limit.up_kbps, matched_limit.down_kbps):
            if isinstance(cap, int) and cap > 0:
                caps.append(cap)
        if not caps:
            return 0.0

        strictest_cap_kbps: int = min(caps)
        return 1_000_000.0 / float(strictest_cap_kbps)

    def warn_missing_radius_identity(record: UsageRecord, request_ip: str | None, detected_mac: str | None) -> None:
        'Log warning when Plus-network client metadata is missing RADIUS user_id.'
        if is_plus_network(record.vlan) and not (record.user_id and record.user_id.strip()):
            flask_app.logger.warning(
                (
                    'Plus-network client missing RADIUS user_id '
                    '(request_ip=%s detected_mac=%s record_mac=%s vlan=%s name=%s ap_name=%s)'
                ),
                request_ip or '',
                detected_mac or '',
                record.mac,
                record.vlan or '',
                record.name or '',
                record.ap_name or '',
            )

    def requester_is_plus_admin() -> bool:
        'Resolve current requester and return whether they are a Plus admin.'
        if dev_force_plus_admin_enabled():
            return True

        if not (request_ip := resolve_request_ip()):
            return False

        detected_mac = find_client_mac_for_ip(request_ip)
        if not detected_mac:
            return False

        usage_history = db.get_usage_history(detected_mac, limit=1)
        if usage_history:
            latest_record = usage_history[0]
            warn_missing_radius_identity(latest_record, request_ip, detected_mac)
            return is_plus_admin_user(latest_record.user_id, latest_record.vlan)

        if live_client := get_live_client_record_by_mac(detected_mac):
            live_user_id = (
                live_client.get('1x_identity')
                or live_client.get('identity')
                or live_client.get('last_1x_identity')
            )
            if not live_user_id:
                last_identities = live_client.get('last_1x_identities')
                if isinstance(last_identities, list) and last_identities:
                    first_identity = last_identities[0]
                    live_user_id = first_identity if isinstance(first_identity, str) else None
            live_vlan_name = live_client.get('network')
            if not isinstance(live_vlan_name, str):
                live_vlan_name = None
            return is_plus_admin_user(live_user_id, live_vlan_name)

        return False

    def get_client_usage_context(mac: str) -> ClientUsageContext:
        'Build shared usage/detail context used by both admin and self-service pages.'
        if usage_history := db.get_usage_history(mac):
            latest_record = usage_history[0]
        else:
            if (live_snapshot := next(
                (
                    snapshot
                    for snapshot in get_connected_clients()
                    if snapshot.client.mac.lower() == mac.lower()
                ),
                None,
            )) is None:
                raise LookupError(f'No usage or live snapshot found for MAC {mac}')

            latest_record = db.UsageRecord(
                mac=live_snapshot.client.mac,
                user_id=live_snapshot.client.user_id,
                name=live_snapshot.client.name,
                vlan=live_snapshot.client.vlan_name,
                mb_used=live_snapshot.interval_mb,
                profile=(
                    live_snapshot.client.speed_limit.name
                    if live_snapshot.client.speed_limit
                    else None
                ),
                ap_name=live_snapshot.client.ap_name,
                signal=live_snapshot.client.signal,
            )
            usage_history = []

        speed_limits_by_name = get_speed_limits_by_name()
        calendar_month_total_mb = db.get_calendar_month_total(mac)
        month_daily_usage: list[DailyUsagePoint] = [
            {
                'day_label': f'{usage_day.strftime("%b")} {usage_day.day}',
                'day_of_month': usage_day.day,
                'total_mb': total_mb,
                'active_minutes': active_minutes,
            }
            for usage_day, total_mb, active_minutes in db.get_calendar_month_daily_totals(mac)
        ]
        month_throttle_rows = db.get_calendar_month_daily_profile_minutes(mac)
        month_throttle_day_labels = [f'{usage_day.strftime("%b")} {usage_day.day}' for usage_day, _ in month_throttle_rows]
        month_throttle_x_labels = [usage_day.day for usage_day, _ in month_throttle_rows]

        totals_by_profile_key: dict[str, int] = {}
        for _, daily_counts in month_throttle_rows:
            for profile_key, minutes in daily_counts.items():
                totals_by_profile_key[profile_key] = totals_by_profile_key.get(profile_key, 0) + minutes

        sorted_profile_keys = sorted(
            totals_by_profile_key.keys(),
            key=lambda key: (
                profile_throttling_impact(key, speed_limits_by_name),
                totals_by_profile_key[key],
            ),
        )
        month_throttle_datasets: list[ThrottleChartDataset] = [
            {
                'label': profile_display_label(profile_key, speed_limits_by_name),
                'data': [daily_counts.get(profile_key, 0) for _, daily_counts in month_throttle_rows],
            }
            for profile_key in sorted_profile_keys
        ]

        return {
            'mac': mac,
            'latest_record': latest_record,
            'usage_history': usage_history,
            'daily_total_mb': db.get_daily_total(mac),
            'last_7_days_total_mb': db.get_last_7_days_total(mac),
            'calendar_month_total_mb': calendar_month_total_mb,
            'month_cost_cents': calculate_month_cost_cents(calendar_month_total_mb),
            'month_daily_usage': month_daily_usage,
            'month_usage_device_series': [
                {
                    'label': '',
                    'data': [point['total_mb'] for point in month_daily_usage],
                }
            ],
            'month_throttle_day_labels': month_throttle_day_labels,
            'month_throttle_x_labels': month_throttle_x_labels,
            'month_throttle_datasets': month_throttle_datasets,
            'current_month_label': render_month_label(datetime.now()),
            'speed_limits_by_name': speed_limits_by_name,
        }

    @flask_app.route("/")
    def dashboard():
        'Render the dashboard with live snapshots and daily usage summaries.'
        if not requester_is_plus_admin():
            return redirect(url_for("my_usage"))

        window_name = normalize_window(request.args.get("window"))
        activity_span = normalize_activity_span(request.args.get("activity_span"))
        dashboard_data = build_live_dashboard_payload(window_name, activity_span, live_update_seconds)
        return render_template(
            "dashboard.html",
            initial_dashboard_payload=dashboard_data,
        )

    @flask_app.route("/insights")
    def insights():
        'Render deeper month-to-date analytics panels.'
        if not requester_is_plus_admin():
            abort(403)

        return render_template(
            "insights.html",
            **build_insights_data(),
        )

    @flask_app.route("/api/dashboard-snapshot")
    def dashboard_snapshot():
        'Return dashboard snapshot data for incremental in-page refresh.'
        if not requester_is_plus_admin():
            abort(403)

        window_name = normalize_window(request.args.get("window"))
        activity_span = normalize_activity_span(request.args.get("activity_span"))
        return jsonify(build_live_dashboard_payload(window_name, activity_span, live_update_seconds))

    @flask_app.route("/api/dashboard-stream")
    def dashboard_stream():
        'Stream dashboard updates over Server-Sent Events.'
        if not requester_is_plus_admin():
            abort(403)

        window_name = normalize_window(request.args.get("window"))
        activity_span = normalize_activity_span(request.args.get("activity_span"))
        response = Response(
            stream_with_context(
                event_stream(
                    window_name,
                    activity_span,
                    live_update_seconds,
                    live_update_boundary_offset_seconds,
                )
            ),
            mimetype="text/event-stream",
        )
        response.headers["Cache-Control"] = "no-cache"
        response.headers["X-Accel-Buffering"] = "no"
        return response

    @flask_app.route("/clients/<mac>")
    def client_detail(mac: str):
        'Render detail view for one client MAC address.'
        if not requester_is_plus_admin():
            abort(403)

        try:
            context = get_client_usage_context(mac)
            return render_template(
                "usage_detail.html",
                page_title=f"{context['latest_record'].name or context['mac']} | UniFi Usage",
                can_set_speed_limit=False,
                speed_limit_options=[],
                selected_speed_limit_name="",
                speed_limit_form_message="",
                **context,
            )
        except LookupError:
            abort(404)

    @flask_app.route("/my-usage", methods=["GET", "POST"])
    def my_usage():
        'Render usage details for the LAN client identified by request IP/MAC mapping.'
        request_ip = resolve_request_ip()
        detected_mac, lookup_error = resolve_my_usage_mac(request_ip)
        if lookup_error:
            return render_template(
                "usage_detail.html",
                page_title="My Usage | UniFi Usage",
                error_message=lookup_error,
                request_ip=request_ip or "",
                detected_mac="",
            )

        try:
            context = get_client_usage_context(detected_mac)
        except LookupError:
            return render_template(
                "usage_detail.html",
                page_title="My Usage | UniFi Usage",
                error_message="We identified your device, but no usage record is available yet.",
                request_ip=request_ip,
                detected_mac=detected_mac,
            )

        warn_missing_radius_identity(context['latest_record'], request_ip, detected_mac)
        plus_user = is_plus_network(context['latest_record'].vlan)
        can_set_speed_limit = plus_user and cfg.SELF_SERVICE_SPEED_LIMIT_ENABLED
        speed_limits = api.get_speed_limits() if can_set_speed_limit else []
        selected_speed_limit_name = context['latest_record'].profile or ''
        speed_limit_form_message = ''

        if request.method == "POST":
            if not can_set_speed_limit:
                speed_limit_form_message = 'Speed-limit changes are temporarily unavailable.'
            else:
                requested_limit_name = request.form.get("speed_limit_name", "").strip()
                speed_limits_by_name = {limit.name: limit for limit in speed_limits}
                selected_limit = speed_limits_by_name.get(requested_limit_name)

                if not selected_limit:
                    speed_limit_form_message = 'Please select a valid speed limit.'
                elif not (live_client := get_live_client_record_by_mac(detected_mac)):
                    speed_limit_form_message = 'Your device must be online to apply a speed-limit change.'
                else:
                    unifi_client_id = live_client.get('_id')
                    if not isinstance(unifi_client_id, str) or not unifi_client_id:
                        speed_limit_form_message = 'Could not identify this device in UniFi right now.'
                    elif api.set_user_group(unifi_client_id, selected_limit.id):
                        selected_speed_limit_name = selected_limit.name
                        context['latest_record'].profile = selected_limit.name
                        speed_limit_form_message = f'Speed limit updated to {speed_limit_option_label(selected_limit)}.'
                    else:
                        speed_limit_form_message = 'Could not apply speed limit. Please try again.'

        speed_limit_options = [
            {'name': limit.name, 'label': speed_limit_option_label(limit)}
            for limit in speed_limits
        ]

        return render_template(
            "usage_detail.html",
            page_title="My Usage | UniFi Usage",
            request_ip=request_ip,
            detected_mac=detected_mac,
            can_set_speed_limit=can_set_speed_limit,
            speed_limit_options=speed_limit_options,
            selected_speed_limit_name=selected_speed_limit_name,
            speed_limit_form_message=speed_limit_form_message,
            **context,
        )

    @flask_app.route("/my-usage/report")
    def my_usage_report():
        'Render print-friendly monthly billing report for the current requester.'
        request_ip = resolve_request_ip()
        detected_mac, lookup_error = resolve_my_usage_mac(request_ip)
        if lookup_error:
            return render_template(
                "my_usage_report.html",
                error_message=lookup_error,
                request_ip=request_ip or "",
                detected_mac="",
            )

        try:
            context = get_client_usage_context(detected_mac)
        except LookupError:
            return render_template(
                "my_usage_report.html",
                error_message="We identified your device, but no usage record is available yet.",
                request_ip=request_ip,
                detected_mac=detected_mac,
            )

        return render_template(
            "my_usage_report.html",
            request_ip=request_ip,
            detected_mac=detected_mac,
            generated_at=datetime.now(),
            organization_title=get_organization_title(),
            **context,
        )

    @flask_app.route("/invoices/plus-users")
    def plus_user_invoices():
        'Render month-to-date Plus-user invoice summaries for admins.'
        if not requester_is_plus_admin():
            abort(403)

        generated_at = datetime.now()
        summaries = db.get_plus_user_invoice_summaries_current_month(
            excluded_user_ids=cfg.ORGANIZATION_PAID_USER_IDS,
        )
        invoice_rows = [
            {
                'summary': summary,
                'cost_usd': calculate_month_cost_cents(summary.total_mb) / 100.0,
            }
            for summary in summaries
        ]
        return render_template(
            "plus_user_invoices.html",
            generated_at=generated_at,
            report_period_label=render_report_period_label(generated_at),
            summaries=summaries,
            invoice_rows=invoice_rows,
            organization_title=get_organization_title(),
            excluded_user_ids=sorted(
                user_id.strip()
                for user_id in cfg.ORGANIZATION_PAID_USER_IDS
                if user_id.strip()
            ),
        )

    @flask_app.route("/invoices/plus-users/<user_id>")
    def plus_user_invoice_summary(user_id: str):
        'Render month-to-date invoice detail for one Plus user.'
        if not requester_is_plus_admin():
            abort(403)

        summary = db.get_plus_user_invoice_summary_current_month(
            user_id,
            excluded_user_ids=cfg.ORGANIZATION_PAID_USER_IDS,
        )
        if summary is None:
            abort(404)

        generated_at = datetime.now()
        return render_template(
            "plus_user_invoice_summary.html",
            generated_at=generated_at,
            report_period_label=render_report_period_label(generated_at),
            month_cost_cents=calculate_month_cost_cents(summary.total_mb),
            organization_title=get_organization_title(),
            plus_report_label=get_plus_network_report_title(),
            summary=summary,
            **build_plus_user_chart_context(summary),
        )

    @flask_app.route("/invoices/plus-users/<user_id>/summary.pdf")
    def plus_user_invoice_pdf(user_id: str):
        'Generate invoice-ready PDF for one Plus user.'
        if not requester_is_plus_admin():
            abort(403)

        summary = db.get_plus_user_invoice_summary_current_month(
            user_id,
            excluded_user_ids=cfg.ORGANIZATION_PAID_USER_IDS,
        )
        if summary is None:
            abort(404)

        generated_at = datetime.now()
        report_period_label = render_report_period_label(generated_at)
        pdf_bytes = build_plus_user_invoice_pdf(summary, report_period_label, generated_at)
        filename = f"plus-user-invoice-{safe_file_stem(summary.user_id)}-{generated_at:%Y-%m}.pdf"
        return send_file(
            io.BytesIO(pdf_bytes),
            mimetype="application/pdf",
            as_attachment=True,
            download_name=filename,
        )

    @flask_app.route("/invoices/plus-users/export.zip")
    def plus_user_invoice_export_zip():
        'Generate one ZIP containing monthly PDF summary for each billable Plus user.'
        if not requester_is_plus_admin():
            abort(403)

        generated_at = datetime.now()
        report_period_label = render_report_period_label(generated_at)
        summaries = db.get_plus_user_invoice_summaries_current_month(
            excluded_user_ids=cfg.ORGANIZATION_PAID_USER_IDS,
        )

        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as zip_file:
            index_csv_buffer = io.StringIO()
            csv_writer = csv.writer(index_csv_buffer)
            csv_writer.writerow([
                "user_id",
                "usage_mb",
                "cost_usd",
                "active_minutes",
                "device_count",
                "first_seen",
                "last_seen",
                "pdf_filename",
            ])

            for summary in summaries:
                pdf_filename = f"plus-user-invoice-{safe_file_stem(summary.user_id)}-{generated_at:%Y-%m}.pdf"
                zip_file.writestr(
                    pdf_filename,
                    build_plus_user_invoice_pdf(summary, report_period_label, generated_at),
                )
                csv_writer.writerow([
                    summary.user_id,
                    f"{summary.total_mb:.3f}",
                    f"{calculate_month_cost_cents(summary.total_mb) / 100.0:.2f}",
                    summary.active_minutes,
                    summary.device_count,
                    summary.first_seen.strftime("%Y-%m-%d %H:%M:%S"),
                    summary.last_seen.strftime("%Y-%m-%d %H:%M:%S"),
                    pdf_filename,
                ])

            zip_file.writestr("plus-user-invoice-index.csv", index_csv_buffer.getvalue())

        zip_buffer.seek(0)
        zip_name = f"plus-user-invoices-{generated_at:%Y-%m}.zip"
        return send_file(
            zip_buffer,
            mimetype="application/zip",
            as_attachment=True,
            download_name=zip_name,
        )

    return flask_app


app = create_app()


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5051)
