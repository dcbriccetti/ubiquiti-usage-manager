'''Plus-user invoice view-model, PDF, and export helpers.'''

from datetime import datetime
import csv
import io
import re
import zipfile
from typing import Callable, TypedDict

import config as cfg
import database as db


class DailyUsagePoint(TypedDict):
    'One day point for month usage chart.'
    day_label: str
    day_of_month: int
    total_mb: float
    active_minutes: int


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


def safe_file_stem(raw_value: str) -> str:
    'Return a filesystem-safe lowercase stem suitable for export filenames.'
    normalized = re.sub(r'[^a-zA-Z0-9._-]+', '-', raw_value.strip()).strip('-').lower()
    return normalized or 'unknown-user'


def plus_user_invoice_pdf_filename(summary: db.PlusUserInvoiceSummary, month_value: str) -> str:
    'Return the PDF filename for one Plus-user invoice summary.'
    return f"plus-user-invoice-{safe_file_stem(summary.user_id)}-{month_value}.pdf"


def build_plus_user_chart_context(
    summary: db.PlusUserInvoiceSummary,
    period_start: datetime | None = None,
    period_end: datetime | None = None,
) -> dict[str, object]:
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
    stacked_day_labels, stacked_device_series = db.get_plus_user_daily_device_usage_current_month(
        summary.user_id,
        period_start=period_start,
        period_end=period_end,
    )
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
    period_start: datetime | None = None,
    period_end: datetime | None = None,
) -> bytes:
    'Render one invoice-ready PDF summary for a Plus user.'
    from reportlab.graphics import renderPDF
    from reportlab.graphics.charts.barcharts import VerticalBarChart
    from reportlab.graphics.shapes import Drawing, Rect, String
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import letter
    from reportlab.pdfgen import canvas

    chart_context = build_plus_user_chart_context(summary, period_start, period_end)
    month_daily_usage = chart_context['month_daily_usage']
    day_labels = [str(point['day_of_month']) for point in month_daily_usage]
    usage_mb_series = [float(point['total_mb']) for point in month_daily_usage]
    stacked_day_labels, stacked_device_series = db.get_plus_user_daily_device_usage_current_month(
        summary.user_id,
        period_start=period_start,
        period_end=period_end,
    )

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
        pdf.drawString(table_left + 6, row_top - 13, f"No device activity found for {report_period_label}.")
    elif omitted_count > 0:
        pdf.setFillColor(text_muted)
        pdf.setFont("Helvetica", 8.5)
        pdf.drawString(table_left + 6, max(devices_panel_y + 8, row_top - 13), f"+ {omitted_count} additional devices not shown")

    pdf.showPage()
    pdf.save()
    pdf_buffer.seek(0)
    return pdf_buffer.getvalue()


def build_plus_user_invoice_zip(
    summaries: list[db.PlusUserInvoiceSummary],
    selected_month_value: str,
    pdf_builder: Callable[[db.PlusUserInvoiceSummary], bytes],
) -> bytes:
    'Return ZIP bytes containing invoice PDFs and a CSV index.'
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
            pdf_filename = plus_user_invoice_pdf_filename(summary, selected_month_value)
            zip_file.writestr(pdf_filename, pdf_builder(summary))
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
    return zip_buffer.getvalue()
