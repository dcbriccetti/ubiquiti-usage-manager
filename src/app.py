'''Flask application entry module.

This module keeps the web entrypoint intentionally small:
- creates the Flask app
- registers HTTP routes
- delegates dashboard data shaping to dashboard_service
- delegates SSE frame generation to dashboard_stream

Keeping route glue here and business/view-model logic in helper modules reduces
merge conflicts and makes testing easier because each module has a tighter scope.
'''
from datetime import date, datetime
import io
import os
import re
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
from plus_invoices import (
    build_plus_user_chart_context,
    build_plus_user_invoice_pdf,
    build_plus_user_invoice_zip,
    calculate_month_cost_cents,
    get_organization_title,
    get_plus_network_report_title,
    plus_user_invoice_pdf_filename,
)
from report_periods import build_report_period_context
from speedlimit import SpeedLimit

SpeedLimitsByName = dict[str, SpeedLimit]


class ThrottleChartDataset(TypedDict):
    'One stacked-bar series for monthly throttling chart.'
    label: str
    data: list[int]


class UsageScalePoint(TypedDict):
    'One bucketed usage point (hourly or daily).'
    bucket_label: str
    bucket_value: int
    total_mb: float
    active_minutes: int


class UsageScaleContext(TypedDict):
    'Renderable chart context for one usage scale section.'
    key: str
    title: str
    x_axis_title: str
    mb_axis_title: str
    minutes_axis_title: str
    summary_text: str
    points: list[UsageScalePoint]
    usage_device_series: list[dict[str, object]]
    access_point_labels: list[str]
    access_point_mb_values: list[float]
    access_point_minutes_values: list[int]
    throttle_x_values: list[int]
    throttle_datasets: list[ThrottleChartDataset]


class ClientUsageContext(TypedDict):
    'Template context for client-detail and my-usage pages.'
    mac: str
    latest_record: UsageRecord
    usage_history: list[UsageRecord]
    daily_total_mb: float
    last_7_days_total_mb: float
    calendar_month_total_mb: float
    month_cost_cents: float
    usage_scales: list[UsageScaleContext]
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

    def build_report_context(available_months: list[date] | None = None) -> dict[str, object]:
        'Build reusable template/query context for month-selected reports.'
        resolved_months = available_months if available_months is not None else db.get_plus_user_invoice_months()
        return build_report_period_context(
            request.args.get('month'),
            resolved_months,
        ).as_template_context()

    def get_speed_limits_by_name() -> SpeedLimitsByName:
        'Return mapping of speed-limit profile name to SpeedLimit object.'
        return {limit.name: limit for limit in api.get_speed_limits()}

    def render_radius_value(value: object) -> str:
        'Return a readable display value for optional RADIUS account fields.'
        if value is None or value == '':
            return ''
        return str(value)

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

        def build_throttle_datasets(
            bucket_rows: list[tuple[int, dict[str, int]]],
            speed_limits_map: SpeedLimitsByName,
        ) -> list[ThrottleChartDataset]:
            'Build sorted stacked-profile datasets for one bucketed time scale.'
            totals_by_profile_key: dict[str, int] = {}
            for _, bucket_counts in bucket_rows:
                for profile_key, minutes in bucket_counts.items():
                    totals_by_profile_key[profile_key] = totals_by_profile_key.get(profile_key, 0) + minutes

            sorted_profile_keys = sorted(
                totals_by_profile_key.keys(),
                key=lambda key: (
                    profile_throttling_impact(key, speed_limits_map),
                    totals_by_profile_key[key],
                ),
            )

            return [
                {
                    'label': profile_display_label(profile_key, speed_limits_map),
                    'data': [bucket_counts.get(profile_key, 0) for _, bucket_counts in bucket_rows],
                }
                for profile_key in sorted_profile_keys
            ]

        speed_limits_by_name = get_speed_limits_by_name()
        now = datetime.now()
        current_month_label = render_month_label(now)
        calendar_month_total_mb = db.get_calendar_month_total(mac)
        month_daily_usage: list[UsageScalePoint] = [
            {
                'bucket_label': f'{usage_day.strftime("%b")} {usage_day.day}',
                'bucket_value': usage_day.day,
                'total_mb': total_mb,
                'active_minutes': active_minutes,
            }
            for usage_day, total_mb, active_minutes in db.get_calendar_month_daily_totals(mac)
        ]
        month_throttle_rows = [
            (usage_day.day, daily_counts)
            for usage_day, daily_counts in db.get_calendar_month_daily_profile_minutes(mac)
        ]
        month_throttle_datasets = build_throttle_datasets(month_throttle_rows, speed_limits_by_name)

        daily_hourly_usage: list[UsageScalePoint] = [
            {
                'bucket_label': f'{hour:02d}:00',
                'bucket_value': hour,
                'total_mb': total_mb,
                'active_minutes': active_minutes,
            }
            for hour, total_mb, active_minutes in db.get_today_hourly_totals(mac)
        ]
        daily_throttle_rows = db.get_today_hourly_profile_minutes(mac)
        daily_throttle_datasets = build_throttle_datasets(daily_throttle_rows, speed_limits_by_name)
        daily_access_points = db.get_today_access_point_totals(mac)

        monthly_access_points = db.get_calendar_month_access_point_totals(mac)

        usage_scales: list[UsageScaleContext] = [
            {
                'key': 'daily',
                'title': f'Usage Today ({now.strftime("%b")} {now.day})',
                'x_axis_title': 'Hour of day',
                'mb_axis_title': 'MB/hour',
                'minutes_axis_title': 'minutes/hour',
                'summary_text': 'Top chart: MB/hour. Bottom chart: active minutes/hour stacked by speed-limit profile.',
                'points': daily_hourly_usage,
                'usage_device_series': [
                    {
                        'label': '',
                        'data': [point['total_mb'] for point in daily_hourly_usage],
                    }
                ],
                'access_point_labels': [ap_name for ap_name, _, _ in daily_access_points],
                'access_point_mb_values': [total_mb for _, total_mb, _ in daily_access_points],
                'access_point_minutes_values': [active_minutes for _, _, active_minutes in daily_access_points],
                'throttle_x_values': [hour for hour, _ in daily_throttle_rows],
                'throttle_datasets': daily_throttle_datasets,
            },
            {
                'key': 'monthly',
                'title': f'{current_month_label} Usage',
                'x_axis_title': 'Day of month',
                'mb_axis_title': 'MB/day',
                'minutes_axis_title': 'minutes/day',
                'summary_text': 'Top chart: MB/day. Bottom chart: active minutes/day stacked by speed-limit profile.',
                'points': month_daily_usage,
                'usage_device_series': [
                    {
                        'label': '',
                        'data': [point['total_mb'] for point in month_daily_usage],
                    }
                ],
                'access_point_labels': [ap_name for ap_name, _, _ in monthly_access_points],
                'access_point_mb_values': [total_mb for _, total_mb, _ in monthly_access_points],
                'access_point_minutes_values': [active_minutes for _, _, active_minutes in monthly_access_points],
                'throttle_x_values': [usage_day for usage_day, _ in month_throttle_rows],
                'throttle_datasets': month_throttle_datasets,
            },
        ]

        return {
            'mac': mac,
            'latest_record': latest_record,
            'usage_history': usage_history,
            'daily_total_mb': db.get_daily_total(mac),
            'last_7_days_total_mb': db.get_last_7_days_total(mac),
            'calendar_month_total_mb': calendar_month_total_mb,
            'month_cost_cents': calculate_month_cost_cents(calendar_month_total_mb),
            'usage_scales': usage_scales,
            'current_month_label': current_month_label,
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
        'Render deeper month analytics panels.'
        if not requester_is_plus_admin():
            abort(403)

        period_context = build_report_context(db.get_usage_months())
        current_month = date(datetime.now().year, datetime.now().month, 1)
        insights_data = build_insights_data(
            period_start=period_context['period_start'],
            period_end=period_context['period_end'],
            current_month_label=period_context['selected_month'].strftime('%b'),
            report_period_label=str(period_context['report_period_label']),
            include_live_organization_paid_clients=period_context['selected_month'] == current_month,
        )
        return render_template(
            "insights.html",
            selected_month=period_context['selected_month'],
            selected_month_value=period_context['selected_month_value'],
            current_month_value=period_context['current_month_value'],
            previous_month_value=period_context['previous_month_value'],
            month_options=period_context['month_options'],
            **insights_data,
        )

    @flask_app.route("/radius/users")
    def radius_users():
        'Render local RADIUS users configured in UniFi.'
        if not requester_is_plus_admin():
            abort(403)

        accounts = api.get_radius_accounts()
        radius_user_rows = [
            {
                'id': render_radius_value(account.get('_id')),
                'name': render_radius_value(account.get('name')),
                'vlan': render_radius_value(account.get('vlan') or account.get('network_id')),
                'tunnel_type': render_radius_value(account.get('tunnel_type')),
                'tunnel_medium_type': render_radius_value(account.get('tunnel_medium_type')),
            }
            for account in accounts
        ]
        radius_user_rows.sort(key=lambda row: row['name'].lower())

        return render_template(
            "radius_users.html",
            generated_at=datetime.now(),
            radius_user_rows=radius_user_rows,
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

    @flask_app.route("/clients/<mac>/usage-today-embed")
    def client_usage_today_embed(mac: str):
        'Render embeddable "Usage Today" panel for one client MAC.'
        if not requester_is_plus_admin():
            abort(403)

        try:
            context = get_client_usage_context(mac)
        except LookupError:
            abort(404)

        daily_scale = next((scale for scale in context['usage_scales'] if scale['key'] == 'daily'), None)
        if daily_scale is None:
            abort(404)

        return render_template(
            "client_usage_today_embed.html",
            mac=context['mac'],
            usage_scale=daily_scale,
        )

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

    @flask_app.route("/invoices/plus-users")
    def plus_user_invoices():
        'Render Plus-user invoice summaries for the selected report month.'
        if not requester_is_plus_admin():
            abort(403)

        generated_at = datetime.now()
        period_context = build_report_context()
        summaries = db.get_plus_user_invoice_summaries_current_month(
            excluded_user_ids=cfg.ORGANIZATION_PAID_USER_IDS,
            period_start=period_context['period_start'],
            period_end=period_context['period_end'],
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
            summaries=summaries,
            invoice_rows=invoice_rows,
            organization_title=get_organization_title(),
            excluded_user_ids=sorted(
                user_id.strip()
                for user_id in cfg.ORGANIZATION_PAID_USER_IDS
                if user_id.strip()
            ),
            **period_context,
        )

    @flask_app.route("/invoices/plus-users/<user_id>")
    def plus_user_invoice_summary(user_id: str):
        'Render invoice detail for one Plus user in the selected report month.'
        if not requester_is_plus_admin():
            abort(403)

        period_context = build_report_context()
        summary = db.get_plus_user_invoice_summary_current_month(
            user_id,
            excluded_user_ids=cfg.ORGANIZATION_PAID_USER_IDS,
            period_start=period_context['period_start'],
            period_end=period_context['period_end'],
        )
        if summary is None:
            abort(404)

        generated_at = datetime.now()
        return render_template(
            "plus_user_invoice_summary.html",
            generated_at=generated_at,
            month_cost_cents=calculate_month_cost_cents(summary.total_mb),
            organization_title=get_organization_title(),
            plus_report_label=get_plus_network_report_title(),
            summary=summary,
            **period_context,
            **build_plus_user_chart_context(
                summary,
                period_start=period_context['period_start'],
                period_end=period_context['period_end'],
            ),
        )

    @flask_app.route("/invoices/plus-users/<user_id>/summary.pdf")
    def plus_user_invoice_pdf(user_id: str):
        'Generate invoice-ready PDF for one Plus user in the selected report month.'
        if not requester_is_plus_admin():
            abort(403)

        period_context = build_report_context()
        summary = db.get_plus_user_invoice_summary_current_month(
            user_id,
            excluded_user_ids=cfg.ORGANIZATION_PAID_USER_IDS,
            period_start=period_context['period_start'],
            period_end=period_context['period_end'],
        )
        if summary is None:
            abort(404)

        generated_at = datetime.now()
        report_period_label = str(period_context['report_period_label'])
        pdf_bytes = build_plus_user_invoice_pdf(
            summary,
            report_period_label,
            generated_at,
            period_start=period_context['period_start'],
            period_end=period_context['period_end'],
        )
        filename = plus_user_invoice_pdf_filename(summary, str(period_context['selected_month_value']))
        return send_file(
            io.BytesIO(pdf_bytes),
            mimetype="application/pdf",
            as_attachment=True,
            download_name=filename,
        )

    @flask_app.route("/invoices/plus-users/export.zip")
    def plus_user_invoice_export_zip():
        'Generate one ZIP containing PDF summaries for each billable Plus user.'
        if not requester_is_plus_admin():
            abort(403)

        generated_at = datetime.now()
        period_context = build_report_context()
        report_period_label = str(period_context['report_period_label'])
        summaries = db.get_plus_user_invoice_summaries_current_month(
            excluded_user_ids=cfg.ORGANIZATION_PAID_USER_IDS,
            period_start=period_context['period_start'],
            period_end=period_context['period_end'],
        )

        zip_bytes = build_plus_user_invoice_zip(
            summaries,
            str(period_context['selected_month_value']),
            lambda summary: build_plus_user_invoice_pdf(
                summary,
                report_period_label,
                generated_at,
                period_start=period_context['period_start'],
                period_end=period_context['period_end'],
            ),
        )
        zip_name = f"plus-user-invoices-{period_context['selected_month_value']}.zip"
        return send_file(
            io.BytesIO(zip_bytes),
            mimetype="application/zip",
            as_attachment=True,
            download_name=zip_name,
        )

    return flask_app


app = create_app()


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5051)
