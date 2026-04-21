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
import os
from typing import Any, TypedDict

from flask import Flask, Response, abort, jsonify, redirect, render_template, request, stream_with_context, url_for

import config as cfg
import database as db
import unifi_api as api
from database import UsageRecord
from dashboard_service import (
    build_dashboard_data,
    build_dashboard_payload,
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
        'Return estimated month cost in cents using configured rate.'
        return (calendar_month_total_mb / 1000.0) * float(cfg.COST_IN_CENTS_PER_GB)

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

        caps = [cap for cap in (matched_limit.up_kbps, matched_limit.down_kbps) if isinstance(cap, int) and cap > 0]
        if not caps:
            return 0.0

        strictest_cap_kbps = min(caps)
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

        try:
            context = get_client_usage_context(detected_mac)
        except LookupError:
            return False

        latest_record = context['latest_record']
        warn_missing_radius_identity(latest_record, request_ip, detected_mac)
        return is_plus_admin_user(latest_record.user_id, latest_record.vlan)

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
        month_daily_usage = [
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
        return render_template(
            "dashboard.html",
            **build_dashboard_data(window_name, activity_span, live_update_seconds),
        )

    @flask_app.route("/api/dashboard-snapshot")
    def dashboard_snapshot():
        'Return dashboard snapshot data for incremental in-page refresh.'
        if not requester_is_plus_admin():
            abort(403)

        window_name = normalize_window(request.args.get("window"))
        activity_span = normalize_activity_span(request.args.get("activity_span"))
        data = build_dashboard_data(window_name, activity_span, live_update_seconds)
        return jsonify(build_dashboard_payload(data))

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
            return render_template("client_detail.html", **get_client_usage_context(mac))
        except LookupError:
            abort(404)

    @flask_app.route("/my-usage", methods=["GET", "POST"])
    def my_usage():
        'Render usage details for the LAN client identified by request IP/MAC mapping.'
        if not (request_ip := resolve_request_ip()):
            return render_template(
                "my_usage.html",
                error_message="Could not determine your client IP address from this request.",
                request_ip="",
                detected_mac="",
            )

        if not (detected_mac := find_client_mac_for_ip(request_ip)):
            return render_template(
                "my_usage.html",
                error_message=(
                    "Could not map your IP to a UniFi client right now. "
                    "Try again in a moment after generating some network activity."
                ),
                request_ip=request_ip,
                detected_mac="",
            )

        try:
            context = get_client_usage_context(detected_mac)
        except LookupError:
            return render_template(
                "my_usage.html",
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
            "my_usage.html",
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
        if not (request_ip := resolve_request_ip()):
            return render_template(
                "my_usage_report.html",
                error_message="Could not determine your client IP address from this request.",
                request_ip="",
                detected_mac="",
            )

        if not (detected_mac := find_client_mac_for_ip(request_ip)):
            return render_template(
                "my_usage_report.html",
                error_message=(
                    "Could not map your IP to a UniFi client right now. "
                    "Try again in a moment after generating some network activity."
                ),
                request_ip=request_ip,
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
            **context,
        )

    return flask_app


app = create_app()


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5051)
