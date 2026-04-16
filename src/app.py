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
from typing import Any, TypedDict

from flask import Flask, Response, abort, jsonify, render_template, request, stream_with_context

import database as db
import unifi_api as api
from database import UsageRecord
from dashboard_service import (
    build_dashboard_data,
    build_dashboard_payload,
    normalize_window,
)
from dashboard_stream import event_stream
from lan_identity import find_client_mac_for_ip, get_request_ip
from monitor import get_connected_clients
from speedlimit import SpeedLimit


class ClientUsageContext(TypedDict):
    'Template context for client-detail and my-usage pages.'
    mac: str
    latest_record: UsageRecord
    usage_history: list[UsageRecord]
    daily_total_mb: float
    last_7_days_total_mb: float
    calendar_month_total_mb: float
    current_month_label: str
    speed_limit_display_by_name: dict[str, str]


def create_app() -> Flask:
    'Create and configure the Flask web application.'
    flask_app = Flask(__name__)
    live_update_seconds = 15

    def get_speed_limit_display_by_name() -> dict[str, str]:
        'Return mapping of speed-limit profile name to rendered display label.'
        return {limit.name: str(limit) for limit in api.get_speed_limits()}

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

    def speed_limit_option_label(limit: SpeedLimit) -> str:
        'Build select-option label for one speed-limit profile.'
        rendered = str(limit)
        if rendered:
            return rendered
        return f'{limit.name} (Unlimited)'

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

        return {
            'mac': mac,
            'latest_record': latest_record,
            'usage_history': usage_history,
            'daily_total_mb': db.get_daily_total(mac),
            'last_7_days_total_mb': db.get_last_7_days_total(mac),
            'calendar_month_total_mb': db.get_calendar_month_total(mac),
            'current_month_label': datetime.now().strftime('%b'),
            'speed_limit_display_by_name': get_speed_limit_display_by_name(),
        }

    @flask_app.route("/")
    def dashboard():
        'Render the dashboard with live snapshots and daily usage summaries.'
        window_name = normalize_window(request.args.get("window"))
        return render_template(
            "dashboard.html",
            **build_dashboard_data(window_name, live_update_seconds),
        )

    @flask_app.route("/api/dashboard-snapshot")
    def dashboard_snapshot():
        'Return dashboard snapshot data for incremental in-page refresh.'
        window_name = normalize_window(request.args.get("window"))
        data = build_dashboard_data(window_name, live_update_seconds)
        return jsonify(build_dashboard_payload(data))

    @flask_app.route("/api/dashboard-stream")
    @flask_app.route("/dashboard-stream")
    def dashboard_stream():
        'Stream dashboard updates over Server-Sent Events.'
        window_name = normalize_window(request.args.get("window"))
        response = Response(
            stream_with_context(event_stream(window_name, live_update_seconds)),
            mimetype="text/event-stream",
        )
        response.headers["Cache-Control"] = "no-cache"
        response.headers["X-Accel-Buffering"] = "no"
        return response

    @flask_app.route("/clients/<mac>")
    def client_detail(mac: str):
        'Render detail view for one client MAC address.'
        try:
            return render_template("client_detail.html", **get_client_usage_context(mac))
        except LookupError:
            abort(404)

    @flask_app.route("/my-usage", methods=["GET", "POST"])
    def my_usage():
        'Render usage details for the LAN client identified by request IP/MAC mapping.'
        if not (request_ip := get_request_ip(request)):
            return render_template(
                "my_usage.html",
                error_message="Could not determine your client IP address from this request.",
                request_ip="",
                detected_mac="",
            )

        request_ip = '192.168.6.227'

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

        plus_user = is_plus_network(context['latest_record'].vlan)
        speed_limits = api.get_speed_limits()
        selected_speed_limit_name = context['latest_record'].profile or ''
        speed_limit_form_message = ''

        if request.method == "POST":
            if not plus_user:
                speed_limit_form_message = 'Speed-limit changes are available only on the Plus network.'
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
            can_set_speed_limit=plus_user,
            speed_limit_options=speed_limit_options,
            selected_speed_limit_name=selected_speed_limit_name,
            speed_limit_form_message=speed_limit_form_message,
            **context,
        )

    return flask_app


app = create_app()


if __name__ == "__main__":
    app.run(debug=True, host="127.0.0.1", port=5051)
