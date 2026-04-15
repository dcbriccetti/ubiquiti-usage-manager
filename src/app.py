'''Flask application entry module.

This module keeps the web entrypoint intentionally small:
- creates the Flask app
- registers HTTP routes
- delegates dashboard data shaping to dashboard_service
- delegates SSE frame generation to dashboard_stream

Keeping route glue here and business/view-model logic in helper modules reduces
merge conflicts and makes testing easier because each module has a tighter scope.
'''

from flask import Flask, Response, abort, jsonify, render_template, request, stream_with_context

import database as db
import unifi_api as api
from dashboard_service import (
    build_dashboard_data,
    build_dashboard_payload,
    normalize_window,
)
from dashboard_stream import event_stream
from lan_identity import find_client_mac_for_ip, get_request_ip
from monitor import get_connected_clients


def create_app() -> Flask:
    'Create and configure the Flask web application.'
    flask_app = Flask(__name__)
    live_update_seconds = 15

    def get_speed_limit_display_by_name() -> dict[str, str]:
        'Return mapping of speed-limit profile name to rendered display label.'
        return {limit.name: str(limit) for limit in api.get_speed_limits()}

    def get_client_usage_context(mac: str) -> dict:
        'Build shared usage/detail context used by both admin and self-service pages.'
        usage_history = db.get_usage_history(mac)
        if usage_history:
            latest_record = usage_history[0]
        else:
            live_snapshot = next(
                (
                    snapshot
                    for snapshot in get_connected_clients()
                    if snapshot.client.mac.lower() == mac.lower()
                ),
                None,
            )
            if live_snapshot is None:
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

    @flask_app.route("/my-usage")
    def my_usage():
        'Render usage details for the LAN client identified by request IP/MAC mapping.'
        request_ip = get_request_ip(request)
        if not request_ip:
            return render_template(
                "my_usage.html",
                error_message="Could not determine your client IP address from this request.",
                request_ip="",
                detected_mac="",
            )

        detected_mac = find_client_mac_for_ip(request_ip)
        if not detected_mac:
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

        return render_template("my_usage.html", request_ip=request_ip, detected_mac=detected_mac, **context)

    return flask_app


app = create_app()


if __name__ == "__main__":
    app.run(debug=True, host="127.0.0.1", port=5051)
