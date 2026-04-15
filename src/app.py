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
from dashboard_service import (
    build_dashboard_data,
    build_dashboard_payload,
    normalize_window,
)
from dashboard_stream import event_stream
from monitor import get_connected_clients


def create_app() -> Flask:
    'Create and configure the Flask web application.'
    flask_app = Flask(__name__)
    live_update_seconds = 15

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
                abort(404)

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
        daily_total_mb = db.get_daily_total(mac)

        return render_template(
            "client_detail.html",
            mac=mac,
            latest_record=latest_record,
            usage_history=usage_history,
            daily_total_mb=daily_total_mb,
        )

    return flask_app


app = create_app()


if __name__ == "__main__":
    app.run(debug=True, host="127.0.0.1", port=5051)
