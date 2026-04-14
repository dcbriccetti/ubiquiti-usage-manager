from flask import Flask, Response, abort, jsonify, render_template, stream_with_context
import json
import time

import database as db
from monitor import get_connected_clients


def create_app() -> Flask:
    'Create and configure the Flask web application.'
    flask_app = Flask(__name__)
    live_update_seconds = 15

    def build_dashboard_data() -> dict:
        connected_clients = get_connected_clients()
        daily_usage = db.get_daily_usage_summary()
        total_usage_mb = sum(row.total_mb for row in daily_usage)
        return {
            "connected_clients": connected_clients,
            "daily_usage": daily_usage,
            "total_usage_mb": total_usage_mb,
            "live_update_seconds": live_update_seconds,
        }

    @flask_app.route("/")
    def dashboard():
        'Render the dashboard with live snapshots and daily usage summaries.'
        return render_template("dashboard.html", **build_dashboard_data())

    @flask_app.route("/api/dashboard-snapshot")
    def dashboard_snapshot():
        'Return dashboard snapshot data for incremental in-page refresh.'
        data = build_dashboard_data()

        connected_clients_payload = [
            {
                "user_id": snapshot.client.user_id or "",
                "name": snapshot.client.name,
                "ap_name": snapshot.client.ap_name or "",
                "mac": snapshot.client.mac,
                "vlan_name": snapshot.client.vlan_name or "Unknown",
                "signal": snapshot.client.signal if snapshot.client.signal else None,
                "interval_mb": snapshot.interval_mb,
                "day_total_mb": snapshot.day_total_mb,
                "last_7_days_total_mb": snapshot.last_7_days_total_mb,
                "calendar_month_total_mb": snapshot.calendar_month_total_mb,
                "effective_speed_limit": str(snapshot.effective_speed_limit) if snapshot.effective_speed_limit else "",
            }
            for snapshot in data["connected_clients"]
        ]

        return jsonify(
            connected_clients_count=len(data["connected_clients"]),
            tracked_today_count=len(data["daily_usage"]),
            total_usage_mb=data["total_usage_mb"],
            connected_clients=connected_clients_payload,
            live_update_seconds=data["live_update_seconds"],
        )

    @flask_app.route("/api/dashboard-stream")
    @flask_app.route("/dashboard-stream")
    def dashboard_stream():
        'Stream dashboard updates over Server-Sent Events.'

        def event_stream():
            while True:
                data = build_dashboard_data()
                connected_clients_payload = [
                    {
                        "user_id": snapshot.client.user_id or "",
                        "name": snapshot.client.name,
                        "ap_name": snapshot.client.ap_name or "",
                        "mac": snapshot.client.mac,
                        "vlan_name": snapshot.client.vlan_name or "Unknown",
                        "signal": snapshot.client.signal if snapshot.client.signal else None,
                        "interval_mb": snapshot.interval_mb,
                        "day_total_mb": snapshot.day_total_mb,
                        "last_7_days_total_mb": snapshot.last_7_days_total_mb,
                        "calendar_month_total_mb": snapshot.calendar_month_total_mb,
                        "effective_speed_limit": str(snapshot.effective_speed_limit) if snapshot.effective_speed_limit else "",
                    }
                    for snapshot in data["connected_clients"]
                ]

                payload = {
                    "connected_clients_count": len(data["connected_clients"]),
                    "tracked_today_count": len(data["daily_usage"]),
                    "total_usage_mb": data["total_usage_mb"],
                    "connected_clients": connected_clients_payload,
                    "live_update_seconds": data["live_update_seconds"],
                }

                yield f"data: {json.dumps(payload)}\n\n"
                time.sleep(live_update_seconds)

        response = Response(stream_with_context(event_stream()), mimetype="text/event-stream")
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
    app.run(debug=True)
