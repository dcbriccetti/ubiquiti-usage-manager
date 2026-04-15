from flask import Flask, Response, abort, jsonify, render_template, request, stream_with_context
from datetime import datetime
import json
import time

import database as db
import unifi_api as api
from monitor import get_connected_clients

WINDOW_ONLINE_NOW = "online_now"
WINDOW_TODAY = "today"
WINDOW_LAST_7_DAYS = "last_7_days"
WINDOW_THIS_MONTH = "this_month"
ALLOWED_WINDOWS = {
    WINDOW_ONLINE_NOW,
    WINDOW_TODAY,
    WINDOW_LAST_7_DAYS,
    WINDOW_THIS_MONTH,
}


def create_app() -> Flask:
    'Create and configure the Flask web application.'
    flask_app = Flask(__name__)
    live_update_seconds = 15

    def normalize_window(window_name: str | None) -> str:
        if window_name in ALLOWED_WINDOWS:
            return window_name
        return WINDOW_ONLINE_NOW

    def build_rows_for_online_clients() -> list[dict]:
        rows = [
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
            for snapshot in get_connected_clients()
        ]
        return sorted(
            rows,
            key=lambda row: (
                -row["interval_mb"],
                -row["day_total_mb"],
                str(row["name"]).lower(),
                str(row["mac"]).lower(),
            ),
        )

    def build_rows_for_historical_window(
        window_name: str,
        speed_limits_by_name: dict[str, str],
    ) -> list[dict]:
        summaries = db.get_usage_window_summary(window_name)
        return [
            {
                "user_id": row.user_id or "",
                "name": row.name or row.mac,
                "ap_name": row.ap_name or "",
                "mac": row.mac,
                "vlan_name": row.vlan or "Unknown",
                "signal": None,
                "interval_mb": 0.0,
                "day_total_mb": row.day_total_mb,
                "last_7_days_total_mb": row.last_7_days_total_mb,
                "calendar_month_total_mb": row.calendar_month_total_mb,
                "effective_speed_limit": (
                    speed_limits_by_name.get(row.profile, row.profile) if row.profile else ""
                ),
            }
            for row in summaries
        ]

    def build_dashboard_data(window_name: str) -> dict:
        speed_limits_by_name = {
            limit.name: str(limit) for limit in api.get_speed_limits()
        }
        rows = (
            build_rows_for_online_clients()
            if window_name == WINDOW_ONLINE_NOW
            else build_rows_for_historical_window(window_name, speed_limits_by_name)
        )
        return {
            "clients": rows,
            "selected_window": window_name,
            "current_month_label": datetime.now().strftime("%b"),
            "total_today_mb": db.get_total_today_usage(),
            "total_last_7_days_mb": db.get_total_last_7_days_usage(),
            "total_calendar_month_mb": db.get_total_calendar_month_usage(),
            "live_update_seconds": live_update_seconds,
        }

    @flask_app.route("/")
    def dashboard():
        'Render the dashboard with live snapshots and daily usage summaries.'
        window_name = normalize_window(request.args.get("window"))
        return render_template("dashboard.html", **build_dashboard_data(window_name))

    @flask_app.route("/api/dashboard-snapshot")
    def dashboard_snapshot():
        'Return dashboard snapshot data for incremental in-page refresh.'
        window_name = normalize_window(request.args.get("window"))
        data = build_dashboard_data(window_name)

        return jsonify(
            selected_window=data["selected_window"],
            current_month_label=data["current_month_label"],
            total_today_mb=data["total_today_mb"],
            total_last_7_days_mb=data["total_last_7_days_mb"],
            total_calendar_month_mb=data["total_calendar_month_mb"],
            clients=data["clients"],
            live_update_seconds=data["live_update_seconds"],
        )

    @flask_app.route("/api/dashboard-stream")
    @flask_app.route("/dashboard-stream")
    def dashboard_stream():
        'Stream dashboard updates over Server-Sent Events.'
        window_name = normalize_window(request.args.get("window"))

        def event_stream():
            while True:
                data = build_dashboard_data(window_name)

                payload = {
                    "selected_window": data["selected_window"],
                    "current_month_label": data["current_month_label"],
                    "total_today_mb": data["total_today_mb"],
                    "total_last_7_days_mb": data["total_last_7_days_mb"],
                    "total_calendar_month_mb": data["total_calendar_month_mb"],
                    "clients": data["clients"],
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
    app.run(debug=True, host="127.0.0.1", port=5051)
