from flask import Flask, abort, render_template

import config as cfg
import database as db
from monitor import get_connected_clients


def create_app() -> Flask:
    'Create and configure the Flask web application.'
    app = Flask(__name__)
    # db.init_db()  # Temporarily disabled: avoid DB writes during Flask startup.

    @app.route("/")
    def dashboard():
        'Render the dashboard with live snapshots and daily usage summaries.'
        connected_clients = get_connected_clients()
        daily_usage = db.get_daily_usage_summary()
        total_usage_mb = sum(row.total_mb for row in daily_usage)
        throttled_count = sum(1 for client in connected_clients if client.is_throttled)

        return render_template(
            "dashboard.html",
            connected_clients=connected_clients,
            daily_usage=daily_usage,
            total_usage_mb=total_usage_mb,
            throttled_count=throttled_count,
            data_limit_mb=cfg.DATA_LIMIT_MB,
        )

    @app.route("/clients/<mac>")
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
            data_limit_mb=cfg.DATA_LIMIT_MB,
        )

    return app


app = create_app()


if __name__ == "__main__":
    app.run(debug=True)
