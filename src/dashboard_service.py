'''Dashboard data service module.

This module owns dashboard-specific data assembly and normalization rules used by:
- initial HTML render (`/`)
- snapshot API (`/api/dashboard-snapshot`)
- live stream API (`/dashboard-stream`)

Responsibilities:
1. Normalize and validate user-selected dashboard windows.
2. Build a consistent row shape for both live and historical views.
3. Apply operational sorting rules for the `online_now` view.
4. Merge usage totals and labels needed by the top summary cards.
5. Produce a compact JSON payload used by both snapshot and SSE endpoints.

The goal is to keep route handlers thin while centralizing dashboard data behavior
in one place, so UI changes do not require route-level rewrites.
'''

from datetime import datetime
from typing import Any

import database as db
import unifi_api as api
from monitor import get_connected_clients

WINDOW_ONLINE_NOW = 'online_now'
WINDOW_TODAY = 'today'
WINDOW_LAST_7_DAYS = 'last_7_days'
WINDOW_THIS_MONTH = 'this_month'
ALLOWED_WINDOWS = {
    WINDOW_ONLINE_NOW,
    WINDOW_TODAY,
    WINDOW_LAST_7_DAYS,
    WINDOW_THIS_MONTH,
}


def normalize_window(window_name: str | None) -> str:
    'Return a safe dashboard window key, defaulting to online_now.'
    if window_name in ALLOWED_WINDOWS:
        return window_name
    return WINDOW_ONLINE_NOW


def build_rows_for_online_clients() -> list[dict[str, Any]]:
    'Build dashboard rows from live controller client snapshots.'
    rows = [
        {
            'user_id': snapshot.client.user_id or '',
            'name': snapshot.client.name,
            'ap_name': snapshot.client.ap_name or '',
            'mac': snapshot.client.mac,
            'vlan_name': snapshot.client.vlan_name or 'Unknown',
            'signal': snapshot.client.signal if snapshot.client.signal else None,
            'interval_mb': snapshot.interval_mb,
            'day_total_mb': snapshot.day_total_mb,
            'last_7_days_total_mb': snapshot.last_7_days_total_mb,
            'calendar_month_total_mb': snapshot.calendar_month_total_mb,
            'effective_speed_limit': str(snapshot.effective_speed_limit) if snapshot.effective_speed_limit else '',
        }
        for snapshot in get_connected_clients()
    ]
    # Sort for operational usefulness: users currently moving data the fastest float to top.
    return sorted(
        rows,
        key=lambda row: (
            -row['interval_mb'],
            -row['day_total_mb'],
            str(row['name']).lower(),
            str(row['mac']).lower(),
        ),
    )


def build_rows_for_historical_window(
    window_name: str,
    speed_limits_by_name: dict[str, str],
) -> list[dict[str, Any]]:
    'Build dashboard rows from usage ledger summaries for non-live windows.'
    summaries = db.get_usage_window_summary(window_name)
    return [
        {
            'user_id': row.user_id or '',
            'name': row.name or row.mac,
            'ap_name': row.ap_name or '',
            'mac': row.mac,
            'vlan_name': row.vlan or 'Unknown',
            'signal': None,
            'interval_mb': 0.0,
            'day_total_mb': row.day_total_mb,
            'last_7_days_total_mb': row.last_7_days_total_mb,
            'calendar_month_total_mb': row.calendar_month_total_mb,
            # Historical rows store only profile names in DB; map to current display text when possible.
            'effective_speed_limit': (
                speed_limits_by_name.get(row.profile, row.profile) if row.profile else ''
            ),
        }
        for row in summaries
    ]


def build_dashboard_data(window_name: str, live_update_seconds: int) -> dict[str, Any]:
    'Assemble all dashboard fields needed by HTML render, API snapshot, and SSE stream.'
    speed_limits_by_name = {
        limit.name: str(limit) for limit in api.get_speed_limits()
    }
    rows = (
        build_rows_for_online_clients()
        if window_name == WINDOW_ONLINE_NOW
        else build_rows_for_historical_window(window_name, speed_limits_by_name)
    )
    return {
        'clients': rows,
        'selected_window': window_name,
        'current_month_label': datetime.now().strftime('%b'),
        'total_today_mb': db.get_total_today_usage(),
        'total_last_7_days_mb': db.get_total_last_7_days_usage(),
        'total_calendar_month_mb': db.get_total_calendar_month_usage(),
        'live_update_seconds': live_update_seconds,
    }


def build_dashboard_payload(data: dict[str, Any]) -> dict[str, Any]:
    'Project dashboard data into the lightweight JSON payload used by snapshot/SSE routes.'
    return {
        'selected_window': data['selected_window'],
        'current_month_label': data['current_month_label'],
        'total_today_mb': data['total_today_mb'],
        'total_last_7_days_mb': data['total_last_7_days_mb'],
        'total_calendar_month_mb': data['total_calendar_month_mb'],
        'clients': data['clients'],
        'live_update_seconds': data['live_update_seconds'],
    }
