'''Dashboard data service module.

This module owns dashboard-specific data assembly and normalization rules used by:
- initial HTML render (`/`)
- snapshot API (`/api/dashboard-snapshot`)
- live stream API (`/api/dashboard-stream`)

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
from typing import Literal, TypedDict, cast

import database as db
import unifi_api as api
from monitor import get_connected_clients
from speedlimit import SpeedLimit

WindowName = Literal['active_now', 'online_now', 'today', 'last_7_days', 'this_month']
ActivitySpan = Literal['12m', '12h', '12d']

WINDOW_ACTIVE_NOW: WindowName = 'active_now'
WINDOW_ONLINE_NOW: WindowName = 'online_now'
WINDOW_TODAY: WindowName = 'today'
WINDOW_LAST_7_DAYS: WindowName = 'last_7_days'
WINDOW_THIS_MONTH: WindowName = 'this_month'
ALLOWED_WINDOWS: frozenset[WindowName] = frozenset({
    WINDOW_ACTIVE_NOW,
    WINDOW_ONLINE_NOW,
    WINDOW_TODAY,
    WINDOW_LAST_7_DAYS,
    WINDOW_THIS_MONTH,
})
ACTIVITY_SPAN_12_MIN: ActivitySpan = '12m'
ACTIVITY_SPAN_12_HOUR: ActivitySpan = '12h'
ACTIVITY_SPAN_12_DAY: ActivitySpan = '12d'
ALLOWED_ACTIVITY_SPANS: frozenset[ActivitySpan] = frozenset({ACTIVITY_SPAN_12_MIN, ACTIVITY_SPAN_12_HOUR, ACTIVITY_SPAN_12_DAY})


class DashboardRow(TypedDict):
    'Serialized row shape consumed by the dashboard table in HTML and JSON.'
    user_id: str
    name: str
    ap_name: str
    mac: str
    ip_half: str
    vlan_name: str
    signal: int | None
    recent_activity: list[float]
    connection_duration: str
    interval_mb: float
    day_total_mb: float
    last_7_days_total_mb: float
    calendar_month_total_mb: float
    speed_limit_name: str
    speed_limit_up_kbps: int | None
    speed_limit_down_kbps: int | None


class DashboardData(TypedDict):
    'Canonical dashboard payload shared by template render, snapshot API, and SSE.'
    clients: list[DashboardRow]
    selected_window: WindowName
    selected_activity_span: ActivitySpan
    current_month_label: str
    total_today_mb: float
    total_last_7_days_mb: float
    total_calendar_month_mb: float
    live_update_seconds: int


def normalize_window(window_name: str | None) -> WindowName:
    'Return a safe dashboard window key, defaulting to active_now.'
    if isinstance(window_name, str) and window_name in ALLOWED_WINDOWS:
        return cast(WindowName, window_name)
    return WINDOW_ACTIVE_NOW


def normalize_activity_span(activity_span: str | None) -> ActivitySpan:
    'Return a safe activity-span key, defaulting to 12m.'
    if isinstance(activity_span, str) and activity_span in ALLOWED_ACTIVITY_SPANS:
        return cast(ActivitySpan, activity_span)
    return ACTIVITY_SPAN_12_MIN


def build_rows_for_online_clients(active_only: bool = False) -> list[DashboardRow]:
    'Build dashboard rows from live controller client snapshots.'
    def right_half_ip(ip_address: str) -> str:
        parts = ip_address.split('.')
        if len(parts) == 4:
            return '.'.join(parts[2:])
        return ''

    rows: list[DashboardRow] = []
    for snapshot in get_connected_clients():
        speed_limit = snapshot.effective_speed_limit
        row: DashboardRow = {
            'user_id': snapshot.client.user_id or '',
            'name': snapshot.client.name,
            'ap_name': snapshot.client.ap_name or '',
            'mac': snapshot.client.mac,
            'ip_half': right_half_ip(snapshot.client.ip_address),
            'vlan_name': snapshot.client.vlan_name or 'Unknown',
            'signal': snapshot.client.signal if snapshot.client.signal else None,
            'recent_activity': [],
            'connection_duration': '',
            'interval_mb': snapshot.interval_mb,
            'day_total_mb': snapshot.day_total_mb,
            'last_7_days_total_mb': snapshot.last_7_days_total_mb,
            'calendar_month_total_mb': snapshot.calendar_month_total_mb,
            'speed_limit_name': speed_limit.name if speed_limit else '',
            'speed_limit_up_kbps': speed_limit.up_kbps if speed_limit else None,
            'speed_limit_down_kbps': speed_limit.down_kbps if speed_limit else None,
        }
        rows.append(row)
    if active_only:
        rows = [row for row in rows if row['interval_mb'] > 0.0]

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
    window_name: WindowName,
    speed_limits_by_name: dict[str, SpeedLimit],
) -> list[DashboardRow]:
    'Build dashboard rows from usage ledger summaries for non-live windows.'
    summaries = db.get_usage_window_summary(window_name)
    rows: list[DashboardRow] = []
    for summary in summaries:
        speed_limit_name = ''
        speed_limit_up_kbps: int | None = None
        speed_limit_down_kbps: int | None = None
        if summary.profile:
            speed_limit_name = summary.profile
            speed_limit = speed_limits_by_name.get(summary.profile)
            if speed_limit is not None:
                speed_limit_name = speed_limit.name
                speed_limit_up_kbps = speed_limit.up_kbps
                speed_limit_down_kbps = speed_limit.down_kbps

        row: DashboardRow = {
            'user_id': summary.user_id or '',
            'name': summary.name or summary.mac,
            'ap_name': summary.ap_name or '',
            'mac': summary.mac,
            'ip_half': '',
            'vlan_name': summary.vlan or 'Unknown',
            'signal': None,
            'recent_activity': [],
            'connection_duration': '',
            'interval_mb': 0.0,
            'day_total_mb': summary.day_total_mb,
            'last_7_days_total_mb': summary.last_7_days_total_mb,
            'calendar_month_total_mb': summary.calendar_month_total_mb,
            'speed_limit_name': speed_limit_name,
            'speed_limit_up_kbps': speed_limit_up_kbps,
            'speed_limit_down_kbps': speed_limit_down_kbps,
        }
        rows.append(row)

    return rows


def add_current_connection_minutes(rows: list[DashboardRow]) -> None:
    'Attach current online/offline minutes from recent UniFi client queries.'
    if not rows:
        return

    def parse_non_negative_int(value: object) -> int | None:
        if isinstance(value, bool):
            return None
        if isinstance(value, int):
            return value if value >= 0 else None
        if isinstance(value, float):
            return int(value) if value >= 0 else None
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return None
            try:
                parsed = int(float(text))
            except ValueError:
                return None
            return parsed if parsed >= 0 else None
        return None

    def normalize_online_seconds(raw_value: object, now_seconds: int) -> int | None:
        parsed = parse_non_negative_int(raw_value)
        if parsed is None:
            return None
        # If this looks like an epoch timestamp, convert to elapsed duration.
        if parsed > 10_000_000_000:
            parsed = parsed // 1000
        if 946684800 <= parsed <= now_seconds + 86400:
            return max(0, now_seconds - parsed)
        return parsed

    def parse_epoch_seconds(value: object) -> int | None:
        parsed = parse_non_negative_int(value)
        if parsed is None:
            return None
        # UniFi may expose last_seen in milliseconds; normalize to seconds.
        if parsed > 10_000_000_000:
            parsed = parsed // 1000
        return parsed

    def format_signed_hhmm(total_minutes: int) -> str:
        sign = '-' if total_minutes < 0 else ''
        absolute_minutes = abs(total_minutes)
        hours = absolute_minutes // 60
        minutes = absolute_minutes % 60
        return f'{sign}{hours:02d}:{minutes:02d}'

    now_seconds = int(datetime.now().timestamp())
    online_assoc_minutes_by_mac: dict[str, int] = {}
    for client in api.get_api_data('stat/sta'):
        mac = client.get('mac')
        if not isinstance(mac, str):
            continue
        assoc_seconds = normalize_online_seconds(client.get('assoc_time'), now_seconds)
        if assoc_seconds is None:
            assoc_seconds = normalize_online_seconds(client.get('latest_assoc_time'), now_seconds)
        online_assoc_minutes_by_mac[mac.lower()] = (assoc_seconds // 60) if assoc_seconds is not None else 0

    offline_minutes_by_mac: dict[str, int] = {}
    for client in api.get_api_data('stat/alluser'):
        mac = client.get('mac')
        if not isinstance(mac, str):
            continue
        key = mac.lower()
        if key in online_assoc_minutes_by_mac:
            continue
        last_seen = parse_epoch_seconds(client.get('last_seen'))
        if last_seen is None:
            continue
        offline_minutes_by_mac[key] = max(0, (now_seconds - last_seen) // 60)

    for row in rows:
        key = row['mac'].lower()
        if key in online_assoc_minutes_by_mac:
            row['connection_duration'] = format_signed_hhmm(online_assoc_minutes_by_mac[key])
        elif key in offline_minutes_by_mac:
            row['connection_duration'] = format_signed_hhmm(-offline_minutes_by_mac[key])


def add_recent_activity(rows: list[DashboardRow], activity_span: ActivitySpan) -> None:
    'Attach recent activity series to each dashboard row in-place.'
    buckets = 12
    if activity_span == ACTIVITY_SPAN_12_MIN:
        bucket_seconds = 60
    elif activity_span == ACTIVITY_SPAN_12_HOUR:
        bucket_seconds = 3600
    else:
        bucket_seconds = 86400
    macs = [row['mac'] for row in rows if row.get('mac')]
    series_by_mac: dict[str, list[float]] = db.get_recent_activity_series(macs, buckets=buckets, bucket_seconds=bucket_seconds)
    default_series = [0.0] * buckets
    for row in rows:
        row['recent_activity'] = series_by_mac.get(row['mac'], default_series.copy())


def build_dashboard_data(window_name: WindowName, activity_span: ActivitySpan, live_update_seconds: int) -> DashboardData:
    'Assemble all dashboard fields needed by HTML render, API snapshot, and SSE stream.'
    speed_limits_by_name = {
        limit.name: limit for limit in api.get_speed_limits()
    }
    if window_name == WINDOW_ONLINE_NOW:
        rows = build_rows_for_online_clients()
    elif window_name == WINDOW_ACTIVE_NOW:
        rows = build_rows_for_online_clients(active_only=True)
    else:
        rows = build_rows_for_historical_window(window_name, speed_limits_by_name)
    add_current_connection_minutes(rows)
    add_recent_activity(rows, activity_span)
    return {
        'clients': rows,
        'selected_window': window_name,
        'selected_activity_span': activity_span,
        'current_month_label': datetime.now().strftime('%b'),
        'total_today_mb': db.get_total_today_usage(),
        'total_last_7_days_mb': db.get_total_last_7_days_usage(),
        'total_calendar_month_mb': db.get_total_calendar_month_usage(),
        'live_update_seconds': live_update_seconds,
    }


def build_dashboard_payload(data: DashboardData) -> DashboardData:
    'Project dashboard data into the lightweight JSON payload used by snapshot/SSE routes.'
    return {
        'selected_window': data['selected_window'],
        'selected_activity_span': data['selected_activity_span'],
        'current_month_label': data['current_month_label'],
        'total_today_mb': data['total_today_mb'],
        'total_last_7_days_mb': data['total_last_7_days_mb'],
        'total_calendar_month_mb': data['total_calendar_month_mb'],
        'clients': data['clients'],
        'live_update_seconds': data['live_update_seconds'],
    }
