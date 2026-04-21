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

import config as cfg
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
    minute_rx_mb: float | None
    minute_tx_mb: float | None
    interval_mb: float
    day_total_mb: float
    last_7_days_total_mb: float
    calendar_month_total_mb: float
    month_cost_cents: float
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
    active_users_daily_min: int
    active_users_daily_mean: float
    active_users_daily_max: int
    active_users_today: int
    active_users_days_in_period: int
    active_users_chart_x_labels: list[int]
    active_users_chart_full_labels: list[str]
    active_users_chart_counts: list[int]
    top_users_this_month: list[dict[str, object]]
    top_access_points_this_month: list[dict[str, object]]
    daily_network_x_labels: list[int]
    daily_network_full_labels: list[str]
    daily_network_basic_mb: list[float]
    daily_network_plus_mb: list[float]
    daily_network_basic_minutes: list[int]
    daily_network_plus_minutes: list[int]
    organization_paid_total_mb: float
    organization_paid_minutes: int
    user_paid_total_mb: float
    user_paid_minutes: int
    organization_paid_clients: list[dict[str, object]]
    organization_paid_vlan_criteria: list[str]
    organization_paid_mac_criteria: list[str]
    organization_paid_user_id_criteria: list[str]
    peak_concurrency_x_labels: list[int]
    peak_concurrency_full_labels: list[str]
    peak_concurrency_counts: list[int]
    peak_concurrency_time_labels: list[str]
    concurrency_heatmap_day_labels: list[str]
    concurrency_heatmap_hour_labels: list[str]
    concurrency_heatmap_values: list[list[float]]
    throttling_profile_labels: list[str]
    throttling_profile_minutes: list[int]
    throttling_total_active_minutes: int
    throttling_minutes: int
    throttling_pct: float
    live_update_seconds: int


class DashboardPayload(TypedDict):
    'Subset payload returned by snapshot/SSE endpoints.'
    selected_window: WindowName
    selected_activity_span: ActivitySpan
    current_month_label: str
    total_today_mb: float
    total_last_7_days_mb: float
    total_calendar_month_mb: float
    clients: list[DashboardRow]
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


def render_month_label(now: datetime) -> str:
    'Return full month name unless it is long, then use abbreviation.'
    full_label = now.strftime('%B')
    if len(full_label) > 5:
        return now.strftime('%b')
    return full_label


def calculate_month_cost_cents(calendar_month_total_mb: float) -> float:
    'Return month cost in cents based on configured cents-per-GB rate.'
    month_total_gb = calendar_month_total_mb / 1000.0
    return month_total_gb * float(cfg.COST_IN_CENTS_PER_GB)


def profile_display_label(profile_key: str, speed_limits_by_name: dict[str, SpeedLimit]) -> str:
    'Render a readable label for one stored profile key.'
    if not profile_key:
        return 'Default'
    if matched_limit := speed_limits_by_name.get(profile_key):
        return f'{matched_limit.name} ({matched_limit.up_kbps}/{matched_limit.down_kbps})'
    return profile_key


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
        direction_total_mb = snapshot.client.tx_mb_since_connection + snapshot.client.rx_mb_since_connection
        minute_tx_mb: float | None = None
        minute_rx_mb: float | None = None
        if snapshot.interval_mb > 0 and direction_total_mb > 0:
            tx_ratio = snapshot.client.tx_mb_since_connection / direction_total_mb
            minute_tx_mb = snapshot.interval_mb * tx_ratio
            minute_rx_mb = max(0.0, snapshot.interval_mb - minute_tx_mb)

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
            'minute_rx_mb': minute_rx_mb,
            'minute_tx_mb': minute_tx_mb,
            'interval_mb': snapshot.interval_mb,
            'day_total_mb': snapshot.day_total_mb,
            'last_7_days_total_mb': snapshot.last_7_days_total_mb,
            'calendar_month_total_mb': snapshot.calendar_month_total_mb,
            'month_cost_cents': calculate_month_cost_cents(snapshot.calendar_month_total_mb),
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
        key=lambda entry: (
            -entry['interval_mb'],
            -entry['day_total_mb'],
            str(entry['name']).lower(),
            str(entry['mac']).lower(),
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
            'minute_rx_mb': None,
            'minute_tx_mb': None,
            'interval_mb': 0.0,
            'day_total_mb': summary.day_total_mb,
            'last_7_days_total_mb': summary.last_7_days_total_mb,
            'calendar_month_total_mb': summary.calendar_month_total_mb,
            'month_cost_cents': calculate_month_cost_cents(summary.calendar_month_total_mb),
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

    def normalize_online_seconds(raw_value: object, now_epoch_seconds: int) -> int | None:
        parsed = parse_non_negative_int(raw_value)
        if parsed is None:
            return None
        # If this looks like an epoch timestamp, convert to elapsed duration.
        if parsed > 10_000_000_000:
            parsed = parsed // 1000
        if 946684800 <= parsed <= now_epoch_seconds + 86400:
            return max(0, now_epoch_seconds - parsed)
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
    organization_paid_vlan_criteria = sorted(name.strip() for name in cfg.ORGANIZATION_PAID_VLAN_NAMES if name.strip())
    organization_paid_mac_criteria = sorted(name.strip() for name in cfg.ORGANIZATION_PAID_DEVICE_MACS if name.strip())
    organization_paid_user_id_criteria = sorted(name.strip() for name in cfg.ORGANIZATION_PAID_USER_IDS if name.strip())

    def is_organization_paid_identity(mac: str, user_id: str | None, vlan_name: str | None) -> bool:
        mac_key = mac.strip().lower()
        user_key = user_id.strip().lower() if user_id else ''
        vlan_key = vlan_name.strip().lower() if vlan_name else ''
        return (
            vlan_key in {name.lower() for name in organization_paid_vlan_criteria}
            or mac_key in {name.lower() for name in organization_paid_mac_criteria}
            or user_key in {name.lower() for name in organization_paid_user_id_criteria}
        )

    insights = db.get_global_month_insights(top_limit=6)
    top_users_this_month = db.get_global_top_users_current_month(
        limit=6,
        exclude_organization_paid_macs=cfg.ORGANIZATION_PAID_DEVICE_MACS,
        exclude_organization_paid_user_ids=cfg.ORGANIZATION_PAID_USER_IDS,
        exclude_organization_paid_vlan_names=cfg.ORGANIZATION_PAID_VLAN_NAMES,
    )
    daily_network_usage = db.get_global_daily_network_usage_current_month()
    payer_split = db.get_global_payer_split_current_month(
        organization_paid_macs=cfg.ORGANIZATION_PAID_DEVICE_MACS,
        organization_paid_user_ids=cfg.ORGANIZATION_PAID_USER_IDS,
        organization_paid_vlan_names=cfg.ORGANIZATION_PAID_VLAN_NAMES,
    )
    organization_paid_clients = db.get_global_organization_paid_clients_current_month(
        organization_paid_macs=cfg.ORGANIZATION_PAID_DEVICE_MACS,
        organization_paid_user_ids=cfg.ORGANIZATION_PAID_USER_IDS,
        organization_paid_vlan_names=cfg.ORGANIZATION_PAID_VLAN_NAMES,
        limit=12,
    )
    organization_paid_clients_by_mac = {entry.mac.lower(): entry for entry in organization_paid_clients}
    for snapshot in get_connected_clients():
        client = snapshot.client
        if not is_organization_paid_identity(client.mac, client.user_id, client.vlan_name):
            continue
        mac_key = client.mac.lower()
        if mac_key in organization_paid_clients_by_mac:
            continue
        organization_paid_clients.append(
            db.GlobalTopUser(
                mac=client.mac,
                name=client.name or client.mac,
                user_id=client.user_id or '',
                vlan_name=client.vlan_name or '',
                total_mb=0.0,
                active_minutes=0,
            )
        )

    organization_paid_clients = sorted(
        organization_paid_clients,
        key=lambda entry: (entry.total_mb, entry.active_minutes, entry.name.lower()),
        reverse=True,
    )[:12]
    speed_limits_by_name = {
        limit.name: limit for limit in api.get_speed_limits()
    }
    concurrency_insights = db.get_global_concurrency_insights_current_month()
    throttling_effectiveness = db.get_global_throttling_effectiveness_current_month()
    allowed_throttling_profiles = {level.profile_name for level in cfg.THROTTLING_LEVELS}
    throttling_profile_totals = sorted(
        (
            (profile_key, minutes)
            for profile_key, minutes in throttling_effectiveness.profile_minutes.items()
            if profile_key in allowed_throttling_profiles
        ),
        key=lambda pair: pair[1],
        reverse=True,
    )
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
        'current_month_label': render_month_label(datetime.now()),
        'total_today_mb': db.get_total_today_usage(),
        'total_last_7_days_mb': db.get_total_last_7_days_usage(),
        'total_calendar_month_mb': db.get_total_calendar_month_usage(),
        'active_users_daily_min': insights.active_users_min,
        'active_users_daily_mean': insights.active_users_mean,
        'active_users_daily_max': insights.active_users_max,
        'active_users_today': insights.active_users_today,
        'active_users_days_in_period': insights.days_in_period,
        'active_users_chart_x_labels': insights.active_users_daily_x_labels,
        'active_users_chart_full_labels': insights.active_users_daily_full_labels,
        'active_users_chart_counts': insights.active_users_daily_counts,
        'top_users_this_month': [
            {
                'mac': top_row.mac,
                'name': top_row.name,
                'user_id': top_row.user_id,
                'total_mb': top_row.total_mb,
                'active_minutes': top_row.active_minutes,
            }
            for top_row in top_users_this_month
        ],
        'top_access_points_this_month': [
            {
                'ap_name': insight_row.ap_name,
                'total_mb': insight_row.total_mb,
                'active_minutes': insight_row.active_minutes,
            }
            for insight_row in insights.top_access_points
        ],
        'daily_network_x_labels': [network_row.usage_day.day for network_row in daily_network_usage],
        'daily_network_full_labels': [f'{network_row.usage_day.strftime("%b")} {network_row.usage_day.day}' for network_row in daily_network_usage],
        'daily_network_basic_mb': [network_row.basic_mb for network_row in daily_network_usage],
        'daily_network_plus_mb': [network_row.plus_mb for network_row in daily_network_usage],
        'daily_network_basic_minutes': [network_row.basic_minutes for network_row in daily_network_usage],
        'daily_network_plus_minutes': [network_row.plus_minutes for network_row in daily_network_usage],
        'organization_paid_total_mb': payer_split.organization_paid_total_mb,
        'organization_paid_minutes': payer_split.organization_paid_minutes,
        'user_paid_total_mb': payer_split.user_paid_total_mb,
        'user_paid_minutes': payer_split.user_paid_minutes,
        'organization_paid_clients': [
            {
                'mac': organization_row.mac,
                'name': organization_row.name,
                'user_id': organization_row.user_id,
                'vlan_name': organization_row.vlan_name,
                'total_mb': organization_row.total_mb,
                'active_minutes': organization_row.active_minutes,
            }
            for organization_row in organization_paid_clients
        ],
        'organization_paid_vlan_criteria': organization_paid_vlan_criteria,
        'organization_paid_mac_criteria': organization_paid_mac_criteria,
        'organization_paid_user_id_criteria': organization_paid_user_id_criteria,
        'peak_concurrency_x_labels': concurrency_insights.daily_x_labels,
        'peak_concurrency_full_labels': concurrency_insights.daily_full_labels,
        'peak_concurrency_counts': concurrency_insights.daily_peak_counts,
        'peak_concurrency_time_labels': concurrency_insights.daily_peak_time_labels,
        'concurrency_heatmap_day_labels': concurrency_insights.heatmap_day_labels,
        'concurrency_heatmap_hour_labels': concurrency_insights.heatmap_hour_labels,
        'concurrency_heatmap_values': concurrency_insights.heatmap_values,
        'throttling_profile_labels': [
            profile_display_label(profile_key, speed_limits_by_name)
            for profile_key, _ in throttling_profile_totals
        ],
        'throttling_profile_minutes': [minutes for _, minutes in throttling_profile_totals],
        'throttling_total_active_minutes': throttling_effectiveness.total_active_minutes,
        'throttling_minutes': throttling_effectiveness.throttled_minutes,
        'throttling_pct': throttling_effectiveness.throttled_pct,
        'live_update_seconds': live_update_seconds,
    }


def build_dashboard_payload(data: DashboardData) -> DashboardPayload:
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
