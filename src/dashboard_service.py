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
import re
from typing import Literal, TypedDict, cast

import config as cfg
import database as db
import unifi_api as api
from monitor import ClientSnapshot, get_connected_clients
from speedlimit import SpeedLimit
from unifi_time import normalize_epoch_seconds, normalize_online_seconds

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
    ap_count: int
    ap_breakdown: str
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


class TopCurrentConsumer(TypedDict):
    'Serialized chart slice for clients currently moving data.'
    label: str
    mac: str
    interval_mb: float


class InsightsData(TypedDict):
    'Insights-page data payload.'
    current_month_label: str
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
    organization_paid_cost_cents: float
    organization_paid_minutes: int
    user_paid_total_mb: float
    user_paid_cost_cents: float
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
    concurrency_heatmap_sample_counts: list[list[int]]
    throttling_profile_labels: list[str]
    throttling_profile_minutes: list[int]
    throttling_total_active_minutes: int
    throttling_minutes: int
    throttling_pct: float


class DashboardPayload(TypedDict):
    'Subset payload returned by snapshot/SSE endpoints.'
    selected_window: WindowName
    selected_activity_span: ActivitySpan
    current_month_label: str
    total_today_mb: float
    total_last_7_days_mb: float
    total_calendar_month_mb: float
    top_current_consumers: list[TopCurrentConsumer]
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


def aggregate_top_users_by_identity(rows: list[db.GlobalTopUser], limit: int = 6) -> list[db.GlobalTopUser]:
    'Merge per-MAC rows by user_id only; keep no-user_id rows per device to avoid name collisions.'
    grouped: dict[str, db.GlobalTopUser] = {}
    for row in rows:
        normalized_user_id = row.user_id.strip()
        normalized_name = row.name.strip() if row.name else row.mac
        if normalized_user_id:
            group_key = f'user:{normalized_user_id.lower()}'
            display_user_id = normalized_user_id
            display_name = normalized_user_id
        else:
            group_key = f'mac:{row.mac.lower()}'
            display_user_id = ''
            display_name = normalized_name

        existing = grouped.get(group_key)
        if existing is None:
            grouped[group_key] = db.GlobalTopUser(
                mac=row.mac,
                name=display_name,
                user_id=display_user_id,
                vlan_name='',
                total_mb=row.total_mb,
                active_minutes=row.active_minutes,
            )
            continue

        grouped[group_key] = db.GlobalTopUser(
            mac=existing.mac,
            name=existing.name,
            user_id=existing.user_id,
            vlan_name='',
            total_mb=existing.total_mb + row.total_mb,
            active_minutes=existing.active_minutes + row.active_minutes,
        )

    return sorted(
        grouped.values(),
        key=lambda item: (item.total_mb, item.active_minutes, item.name.lower()),
        reverse=True,
    )[:max(1, limit)]


def profile_display_label(profile_key: str, speed_limits_by_name: dict[str, SpeedLimit]) -> str:
    'Render a readable label for one stored profile key.'
    if not profile_key:
        return 'Default'
    if matched_limit := speed_limits_by_name.get(profile_key):
        return f'{matched_limit.name} ({matched_limit.up_kbps}/{matched_limit.down_kbps})'
    return profile_key


def _serialize_access_point_row(row: db.GlobalTopAccessPoint) -> dict[str, object]:
    'Serialize one AP insight row for template consumption.'
    return {
        'ap_name': row.ap_name,
        'total_mb': row.total_mb,
        'active_minutes': row.active_minutes,
    }


def _serialize_usage_actor_row(row: db.GlobalTopUser, include_vlan_name: bool = False) -> dict[str, object]:
    'Serialize a usage actor row for top-users and organization-paid tables.'
    data: dict[str, object] = {
        'mac': row.mac,
        'name': row.name,
        'user_id': row.user_id,
        'total_mb': row.total_mb,
        'month_cost_cents': calculate_month_cost_cents(row.total_mb),
        'active_minutes': row.active_minutes,
    }
    if include_vlan_name:
        data['vlan_name'] = row.vlan_name
    return data


def build_rows_for_online_clients(
    active_only: bool = False,
    snapshots: list[ClientSnapshot] | None = None,
) -> list[DashboardRow]:
    'Build dashboard rows from live controller client snapshots.'
    def right_half_ip(ip_address: str) -> str:
        parts = ip_address.split('.')
        if len(parts) == 4:
            return '.'.join(parts[2:])
        return ''

    rows: list[DashboardRow] = []
    source_snapshots = snapshots if snapshots is not None else get_connected_clients()
    for snapshot in source_snapshots:
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
            'ap_count': 1 if snapshot.client.ap_name else 0,
            'ap_breakdown': snapshot.client.ap_name or '',
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
    ap_rollups_by_mac = db.get_usage_window_access_point_minutes(window_name)
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

        ap_rollups = ap_rollups_by_mac.get(summary.mac, [])
        if ap_rollups:
            primary_ap_name = ap_rollups[0][0]
            ap_count = len(ap_rollups)
            ap_breakdown_parts = [f'{ap_name} ({minutes}m)' for ap_name, minutes in ap_rollups[:4]]
            if len(ap_rollups) > 4:
                ap_breakdown_parts.append(f'+{len(ap_rollups) - 4} more')
            ap_breakdown = ', '.join(ap_breakdown_parts)
        else:
            primary_ap_name = summary.ap_name or ''
            ap_count = 1 if primary_ap_name else 0
            ap_breakdown = primary_ap_name

        row: DashboardRow = {
            'user_id': summary.user_id or '',
            'name': summary.name or summary.mac,
            'ap_name': primary_ap_name,
            'ap_count': ap_count,
            'ap_breakdown': ap_breakdown,
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


def add_current_connection_minutes(
    rows: list[DashboardRow],
    online_assoc_minutes_by_mac: dict[str, int] | None = None,
) -> None:
    'Attach current online/offline minutes from recent UniFi client queries.'
    if not rows:
        return

    def format_signed_hhmm(total_minutes: int) -> str:
        sign = '-' if total_minutes < 0 else ''
        absolute_minutes = abs(total_minutes)
        hours = absolute_minutes // 60
        minutes = absolute_minutes % 60
        return f'{sign}{hours:02d}:{minutes:02d}'

    now_seconds = int(datetime.now().timestamp())
    if online_assoc_minutes_by_mac is None:
        online_assoc_minutes_by_mac = {}
        for client in api.get_api_data('stat/sta'):
            mac = client.get('mac')
            if not isinstance(mac, str):
                continue
            assoc_seconds = normalize_online_seconds(client.get('assoc_time'), now_seconds)
            if assoc_seconds is None:
                assoc_seconds = normalize_online_seconds(client.get('latest_assoc_time'), now_seconds)
            online_assoc_minutes_by_mac[mac.lower()] = (assoc_seconds // 60) if assoc_seconds is not None else 0
    else:
        online_assoc_minutes_by_mac = {
            mac.lower(): max(0, int(minutes))
            for mac, minutes in online_assoc_minutes_by_mac.items()
        }

    offline_minutes_by_mac: dict[str, int] = {}
    needs_offline_lookup = any(row['mac'].lower() not in online_assoc_minutes_by_mac for row in rows)
    if needs_offline_lookup:
        for client in api.get_api_data('stat/alluser'):
            mac = client.get('mac')
            if not isinstance(mac, str):
                continue
            key = mac.lower()
            if key in online_assoc_minutes_by_mac:
                continue
            last_seen = normalize_epoch_seconds(client.get('last_seen'))
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


def build_top_current_consumers(snapshots: list[ClientSnapshot], limit: int = 6) -> list[TopCurrentConsumer]:
    'Return the top currently active clients for the dashboard pie chart.'
    def duplicate_suffix(client_label: str, device_name: str, mac: str) -> str:
        'Return a compact secondary label for repeated chart labels.'
        if not device_name or device_name == client_label:
            return mac[-5:]
        suffix = device_name.strip()
        compact_device_patterns = (
            (r'\b(macbook(?:\s+(?:air|pro))?)\b', 'MacBook'),
            (r'\b(iphone(?:\s+\d+\w*)?(?:\s+plus|\s+pro|\s+max)?)\b', 'iPhone'),
            (r'\b(ipad(?:\s+pro|\s+air|\s+mini)?)\b', 'iPad'),
            (r'\b(galaxy(?:[-\s]s?\d+\w*)?)\b', 'Galaxy'),
            (r'\b(pixel(?:\s+\d+\w*)?)\b', 'Pixel'),
        )
        for pattern, fallback in compact_device_patterns:
            if matched_device := re.search(pattern, suffix, flags=re.IGNORECASE):
                suffix = matched_device.group(1) or fallback
                break
        if len(suffix) > 18:
            suffix = f'{suffix[:15].rstrip()}...'
        return suffix

    active_snapshots = sorted(
        (snapshot for snapshot in snapshots if snapshot.interval_mb > 0.0),
        key=lambda snapshot: (
            snapshot.interval_mb,
            snapshot.day_total_mb,
            (snapshot.client.user_id or snapshot.client.name or snapshot.client.mac).lower(),
        ),
        reverse=True,
    )
    top_snapshots = active_snapshots[:max(1, limit)]
    base_labels = [
        (snapshot.client.user_id or snapshot.client.name or snapshot.client.mac)
        for snapshot in top_snapshots
    ]
    duplicate_base_labels = {
        label for label in base_labels
        if sum(1 for candidate in base_labels if candidate == label) > 1
    }
    consumers: list[TopCurrentConsumer] = []
    for snapshot, base_label in zip(top_snapshots, base_labels):
        client = snapshot.client
        label = base_label
        if base_label in duplicate_base_labels:
            secondary_label = duplicate_suffix(base_label, client.name, client.mac)
            label = f'{base_label} - {secondary_label}'
        consumers.append({
            'label': label,
            'mac': client.mac,
            'interval_mb': snapshot.interval_mb,
        })
    return consumers


def _build_live_dashboard_rows(
    window_name: WindowName,
    activity_span: ActivitySpan,
    connected_snapshots: list[ClientSnapshot],
) -> list[DashboardRow]:
    if window_name == WINDOW_ONLINE_NOW:
        rows = build_rows_for_online_clients(snapshots=connected_snapshots)
    elif window_name == WINDOW_ACTIVE_NOW:
        rows = build_rows_for_online_clients(active_only=True, snapshots=connected_snapshots)
    else:
        speed_limits_by_name = {limit.name: limit for limit in api.get_speed_limits()}
        rows = build_rows_for_historical_window(window_name, speed_limits_by_name)

    online_assoc_minutes_by_mac: dict[str, int] | None = None
    if connected_snapshots:
        online_assoc_minutes_by_mac = {
            snapshot.client.mac.lower(): (
                (snapshot.client.assoc_time_seconds // 60)
                if isinstance(snapshot.client.assoc_time_seconds, int) and snapshot.client.assoc_time_seconds >= 0
                else 0
            )
            for snapshot in connected_snapshots
        }
    add_current_connection_minutes(rows, online_assoc_minutes_by_mac=online_assoc_minutes_by_mac)
    add_recent_activity(rows, activity_span)
    return rows


def build_live_dashboard_payload(
    window_name: WindowName,
    activity_span: ActivitySpan,
    live_update_seconds: int,
) -> DashboardPayload:
    'Assemble live dashboard payload used by /, snapshot API, and SSE.'
    connected_snapshots = get_connected_clients()
    rows = _build_live_dashboard_rows(window_name, activity_span, connected_snapshots)
    return {
        'selected_window': window_name,
        'selected_activity_span': activity_span,
        'current_month_label': render_month_label(datetime.now()),
        'total_today_mb': db.get_total_today_usage(),
        'total_last_7_days_mb': db.get_total_last_7_days_usage(),
        'total_calendar_month_mb': db.get_total_calendar_month_usage(),
        'top_current_consumers': build_top_current_consumers(connected_snapshots),
        'clients': rows,
        'live_update_seconds': live_update_seconds,
    }


def build_insights_data() -> InsightsData:
    'Assemble data payload for the /insights page.'
    organization_paid_vlan_criteria = list(cfg.ORGANIZATION_PAID_VLAN_NAMES)
    organization_paid_mac_criteria = list(cfg.ORGANIZATION_PAID_DEVICE_MACS)
    organization_paid_user_id_criteria = list(cfg.ORGANIZATION_PAID_USER_IDS)

    connected_snapshots: list[ClientSnapshot] = []
    if organization_paid_vlan_criteria or organization_paid_mac_criteria or organization_paid_user_id_criteria:
        connected_snapshots = get_connected_clients()

    def is_organization_paid_identity(mac: str, user_id: str | None, vlan_name: str | None) -> bool:
        return (
            (vlan_name or '') in organization_paid_vlan_criteria
            or mac in organization_paid_mac_criteria
            or (user_id or '') in organization_paid_user_id_criteria
        )

    insights = db.get_global_month_insights(top_limit=6)
    top_users_by_mac_this_month = db.get_global_top_users_current_month(
        limit=60,
        exclude_organization_paid_macs=organization_paid_mac_criteria,
        exclude_organization_paid_user_ids=organization_paid_user_id_criteria,
        exclude_organization_paid_vlan_names=organization_paid_vlan_criteria,
    )
    top_users_this_month = aggregate_top_users_by_identity(top_users_by_mac_this_month, limit=6)
    daily_network_usage = db.get_global_daily_network_usage_current_month()
    payer_split = db.get_global_payer_split_current_month(
        organization_paid_macs=organization_paid_mac_criteria,
        organization_paid_user_ids=organization_paid_user_id_criteria,
        organization_paid_vlan_names=organization_paid_vlan_criteria,
    )
    organization_paid_clients = db.get_global_organization_paid_clients_current_month(
        organization_paid_macs=organization_paid_mac_criteria,
        organization_paid_user_ids=organization_paid_user_id_criteria,
        organization_paid_vlan_names=organization_paid_vlan_criteria,
        limit=12,
    )
    organization_paid_clients_by_mac = {entry.mac.lower(): entry for entry in organization_paid_clients}
    for snapshot in connected_snapshots:
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
    speed_limits_by_name = {limit.name: limit for limit in api.get_speed_limits()}
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

    return {
        'current_month_label': render_month_label(datetime.now()),
        'active_users_daily_min': insights.active_users_min,
        'active_users_daily_mean': insights.active_users_mean,
        'active_users_daily_max': insights.active_users_max,
        'active_users_today': insights.active_users_today,
        'active_users_days_in_period': insights.days_in_period,
        'active_users_chart_x_labels': insights.active_users_daily_x_labels,
        'active_users_chart_full_labels': insights.active_users_daily_full_labels,
        'active_users_chart_counts': insights.active_users_daily_counts,
        'top_users_this_month': [_serialize_usage_actor_row(top_row) for top_row in top_users_this_month],
        'top_access_points_this_month': [_serialize_access_point_row(ap_row) for ap_row in insights.top_access_points],
        'daily_network_x_labels': [row.usage_day.day for row in daily_network_usage],
        'daily_network_full_labels': [f'{row.usage_day.strftime("%b")} {row.usage_day.day}' for row in daily_network_usage],
        'daily_network_basic_mb': [row.basic_mb for row in daily_network_usage],
        'daily_network_plus_mb': [row.plus_mb for row in daily_network_usage],
        'daily_network_basic_minutes': [row.basic_minutes for row in daily_network_usage],
        'daily_network_plus_minutes': [row.plus_minutes for row in daily_network_usage],
        'organization_paid_total_mb': payer_split.organization_paid_total_mb,
        'organization_paid_cost_cents': calculate_month_cost_cents(payer_split.organization_paid_total_mb),
        'organization_paid_minutes': payer_split.organization_paid_minutes,
        'user_paid_total_mb': payer_split.user_paid_total_mb,
        'user_paid_cost_cents': calculate_month_cost_cents(payer_split.user_paid_total_mb),
        'user_paid_minutes': payer_split.user_paid_minutes,
        'organization_paid_clients': [
            _serialize_usage_actor_row(org_row, include_vlan_name=True)
            for org_row in organization_paid_clients
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
        'concurrency_heatmap_sample_counts': concurrency_insights.heatmap_sample_counts,
        'throttling_profile_labels': [
            profile_display_label(profile_key, speed_limits_by_name)
            for profile_key, _ in throttling_profile_totals
        ],
        'throttling_profile_minutes': [minutes for _, minutes in throttling_profile_totals],
        'throttling_total_active_minutes': throttling_effectiveness.total_active_minutes,
        'throttling_minutes': throttling_effectiveness.throttled_minutes,
        'throttling_pct': throttling_effectiveness.throttled_pct,
    }
