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

from dataclasses import dataclass
from datetime import datetime, time, timedelta
import re
import time as monotonic_time
from threading import Lock
from typing import Literal, TypedDict, cast

import config as cfg
import database as db
import unifi_api as api
from monitor import ClientSnapshot, get_connected_clients
from speedlimit import SpeedLimit
from unifi_time import normalize_epoch_seconds, normalize_online_seconds

WindowName = Literal['active_now', 'online_now', 'today', 'last_7_days', 'this_month']
ActivitySpan = Literal['1h', '6h', '24h', '7d']

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
ACTIVITY_SPAN_1_HOUR: ActivitySpan = '1h'
ACTIVITY_SPAN_6_HOUR: ActivitySpan = '6h'
ACTIVITY_SPAN_24_HOUR: ActivitySpan = '24h'
ACTIVITY_SPAN_7_DAY: ActivitySpan = '7d'
ALLOWED_ACTIVITY_SPANS: frozenset[ActivitySpan] = frozenset({
    ACTIVITY_SPAN_1_HOUR,
    ACTIVITY_SPAN_6_HOUR,
    ACTIVITY_SPAN_24_HOUR,
    ACTIVITY_SPAN_7_DAY,
})
DASHBOARD_WAN_CACHE_SECONDS = 30.0
RECENT_INTERNET_WINDOW_SECONDS = 3600
SPEED_LIMIT_CACHE_SECONDS = 300.0
INSIGHTS_CACHE_SECONDS_CURRENT = 30.0
INSIGHTS_CACHE_SECONDS_HISTORICAL = 600.0
InsightsCacheKey = tuple[str, str, str, str, bool]


class DashboardRow(TypedDict):
    'Serialized row shape consumed by the dashboard table in HTML and JSON.'
    user_id: str
    name: str
    ap_name: str
    ap_count: int
    ap_breakdown: str
    mac: str
    ip_prefix: str
    ip_half: str
    vlan_name: str
    frequency_band: str
    channel: str
    signal: int | None
    recent_activity: list[float]
    recent_total_mb: float
    last_5_min_mb: float
    last_5_min_mbps: float
    connection_duration: str
    day_total_mb: float
    day_cost_cents: float
    last_7_days_total_mb: float
    last_7_days_cost_cents: float
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
    slice_type: str


class InsightsData(TypedDict):
    'Insights-page data payload.'
    current_month_label: str
    report_period_label: str
    active_users_terminal_label: str
    isp_terminal_access_cost_usd: float
    isp_included_usage_gb: int
    isp_included_usage_cost_usd: float
    isp_topoff_usage_gb: int
    isp_topoff_cost_usd: float
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
    daily_wan_vlan_labels: list[str]
    daily_wan_vlan_mb: list[list[float]]
    wan_hourly_title: str
    wan_hourly_labels: list[str]
    wan_hourly_tick_labels: list[str]
    wan_hourly_full_labels: list[str]
    wan_hourly_mb: list[float]
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


class DashboardPayload(TypedDict):
    'Subset payload returned by snapshot/SSE endpoints.'
    selected_window: WindowName
    selected_activity_span: ActivitySpan
    current_month_label: str
    total_today_mb: float
    total_last_7_days_mb: float
    total_calendar_month_mb: float
    last_5_min_mb: float
    last_5_min_mbps: float
    wan_import_status: str
    wan_import_stale: bool
    top_consumers_title: str
    top_current_consumers: list[TopCurrentConsumer]
    clients: list[DashboardRow]
    live_update_seconds: int
    throttling_enabled: bool


@dataclass(frozen=True, kw_only=True)
class DashboardWanData:
    'Cached WAN attribution data shared across dashboard window changes.'
    recent_rows: list[db.WanIdentityUsageSummary]
    today_rows: list[db.WanIdentityUsageSummary]
    seven_day_rows: list[db.WanIdentityUsageSummary]
    month_rows: list[db.WanIdentityUsageSummary]
    last_5_min_totals_by_mac: dict[str, float]
    recent_totals_by_mac: dict[str, float]
    today_totals_by_mac: dict[str, float]
    seven_day_totals_by_mac: dict[str, float]
    month_totals_by_mac: dict[str, float]
    total_today_mb: float
    total_last_7_days_mb: float
    total_calendar_month_mb: float
    last_5_min_mb: float
    last_5_min_mbps: float
    wan_import_status: str
    wan_import_stale: bool


_dashboard_wan_cache_lock = Lock()
_dashboard_wan_cache: tuple[float, DashboardWanData] | None = None
_speed_limits_cache_lock = Lock()
_speed_limits_cache: tuple[float, dict[str, SpeedLimit]] | None = None
_insights_cache_lock = Lock()
_insights_cache_by_key: dict[InsightsCacheKey, tuple[float, InsightsData]] = {}


def clear_dashboard_wan_cache() -> None:
    'Clear cached WAN attribution data after new flow imports arrive.'
    global _dashboard_wan_cache
    with _dashboard_wan_cache_lock:
        _dashboard_wan_cache = None
    with _insights_cache_lock:
        _insights_cache_by_key.clear()


def get_speed_limits_by_name_cached() -> dict[str, SpeedLimit]:
    'Return UniFi speed-limit profiles without blocking every dashboard render on the controller.'
    global _speed_limits_cache
    now_monotonic = monotonic_time.monotonic()
    with _speed_limits_cache_lock:
        if _speed_limits_cache and _speed_limits_cache[0] > now_monotonic:
            return _speed_limits_cache[1]

    speed_limits_by_name = {limit.name: limit for limit in api.get_speed_limits()}
    with _speed_limits_cache_lock:
        _speed_limits_cache = (now_monotonic + SPEED_LIMIT_CACHE_SECONDS, speed_limits_by_name)
    return speed_limits_by_name


def normalize_window(window_name: str | None) -> WindowName:
    'Return a safe dashboard window key, defaulting to active_now.'
    if isinstance(window_name, str) and window_name in ALLOWED_WINDOWS:
        return cast(WindowName, window_name)
    return WINDOW_ACTIVE_NOW


def normalize_activity_span(activity_span: str | None) -> ActivitySpan:
    'Return a safe activity-span key, defaulting to a one-hour WAN sparkline.'
    if activity_span == '12m':
        return ACTIVITY_SPAN_1_HOUR
    if activity_span == '12h':
        return ACTIVITY_SPAN_24_HOUR
    if activity_span == '12d':
        return ACTIVITY_SPAN_7_DAY
    if isinstance(activity_span, str) and activity_span in ALLOWED_ACTIVITY_SPANS:
        return cast(ActivitySpan, activity_span)
    return ACTIVITY_SPAN_1_HOUR


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


def _global_top_user_from_wan_row(
    row: db.WanIdentityUsageSummary,
    sampled_metadata_by_mac: dict[str, db.GlobalTopUser],
) -> db.GlobalTopUser:
    'Return an insights-table user row with WAN MB and sampled active minutes.'
    mac = row.mac or ''
    mac_key = mac.lower()
    sampled_metadata = sampled_metadata_by_mac.get(mac_key)
    return db.GlobalTopUser(
        mac=mac,
        name=row.name or (sampled_metadata.name if sampled_metadata else '') or row.client_ip,
        user_id=row.user_id or (sampled_metadata.user_id if sampled_metadata else ''),
        vlan_name=row.vlan if row.vlan != 'Unknown' else (sampled_metadata.vlan_name if sampled_metadata else ''),
        total_mb=_wan_total_mb(row),
        active_minutes=sampled_metadata.active_minutes if sampled_metadata else 0,
    )


def _wan_total_mb(row: db.WanIdentityUsageSummary) -> float:
    return (row.download_bytes + row.upload_bytes) / 1_000_000.0


def _wan_totals_by_mac(rows: list[db.WanIdentityUsageSummary]) -> dict[str, float]:
    totals_by_mac: dict[str, float] = {}
    for row in rows:
        mac = row.mac.lower()
        if not mac:
            continue
        totals_by_mac[mac] = totals_by_mac.get(mac, 0.0) + _wan_total_mb(row)
    return totals_by_mac


def _total_wan_mb(rows: list[db.WanIdentityUsageSummary]) -> float:
    return sum(_wan_total_mb(row) for row in rows)


def _activity_window_seconds(activity_span: ActivitySpan) -> int:
    if activity_span == ACTIVITY_SPAN_6_HOUR:
        return 6 * 3600
    if activity_span == ACTIVITY_SPAN_24_HOUR:
        return 24 * 3600
    if activity_span == ACTIVITY_SPAN_7_DAY:
        return 7 * 86400
    return 3600


def render_wan_import_status(age_minutes: int | None) -> str:
    'Return a dashboard freshness label for the latest Internet data import.'
    if age_minutes is None:
        return 'Internet data: none yet'
    if age_minutes <= 0:
        return 'Internet data updated just now'
    if age_minutes == 1:
        return 'Internet data updated 1m ago'
    return f'Internet data updated {age_minutes}m ago'


def _last_5_min_metrics(rows: list[db.WanIdentityUsageSummary]) -> tuple[float, float]:
    'Return MB and average Mbps for one five-minute WAN summary window.'
    total_mb = _total_wan_mb(rows)
    return total_mb, total_mb * 8.0 / 300.0


def _build_dashboard_wan_data(now: datetime) -> DashboardWanData:
    'Build the WAN attribution slice used by all dashboard views.'
    today_start = datetime.combine(now.date(), time.min)
    seven_days_ago = now - timedelta(days=7)
    month_start = datetime.combine(now.date().replace(day=1), time.min)
    last_5_min_start = now - timedelta(minutes=5)
    recent_start = now - timedelta(seconds=RECENT_INTERNET_WINDOW_SECONDS)
    period_rows = db.get_wan_usage_by_identity_for_periods(
        {
            'last_5_min': last_5_min_start,
            'recent': recent_start,
            'today': today_start,
            'seven_day': seven_days_ago,
            'month': month_start,
        },
        period_end=now,
    )
    last_5_min_wan_rows = period_rows.get('last_5_min', [])
    recent_wan_rows = period_rows.get('recent', [])
    today_wan_rows = period_rows.get('today', [])
    seven_day_wan_rows = period_rows.get('seven_day', [])
    month_wan_rows = period_rows.get('month', [])
    recent_imports = db.get_recent_flow_imports(limit=1)
    latest_import = recent_imports[0] if recent_imports else None
    wan_import_status = 'Internet data: none yet'
    wan_import_stale = True
    if latest_import:
        age_minutes = int((now - latest_import.imported_at).total_seconds() // 60)
        wan_import_status = render_wan_import_status(age_minutes)
        wan_import_stale = age_minutes > 15
    last_5_min_mb, last_5_min_mbps = _last_5_min_metrics(last_5_min_wan_rows)

    return DashboardWanData(
        recent_rows=recent_wan_rows,
        today_rows=today_wan_rows,
        seven_day_rows=seven_day_wan_rows,
        month_rows=month_wan_rows,
        last_5_min_totals_by_mac=_wan_totals_by_mac(last_5_min_wan_rows),
        recent_totals_by_mac=_wan_totals_by_mac(recent_wan_rows),
        today_totals_by_mac=_wan_totals_by_mac(today_wan_rows),
        seven_day_totals_by_mac=_wan_totals_by_mac(seven_day_wan_rows),
        month_totals_by_mac=_wan_totals_by_mac(month_wan_rows),
        total_today_mb=_total_wan_mb(today_wan_rows),
        total_last_7_days_mb=_total_wan_mb(seven_day_wan_rows),
        total_calendar_month_mb=_total_wan_mb(month_wan_rows),
        last_5_min_mb=last_5_min_mb,
        last_5_min_mbps=last_5_min_mbps,
        wan_import_status=wan_import_status,
        wan_import_stale=wan_import_stale,
    )


def get_dashboard_wan_data(now: datetime) -> DashboardWanData:
    'Return cached WAN attribution data so changing dashboard windows is fast.'
    global _dashboard_wan_cache
    now_monotonic = monotonic_time.monotonic()
    with _dashboard_wan_cache_lock:
        if _dashboard_wan_cache and _dashboard_wan_cache[0] > now_monotonic:
            return _dashboard_wan_cache[1]

    wan_data = _build_dashboard_wan_data(now)
    with _dashboard_wan_cache_lock:
        _dashboard_wan_cache = (
            monotonic_time.monotonic() + DASHBOARD_WAN_CACHE_SECONDS,
            wan_data,
        )
    return wan_data


def build_rows_for_online_clients(
    active_only: bool = False,
    snapshots: list[ClientSnapshot] | None = None,
    last_5_min_totals_by_mac: dict[str, float] | None = None,
    recent_totals_by_mac: dict[str, float] | None = None,
    today_totals_by_mac: dict[str, float] | None = None,
    seven_day_totals_by_mac: dict[str, float] | None = None,
    month_totals_by_mac: dict[str, float] | None = None,
) -> list[DashboardRow]:
    'Build dashboard rows from live controller client snapshots.'
    def left_half_ip(ip_address: str) -> str:
        parts = ip_address.split('.')
        if len(parts) == 4:
            return '.'.join(parts[:2])
        return ''

    def right_half_ip(ip_address: str) -> str:
        parts = ip_address.split('.')
        if len(parts) == 4:
            return '.'.join(parts[2:])
        return ''

    rows: list[DashboardRow] = []
    source_snapshots = snapshots if snapshots is not None else get_connected_clients()
    last_5_min_totals_by_mac = last_5_min_totals_by_mac or {}
    recent_totals_by_mac = recent_totals_by_mac or {}
    today_totals_by_mac = today_totals_by_mac or {}
    seven_day_totals_by_mac = seven_day_totals_by_mac or {}
    month_totals_by_mac = month_totals_by_mac or {}
    for snapshot in source_snapshots:
        speed_limit = snapshot.effective_speed_limit
        mac_key = snapshot.client.mac.lower()
        last_5_min_mb = last_5_min_totals_by_mac.get(mac_key, 0.0)
        recent_total_mb = recent_totals_by_mac.get(mac_key, 0.0)
        day_total_mb = today_totals_by_mac.get(mac_key, 0.0)
        last_7_days_total_mb = seven_day_totals_by_mac.get(mac_key, 0.0)
        calendar_month_total_mb = month_totals_by_mac.get(mac_key, 0.0)

        row: DashboardRow = {
            'user_id': snapshot.client.user_id or '',
            'name': snapshot.client.name,
            'ap_name': snapshot.client.ap_name or '',
            'ap_count': 1 if snapshot.client.ap_name else 0,
            'ap_breakdown': snapshot.client.ap_name or '',
            'mac': snapshot.client.mac,
            'ip_prefix': left_half_ip(snapshot.client.ip_address),
            'ip_half': right_half_ip(snapshot.client.ip_address),
            'vlan_name': snapshot.client.vlan_name or 'Unknown',
            'frequency_band': snapshot.client.frequency_band,
            'channel': snapshot.client.channel,
            'signal': snapshot.client.signal if snapshot.client.signal else None,
            'recent_activity': [],
            'recent_total_mb': recent_total_mb,
            'last_5_min_mb': last_5_min_mb,
            'last_5_min_mbps': last_5_min_mb * 8.0 / 300.0,
            'connection_duration': '',
            'day_total_mb': day_total_mb,
            'day_cost_cents': calculate_month_cost_cents(day_total_mb),
            'last_7_days_total_mb': last_7_days_total_mb,
            'last_7_days_cost_cents': calculate_month_cost_cents(last_7_days_total_mb),
            'calendar_month_total_mb': calendar_month_total_mb,
            'month_cost_cents': calculate_month_cost_cents(calendar_month_total_mb),
            'speed_limit_name': speed_limit.name if speed_limit else '',
            'speed_limit_up_kbps': speed_limit.up_kbps if speed_limit else None,
            'speed_limit_down_kbps': speed_limit.down_kbps if speed_limit else None,
        }
        rows.append(row)
    if active_only:
        rows = [row for row in rows if row['recent_total_mb'] > 0.0]

    # Sort for operational usefulness: users recently moving WAN data float to top.
    return sorted(
        rows,
        key=lambda entry: (
            -entry['recent_total_mb'],
            -entry['day_total_mb'],
            str(entry['name']).lower(),
            str(entry['mac']).lower(),
        ),
    )


def build_rows_for_historical_window(
    window_name: WindowName,
    speed_limits_by_name: dict[str, SpeedLimit],
    selected_wan_rows: list[db.WanIdentityUsageSummary],
    last_5_min_totals_by_mac: dict[str, float],
    recent_totals_by_mac: dict[str, float],
    today_totals_by_mac: dict[str, float],
    seven_day_totals_by_mac: dict[str, float],
    month_totals_by_mac: dict[str, float],
) -> list[DashboardRow]:
    'Build dashboard rows from WAN identity summaries for non-live windows.'
    metadata_rows = db.get_usage_window_summary(WINDOW_THIS_MONTH)
    metadata_by_mac = {summary.mac.lower(): summary for summary in metadata_rows}
    ap_rollups_by_mac = db.get_usage_window_access_point_minutes(window_name)
    selected_totals_by_mac = _wan_totals_by_mac(selected_wan_rows)
    rows: list[DashboardRow] = []
    seen_macs: set[str] = set()
    for wan_row in selected_wan_rows:
        if not wan_row.mac:
            continue
        mac_key = wan_row.mac.lower()
        if mac_key not in selected_totals_by_mac or mac_key in seen_macs:
            continue
        seen_macs.add(mac_key)
        summary = metadata_by_mac.get(mac_key)
        speed_limit_name = ''
        speed_limit_up_kbps: int | None = None
        speed_limit_down_kbps: int | None = None
        if summary and summary.profile:
            speed_limit_name = summary.profile
            speed_limit = speed_limits_by_name.get(summary.profile)
            if speed_limit is not None:
                speed_limit_name = speed_limit.name
                speed_limit_up_kbps = speed_limit.up_kbps
                speed_limit_down_kbps = speed_limit.down_kbps

        ap_rollups = ap_rollups_by_mac.get(wan_row.mac, [])
        if ap_rollups:
            primary_ap_name = ap_rollups[0][0]
            ap_count = len(ap_rollups)
            ap_breakdown_parts = [f'{ap_name} ({minutes}m)' for ap_name, minutes in ap_rollups[:4]]
            if len(ap_rollups) > 4:
                ap_breakdown_parts.append(f'+{len(ap_rollups) - 4} more')
            ap_breakdown = ', '.join(ap_breakdown_parts)
        else:
            primary_ap_name = (summary.ap_name if summary else '') or ''
            ap_count = 1 if primary_ap_name else 0
            ap_breakdown = primary_ap_name
        last_5_min_mb = last_5_min_totals_by_mac.get(mac_key, 0.0)
        recent_total_mb = recent_totals_by_mac.get(mac_key, 0.0)
        day_total_mb = today_totals_by_mac.get(mac_key, 0.0)
        last_7_days_total_mb = seven_day_totals_by_mac.get(mac_key, 0.0)
        calendar_month_total_mb = month_totals_by_mac.get(mac_key, 0.0)

        row: DashboardRow = {
            'user_id': wan_row.user_id or (summary.user_id if summary else '') or '',
            'name': wan_row.name or (summary.name if summary else '') or wan_row.mac,
            'ap_name': primary_ap_name,
            'ap_count': ap_count,
            'ap_breakdown': ap_breakdown,
            'mac': wan_row.mac,
            'ip_prefix': '',
            'ip_half': '',
            'vlan_name': wan_row.vlan or (summary.vlan if summary else '') or 'Unknown',
            'frequency_band': '',
            'channel': '',
            'signal': None,
            'recent_activity': [],
            'recent_total_mb': recent_total_mb,
            'last_5_min_mb': last_5_min_mb,
            'last_5_min_mbps': last_5_min_mb * 8.0 / 300.0,
            'connection_duration': '',
            'day_total_mb': day_total_mb,
            'day_cost_cents': calculate_month_cost_cents(day_total_mb),
            'last_7_days_total_mb': last_7_days_total_mb,
            'last_7_days_cost_cents': calculate_month_cost_cents(last_7_days_total_mb),
            'calendar_month_total_mb': calendar_month_total_mb,
            'month_cost_cents': calculate_month_cost_cents(calendar_month_total_mb),
            'speed_limit_name': speed_limit_name,
            'speed_limit_up_kbps': speed_limit_up_kbps,
            'speed_limit_down_kbps': speed_limit_down_kbps,
        }
        rows.append(row)

    return sorted(
        rows,
        key=lambda entry: (
            -usage_value_for_window(entry, window_name),
            str(entry['name']).lower(),
            str(entry['mac']).lower(),
        ),
    )


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
    'Attach recent WAN activity series to each dashboard row in-place.'
    if activity_span == ACTIVITY_SPAN_1_HOUR:
        buckets = 12
        bucket_seconds = 300
    elif activity_span == ACTIVITY_SPAN_6_HOUR:
        buckets = 12
        bucket_seconds = 1800
    elif activity_span == ACTIVITY_SPAN_24_HOUR:
        buckets = 24
        bucket_seconds = 3600
    else:
        buckets = 14
        bucket_seconds = 12 * 3600
    macs = [row['mac'] for row in rows if row.get('mac')]
    series_by_mac: dict[str, list[float]] = db.get_wan_activity_series_by_mac(macs, buckets=buckets, bucket_seconds=bucket_seconds)
    default_series = [0.0] * buckets
    for row in rows:
        mac_key = row['mac'].lower()
        series = series_by_mac.get(mac_key, default_series.copy())
        row['recent_activity'] = series


def render_dashboard_window_label(window_name: WindowName, current_month_label: str) -> str:
    'Return a short display label for a dashboard window.'
    if window_name == WINDOW_ACTIVE_NOW:
        return 'Recent Internet'
    if window_name == WINDOW_ONLINE_NOW:
        return 'Online Now'
    if window_name == WINDOW_TODAY:
        return 'Today'
    if window_name == WINDOW_LAST_7_DAYS:
        return '7 Days'
    return current_month_label


def usage_value_for_window(row: DashboardRow, window_name: WindowName) -> float:
    'Return row usage in MB for the selected dashboard window.'
    if window_name in {WINDOW_ACTIVE_NOW, WINDOW_ONLINE_NOW}:
        return float(row['recent_total_mb'] or 0.0)
    if window_name == WINDOW_TODAY:
        return float(row['day_total_mb'] or 0.0)
    if window_name == WINDOW_LAST_7_DAYS:
        return float(row['last_7_days_total_mb'] or 0.0)
    return float(row['calendar_month_total_mb'] or 0.0)


def build_top_consumers_for_window(
    rows: list[DashboardRow],
    window_name: WindowName,
    now: datetime,
    limit: int = 5,
) -> list[TopCurrentConsumer]:
    'Return mode-specific usage/capacity slices for the dashboard pie chart.'
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

    usage_rows = sorted(
        (
            (row, usage_value_for_window(row, window_name))
            for row in rows
            if usage_value_for_window(row, window_name) > 0.0
        ),
        key=lambda entry: (
            entry[1],
            float(entry[0]['day_total_mb'] or 0.0),
            str(entry[0]['name'] or entry[0]['mac']).lower(),
        ),
        reverse=True,
    )
    top_rows = usage_rows[:max(1, limit)]
    base_labels = [
        str(row['user_id'] or row['name'] or row['mac'])
        for row, _ in top_rows
    ]
    duplicate_base_labels = {
        label for label in base_labels
        if sum(1 for candidate in base_labels if candidate == label) > 1
    }
    consumers: list[TopCurrentConsumer] = []
    for (row, usage_mb), base_label in zip(top_rows, base_labels):
        label = base_label
        if base_label in duplicate_base_labels:
            secondary_label = duplicate_suffix(base_label, str(row['name']), str(row['mac']))
            label = f'{base_label} - {secondary_label}'
        consumers.append({
            'label': label,
            'mac': str(row['mac']),
            'interval_mb': usage_mb,
            'slice_type': 'usage',
        })

    total_usage_mb = sum(usage_mb for _, usage_mb in usage_rows)
    top_usage_mb = sum(usage_mb for _, usage_mb in top_rows)
    other_usage_mb = max(0.0, total_usage_mb - top_usage_mb)
    if other_usage_mb > 0.0:
        consumers.append({
            'label': 'Other usage',
            'mac': '',
            'interval_mb': other_usage_mb,
            'slice_type': 'other',
        })

    return consumers


def _build_live_dashboard_rows(
    window_name: WindowName,
    activity_span: ActivitySpan,
    connected_snapshots: list[ClientSnapshot],
    selected_wan_rows: list[db.WanIdentityUsageSummary],
    last_5_min_totals_by_mac: dict[str, float],
    recent_totals_by_mac: dict[str, float],
    today_totals_by_mac: dict[str, float],
    seven_day_totals_by_mac: dict[str, float],
    month_totals_by_mac: dict[str, float],
) -> list[DashboardRow]:
    if window_name == WINDOW_ONLINE_NOW:
        rows = build_rows_for_online_clients(
            snapshots=connected_snapshots,
            last_5_min_totals_by_mac=last_5_min_totals_by_mac,
            recent_totals_by_mac=recent_totals_by_mac,
            today_totals_by_mac=today_totals_by_mac,
            seven_day_totals_by_mac=seven_day_totals_by_mac,
            month_totals_by_mac=month_totals_by_mac,
        )
    elif window_name == WINDOW_ACTIVE_NOW:
        rows = build_rows_for_online_clients(
            active_only=True,
            snapshots=connected_snapshots,
            last_5_min_totals_by_mac=last_5_min_totals_by_mac,
            recent_totals_by_mac=recent_totals_by_mac,
            today_totals_by_mac=today_totals_by_mac,
            seven_day_totals_by_mac=seven_day_totals_by_mac,
            month_totals_by_mac=month_totals_by_mac,
        )
    else:
        speed_limits_by_name = get_speed_limits_by_name_cached()
        rows = build_rows_for_historical_window(
            window_name,
            speed_limits_by_name,
            selected_wan_rows,
            last_5_min_totals_by_mac,
            recent_totals_by_mac,
            today_totals_by_mac,
            seven_day_totals_by_mac,
            month_totals_by_mac,
        )

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
    now = datetime.now()
    current_month_label = render_month_label(now)
    connected_snapshots = get_connected_clients()
    wan_data = get_dashboard_wan_data(now)
    selected_wan_rows = wan_data.recent_rows
    if window_name == WINDOW_TODAY:
        selected_wan_rows = wan_data.today_rows
    elif window_name == WINDOW_LAST_7_DAYS:
        selected_wan_rows = wan_data.seven_day_rows
    elif window_name == WINDOW_THIS_MONTH:
        selected_wan_rows = wan_data.month_rows

    rows = _build_live_dashboard_rows(
        window_name,
        activity_span,
        connected_snapshots,
        selected_wan_rows,
        wan_data.last_5_min_totals_by_mac,
        wan_data.recent_totals_by_mac,
        wan_data.today_totals_by_mac,
        wan_data.seven_day_totals_by_mac,
        wan_data.month_totals_by_mac,
    )
    return {
        'selected_window': window_name,
        'selected_activity_span': activity_span,
        'current_month_label': current_month_label,
        'total_today_mb': wan_data.total_today_mb,
        'total_last_7_days_mb': wan_data.total_last_7_days_mb,
        'total_calendar_month_mb': wan_data.total_calendar_month_mb,
        'last_5_min_mb': wan_data.last_5_min_mb,
        'last_5_min_mbps': wan_data.last_5_min_mbps,
        'wan_import_status': wan_data.wan_import_status,
        'wan_import_stale': wan_data.wan_import_stale,
        'top_consumers_title': f"Usage Share ({render_dashboard_window_label(window_name, current_month_label)})",
        'top_current_consumers': build_top_consumers_for_window(rows, window_name, now),
        'clients': rows,
        'live_update_seconds': live_update_seconds,
        'throttling_enabled': cfg.THROTTLING_ENABLED,
    }


def build_insights_data(
    period_start: datetime | None = None,
    period_end: datetime | None = None,
    current_month_label: str | None = None,
    report_period_label: str | None = None,
    include_live_organization_paid_clients: bool = True,
) -> InsightsData:
    'Assemble data payload for the /insights page.'
    organization_paid_vlan_criteria = set(cfg.ORGANIZATION_PAID_VLAN_NAMES)
    organization_paid_mac_criteria = set(cfg.ORGANIZATION_PAID_DEVICE_MACS)
    organization_paid_user_id_criteria = set(cfg.ORGANIZATION_PAID_USER_IDS)
    organization_paid_vlan_keys = {vlan.strip().lower() for vlan in organization_paid_vlan_criteria if vlan.strip()}
    organization_paid_mac_keys = {mac.strip().lower() for mac in organization_paid_mac_criteria if mac.strip()}
    organization_paid_user_id_keys = {user_id.strip().lower() for user_id in organization_paid_user_id_criteria if user_id.strip()}
    selected_month_label = current_month_label or render_month_label(datetime.now())
    selected_report_label = report_period_label or datetime.now().strftime('%B %Y')
    resolved_period_end = period_end or datetime.now()
    resolved_period_start = period_start or datetime.combine(resolved_period_end.date().replace(day=1), time.min)
    cache_period_end = (
        resolved_period_end.replace(second=0, microsecond=0)
        if include_live_organization_paid_clients
        else resolved_period_end
    )
    cache_key: InsightsCacheKey = (
        resolved_period_start.isoformat(),
        cache_period_end.isoformat(),
        selected_month_label,
        selected_report_label,
        include_live_organization_paid_clients,
    )
    cache_seconds = (
        INSIGHTS_CACHE_SECONDS_CURRENT
        if include_live_organization_paid_clients
        else INSIGHTS_CACHE_SECONDS_HISTORICAL
    )
    now_monotonic = monotonic_time.monotonic()
    with _insights_cache_lock:
        cached = _insights_cache_by_key.get(cache_key)
        if cached and cached[0] > now_monotonic:
            return cached[1]

    def is_organization_paid_identity(mac: str, user_id: str | None, vlan_name: str | None) -> bool:
        return (
            (vlan_name or '').strip().lower() in organization_paid_vlan_keys
            or mac.strip().lower() in organization_paid_mac_keys
            or (user_id or '').strip().lower() in organization_paid_user_id_keys
        )

    insights = db.get_global_month_insights(
        top_limit=10_000,
        period_start=period_start,
        period_end=period_end,
    )
    sampled_users_by_mac_this_month = db.get_global_top_users_current_month(
        limit=10_000,
        period_start=period_start,
        period_end=period_end,
    )
    sampled_metadata_by_mac = {
        row.mac.lower(): row
        for row in sampled_users_by_mac_this_month
        if row.mac
    }
    monthly_wan_rows = db.get_wan_usage_by_identity(
        period_start=resolved_period_start,
        period_end=resolved_period_end,
    )
    user_paid_wan_rows = [
        row
        for row in monthly_wan_rows
        if not is_organization_paid_identity(row.mac, row.user_id, row.vlan)
    ]
    top_users_by_mac_this_month = [
        _global_top_user_from_wan_row(row, sampled_metadata_by_mac)
        for row in user_paid_wan_rows
    ]
    top_users_this_month = aggregate_top_users_by_identity(top_users_by_mac_this_month, limit=6)
    daily_network_usage = db.get_global_daily_network_usage_current_month(
        period_start=period_start,
        period_end=period_end,
    )
    wan_hourly_usage = db.get_global_wan_hourly_usage_current_month(
        period_start=period_start,
        period_end=period_end,
    )
    daily_wan_vlan_usage = db.get_global_daily_wan_usage_by_vlan(
        period_start=resolved_period_start,
        period_end=resolved_period_end,
    )
    payer_split = db.get_global_payer_split_current_month(
        organization_paid_macs=organization_paid_mac_criteria,
        organization_paid_user_ids=organization_paid_user_id_criteria,
        organization_paid_vlan_names=organization_paid_vlan_criteria,
        period_start=period_start,
        period_end=period_end,
    )
    organization_paid_total_mb = 0.0
    user_paid_total_mb = 0.0
    organization_paid_wan_rows: list[db.WanIdentityUsageSummary] = []
    for wan_row in monthly_wan_rows:
        row_total_mb = _wan_total_mb(wan_row)
        if is_organization_paid_identity(wan_row.mac, wan_row.user_id, wan_row.vlan):
            organization_paid_total_mb += row_total_mb
            organization_paid_wan_rows.append(wan_row)
        else:
            user_paid_total_mb += row_total_mb

    organization_paid_sampled_clients = db.get_global_organization_paid_clients_current_month(
        organization_paid_macs=organization_paid_mac_criteria,
        organization_paid_user_ids=organization_paid_user_id_criteria,
        organization_paid_vlan_names=organization_paid_vlan_criteria,
        limit=10_000,
        period_start=period_start,
        period_end=period_end,
    )
    organization_paid_metadata_by_mac = {
        entry.mac.lower(): entry
        for entry in organization_paid_sampled_clients
        if entry.mac
    }
    organization_paid_clients = [
        _global_top_user_from_wan_row(row, organization_paid_metadata_by_mac)
        for row in organization_paid_wan_rows
    ]
    organization_paid_clients = sorted(
        organization_paid_clients,
        key=lambda entry: (entry.total_mb, entry.active_minutes, entry.name.lower()),
        reverse=True,
    )[:12]
    top_access_points_this_month = sorted(
        insights.top_access_points,
        key=lambda entry: (entry.active_minutes, entry.ap_name.lower()),
        reverse=True,
    )[:6]
    concurrency_insights = db.get_global_concurrency_insights_current_month(
        period_start=period_start,
        period_end=period_end,
    )

    result: InsightsData = {
        'current_month_label': selected_month_label,
        'report_period_label': selected_report_label,
        'active_users_terminal_label': 'Active Users Today' if include_live_organization_paid_clients else 'Active Users Final Day',
        'isp_terminal_access_cost_usd': float(cfg.ISP_TERMINAL_ACCESS_COST_USD),
        'isp_included_usage_gb': int(cfg.ISP_INCLUDED_USAGE_GB),
        'isp_included_usage_cost_usd': float(cfg.ISP_INCLUDED_USAGE_COST_USD),
        'isp_topoff_usage_gb': int(cfg.ISP_TOPOFF_USAGE_GB),
        'isp_topoff_cost_usd': float(cfg.ISP_TOPOFF_COST_USD),
        'active_users_daily_min': insights.active_users_min,
        'active_users_daily_mean': insights.active_users_mean,
        'active_users_daily_max': insights.active_users_max,
        'active_users_today': insights.active_users_today,
        'active_users_days_in_period': insights.days_in_period,
        'active_users_chart_x_labels': insights.active_users_daily_x_labels,
        'active_users_chart_full_labels': insights.active_users_daily_full_labels,
        'active_users_chart_counts': insights.active_users_daily_counts,
        'top_users_this_month': [_serialize_usage_actor_row(top_row) for top_row in top_users_this_month],
        'top_access_points_this_month': [_serialize_access_point_row(ap_row) for ap_row in top_access_points_this_month],
        'daily_network_x_labels': [row.usage_day.day for row in daily_network_usage],
        'daily_network_full_labels': [f'{row.usage_day.strftime("%b")} {row.usage_day.day}' for row in daily_network_usage],
        'daily_wan_vlan_labels': [row.vlan for row in daily_wan_vlan_usage],
        'daily_wan_vlan_mb': [row.daily_mb for row in daily_wan_vlan_usage],
        'wan_hourly_title': (
            'Hourly Internet Usage (Month to Date)'
            if include_live_organization_paid_clients
            else f'Hourly Internet Usage ({selected_report_label})'
        ),
        'wan_hourly_labels': [f'{row.bucket_start.day} {row.bucket_start:%H}:00' for row in wan_hourly_usage],
        'wan_hourly_tick_labels': [
            str(row.bucket_start.day) if row.bucket_start.hour == 0 else ''
            for row in wan_hourly_usage
        ],
        'wan_hourly_full_labels': [
            f'{row.bucket_start:%b} {row.bucket_start.day} {row.bucket_start:%H}:00'
            for row in wan_hourly_usage
        ],
        'wan_hourly_mb': [row.total_mb for row in wan_hourly_usage],
        'organization_paid_total_mb': organization_paid_total_mb,
        'organization_paid_cost_cents': calculate_month_cost_cents(organization_paid_total_mb),
        'organization_paid_minutes': payer_split.organization_paid_minutes,
        'user_paid_total_mb': user_paid_total_mb,
        'user_paid_cost_cents': calculate_month_cost_cents(user_paid_total_mb),
        'user_paid_minutes': payer_split.user_paid_minutes,
        'organization_paid_clients': [
            _serialize_usage_actor_row(org_row, include_vlan_name=True)
            for org_row in organization_paid_clients
        ],
        'organization_paid_vlan_criteria': sorted(organization_paid_vlan_criteria),
        'organization_paid_mac_criteria': sorted(organization_paid_mac_criteria),
        'organization_paid_user_id_criteria': sorted(organization_paid_user_id_criteria),
        'peak_concurrency_x_labels': concurrency_insights.daily_x_labels,
        'peak_concurrency_full_labels': concurrency_insights.daily_full_labels,
        'peak_concurrency_counts': concurrency_insights.daily_peak_counts,
        'peak_concurrency_time_labels': concurrency_insights.daily_peak_time_labels,
        'concurrency_heatmap_day_labels': concurrency_insights.heatmap_day_labels,
        'concurrency_heatmap_hour_labels': concurrency_insights.heatmap_hour_labels,
        'concurrency_heatmap_values': concurrency_insights.heatmap_values,
        'concurrency_heatmap_sample_counts': concurrency_insights.heatmap_sample_counts,
    }
    with _insights_cache_lock:
        _insights_cache_by_key[cache_key] = (now_monotonic + cache_seconds, result)
    return result
