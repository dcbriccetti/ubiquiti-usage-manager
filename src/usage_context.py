'''Client usage detail view-model construction.'''

from datetime import datetime, time, timedelta
import time as monotonic_time
from typing import TypedDict

import database as db
import unifi_api as api
from billing import calculate_month_cost_cents
from database import UsageRecord
from monitor import get_connected_clients
from speedlimit import SpeedLimit


SpeedLimitsByName = dict[str, SpeedLimit]
SPEED_LIMIT_CACHE_SECONDS = 300.0
_speed_limits_cache: tuple[float, SpeedLimitsByName] | None = None


class ThrottleChartDataset(TypedDict):
    'One stacked-bar series for monthly throttling chart.'
    label: str
    data: list[int]


class UsageScalePoint(TypedDict):
    'One bucketed usage point (hourly or daily).'
    bucket_label: str
    bucket_value: int
    total_mb: float
    active_minutes: int


class UsageScaleContext(TypedDict):
    'Renderable chart context for one usage scale section.'
    key: str
    title: str
    x_axis_title: str
    mb_axis_title: str
    minutes_axis_title: str
    summary_text: str
    points: list[UsageScalePoint]
    usage_device_series: list[dict[str, object]]
    access_point_labels: list[str]
    access_point_mb_values: list[float]
    access_point_minutes_values: list[int]
    throttle_x_values: list[int]
    throttle_datasets: list[ThrottleChartDataset]


class VoucherUsageContext(TypedDict):
    'Lifetime usage against one active Plus voucher.'
    user_id: int
    allocation_gb: int
    allocation_mb: float
    used_mb: float
    remaining_mb: float
    used_pct: float
    created_at: datetime
    activated_at: datetime | None
    is_over_allocation: bool


class ClientUsageContext(TypedDict):
    'Template context for client-detail and my-usage pages.'
    mac: str
    latest_record: UsageRecord
    usage_history: list[UsageRecord]
    daily_total_mb: float
    last_7_days_total_mb: float
    calendar_month_total_mb: float
    month_cost_cents: float
    wan_client_ip: str
    wan_identity_observed_at: datetime | None
    wan_today_download_mb: float
    wan_today_upload_mb: float
    wan_today_total_mb: float
    wan_month_download_mb: float
    wan_month_upload_mb: float
    wan_month_total_mb: float
    wan_usage_available: bool
    voucher_usage: VoucherUsageContext | None
    usage_scales: list[UsageScaleContext]
    current_month_label: str
    speed_limits_by_name: SpeedLimitsByName


def render_month_label(now: datetime) -> str:
    'Return full month name unless it is long, then use abbreviation.'
    full_label = now.strftime('%B')
    if len(full_label) > 5:
        return now.strftime('%b')
    return full_label


def get_speed_limits_by_name() -> SpeedLimitsByName:
    'Return mapping of speed-limit profile name to SpeedLimit object.'
    global _speed_limits_cache
    now_monotonic = monotonic_time.monotonic()
    if _speed_limits_cache and _speed_limits_cache[0] > now_monotonic:
        return _speed_limits_cache[1]

    speed_limits_by_name = {limit.name: limit for limit in api.get_speed_limits()}
    _speed_limits_cache = (now_monotonic + SPEED_LIMIT_CACHE_SECONDS, speed_limits_by_name)
    return speed_limits_by_name


def speed_limit_option_label(limit: SpeedLimit) -> str:
    'Build select-option label for one speed-limit profile.'
    rendered = str(limit)
    if rendered:
        return rendered
    return f'{limit.name} (Unlimited)'


def profile_display_label(profile_key: str, speed_limits_by_name: SpeedLimitsByName) -> str:
    'Render chart/display label for one stored profile name key.'
    if not profile_key:
        return 'Default'
    if matched_limit := speed_limits_by_name.get(profile_key):
        return speed_limit_option_label(matched_limit)
    return profile_key


def profile_throttling_impact(profile_key: str, speed_limits_by_name: SpeedLimitsByName) -> float:
    'Return throttling-impact score where larger means more restrictive.'
    if not profile_key:
        return -1.0

    matched_limit = speed_limits_by_name.get(profile_key)
    if not matched_limit:
        return -0.5

    caps: list[int] = []
    for cap in (matched_limit.up_kbps, matched_limit.down_kbps):
        if isinstance(cap, int) and cap > 0:
            caps.append(cap)
    if not caps:
        return 0.0

    strictest_cap_kbps: int = min(caps)
    return 1_000_000.0 / float(strictest_cap_kbps)


def build_throttle_datasets(
    bucket_rows: list[tuple[int, dict[str, int]]],
    speed_limits_map: SpeedLimitsByName,
) -> list[ThrottleChartDataset]:
    'Build sorted stacked-profile datasets for one bucketed time scale.'
    totals_by_profile_key: dict[str, int] = {}
    for _, bucket_counts in bucket_rows:
        for profile_key, minutes in bucket_counts.items():
            totals_by_profile_key[profile_key] = totals_by_profile_key.get(profile_key, 0) + minutes

    sorted_profile_keys = sorted(
        totals_by_profile_key.keys(),
        key=lambda key: (
            profile_throttling_impact(key, speed_limits_map),
            totals_by_profile_key[key],
        ),
    )

    return [
        {
            'label': profile_display_label(profile_key, speed_limits_map),
            'data': [bucket_counts.get(profile_key, 0) for _, bucket_counts in bucket_rows],
        }
        for profile_key in sorted_profile_keys
    ]


def summarize_wan_flows(
    flows: list[db.WanMacFlowUsage],
    period_start: datetime,
    period_end: datetime,
) -> tuple[float, float]:
    'Return download/upload MB for attributed MAC flows in one period.'
    download_bytes = 0
    upload_bytes = 0
    for flow in flows:
        if flow.started_at < period_start or flow.started_at > period_end:
            continue
        if flow.direction == 'upload':
            upload_bytes += flow.bytes
        else:
            download_bytes += flow.bytes
    return download_bytes / 1_000_000.0, upload_bytes / 1_000_000.0


def build_wan_flow_bucket_totals(
    flows: list[db.WanMacFlowUsage],
    period_start: datetime,
    period_end: datetime,
    bucket: str,
) -> dict[int, float]:
    'Return WAN-attributed MB totals by day or hour bucket.'
    totals_by_bucket: dict[int, float] = {}
    for flow in flows:
        if flow.started_at < period_start or flow.started_at > period_end:
            continue
        bucket_value = flow.started_at.day if bucket == 'day' else flow.started_at.hour
        totals_by_bucket[bucket_value] = totals_by_bucket.get(bucket_value, 0.0) + flow.bytes / 1_000_000.0
    return totals_by_bucket


def summarize_wan_flows_for_voucher(
    voucher: db.PlusVoucherRecord,
    flows: list[db.WanMacFlowUsage],
) -> tuple[datetime | None, float]:
    'Return first flow and MB for current-device WAN usage after voucher creation.'
    voucher_flows = [flow for flow in flows if flow.started_at >= voucher.generated_at]
    if not voucher_flows:
        return None, 0.0
    first_usage_at = min(flow.started_at for flow in voucher_flows)
    total_mb = sum(flow.bytes for flow in voucher_flows) / 1_000_000.0
    return first_usage_at, total_mb


def build_voucher_usage_context(
    user_id: str | None,
    mac: str | None = None,
    voucher: db.PlusVoucherRecord | None = None,
    mac_wan_flows: list[db.WanMacFlowUsage] | None = None,
) -> VoucherUsageContext | None:
    'Return remaining lifetime voucher allocation for one RADIUS user ID.'
    voucher = voucher or db.get_active_plus_voucher_for_user_id(user_id)
    if voucher is None:
        return None

    allocation_mb = float(voucher.allocation_gb * 1000)
    activated_at, used_mb = db.get_plus_voucher_usage_summary(voucher)
    if mac_wan_flows is not None:
        mac_activated_at, mac_used_mb = summarize_wan_flows_for_voucher(voucher, mac_wan_flows)
        if mac_used_mb > used_mb:
            activated_at = mac_activated_at
            used_mb = mac_used_mb
    elif mac:
        mac_activated_at, mac_used_mb = db.get_wan_usage_summary_for_mac(
            mac,
            period_start=voucher.generated_at,
        )
        if mac_used_mb > used_mb:
            activated_at = mac_activated_at
            used_mb = mac_used_mb
    remaining_mb = max(0.0, allocation_mb - used_mb)
    used_pct = (used_mb / allocation_mb * 100.0) if allocation_mb else 0.0
    voucher_context: VoucherUsageContext = {
        'user_id': voucher.user_id,
        'allocation_gb': voucher.allocation_gb,
        'allocation_mb': allocation_mb,
        'used_mb': used_mb,
        'remaining_mb': remaining_mb,
        'used_pct': used_pct,
        'created_at': voucher.generated_at,
        'activated_at': activated_at,
        'is_over_allocation': used_mb >= allocation_mb,
    }
    return voucher_context


def hydrate_usage_record_identity(
    latest_record: UsageRecord,
    latest_ip_identity: db.ClientIpIdentityRecord | None,
    wan_rows: list[db.WanIdentityUsageSummary],
    mac: str,
) -> None:
    'Fill missing client identity fields from WAN identity observations.'
    target_mac = mac.lower()
    identity_user_id = latest_ip_identity.user_id if latest_ip_identity else ''
    identity_vlan = latest_ip_identity.vlan if latest_ip_identity else ''
    identity_name = latest_ip_identity.name if latest_ip_identity else ''

    for row in wan_rows:
        if row.mac.lower() != target_mac:
            continue
        identity_user_id = identity_user_id or row.user_id
        identity_vlan = identity_vlan or row.vlan
        identity_name = identity_name or row.name
        if identity_user_id and identity_vlan and identity_name:
            break

    if not latest_record.user_id and identity_user_id:
        latest_record.user_id = identity_user_id
    if not latest_record.vlan and identity_vlan:
        latest_record.vlan = identity_vlan
    if not latest_record.name and identity_name:
        latest_record.name = identity_name


def needs_identity_hydration(latest_record: UsageRecord) -> bool:
    'Return True when the display/client record still lacks identity fields.'
    return not (
        latest_record.user_id
        and latest_record.vlan
        and latest_record.name
    )


def merge_wan_totals_into_usage_points(
    points: list[UsageScalePoint],
    wan_totals_by_bucket: dict[int, float],
) -> list[UsageScalePoint]:
    'Prefer WAN flow totals for chart buckets when they exceed sampled usage.'
    merged_points: list[UsageScalePoint] = []
    for point in points:
        merged_point: UsageScalePoint = {
            'bucket_label': point['bucket_label'],
            'bucket_value': point['bucket_value'],
            'total_mb': max(point['total_mb'], wan_totals_by_bucket.get(point['bucket_value'], 0.0)),
            'active_minutes': point['active_minutes'],
        }
        merged_points.append(merged_point)
    return merged_points


def get_client_usage_context(mac: str) -> ClientUsageContext:
    'Build shared usage/detail context used by both admin and self-service pages.'
    if usage_history := db.get_usage_history(mac):
        latest_record = usage_history[0]
    else:
        if (live_snapshot := next(
            (
                snapshot
                for snapshot in get_connected_clients()
                if snapshot.client.mac.lower() == mac.lower()
            ),
            None,
        )) is None:
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

    speed_limits_by_name = get_speed_limits_by_name()
    now = datetime.now()
    current_month_label = render_month_label(now)
    latest_ip_identity = db.get_latest_client_identity_by_mac(mac)
    wan_client_ip = latest_ip_identity.ip_address if latest_ip_identity else ''
    today_start = datetime.combine(now.date(), time.min)
    seven_days_ago = now - timedelta(days=7)
    month_start = datetime.combine(now.date().replace(day=1), time.min)
    month_wan_rows: list[db.WanIdentityUsageSummary] = []
    hydrate_usage_record_identity(latest_record, latest_ip_identity, month_wan_rows, mac)
    if needs_identity_hydration(latest_record):
        month_wan_rows = db.get_wan_usage_by_identity(period_start=month_start, period_end=now)
        hydrate_usage_record_identity(latest_record, latest_ip_identity, month_wan_rows, mac)

    voucher = db.get_active_plus_voucher_for_user_id(latest_record.user_id)
    wan_flow_start = min(month_start, voucher.generated_at) if voucher else month_start
    mac_wan_flows = db.get_wan_flow_rows_for_mac(mac, wan_flow_start, now)
    wan_today_download_mb, wan_today_upload_mb = summarize_wan_flows(
        mac_wan_flows,
        today_start,
        now,
    )
    wan_last_7_days_download_mb, wan_last_7_days_upload_mb = summarize_wan_flows(
        mac_wan_flows,
        seven_days_ago,
        now,
    )
    wan_month_download_mb, wan_month_upload_mb = summarize_wan_flows(
        mac_wan_flows,
        month_start,
        now,
    )
    wan_today_total_mb = wan_today_download_mb + wan_today_upload_mb
    wan_last_7_days_total_mb = wan_last_7_days_download_mb + wan_last_7_days_upload_mb
    wan_month_total_mb = wan_month_download_mb + wan_month_upload_mb
    wan_usage_available = bool(
        wan_client_ip
        or wan_today_download_mb
        or wan_today_upload_mb
        or wan_month_download_mb
        or wan_month_upload_mb
    )
    legacy_daily_total_mb = db.get_daily_total(mac)
    legacy_last_7_days_total_mb = db.get_last_7_days_total(mac)
    legacy_calendar_month_total_mb = db.get_calendar_month_total(mac)
    daily_total_mb = max(legacy_daily_total_mb, wan_today_total_mb)
    last_7_days_total_mb = max(legacy_last_7_days_total_mb, wan_last_7_days_total_mb)
    calendar_month_total_mb = max(legacy_calendar_month_total_mb, wan_month_total_mb)
    month_daily_usage: list[UsageScalePoint] = [
        {
            'bucket_label': f'{usage_day.strftime("%b")} {usage_day.day}',
            'bucket_value': usage_day.day,
            'total_mb': total_mb,
            'active_minutes': active_minutes,
        }
        for usage_day, total_mb, active_minutes in db.get_calendar_month_daily_totals(mac)
    ]
    month_wan_totals_by_day = build_wan_flow_bucket_totals(mac_wan_flows, month_start, now, 'day')
    month_daily_usage = merge_wan_totals_into_usage_points(month_daily_usage, month_wan_totals_by_day)
    month_throttle_rows = [
        (usage_day.day, daily_counts)
        for usage_day, daily_counts in db.get_calendar_month_daily_profile_minutes(mac)
    ]
    month_throttle_datasets = build_throttle_datasets(month_throttle_rows, speed_limits_by_name)

    daily_hourly_usage: list[UsageScalePoint] = [
        {
            'bucket_label': f'{hour:02d}:00',
            'bucket_value': hour,
            'total_mb': total_mb,
            'active_minutes': active_minutes,
        }
        for hour, total_mb, active_minutes in db.get_today_hourly_totals(mac)
    ]
    daily_hourly_usage = merge_wan_totals_into_usage_points(
        daily_hourly_usage,
        build_wan_flow_bucket_totals(mac_wan_flows, today_start, now, 'hour'),
    )
    daily_throttle_rows = db.get_today_hourly_profile_minutes(mac)
    daily_throttle_datasets = build_throttle_datasets(daily_throttle_rows, speed_limits_by_name)
    daily_access_points = db.get_today_access_point_totals(mac)
    monthly_access_points = db.get_calendar_month_access_point_totals(mac)

    usage_scales: list[UsageScaleContext] = [
        {
            'key': 'daily',
            'title': f'Usage Today ({now.strftime("%b")} {now.day})',
            'x_axis_title': 'Hour of day',
            'mb_axis_title': 'MB/hour',
            'minutes_axis_title': 'minutes/hour',
            'summary_text': 'Top chart: MB/hour. Bottom chart: active minutes/hour stacked by speed-limit profile.',
            'points': daily_hourly_usage,
            'usage_device_series': [
                {
                    'label': '',
                    'data': [point['total_mb'] for point in daily_hourly_usage],
                }
            ],
            'access_point_labels': [ap_name for ap_name, _, _ in daily_access_points],
            'access_point_mb_values': [total_mb for _, total_mb, _ in daily_access_points],
            'access_point_minutes_values': [active_minutes for _, _, active_minutes in daily_access_points],
            'throttle_x_values': [hour for hour, _ in daily_throttle_rows],
            'throttle_datasets': daily_throttle_datasets,
        },
        {
            'key': 'monthly',
            'title': f'{current_month_label} Usage',
            'x_axis_title': 'Day of month',
            'mb_axis_title': 'MB/day',
            'minutes_axis_title': 'minutes/day',
            'summary_text': 'Top chart: MB/day. Bottom chart: active minutes/day stacked by speed-limit profile.',
            'points': month_daily_usage,
            'usage_device_series': [
                {
                    'label': '',
                    'data': [point['total_mb'] for point in month_daily_usage],
                }
            ],
            'access_point_labels': [ap_name for ap_name, _, _ in monthly_access_points],
            'access_point_mb_values': [total_mb for _, total_mb, _ in monthly_access_points],
            'access_point_minutes_values': [active_minutes for _, _, active_minutes in monthly_access_points],
            'throttle_x_values': [usage_day for usage_day, _ in month_throttle_rows],
            'throttle_datasets': month_throttle_datasets,
        },
    ]

    return {
        'mac': mac,
        'latest_record': latest_record,
        'usage_history': usage_history,
        'daily_total_mb': daily_total_mb,
        'last_7_days_total_mb': last_7_days_total_mb,
        'calendar_month_total_mb': calendar_month_total_mb,
        'month_cost_cents': calculate_month_cost_cents(calendar_month_total_mb),
        'wan_client_ip': wan_client_ip,
        'wan_usage_available': wan_usage_available,
        'wan_identity_observed_at': latest_ip_identity.observed_at if latest_ip_identity else None,
        'wan_today_download_mb': wan_today_download_mb,
        'wan_today_upload_mb': wan_today_upload_mb,
        'wan_today_total_mb': wan_today_total_mb,
        'wan_month_download_mb': wan_month_download_mb,
        'wan_month_upload_mb': wan_month_upload_mb,
        'wan_month_total_mb': wan_month_total_mb,
        'voucher_usage': build_voucher_usage_context(
            latest_record.user_id,
            mac,
            voucher=voucher,
            mac_wan_flows=mac_wan_flows,
        ),
        'usage_scales': usage_scales,
        'current_month_label': current_month_label,
        'speed_limits_by_name': speed_limits_by_name,
    }
