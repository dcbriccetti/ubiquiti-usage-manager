'''Client usage detail view-model construction.'''

from dataclasses import dataclass
from datetime import datetime, time, timedelta
import time as monotonic_time
from typing import TypedDict

import database as db
import unifi_api as api
from billing import calculate_month_cost_cents
from database import UsageRecord
from monitor import get_connected_clients
from reverse_dns import resolve_host_labels
from speedlimit import SpeedLimit


SpeedLimitsByName = dict[str, SpeedLimit]
SPEED_LIMIT_CACHE_SECONDS = 300.0
_speed_limits_cache: tuple[float, SpeedLimitsByName] | None = None
ACCESS_MODE_LABELS = {
    'basic': 'Basic',
    'plus_paid': 'Plus without voucher',
    'plus_voucher': 'Plus with voucher',
    'unclassified': 'Unclassified',
}
ACCESS_MODE_NOTES = {
    'basic': 'Included access',
    'plus_paid': 'Charged at configured Plus rate',
    'plus_voucher': 'Counts against prepaid voucher allocation',
    'unclassified': 'Missing flow-time identity',
}
ACCESS_MODE_ORDER = ('basic', 'plus_paid', 'plus_voucher', 'unclassified')
SERVICE_LABEL_BY_PROTO_PORT = {
    ('TCP', 80): 'Web browsing',
    ('TCP', 443): 'Secure web and apps',
    ('UDP', 443): 'QUIC web and streaming',
    ('UDP', 53): 'DNS lookups',
    ('TCP', 53): 'DNS lookups',
    ('UDP', 123): 'Time sync',
    ('TCP', 25): 'Email sending',
    ('TCP', 465): 'Email sending',
    ('TCP', 587): 'Email sending',
    ('TCP', 993): 'Email reading',
    ('TCP', 995): 'Email reading',
    ('TCP', 22): 'SSH / remote login',
    ('TCP', 3389): 'Remote desktop',
    ('UDP', 500): 'VPN',
    ('UDP', 4500): 'VPN',
    ('UDP', 51820): 'WireGuard VPN',
}


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
    wan_direction_labels: list[str]
    wan_direction_mb_values: list[float]
    access_point_labels: list[str]
    access_point_mb_values: list[float]
    access_point_minutes_values: list[int]
    throttle_x_values: list[int]
    throttle_datasets: list[ThrottleChartDataset]
    show_access_point_activity: bool


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


class WanImportUsageContext(TypedDict):
    'WAN usage attributed to the client from one imported capture.'
    source_file: str
    source_label: str
    imported_label: str
    flow_window_label: str
    imported_at: datetime | None
    first_flow_at: datetime
    last_flow_at: datetime
    download_mb: float
    upload_mb: float
    total_mb: float
    flow_count: int


class AccessModeUsageContext(TypedDict):
    'WAN usage attributed to one client access mode.'
    key: str
    label: str
    note: str
    today_mb: float
    last_7_days_mb: float
    month_mb: float
    month_cost_cents: float


class FlowActivityContext(TypedDict):
    'Client WAN usage grouped by likely network activity.'
    label: str
    detail: str
    download_mb: float
    upload_mb: float
    total_mb: float
    pct: float
    flow_count: int


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
    wan_import_usage_rows: list[WanImportUsageContext]
    access_mode_usage_rows: list[AccessModeUsageContext]
    flow_activity_rows: list[FlowActivityContext]
    voucher_usage: VoucherUsageContext | None
    usage_scales: list[UsageScaleContext]
    current_month_label: str
    speed_limits_by_name: SpeedLimitsByName


@dataclass
class WanImportUsageAccumulator:
    'Mutable accumulator for one client/source-file WAN import row.'
    source_file: str
    first_flow_at: datetime
    last_flow_at: datetime
    download_bytes: int = 0
    upload_bytes: int = 0
    flow_count: int = 0


@dataclass
class FlowActivityAccumulator:
    'Mutable accumulator for one protocol/port activity row.'
    label: str
    download_bytes: int = 0
    upload_bytes: int = 0
    flow_count: int = 0
    endpoint_bytes: dict[str, int] | None = None


def wan_flow_usage_at(flow: db.WanMacFlowUsage | db.WanMacIdentityFlowUsage) -> datetime:
    'Return the timestamp used to place WAN bytes in reporting buckets.'
    return flow.ended_at


def render_datetime_label(value: datetime) -> str:
    'Return a compact, user-facing timestamp label.'
    return f'{value.strftime("%b")} {value.day}, {value.strftime("%H:%M")}'


def render_time_range_label(start: datetime, end: datetime) -> str:
    'Return a compact datetime range, eliding the ending date when unchanged.'
    start_label = render_datetime_label(start)
    if start == end:
        return start_label
    if start.date() == end.date():
        return f'{start_label}–{end.strftime("%H:%M")}'
    return f'{start_label}–{render_datetime_label(end)}'


def render_capture_source_label(source_file: str) -> str:
    'Return a readable nfcapd capture label when possible.'
    source_name = source_file.rsplit('/', 1)[-1]
    if source_name.startswith('nfcapd.') and len(source_name) == 19:
        try:
            captured_at = datetime.strptime(source_name.removeprefix('nfcapd.'), '%Y%m%d%H%M')
        except ValueError:
            return source_name
        return f'Capture {render_datetime_label(captured_at)}'
    return source_name


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


def build_active_minutes_datasets(bucket_rows: list[tuple[int, dict[str, int]]]) -> list[ThrottleChartDataset]:
    'Build sorted stacked active-minute datasets for one bucketed time scale.'
    totals_by_label: dict[str, int] = {}
    for _, bucket_counts in bucket_rows:
        for label, minutes in bucket_counts.items():
            totals_by_label[label] = totals_by_label.get(label, 0) + minutes

    sorted_labels = sorted(
        totals_by_label,
        key=lambda label: (totals_by_label[label], label.lower()),
        reverse=True,
    )
    return [
        {
            'label': label,
            'data': [bucket_counts.get(label, 0) for _, bucket_counts in bucket_rows],
        }
        for label in sorted_labels
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
        usage_at = wan_flow_usage_at(flow)
        if usage_at < period_start or usage_at > period_end:
            continue
        if flow.direction == 'upload':
            upload_bytes += flow.bytes
        else:
            download_bytes += flow.bytes
    return download_bytes / 1_000_000.0, upload_bytes / 1_000_000.0


def build_wan_import_usage_context(
    flows: list[db.WanMacFlowUsage],
    period_start: datetime,
    period_end: datetime,
    limit: int = 40,
) -> list[WanImportUsageContext]:
    'Return client-detail rows grouped by non-zero WAN capture import.'
    summaries_by_source: dict[str, WanImportUsageAccumulator] = {}
    for flow in flows:
        usage_at = wan_flow_usage_at(flow)
        if usage_at < period_start or usage_at > period_end or flow.bytes <= 0:
            continue

        summary = summaries_by_source.get(flow.source_file)
        if summary is None:
            summary = WanImportUsageAccumulator(
                source_file=flow.source_file,
                first_flow_at=flow.started_at,
                last_flow_at=flow.ended_at,
            )
            summaries_by_source[flow.source_file] = summary

        summary.first_flow_at = min(summary.first_flow_at, flow.started_at)
        summary.last_flow_at = max(summary.last_flow_at, flow.ended_at)
        if flow.direction == 'upload':
            summary.upload_bytes += flow.bytes
        else:
            summary.download_bytes += flow.bytes
        summary.flow_count += 1

    imported_at_by_source = db.get_flow_import_times_by_source_file(set(summaries_by_source))
    rows: list[WanImportUsageContext] = []
    for summary in summaries_by_source.values():
        total_bytes = summary.download_bytes + summary.upload_bytes
        if total_bytes <= 0:
            continue
        rows.append(
            {
                'source_file': summary.source_file,
                'source_label': render_capture_source_label(summary.source_file),
                'imported_label': (
                    render_datetime_label(imported_at_by_source[summary.source_file])
                    if summary.source_file in imported_at_by_source
                    else 'Not recorded'
                ),
                'flow_window_label': render_time_range_label(summary.first_flow_at, summary.last_flow_at),
                'imported_at': imported_at_by_source.get(summary.source_file),
                'first_flow_at': summary.first_flow_at,
                'last_flow_at': summary.last_flow_at,
                'download_mb': summary.download_bytes / 1_000_000.0,
                'upload_mb': summary.upload_bytes / 1_000_000.0,
                'total_mb': total_bytes / 1_000_000.0,
                'flow_count': summary.flow_count,
            }
        )

    return sorted(
        rows,
        key=lambda row: (row['imported_at'] or row['last_flow_at'], row['last_flow_at'], row['source_file']),
        reverse=True,
    )[:max(1, limit)]


def build_wan_flow_bucket_totals(
    flows: list[db.WanMacFlowUsage],
    period_start: datetime,
    period_end: datetime,
    bucket: str,
) -> dict[int, float]:
    'Return WAN-attributed MB totals by day or hour bucket.'
    totals_by_bucket: dict[int, float] = {}
    for flow in flows:
        usage_at = wan_flow_usage_at(flow)
        if usage_at < period_start or usage_at > period_end:
            continue
        bucket_value = usage_at.day if bucket == 'day' else usage_at.hour
        totals_by_bucket[bucket_value] = totals_by_bucket.get(bucket_value, 0.0) + flow.bytes / 1_000_000.0
    return totals_by_bucket


def build_wan_flow_direction_series(
    flows: list[db.WanMacFlowUsage],
    period_start: datetime,
    period_end: datetime,
    bucket: str,
    bucket_values: list[int],
) -> list[dict[str, object]]:
    'Return download/upload MB series aligned to rendered client chart buckets.'
    download_totals: dict[int, float] = {}
    upload_totals: dict[int, float] = {}
    for flow in flows:
        usage_at = wan_flow_usage_at(flow)
        if usage_at < period_start or usage_at > period_end:
            continue
        bucket_value = usage_at.day if bucket == 'day' else usage_at.hour
        totals = upload_totals if flow.direction == 'upload' else download_totals
        totals[bucket_value] = totals.get(bucket_value, 0.0) + flow.bytes / 1_000_000.0

    return [
        {
            'label': 'Down',
            'data': [download_totals.get(bucket_value, 0.0) for bucket_value in bucket_values],
        },
        {
            'label': 'Up',
            'data': [upload_totals.get(bucket_value, 0.0) for bucket_value in bucket_values],
        },
    ]


def access_mode_key_for_flow(
    flow: db.WanMacIdentityFlowUsage,
    vouchers_by_user_id: dict[str, db.PlusVoucherRecord],
) -> str:
    'Classify one attributed WAN flow by its access/payment mode.'
    user_id = flow.user_id.strip()
    voucher = vouchers_by_user_id.get(user_id)
    if voucher is not None and flow.started_at >= voucher.generated_at:
        return 'plus_voucher'

    vlan_key = flow.vlan.strip().lower()
    if vlan_key == 'basic':
        return 'basic'
    if vlan_key == 'plus':
        return 'plus_paid'
    return 'unclassified'


def remote_endpoint_for_flow(flow: db.WanMacIdentityFlowUsage) -> tuple[str, int | None]:
    'Return the external endpoint IP and service-side port for an attributed flow.'
    if flow.src_ip == flow.client_ip:
        return flow.dst_ip, flow.dst_port
    return flow.src_ip, flow.src_port


def service_label_for_flow(proto: str, port: int | None) -> str:
    'Return a conservative user-facing label for protocol/port traffic.'
    normalized_proto = proto.strip().upper()
    if port is None:
        return normalized_proto or 'Unknown traffic'
    label = SERVICE_LABEL_BY_PROTO_PORT.get((normalized_proto, port))
    if label:
        return label
    if normalized_proto == 'TCP':
        return f'TCP/{port}'
    if normalized_proto == 'UDP':
        return f'UDP/{port}'
    return f'{normalized_proto}/{port}' if normalized_proto else f'Port {port}'


def build_flow_activity_context(
    flows: list[db.WanMacIdentityFlowUsage],
    period_start: datetime,
    period_end: datetime,
    limit: int = 8,
) -> list[FlowActivityContext]:
    'Return top client WAN activity groups for one period.'
    activity_by_key: dict[tuple[str, int | None], FlowActivityAccumulator] = {}
    total_bytes = 0
    for flow in flows:
        usage_at = wan_flow_usage_at(flow)
        if usage_at < period_start or usage_at > period_end or flow.bytes <= 0:
            continue

        remote_ip, service_port = remote_endpoint_for_flow(flow)
        proto = flow.proto.strip().upper()
        key = (proto, service_port)
        activity = activity_by_key.setdefault(
            key,
            FlowActivityAccumulator(label=service_label_for_flow(proto, service_port), endpoint_bytes={}),
        )
        if flow.direction == 'upload':
            activity.upload_bytes += flow.bytes
        else:
            activity.download_bytes += flow.bytes
        activity.flow_count += 1
        endpoint_bytes = activity.endpoint_bytes or {}
        endpoint_bytes[remote_ip] = endpoint_bytes.get(remote_ip, 0) + flow.bytes
        activity.endpoint_bytes = endpoint_bytes
        total_bytes += flow.bytes

    rows: list[FlowActivityContext] = []
    for activity in activity_by_key.values():
        download_bytes = activity.download_bytes
        upload_bytes = activity.upload_bytes
        activity_total_bytes = download_bytes + upload_bytes
        endpoint_bytes = activity.endpoint_bytes or {}
        top_endpoints = sorted(endpoint_bytes.items(), key=lambda item: item[1], reverse=True)[:3]
        host_labels = resolve_host_labels([endpoint for endpoint, _ in top_endpoints])
        rendered_endpoints = [
            f'{host_labels[endpoint]} ({endpoint})' if endpoint in host_labels else endpoint
            for endpoint, _ in top_endpoints
            if endpoint
        ]
        endpoint_text = ', '.join(rendered_endpoints)
        if len(endpoint_bytes) > len(top_endpoints):
            endpoint_text = f'{endpoint_text}, +{len(endpoint_bytes) - len(top_endpoints)} more'
        rows.append(
            {
                'label': activity.label,
                'detail': endpoint_text or 'No remote endpoint',
                'download_mb': download_bytes / 1_000_000.0,
                'upload_mb': upload_bytes / 1_000_000.0,
                'total_mb': activity_total_bytes / 1_000_000.0,
                'pct': (activity_total_bytes / total_bytes * 100.0) if total_bytes else 0.0,
                'flow_count': activity.flow_count,
            }
        )

    return sorted(rows, key=lambda row: row['total_mb'], reverse=True)[:max(1, limit)]


def build_access_mode_usage_context(
    flows: list[db.WanMacIdentityFlowUsage],
    vouchers_by_user_id: dict[str, db.PlusVoucherRecord],
    today_start: datetime,
    seven_days_ago: datetime,
) -> list[AccessModeUsageContext]:
    'Return client WAN usage split by Basic, paid Plus, and voucher Plus.'
    totals_by_mode = {
        key: {'today_mb': 0.0, 'last_7_days_mb': 0.0, 'month_mb': 0.0}
        for key in ACCESS_MODE_ORDER
    }

    for flow in flows:
        mode_key = access_mode_key_for_flow(flow, vouchers_by_user_id)
        usage_at = wan_flow_usage_at(flow)
        total_mb = flow.bytes / 1_000_000.0
        totals_by_mode[mode_key]['month_mb'] += total_mb
        if usage_at >= seven_days_ago:
            totals_by_mode[mode_key]['last_7_days_mb'] += total_mb
        if usage_at >= today_start:
            totals_by_mode[mode_key]['today_mb'] += total_mb

    rows: list[AccessModeUsageContext] = []
    for mode_key in ACCESS_MODE_ORDER:
        totals = totals_by_mode[mode_key]
        if (
            mode_key == 'unclassified'
            and not totals['today_mb']
            and not totals['last_7_days_mb']
            and not totals['month_mb']
        ):
            continue
        month_cost_cents = (
            calculate_month_cost_cents(totals['month_mb'])
            if mode_key == 'plus_paid'
            else 0.0
        )
        rows.append(
            {
                'key': mode_key,
                'label': ACCESS_MODE_LABELS[mode_key],
                'note': ACCESS_MODE_NOTES[mode_key],
                'today_mb': totals['today_mb'],
                'last_7_days_mb': totals['last_7_days_mb'],
                'month_mb': totals['month_mb'],
                'month_cost_cents': month_cost_cents,
            }
        )
    return rows


def build_voucher_usage_context(
    user_id: str | None,
    voucher: db.PlusVoucherRecord | None = None,
) -> VoucherUsageContext | None:
    'Return remaining lifetime voucher allocation for one RADIUS user ID.'
    voucher = voucher or db.get_active_plus_voucher_for_user_id(user_id)
    if voucher is None:
        return None

    allocation_mb = float(voucher.allocation_gb * 1000)
    activated_at, used_mb = db.get_plus_voucher_usage_summary(voucher)
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


def has_wireless_access_point(latest_record: UsageRecord) -> bool:
    'Return True when the latest client state is associated with a Wi-Fi AP.'
    ap_name = latest_record.ap_name or ''
    return bool(ap_name.strip())


def merge_wan_totals_into_usage_points(
    points: list[UsageScalePoint],
    wan_totals_by_bucket: dict[int, float],
) -> list[UsageScalePoint]:
    'Attach WAN flow totals to chart buckets while preserving sampled active minutes.'
    merged_points: list[UsageScalePoint] = []
    for point in points:
        merged_point: UsageScalePoint = {
            'bucket_label': point['bucket_label'],
            'bucket_value': point['bucket_value'],
            'total_mb': wan_totals_by_bucket.get(point['bucket_value'], 0.0),
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

    speed_limits_by_name: SpeedLimitsByName = {}
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
    mac_identity_wan_flows = db.get_wan_identity_flow_rows_for_mac(mac, month_start, now)
    mac_wan_flows = [
        db.WanMacFlowUsage(
            source_file=flow.source_file,
            started_at=flow.started_at,
            ended_at=flow.ended_at,
            bytes=flow.bytes,
            direction=flow.direction,
        )
        for flow in mac_identity_wan_flows
    ]
    vouchers_by_user_id = db.get_active_plus_vouchers_by_user_id(
        {flow.user_id for flow in mac_identity_wan_flows}
    )
    access_mode_usage_rows = build_access_mode_usage_context(
        mac_identity_wan_flows,
        vouchers_by_user_id,
        today_start,
        seven_days_ago,
    )
    flow_activity_rows = build_flow_activity_context(mac_identity_wan_flows, month_start, now)
    paid_plus_month_mb = next(
        (row['month_mb'] for row in access_mode_usage_rows if row['key'] == 'plus_paid'),
        0.0,
    )
    wan_import_usage_rows = build_wan_import_usage_context(mac_wan_flows, month_start, now)
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
    daily_total_mb = wan_today_total_mb
    last_7_days_total_mb = wan_last_7_days_total_mb
    calendar_month_total_mb = wan_month_total_mb
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
    daily_hour_values = [point['bucket_value'] for point in daily_hourly_usage]
    month_day_values = [point['bucket_value'] for point in month_daily_usage]

    usage_scales: list[UsageScaleContext] = [
        {
            'key': 'daily',
            'title': f'Usage Today ({now.strftime("%b")} {now.day})',
            'x_axis_title': 'Hour of day',
            'mb_axis_title': 'MB/hour',
            'minutes_axis_title': 'minutes/hour',
            'summary_text': 'Chart: attributed WAN MB/hour stacked by down/up direction.',
            'points': daily_hourly_usage,
            'usage_device_series': build_wan_flow_direction_series(
                mac_wan_flows,
                today_start,
                now,
                'hour',
                daily_hour_values,
            ),
            'wan_direction_labels': ['Down', 'Up'],
            'wan_direction_mb_values': [wan_today_download_mb, wan_today_upload_mb],
            'access_point_labels': [],
            'access_point_mb_values': [],
            'access_point_minutes_values': [],
            'throttle_x_values': daily_hour_values,
            'throttle_datasets': [],
            'show_access_point_activity': False,
        },
        {
            'key': 'monthly',
            'title': f'{current_month_label} Usage',
            'x_axis_title': 'Day of month',
            'mb_axis_title': 'MB/day',
            'minutes_axis_title': 'minutes/day',
            'summary_text': 'Chart: attributed WAN MB/day stacked by down/up direction.',
            'points': month_daily_usage,
            'usage_device_series': build_wan_flow_direction_series(
                mac_wan_flows,
                month_start,
                now,
                'day',
                month_day_values,
            ),
            'wan_direction_labels': ['Down', 'Up'],
            'wan_direction_mb_values': [wan_month_download_mb, wan_month_upload_mb],
            'access_point_labels': [],
            'access_point_mb_values': [],
            'access_point_minutes_values': [],
            'throttle_x_values': month_day_values,
            'throttle_datasets': [],
            'show_access_point_activity': False,
        },
    ]

    return {
        'mac': mac,
        'latest_record': latest_record,
        'usage_history': usage_history,
        'daily_total_mb': daily_total_mb,
        'last_7_days_total_mb': last_7_days_total_mb,
        'calendar_month_total_mb': calendar_month_total_mb,
        'month_cost_cents': calculate_month_cost_cents(paid_plus_month_mb),
        'wan_client_ip': wan_client_ip,
        'wan_usage_available': wan_usage_available,
        'wan_identity_observed_at': latest_ip_identity.observed_at if latest_ip_identity else None,
        'wan_today_download_mb': wan_today_download_mb,
        'wan_today_upload_mb': wan_today_upload_mb,
        'wan_today_total_mb': wan_today_total_mb,
        'wan_month_download_mb': wan_month_download_mb,
        'wan_month_upload_mb': wan_month_upload_mb,
        'wan_month_total_mb': wan_month_total_mb,
        'wan_import_usage_rows': wan_import_usage_rows,
        'access_mode_usage_rows': access_mode_usage_rows,
        'flow_activity_rows': flow_activity_rows,
        'voucher_usage': build_voucher_usage_context(
            latest_record.user_id,
            voucher=voucher,
        ),
        'usage_scales': usage_scales,
        'current_month_label': current_month_label,
        'speed_limits_by_name': speed_limits_by_name,
    }
