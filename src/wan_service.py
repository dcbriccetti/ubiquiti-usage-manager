'''WAN usage view-model helpers for dashboard and client-detail pages.'''

from datetime import datetime
from typing import TypedDict

import database as db


def bytes_to_mb(byte_count: int) -> float:
    'Return decimal MB for network byte counters.'
    return byte_count / 1_000_000.0


class WanIdentityRow(TypedDict):
    'Serialized WAN usage row attributed to an identity or client IP.'
    client_ip: str
    name: str
    user_id: str
    vlan: str
    mac: str
    identity_observed_at: datetime | None
    identity_is_fallback: bool
    upload_bytes: int
    download_bytes: int
    flow_count: int


def serialize_wan_identity_rows(rows: list[db.WanIdentityUsageSummary]) -> list[WanIdentityRow]:
    'Serialize timestamp-attributed WAN identity rollups for templates.'
    latest_identities_by_ip = db.get_latest_client_identities_by_ip([row.client_ip for row in rows])
    serialized_rows: list[WanIdentityRow] = []
    for row in rows:
        fallback_identity = latest_identities_by_ip.get(row.client_ip) if not row.mac else None
        serialized_rows.append(
            {
                'client_ip': row.client_ip,
                'name': row.name or (fallback_identity.name if fallback_identity else ''),
                'user_id': row.user_id or (fallback_identity.user_id if fallback_identity else ''),
                'vlan': row.vlan if row.vlan != 'Unknown' else (fallback_identity.vlan if fallback_identity else row.vlan),
                'mac': row.mac or (fallback_identity.mac if fallback_identity else ''),
                'identity_observed_at': fallback_identity.observed_at if fallback_identity else None,
                'identity_is_fallback': fallback_identity is not None,
                'upload_bytes': row.upload_bytes,
                'download_bytes': row.download_bytes,
                'flow_count': row.flow_count,
            }
        )
    return serialized_rows


class NetworkSummary(TypedDict):
    'WAN totals grouped by network/VLAN name.'
    network: str
    download_bytes: int
    upload_bytes: int
    flow_count: int
    client_count: int


class WanDiagnosticRow(TypedDict):
    'Decorated WAN row with MB total for diagnostic sorting.'
    client_ip: str
    name: str
    user_id: str
    vlan: str
    mac: str
    identity_observed_at: datetime | None
    identity_is_fallback: bool
    upload_bytes: int
    download_bytes: int
    flow_count: int
    total_mb: float


class WanAttributionDiagnostics(TypedDict):
    'WAN identity attribution summary for one reporting period.'
    total_mb: float
    attributed_mb: float
    fallback_mb: float
    unattributed_mb: float
    attributed_pct: float
    fallback_pct: float
    unattributed_pct: float
    attributed_client_count: int
    fallback_client_count: int
    unattributed_client_count: int
    top_unattributed_rows: list[WanDiagnosticRow]
    top_fallback_rows: list[WanDiagnosticRow]


class UsageComparisonRow(TypedDict):
    'Comparison row for sampled UniFi usage and WAN flow usage.'
    client_ip: str
    name: str
    user_id: str
    vlan: str
    mac: str
    identity_is_fallback: bool
    unifi_period_mb: float
    wan_period_mb: float
    wan_flow_count: int
    difference_mb: float


def summarize_wan_by_network(rows: list[WanIdentityRow]) -> list[NetworkSummary]:
    'Aggregate decorated WAN rows by latest-known network/VLAN label.'
    summary_by_network: dict[str, NetworkSummary] = {}
    for row in rows:
        network_name = row['vlan'] or 'Unknown'
        summary = summary_by_network.setdefault(
            network_name,
            {
                'network': network_name,
                'download_bytes': 0,
                'upload_bytes': 0,
                'flow_count': 0,
                'client_count': 0,
            },
        )
        summary['download_bytes'] += row['download_bytes']
        summary['upload_bytes'] += row['upload_bytes']
        summary['flow_count'] += row['flow_count']
        summary['client_count'] += 1

    return sorted(
        summary_by_network.values(),
        key=lambda summary: summary['download_bytes'] + summary['upload_bytes'],
        reverse=True,
    )


def total_wan_mb(row: WanIdentityRow) -> float:
    'Return total decimal MB for one decorated WAN row.'
    download_bytes = row['download_bytes']
    upload_bytes = row['upload_bytes']
    total_bytes = download_bytes + upload_bytes
    return bytes_to_mb(total_bytes)


def build_wan_attribution_diagnostics(rows: list[WanIdentityRow]) -> WanAttributionDiagnostics:
    'Summarize how much WAN usage has confident, fallback, or missing identity attribution.'
    diagnostic_rows: list[WanDiagnosticRow] = []
    for row in rows:
        diagnostic_rows.append(
            {
                'client_ip': row['client_ip'],
                'name': row['name'],
                'user_id': row['user_id'],
                'vlan': row['vlan'],
                'mac': row['mac'],
                'identity_observed_at': row['identity_observed_at'],
                'identity_is_fallback': row['identity_is_fallback'],
                'upload_bytes': row['upload_bytes'],
                'download_bytes': row['download_bytes'],
                'flow_count': row['flow_count'],
                'total_mb': total_wan_mb(row),
            }
        )
    total_mb = sum(row['total_mb'] for row in diagnostic_rows)
    fallback_rows = [row for row in diagnostic_rows if row['identity_is_fallback']]
    unattributed_rows = [row for row in diagnostic_rows if not row['mac']]
    attributed_rows = [
        row
        for row in diagnostic_rows
        if row['mac'] and not row['identity_is_fallback']
    ]
    fallback_mb = sum(row['total_mb'] for row in fallback_rows)
    unattributed_mb = sum(row['total_mb'] for row in unattributed_rows)
    attributed_mb = sum(row['total_mb'] for row in attributed_rows)

    def pct(part_mb: float) -> float:
        return (part_mb / total_mb * 100.0) if total_mb else 0.0

    return {
        'total_mb': total_mb,
        'attributed_mb': attributed_mb,
        'fallback_mb': fallback_mb,
        'unattributed_mb': unattributed_mb,
        'attributed_pct': pct(attributed_mb),
        'fallback_pct': pct(fallback_mb),
        'unattributed_pct': pct(unattributed_mb),
        'attributed_client_count': len(attributed_rows),
        'fallback_client_count': len(fallback_rows),
        'unattributed_client_count': len(unattributed_rows),
        'top_unattributed_rows': sorted(
            unattributed_rows,
            key=lambda row: row['total_mb'],
            reverse=True,
        )[:10],
        'top_fallback_rows': sorted(
            fallback_rows,
            key=lambda row: row['total_mb'],
            reverse=True,
        )[:10],
    }


def build_wan_attribution_period_rows(
    today_diagnostics: WanAttributionDiagnostics,
    month_diagnostics: WanAttributionDiagnostics,
) -> list[dict[str, object]]:
    'Return compact today/month attribution coverage rows.'
    return [
        {'label': 'Today', **today_diagnostics},
        {'label': 'Month', **month_diagnostics},
    ]


def build_wan_billing_readiness(
    attribution_diagnostics: WanAttributionDiagnostics,
    latest_import_age_minutes: int | None,
) -> dict[str, str]:
    'Return a concise status for deciding whether WAN usage is ready to drive billing.'
    total_mb = attribution_diagnostics['total_mb']
    unattributed_pct = attribution_diagnostics['unattributed_pct']
    fallback_pct = attribution_diagnostics['fallback_pct']
    if latest_import_age_minutes is None:
        return {
            'label': 'No imports',
            'class': 'warn-text',
            'detail': 'Internet data import has not produced any completed data yet.',
        }
    if latest_import_age_minutes > 15:
        return {
            'label': 'Import stale',
            'class': 'warn-text',
            'detail': 'Internet data is old enough that billing comparisons may be incomplete.',
        }
    if total_mb < 100.0:
        return {
            'label': 'Collecting',
            'class': 'muted',
            'detail': 'Internet volume is still low; wait for a busy period before switching billing.',
        }
    if unattributed_pct <= 2.0 and fallback_pct <= 10.0:
        return {
            'label': 'Strong',
            'class': '',
            'detail': 'Most Internet usage is confidently attributed to client identities.',
        }
    if unattributed_pct <= 10.0:
        return {
            'label': 'Watch',
            'class': '',
            'detail': 'Internet attribution is usable for comparison, but approximate or unknown matches still need review.',
        }
    return {
        'label': 'Needs identity work',
        'class': 'warn-text',
        'detail': 'Too much Internet usage is still unidentified to use as the billing source.',
    }


def build_month_usage_comparison_rows(
    month_wan_rows: list[WanIdentityRow],
    period_start: datetime | None = None,
    period_end: datetime | None = None,
) -> list[UsageComparisonRow]:
    'Compare sampled UniFi usage with WAN flow-attributed usage for the same period.'
    comparison_by_key: dict[str, UsageComparisonRow] = {}

    if period_start is not None and period_end is not None:
        unifi_rows = db.get_usage_summary_for_period(period_start, period_end)
    else:
        unifi_rows = db.get_usage_window_summary('this_month')

    for unifi_row in unifi_rows:
        key = unifi_row.mac.lower()
        comparison_by_key[key] = {
            'client_ip': '',
            'name': unifi_row.name or '',
            'user_id': unifi_row.user_id or '',
            'vlan': unifi_row.vlan or '',
            'mac': unifi_row.mac,
            'identity_is_fallback': False,
            'unifi_period_mb': unifi_row.calendar_month_total_mb,
            'wan_period_mb': 0.0,
            'wan_flow_count': 0,
            'difference_mb': 0.0,
        }

    for wan_row in month_wan_rows:
        mac = wan_row['mac']
        client_ip = wan_row['client_ip']
        key = mac.lower() if mac else f'ip:{client_ip}'
        comparison = comparison_by_key.setdefault(
            key,
            {
                'client_ip': client_ip,
                'name': '',
                'user_id': '',
                'vlan': '',
                'mac': mac,
                'identity_is_fallback': False,
                'unifi_period_mb': 0.0,
                'wan_period_mb': 0.0,
                'wan_flow_count': 0,
                'difference_mb': 0.0,
            },
        )
        comparison['client_ip'] = comparison['client_ip'] or client_ip
        comparison['name'] = comparison['name'] or wan_row['name']
        comparison['user_id'] = comparison['user_id'] or wan_row['user_id']
        comparison['vlan'] = comparison['vlan'] or wan_row['vlan']
        comparison['mac'] = comparison['mac'] or mac
        comparison['identity_is_fallback'] = comparison['identity_is_fallback'] or wan_row['identity_is_fallback']
        comparison['wan_period_mb'] += total_wan_mb(wan_row)
        comparison['wan_flow_count'] += wan_row['flow_count']

    comparison_rows = list(comparison_by_key.values())
    for comparison_row in comparison_rows:
        comparison_row['difference_mb'] = comparison_row['wan_period_mb'] - comparison_row['unifi_period_mb']

    return sorted(
        comparison_rows,
        key=lambda row: max(row['unifi_period_mb'], row['wan_period_mb']),
        reverse=True,
    )


def summarize_wan_identity_rows_for_mac(
    rows: list[db.WanIdentityUsageSummary],
    mac: str,
) -> tuple[float, float]:
    'Return download/upload MB from timestamp-attributed WAN rows for one MAC.'
    target_mac = mac.lower()
    download_bytes = sum(row.download_bytes for row in rows if row.mac.lower() == target_mac)
    upload_bytes = sum(row.upload_bytes for row in rows if row.mac.lower() == target_mac)
    return bytes_to_mb(download_bytes), bytes_to_mb(upload_bytes)
