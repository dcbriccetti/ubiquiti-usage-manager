'''WAN usage view-model helpers for dashboard and client-detail pages.'''

import database as db


def bytes_to_mb(byte_count: int) -> float:
    'Return decimal MB for network byte counters.'
    return byte_count / 1_000_000.0


def serialize_wan_identity_rows(rows: list[db.WanIdentityUsageSummary]) -> list[dict[str, object]]:
    'Serialize timestamp-attributed WAN identity rollups for templates.'
    latest_identities_by_ip = db.get_latest_client_identities_by_ip([row.client_ip for row in rows])
    serialized_rows: list[dict[str, object]] = []
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


def summarize_wan_by_network(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    'Aggregate decorated WAN rows by latest-known network/VLAN label.'
    summary_by_network: dict[str, dict[str, object]] = {}
    for row in rows:
        network_name = str(row.get('vlan') or 'Unknown')
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
        summary['download_bytes'] = int(summary['download_bytes']) + int(row.get('download_bytes') or 0)
        summary['upload_bytes'] = int(summary['upload_bytes']) + int(row.get('upload_bytes') or 0)
        summary['flow_count'] = int(summary['flow_count']) + int(row.get('flow_count') or 0)
        summary['client_count'] = int(summary['client_count']) + 1

    return sorted(
        summary_by_network.values(),
        key=lambda summary: int(summary['download_bytes']) + int(summary['upload_bytes']),
        reverse=True,
    )


def total_wan_mb(row: dict[str, object]) -> float:
    'Return total decimal MB for one decorated WAN row.'
    total_bytes = int(row.get('download_bytes') or 0) + int(row.get('upload_bytes') or 0)
    return bytes_to_mb(total_bytes)


def build_wan_attribution_diagnostics(rows: list[dict[str, object]]) -> dict[str, object]:
    'Summarize how much WAN usage has confident, fallback, or missing identity attribution.'
    diagnostic_rows = [
        {
            **row,
            'total_mb': total_wan_mb(row),
        }
        for row in rows
    ]
    total_mb = sum(float(row['total_mb']) for row in diagnostic_rows)
    fallback_rows = [row for row in diagnostic_rows if row.get('identity_is_fallback')]
    unattributed_rows = [row for row in diagnostic_rows if not row.get('mac')]
    attributed_rows = [
        row
        for row in diagnostic_rows
        if row.get('mac') and not row.get('identity_is_fallback')
    ]
    fallback_mb = sum(float(row['total_mb']) for row in fallback_rows)
    unattributed_mb = sum(float(row['total_mb']) for row in unattributed_rows)
    attributed_mb = sum(float(row['total_mb']) for row in attributed_rows)

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
            key=lambda row: float(row['total_mb']),
            reverse=True,
        )[:10],
        'top_fallback_rows': sorted(
            fallback_rows,
            key=lambda row: float(row['total_mb']),
            reverse=True,
        )[:10],
    }


def build_wan_attribution_period_rows(
    today_diagnostics: dict[str, object],
    month_diagnostics: dict[str, object],
) -> list[dict[str, object]]:
    'Return compact today/month attribution coverage rows.'
    return [
        {'label': 'Today', **today_diagnostics},
        {'label': 'Month', **month_diagnostics},
    ]


def build_wan_billing_readiness(
    attribution_diagnostics: dict[str, object],
    latest_import_age_minutes: int | None,
) -> dict[str, str]:
    'Return a concise status for deciding whether WAN usage is ready to drive billing.'
    total_mb = float(attribution_diagnostics['total_mb'])
    unattributed_pct = float(attribution_diagnostics['unattributed_pct'])
    fallback_pct = float(attribution_diagnostics['fallback_pct'])
    if latest_import_age_minutes is None:
        return {
            'label': 'No imports',
            'class': 'warn-text',
            'detail': 'WAN capture import has not produced any completed data yet.',
        }
    if latest_import_age_minutes > 15:
        return {
            'label': 'Import stale',
            'class': 'warn-text',
            'detail': 'WAN capture data is old enough that billing comparisons may be incomplete.',
        }
    if total_mb < 100.0:
        return {
            'label': 'Collecting',
            'class': 'muted',
            'detail': 'WAN volume is still low; wait for a busy period before switching billing.',
        }
    if unattributed_pct <= 2.0 and fallback_pct <= 10.0:
        return {
            'label': 'Strong',
            'class': '',
            'detail': 'Most WAN usage is confidently attributed to client identities.',
        }
    if unattributed_pct <= 10.0:
        return {
            'label': 'Watch',
            'class': '',
            'detail': 'WAN attribution is usable for comparison, but fallback or unknown rows still need review.',
        }
    return {
        'label': 'Needs identity work',
        'class': 'warn-text',
        'detail': 'Too much WAN usage is still unidentified to use as the billing source.',
    }


def build_month_usage_comparison_rows(month_wan_rows: list[dict[str, object]]) -> list[dict[str, object]]:
    'Compare legacy UniFi month usage with WAN flow-attributed month usage.'
    comparison_by_key: dict[str, dict[str, object]] = {}

    for row in db.get_usage_window_summary('this_month'):
        key = row.mac.lower()
        comparison_by_key[key] = {
            'client_ip': '',
            'name': row.name or '',
            'user_id': row.user_id or '',
            'vlan': row.vlan or '',
            'mac': row.mac,
            'identity_is_fallback': False,
            'unifi_month_mb': row.calendar_month_total_mb,
            'wan_month_mb': 0.0,
            'wan_flow_count': 0,
        }

    for row in month_wan_rows:
        mac = str(row.get('mac') or '')
        client_ip = str(row.get('client_ip') or '')
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
                'unifi_month_mb': 0.0,
                'wan_month_mb': 0.0,
                'wan_flow_count': 0,
            },
        )
        comparison['client_ip'] = comparison.get('client_ip') or client_ip
        comparison['name'] = comparison.get('name') or str(row.get('name') or '')
        comparison['user_id'] = comparison.get('user_id') or str(row.get('user_id') or '')
        comparison['vlan'] = comparison.get('vlan') or str(row.get('vlan') or '')
        comparison['mac'] = comparison.get('mac') or mac
        comparison['identity_is_fallback'] = bool(comparison.get('identity_is_fallback')) or bool(
            row.get('identity_is_fallback')
        )
        comparison['wan_month_mb'] = float(comparison['wan_month_mb']) + total_wan_mb(row)
        comparison['wan_flow_count'] = int(comparison['wan_flow_count']) + int(row.get('flow_count') or 0)

    comparison_rows = list(comparison_by_key.values())
    for row in comparison_rows:
        row['difference_mb'] = float(row['wan_month_mb']) - float(row['unifi_month_mb'])

    return sorted(
        comparison_rows,
        key=lambda row: max(float(row['unifi_month_mb']), float(row['wan_month_mb'])),
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
