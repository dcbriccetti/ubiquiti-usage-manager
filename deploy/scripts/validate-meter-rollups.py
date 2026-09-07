#!/usr/bin/env python3
"""Offline, rebuild-only prototype. Never deploy this as a live accounting writer.

Input must be an extracted SQLite backup. Output is a NEW private directory.
No application imports, network access, pruning, or source writes are performed.
The prototype uses flow START time, as WAN reports and voucher accounting do.
Client detail panels using END time are deliberately outside its scope.
"""

import argparse
from bisect import bisect_right
from collections import defaultdict
from datetime import datetime, timedelta
import json
from pathlib import Path
import sqlite3
import time
from collections.abc import Iterator, Mapping, Sequence
from typing import Any, NamedTuple, TypeAlias


Identity: TypeAlias = tuple[str, str, str, str]
IdentityHistory: TypeAlias = dict[str, tuple[list[datetime], list[Identity]]]
Flow: TypeAlias = tuple[datetime, str, str, int]
ReportKey: TypeAlias = tuple[str, str, str, str, str]
HourlyKey: TypeAlias = tuple[str, str, str, str, str, str]


class VoucherCharge(NamedTuple):
    bytes: int
    first_usage: str


def stamp(value: datetime) -> str:
    return value.isoformat(sep=" ", timespec="microseconds")


# The source database is selected at runtime, so IntelliJ cannot bind these SQL
# references to the project's example meter.db data source.
# noinspection SqlResolve
def identities(
    connection: sqlite3.Connection,
    start: datetime | None = None,
    end: datetime | None = None,
) -> IdentityHistory:
    """Compress identical observations AFTER applying the legacy query window.

    Preserve ordering by observed_at/id, including different identities at equal
    timestamps. Each entry is (timestamp, (mac, name, user_id, vlan)).
    """
    sql = 'SELECT ip_address, observed_at, mac, name, user_id, vlan FROM client_ip_identities'
    args: list[str] = []
    if start is not None:
        if end is None:
            raise ValueError('end is required when start is provided')
        sql += ' WHERE observed_at >= ? AND observed_at <= ?'
        args = [stamp(start - timedelta(days=1)), stamp(end + timedelta(minutes=10))]
    sql += ' ORDER BY ip_address, observed_at, id'
    result: IdentityHistory = {}
    for ip, at, *identity in connection.execute(sql, args):
        ip_text = str(ip)
        times, values = result.setdefault(ip_text, ([], []))
        value: Identity = tuple(str(item or '') for item in identity)  # type: ignore[assignment]
        if not values or values[-1] != value:
            times.append(datetime.fromisoformat(str(at)))
            values.append(value)
    return result


def resolve(history: IdentityHistory, ip: str, at: datetime) -> Identity:
    times, values = history.get(ip, ([], []))
    i = bisect_right(times, at) - 1
    if i >= 0:
        return values[i]
    if times and times[0] <= at + timedelta(minutes=10):
        return values[0]
    return ('', '', '', 'Unknown')


# noinspection SqlResolve
def flows(
    connection: sqlite3.Connection,
    start: datetime | None = None,
    end: datetime | None = None,
) -> Iterator[Flow]:
    sql = 'SELECT started_at, client_ip, direction, bytes FROM wan_flow_usage'
    args: list[str] = []
    if start is not None:
        if end is None:
            raise ValueError('end is required when start is provided')
        sql += ' WHERE started_at >= ? AND started_at <= ?'
        args = [stamp(start), stamp(end)]
    sql += ' ORDER BY started_at, id'
    for at, ip, direction, byte_count in connection.execute(sql, args):
        yield datetime.fromisoformat(str(at)), str(ip), str(direction), int(byte_count or 0)


def report_key(ip: str, identity: Identity) -> ReportKey:
    mac, name, user, vlan = identity
    return (mac or f'ip:{ip}', ip, name, user, vlan)


def compare(
    expected: Mapping[Any, Sequence[Any]],
    actual: Mapping[Any, Sequence[Any]],
) -> dict[str, Any]:
    differences: list[dict[str, Any]] = []
    for key in sorted(expected.keys() | actual.keys()):
        before, after = expected.get(key, (0, 0, 0)), actual.get(key, (0, 0, 0))
        if tuple(before) != tuple(after):
            differences.append({'key': key, 'legacy': before, 'prototype': after})
    return {'passed': not differences, 'different_groups': len(differences),
            'examples': differences[:20]}


# This function queries the runtime-selected source and creates a new target
# schema in the same operation; neither can be resolved statically by the IDE.
# noinspection SqlResolve
def build(source: Path, output: Path, progress: bool = True) -> dict[str, Any]:
    output.mkdir(mode=0o700, parents=True, exist_ok=False)
    connection = sqlite3.connect(source.resolve().as_uri() + '?mode=ro', uri=True)
    connection.execute('PRAGMA query_only=ON')
    connection.execute('BEGIN')
    target = sqlite3.connect(output / 'summaries.db')
    began = time.perf_counter()
    try:
        if progress:
            print('Checking the complete offline backup (may be I/O intensive)...', flush=True)
        check_row = connection.execute('PRAGMA quick_check').fetchone()
        check = str(check_row[0]) if check_row else ''
        if check != 'ok':
            raise ValueError(f'Source quick_check failed: {check}')
        if progress:
            print(f'Backup quick_check passed in {time.perf_counter()-began:.1f}s', flush=True)
        bounds = connection.execute('SELECT MIN(started_at), MAX(started_at) FROM wan_flow_usage').fetchone()
        if bounds is None or bounds[0] is None or bounds[1] is None:
            raise ValueError('Source has no WAN flows')
        first, end = (datetime.fromisoformat(str(value)) for value in bounds)
        vouchers: list[tuple[int, int, str, int]] = [
            (int(voucher_id), int(user_id), str(generated_at), int(allocation_gb))
            for voucher_id, user_id, generated_at, allocation_gb in connection.execute(
                'SELECT id,user_id,generated_at,allocation_gb FROM plus_vouchers '
                'WHERE consumed_at IS NULL ORDER BY id'
            )
        ]
        if len({str(row[1]) for row in vouchers}) != len(vouchers):
            raise ValueError('Multiple active vouchers share a user ID; define accounting semantics first')
        voucher_by_user: dict[str, tuple[int, datetime]] = {
            str(user): (voucher_id, datetime.fromisoformat(generated_at))
            for voucher_id, user, generated_at, _allocation in vouchers
        }
        # The generated target schema exists only in the output database and uses
        # SQLite-specific WITHOUT ROWID syntax.
        # noinspection SqlDialect,SqlResolve
        target.executescript('''
            CREATE TABLE hourly_usage (
                hour TEXT, client_ip TEXT, mac TEXT, name TEXT, user_id TEXT, vlan TEXT,
                upload_bytes INTEGER NOT NULL, download_bytes INTEGER NOT NULL, flow_count INTEGER NOT NULL,
                PRIMARY KEY(hour,client_ip,mac,name,user_id,vlan)
            ) WITHOUT ROWID;
            CREATE TABLE voucher_daily (
                voucher_id INTEGER, day TEXT, bytes INTEGER NOT NULL, first_usage TEXT,
                PRIMARY KEY(voucher_id,day)
            ) WITHOUT ROWID;
        ''')
        if progress:
            print('Loading identity history...', flush=True)
        history = identities(connection)
        summary: defaultdict[HourlyKey, list[int]] = defaultdict(lambda: [0, 0, 0])
        charges: dict[tuple[int, str], VoucherCharge] = {}
        count = total_bytes = unknown_bytes = 0

        def flush() -> None:
            # noinspection SqlResolve
            target.executemany('''INSERT INTO hourly_usage VALUES (?,?,?,?,?,?,?,?,?)
                ON CONFLICT DO UPDATE SET upload_bytes=upload_bytes+excluded.upload_bytes,
                download_bytes=download_bytes+excluded.download_bytes, flow_count=flow_count+excluded.flow_count''',
                (key + tuple(value) for key,value in summary.items()))
            target.commit()
            summary.clear()
        for at,ip,direction,byte_count in flows(connection):
            identity = resolve(history, ip, at)
            key = (stamp(at.replace(minute=0,second=0,microsecond=0)), ip) + identity
            value = summary[key]
            value[0 if direction == 'upload' else 1] += byte_count
            value[2] += 1
            total_bytes += byte_count
            count += 1
            if not identity[0]:
                unknown_bytes += byte_count
            voucher = voucher_by_user.get(identity[2].strip())
            if voucher and at >= voucher[1]:
                charge_key = (voucher[0], at.date().isoformat())
                prior_charge = charges.get(charge_key)
                charges[charge_key] = VoucherCharge(
                    bytes=(prior_charge.bytes if prior_charge else 0) + byte_count,
                    first_usage=prior_charge.first_usage if prior_charge else stamp(at),
                )
            if count % 250000 == 0:
                flush()
                if progress:
                    print(f'Built {count:,} flows in {time.perf_counter()-began:.1f}s', flush=True)
        flush()
        target.executemany('INSERT INTO voucher_daily VALUES (?,?,?,?)',
                           (key + tuple(value) for key,value in charges.items()))
        target.execute('''CREATE TABLE daily_usage AS SELECT substr(hour,1,10) AS day,
            client_ip,mac,name,user_id,vlan,SUM(upload_bytes) AS upload_bytes,
            SUM(download_bytes) AS download_bytes,SUM(flow_count) AS flow_count
            FROM hourly_usage GROUP BY day,client_ip,mac,name,user_id,vlan''')
        target.commit()
        build_seconds = time.perf_counter()-began
        if progress:
            print('Checking conservation and report windows...', flush=True)
        results: dict[str, Any] = {'source': str(source.resolve()), 'scope': 'start-time WAN reports and active vouchers; offline full rebuild only',
                   'quick_check': check, 'first_flow': stamp(first), 'last_flow': stamp(end),
                   'flow_count': count, 'bytes': total_bytes, 'unattributed_bytes': unknown_bytes,
                   'build_seconds': round(build_seconds,3), 'report_comparisons': []}
        # Independent SQL conservation checks; source bytes never pass through identity matching.
        raw = connection.execute('SELECT COUNT(*),SUM(bytes) FROM wan_flow_usage').fetchone()
        hourly = target.execute('SELECT SUM(flow_count),SUM(upload_bytes+download_bytes) FROM hourly_usage').fetchone()
        daily = target.execute('SELECT SUM(flow_count),SUM(upload_bytes+download_bytes) FROM daily_usage').fetchone()
        results['conservation'] = {'passed': raw == hourly == daily, 'raw': raw, 'hourly': hourly, 'daily': daily}
        periods: dict[str, tuple[datetime, datetime]] = {}
        month = first.replace(day=1,hour=0,minute=0,second=0,microsecond=0)
        while month <= end:
            next_month = (month.replace(day=28) + timedelta(days=4)).replace(day=1)
            periods[month.strftime('%Y-%m')] = (month, min(end,next_month-timedelta(microseconds=1)))
            month = next_month
        periods['today'] = (end.replace(hour=0,minute=0,second=0,microsecond=0),end)
        periods['last_7_days'] = (end-timedelta(days=7),end)
        for label,(start,stop) in periods.items():
            t = time.perf_counter()
            # Streamed equivalent of get_wan_usage_by_identity. Applying its query
            # window BEFORE compression is critical: a canonical identity may differ.
            legacy_history = identities(connection,start,stop)
            expected: defaultdict[ReportKey, list[int]] = defaultdict(lambda: [0, 0, 0])
            for at,ip,direction,byte_count in flows(connection,start,stop):
                value = expected[report_key(ip,resolve(legacy_history,ip,at))]
                value[0 if direction == 'upload' else 1] += byte_count
                value[2] += 1
            legacy_seconds = time.perf_counter()-t
            t = time.perf_counter()
            # Fully covered hours use summaries. Partial edge hours use retained
            # raw flows, avoiding rounding a seven-day window to whole hours.
            lo = start.replace(minute=0,second=0,microsecond=0)
            if lo < start:
                lo += timedelta(hours=1)
            hi = stop.replace(minute=0,second=0,microsecond=0)
            actual: defaultdict[ReportKey, list[int]] = defaultdict(lambda: [0, 0, 0])
            for ip,mac,name,user,vlan,up,down,n in target.execute('''SELECT client_ip,mac,name,user_id,vlan,
                    SUM(upload_bytes),SUM(download_bytes),SUM(flow_count) FROM hourly_usage
                    WHERE hour>=? AND hour<? GROUP BY client_ip,mac,name,user_id,vlan''',(stamp(lo),stamp(hi))):
                value = actual[report_key(ip,(mac,name,user,vlan))]
                for i,number in enumerate((up,down,n)):
                    value[i] += number
            edges = [(start,stop)] if lo >= hi else [(start,lo-timedelta(microseconds=1)),(hi,stop)]
            for edge_start,edge_end in edges:
                if edge_start > edge_end:
                    continue
                for at,ip,direction,byte_count in flows(connection,edge_start,edge_end):
                    value = actual[report_key(ip,resolve(history,ip,at))]
                    value[0 if direction == 'upload' else 1] += byte_count
                    value[2] += 1
            comparison = compare(expected,actual)
            comparison.update(period=label, legacy_seconds=round(legacy_seconds,3),
                              prototype_seconds=round(time.perf_counter()-t,3))
            results['report_comparisons'].append(comparison)
            if progress:
                print(f'Compared {label}: passed={comparison["passed"]}, '
                      f'different_groups={comparison["different_groups"]}, '
                      f'legacy={comparison["legacy_seconds"]}s, '
                      f'prototype={comparison["prototype_seconds"]}s', flush=True)
        # Match the existing multi-voucher query's candidate-IP rule and identity
        # window, independently of the prototype's canonical history.
        legacy_charges: dict[tuple[int, str], VoucherCharge] = {}
        if progress:
            print('Comparing active-voucher accounting...', flush=True)
        if vouchers:
            start = min(datetime.fromisoformat(row[2]) for row in vouchers)
            old_history = identities(connection,start,end)
            users = set(voucher_by_user)
            candidates = {ip for ip,user in connection.execute('''SELECT DISTINCT ip_address,user_id
                FROM client_ip_identities WHERE observed_at>=? AND observed_at<=?''',
                (stamp(start-timedelta(days=1)),stamp(end+timedelta(minutes=10)))) if user in users}
            for at,ip,_,byte_count in flows(connection,start,end):
                if ip not in candidates:
                    continue
                identity = resolve(old_history,ip,at)
                voucher = voucher_by_user.get(identity[2].strip())
                if voucher and at >= voucher[1]:
                    charge_key = (voucher[0], at.date().isoformat())
                    prior_charge = legacy_charges.get(charge_key)
                    legacy_charges[charge_key] = VoucherCharge(
                        bytes=(prior_charge.bytes if prior_charge else 0) + byte_count,
                        first_usage=prior_charge.first_usage if prior_charge else stamp(at),
                    )
        results['voucher_daily_comparison'] = compare(legacy_charges,charges)
        results['vouchers'] = []
        for vid,_,_,allocation in vouchers:
            old_rows = [value for (v,_),value in legacy_charges.items() if v==vid]
            new_rows = [value for (v,_),value in charges.items() if v==vid]
            old_bytes = sum(row.bytes for row in old_rows)
            new_bytes = sum(row.bytes for row in new_rows)
            old_first = min((row.first_usage for row in old_rows), default=None)
            new_first = min((row.first_usage for row in new_rows), default=None)
            results['vouchers'].append({'voucher_id': vid,'legacy_bytes':old_bytes,'prototype_bytes':new_bytes,
                'legacy_activated_at':old_first,'prototype_activated_at':new_first,
                'remaining_bytes':max(0,allocation*1_000_000_000-new_bytes),
                'passed': old_bytes==new_bytes and old_first==new_first})
        results['rows'] = {table: target.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0]
                           for table in ['hourly_usage','daily_usage','voucher_daily']}
        results['passed'] = (results['conservation']['passed'] and results['voucher_daily_comparison']['passed']
            and all(r['passed'] for r in results['vouchers'])
            and all(r['passed'] for r in results['report_comparisons']))
        results['total_seconds'] = round(time.perf_counter()-began,3)
        results['summary_db_bytes'] = (output/'summaries.db').stat().st_size
        results['limitations'] = ['No live dual writing, restart/retry, concurrent import or late-arrival validation.',
            'No end-time client detail, sampled usage, concurrency or throttling summary validation.',
            'Does not authorize pruning; attribution can depend on the selected report window.',
            'Timings are sequential offline runs with cache effects, not live request benchmarks.',
            'Active vouchers only; consumed voucher history is not reconstructed.']
        (output/'report.json').write_text(json.dumps(results,indent=2)+'\n')
        return results
    finally:
        target.close()
        connection.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source',type=Path,required=True,help='Extracted consistent backup; never the live DB')
    parser.add_argument('--output',type=Path,required=True,help='New private directory; existing output is refused')
    args = parser.parse_args()
    result = build(args.source,args.output)
    print(json.dumps(result,indent=2))
    raise SystemExit(0 if result['passed'] else 1)
