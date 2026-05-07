'''Import WAN flow usage from nfdump capture files.

This importer intentionally reads completed nfcapd files and records each
WAN-classified flow. The application can roll those rows up later without
discarding the original client/service detail.
'''

from __future__ import annotations

import argparse
import csv
import ipaddress
import logging
import re
import subprocess
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import config as cfg
import database as db
from database import WanFlowUsageRecord
from logging_config import configure_logging

logger = logging.getLogger(__name__)

CAPTURE_FILE_PATTERN = re.compile(r'^nfcapd\.\d{12}$')
NFDUMP_FORMAT = 'fmt:%ts,%td,%pr,%sa,%sp,%da,%dp,%pkt,%byt'


@dataclass(frozen=True, kw_only=True)
class ParsedFlow:
    'Flow fields parsed from nfdump custom CSV output.'
    started_at: datetime
    duration_seconds: float
    proto: str
    src_ip: str
    src_port: int | None
    dst_ip: str
    dst_port: int | None
    packets: int
    bytes: int


def parse_internal_networks(raw_networks: object) -> list[ipaddress._BaseNetwork]:
    'Return configured internal IP networks, skipping invalid entries.'
    networks: list[ipaddress._BaseNetwork] = []
    for raw_network in raw_networks if isinstance(raw_networks, (set, list, tuple)) else []:
        network_text = str(raw_network).strip()
        if not network_text:
            continue
        try:
            networks.append(ipaddress.ip_network(network_text, strict=False))
        except ValueError:
            logger.warning('Ignoring invalid INTERNAL_NETWORKS entry: %s', network_text)
    return networks


def ip_is_internal(ip_text: str, internal_networks: list[ipaddress._BaseNetwork]) -> bool:
    'Return True when an IP address belongs to one configured internal network.'
    try:
        ip_address = ipaddress.ip_address(ip_text)
    except ValueError:
        return False
    return any(ip_address in network for network in internal_networks)


def parse_datetime(value: str) -> datetime:
    'Parse nfdump timestamp text.'
    value = value.strip()
    for fmt in ('%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S'):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    raise ValueError(f'Unsupported nfdump timestamp: {value}')


def parse_int(value: str) -> int:
    'Parse an integer field that may include separators or K/M/G/T suffixes.'
    normalized = value.strip().replace(',', '')
    if not normalized:
        return 0

    parts = normalized.split()
    if len(parts) == 2 and parts[1].upper() in {'K', 'M', 'G', 'T'}:
        multiplier_by_suffix = {
            'K': 1_000,
            'M': 1_000_000,
            'G': 1_000_000_000,
            'T': 1_000_000_000_000,
        }
        return int(float(parts[0]) * multiplier_by_suffix[parts[1].upper()])

    return int(normalized)


def parse_duration_seconds(value: str) -> float:
    'Parse nfdump duration as seconds or HH:MM:SS.fraction.'
    value = value.strip()
    if ':' not in value:
        return float(value)

    parts = value.split(':')
    if len(parts) != 3:
        raise ValueError(f'Unsupported nfdump duration: {value}')

    hours, minutes, seconds = parts
    return (int(hours) * 3600) + (int(minutes) * 60) + float(seconds)


def parse_optional_port(value: str) -> int | None:
    'Parse a transport port, returning None for non-port protocols.'
    value = value.strip()
    if not value or value == '0':
        return None
    try:
        return int(value)
    except ValueError:
        return None


def parse_nfdump_line(line: str) -> ParsedFlow | None:
    'Parse one custom CSV nfdump output line.'
    if not line.strip() or line.startswith(('Summary:', 'Time window:', 'Sys:', 'Date ')):
        return None

    cells = next(csv.reader([line]))
    if len(cells) != 9:
        logger.debug('Skipping unexpected nfdump row with %s fields: %s', len(cells), line)
        return None

    started_at, duration, proto, src_ip, src_port, dst_ip, dst_port, packets, bytes_used = cells
    return ParsedFlow(
        started_at=parse_datetime(started_at),
        duration_seconds=parse_duration_seconds(duration),
        proto=proto.strip().upper(),
        src_ip=src_ip.strip(),
        src_port=parse_optional_port(src_port),
        dst_ip=dst_ip.strip(),
        dst_port=parse_optional_port(dst_port),
        packets=parse_int(packets),
        bytes=parse_int(bytes_used),
    )


def classify_wan_flow(
    flow: ParsedFlow,
    source_file: str,
    internal_networks: list[ipaddress._BaseNetwork],
) -> WanFlowUsageRecord | None:
    'Return a WAN usage row for internal-external flows, otherwise None.'
    src_internal = ip_is_internal(flow.src_ip, internal_networks)
    dst_internal = ip_is_internal(flow.dst_ip, internal_networks)
    if src_internal == dst_internal:
        return None

    direction = 'upload' if src_internal else 'download'
    client_ip = flow.src_ip if src_internal else flow.dst_ip
    return WanFlowUsageRecord(
        source_file=source_file,
        started_at=flow.started_at,
        ended_at=flow.started_at + timedelta(seconds=flow.duration_seconds),
        duration_seconds=flow.duration_seconds,
        proto=flow.proto,
        src_ip=flow.src_ip,
        src_port=flow.src_port,
        dst_ip=flow.dst_ip,
        dst_port=flow.dst_port,
        packets=flow.packets,
        bytes=flow.bytes,
        direction=direction,
        client_ip=client_ip,
    )


def completed_capture_files(capture_dir: Path) -> list[Path]:
    'Return completed nfcapd files in chronological filename order.'
    if not capture_dir.exists():
        return []
    return sorted(
        path
        for path in capture_dir.iterdir()
        if path.is_file() and CAPTURE_FILE_PATTERN.fullmatch(path.name)
    )


def read_nfdump_file(path: Path, nfdump_bin: str) -> str:
    'Run nfdump with a stable comma-separated output format.'
    command = [
        nfdump_bin,
        '-q',
        '-r',
        str(path),
        '-o',
        NFDUMP_FORMAT,
    ]
    result = subprocess.run(command, check=True, capture_output=True, text=True)
    return result.stdout


def import_capture_file(
    path: Path,
    internal_networks: list[ipaddress._BaseNetwork],
    nfdump_bin: str,
    dry_run: bool = False,
) -> tuple[int, int]:
    'Import one completed nfcapd file and return imported/skipped counts.'
    source_file = path.name
    if db.flow_import_exists(source_file):
        return 0, 0

    rows: list[WanFlowUsageRecord] = []
    skipped_count = 0
    for line in read_nfdump_file(path, nfdump_bin).splitlines():
        try:
            parsed_flow = parse_nfdump_line(line)
        except ValueError as exc:
            logger.warning('Skipping unparsable nfdump row from %s: %s', source_file, exc)
            skipped_count += 1
            continue
        if parsed_flow is None:
            continue
        if wan_row := classify_wan_flow(parsed_flow, source_file, internal_networks):
            rows.append(wan_row)
        else:
            skipped_count += 1

    if dry_run:
        return len(rows), skipped_count

    return db.record_flow_import(source_file, rows, skipped_count), skipped_count


def import_completed_captures(
    capture_dir: Path,
    internal_networks: list[ipaddress._BaseNetwork],
    nfdump_bin: str,
    dry_run: bool = False,
) -> tuple[int, int, int]:
    'Import all completed captures and return file/import/skipped counts.'
    imported_files = 0
    imported_rows = 0
    skipped_rows = 0
    for capture_file in completed_capture_files(capture_dir):
        if db.flow_import_exists(capture_file.name):
            continue
        rows, skipped = import_capture_file(capture_file, internal_networks, nfdump_bin, dry_run)
        imported_files += 1
        imported_rows += rows
        skipped_rows += skipped
        logger.info(
            '%s %s: imported=%s skipped=%s',
            'Would import' if dry_run else 'Imported',
            capture_file.name,
            rows,
            skipped,
        )
    return imported_files, imported_rows, skipped_rows


def main() -> None:
    'CLI entrypoint for flow ingestion.'
    configure_logging()
    parser = argparse.ArgumentParser(description='Import WAN flow usage from nfdump capture files.')
    parser.add_argument('--dir', default=str(getattr(cfg, 'NFDUMP_DIR', '/var/cache/nfdump')))
    parser.add_argument('--nfdump-bin', default=str(getattr(cfg, 'NFDUMP_BIN', 'nfdump')))
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--summary-limit', type=int, default=10)
    args = parser.parse_args()

    db.init_db()
    internal_networks = parse_internal_networks(getattr(cfg, 'INTERNAL_NETWORKS', set()))
    if not internal_networks:
        raise SystemExit('No valid INTERNAL_NETWORKS configured; refusing to import WAN flows.')

    files, rows, skipped = import_completed_captures(
        capture_dir=Path(args.dir),
        internal_networks=internal_networks,
        nfdump_bin=args.nfdump_bin,
        dry_run=args.dry_run,
    )
    print(f'Flow import complete: files={files} imported_rows={rows} skipped_rows={skipped}')
    if not args.dry_run and args.summary_limit > 0:
        summaries = db.get_wan_usage_by_client(limit=args.summary_limit)
        if summaries:
            print('Top WAN clients:')
            for summary in summaries:
                upload_mb = summary.upload_bytes / 1_000_000.0
                download_mb = summary.download_bytes / 1_000_000.0
                total_mb = upload_mb + download_mb
                print(
                    f'  {summary.client_ip:>15} '
                    f'total={total_mb:10.2f} MB '
                    f'down={download_mb:10.2f} MB '
                    f'up={upload_mb:10.2f} MB '
                    f'flows={summary.flow_count}'
                )


if __name__ == '__main__':
    main()
