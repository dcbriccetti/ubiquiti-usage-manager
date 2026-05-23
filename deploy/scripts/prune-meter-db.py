#!/usr/bin/env python3
"""Prune old WAN-flow rows from the production meter database."""

from __future__ import annotations

import argparse
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path


DEFAULT_DB_PATH = Path("/home/daveb/devel/ubiquiti-usage-manager/meter.db")
DEFAULT_RETENTION_DAYS = 90
DEFAULT_CHUNK_SIZE = 10_000


@dataclass(frozen=True)
class Cutoff:
    flow_started_at: str
    identity_observed_at: str
    reason: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Delete WAN-flow and identity rows that are no longer needed for "
            "active Plus voucher accounting."
        )
    )
    parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--retention-days", type=int, default=DEFAULT_RETENTION_DAYS)
    parser.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--vacuum", action="store_true")
    parser.add_argument("--yes-i-have-a-backup", action="store_true")
    return parser.parse_args()


def sqlite_datetime(value: datetime) -> str:
    return value.replace(microsecond=0).isoformat(sep=" ")


def oldest_active_voucher_generated_at(connection: sqlite3.Connection) -> str | None:
    row = connection.execute(
        """
        SELECT MIN(generated_at)
        FROM plus_vouchers
        WHERE consumed_at IS NULL
        """
    ).fetchone()
    return str(row[0]) if row and row[0] else None


def active_voucher_count(connection: sqlite3.Connection) -> int:
    row = connection.execute(
        "SELECT COUNT(*) FROM plus_vouchers WHERE consumed_at IS NULL"
    ).fetchone()
    return int(row[0] or 0)


def calculate_cutoff(connection: sqlite3.Connection, retention_days: int) -> Cutoff:
    active_cutoff = oldest_active_voucher_generated_at(connection)
    if active_cutoff:
        flow_cutoff = active_cutoff
        reason = "oldest active voucher generated_at"
    else:
        flow_cutoff = sqlite_datetime(datetime.now() - timedelta(days=retention_days))
        reason = f"no active vouchers; {retention_days}-day retention"

    try:
        parsed_flow_cutoff = datetime.fromisoformat(flow_cutoff)
    except ValueError:
        parsed_flow_cutoff = datetime.strptime(flow_cutoff.split(".")[0], "%Y-%m-%d %H:%M:%S")
    identity_cutoff = sqlite_datetime(parsed_flow_cutoff - timedelta(days=1))
    return Cutoff(flow_started_at=flow_cutoff, identity_observed_at=identity_cutoff, reason=reason)


def table_count(connection: sqlite3.Connection, table_name: str) -> int:
    quoted = '"' + table_name.replace('"', '""') + '"'
    row = connection.execute(f"SELECT COUNT(*) FROM {quoted}").fetchone()
    return int(row[0] or 0)


def candidate_summary(
    connection: sqlite3.Connection,
    table_name: str,
    timestamp_column: str,
    cutoff: str,
) -> tuple[int, str | None, str | None]:
    row = connection.execute(
        f"""
        SELECT COUNT(*), MIN({timestamp_column}), MAX({timestamp_column})
        FROM {table_name}
        WHERE {timestamp_column} < ?
        """,
        (cutoff,),
    ).fetchone()
    return int(row[0] or 0), row[1], row[2]


def candidate_flow_bytes(connection: sqlite3.Connection, cutoff: str) -> int:
    row = connection.execute(
        """
        SELECT COALESCE(SUM(bytes), 0)
        FROM wan_flow_usage
        WHERE started_at < ?
        """,
        (cutoff,),
    ).fetchone()
    return int(row[0] or 0)


def page_stats(connection: sqlite3.Connection) -> tuple[int, int, int]:
    page_count = int(connection.execute("PRAGMA page_count").fetchone()[0])
    freelist_count = int(connection.execute("PRAGMA freelist_count").fetchone()[0])
    page_size = int(connection.execute("PRAGMA page_size").fetchone()[0])
    return page_count, freelist_count, page_size


def delete_in_chunks(
    connection: sqlite3.Connection,
    table_name: str,
    timestamp_column: str,
    cutoff: str,
    chunk_size: int,
) -> int:
    deleted_total = 0
    while True:
        cursor = connection.execute(
            f"""
            DELETE FROM {table_name}
            WHERE id IN (
                SELECT id
                FROM {table_name}
                WHERE {timestamp_column} < ?
                LIMIT ?
            )
            """,
            (cutoff, chunk_size),
        )
        connection.commit()
        deleted_count = cursor.rowcount if cursor.rowcount is not None else 0
        deleted_total += deleted_count
        if deleted_count == 0:
            return deleted_total
        print(f"deleted {deleted_total} rows from {table_name}", flush=True)


def print_report(connection: sqlite3.Connection, cutoff: Cutoff) -> None:
    flow_candidates = candidate_summary(
        connection,
        "wan_flow_usage",
        "started_at",
        cutoff.flow_started_at,
    )
    identity_candidates = candidate_summary(
        connection,
        "client_ip_identities",
        "observed_at",
        cutoff.identity_observed_at,
    )
    flow_bytes = candidate_flow_bytes(connection, cutoff.flow_started_at)
    page_count, freelist_count, page_size = page_stats(connection)

    print(f"database: {connection.execute('PRAGMA database_list').fetchone()[2]}")
    print(f"active_vouchers: {active_voucher_count(connection)}")
    print(f"flow_cutoff: {cutoff.flow_started_at} ({cutoff.reason})")
    print(f"identity_cutoff: {cutoff.identity_observed_at} (flow cutoff minus 1 day)")
    print(f"wan_flow_usage_rows: {table_count(connection, 'wan_flow_usage')}")
    print(f"client_ip_identity_rows: {table_count(connection, 'client_ip_identities')}")
    print(
        "candidate_wan_flow_usage_rows: "
        f"{flow_candidates[0]} oldest={flow_candidates[1]} newest={flow_candidates[2]}"
    )
    print(
        "candidate_client_ip_identity_rows: "
        f"{identity_candidates[0]} oldest={identity_candidates[1]} newest={identity_candidates[2]}"
    )
    print(f"candidate_wan_flow_usage_bytes: {flow_bytes / 1_000_000_000:.2f} GB")
    print(f"sqlite_pages: total={page_count} free={freelist_count} page_size={page_size}")


def main() -> int:
    args = parse_args()
    if args.retention_days < 1:
        raise SystemExit("--retention-days must be at least 1")
    if args.chunk_size < 1:
        raise SystemExit("--chunk-size must be at least 1")
    if not args.db.exists():
        raise SystemExit(f"Database not found: {args.db}")
    if args.apply and not args.yes_i_have_a_backup:
        raise SystemExit("--apply requires --yes-i-have-a-backup")

    connection = sqlite3.connect(args.db)
    try:
        connection.execute("PRAGMA foreign_keys=ON")
        cutoff = calculate_cutoff(connection, args.retention_days)
        print_report(connection, cutoff)
        if not args.apply:
            print("dry_run: true")
            return 0

        print("dry_run: false")
        deleted_flows = delete_in_chunks(
            connection,
            "wan_flow_usage",
            "started_at",
            cutoff.flow_started_at,
            args.chunk_size,
        )
        deleted_identities = delete_in_chunks(
            connection,
            "client_ip_identities",
            "observed_at",
            cutoff.identity_observed_at,
            args.chunk_size,
        )
        print(f"deleted_wan_flow_usage_rows: {deleted_flows}")
        print(f"deleted_client_ip_identity_rows: {deleted_identities}")

        if args.vacuum:
            print("running_vacuum: true", flush=True)
            connection.execute("VACUUM")
            connection.execute("ANALYZE")
            connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            print("vacuum_complete: true")

        quick_check = connection.execute("PRAGMA quick_check").fetchone()[0]
        print(f"quick_check: {quick_check}")
        print_report(connection, cutoff)
    finally:
        connection.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
