#!/usr/bin/env python3
"""Prune imported nfdump capture files from the production cache."""

from __future__ import annotations

import argparse
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path


DEFAULT_CAPTURE_DIR = Path("/var/cache/nfdump")
DEFAULT_DB_PATH = Path("/home/daveb/devel/ubiquiti-usage-manager/meter.db")
DEFAULT_RETENTION_DAYS = 3
COMPLETED_CAPTURE_PATTERN = re.compile(r"^nfcapd\.[0-9]{12}$")


@dataclass(frozen=True)
class PruneResult:
    cutoff: datetime
    imported_names: int
    scanned_files: int
    candidate_files: int
    deleted_files: int
    deleted_bytes: int
    skipped_recent_files: int
    skipped_unimported_files: int
    skipped_non_capture_files: int
    errors: tuple[str, ...]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Delete completed nfdump capture files only after they have been "
            "recorded in the meter database flow_imports table."
        )
    )
    parser.add_argument("--capture-dir", type=Path, default=DEFAULT_CAPTURE_DIR)
    parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--retention-days", type=int, default=DEFAULT_RETENTION_DAYS)
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args()


def imported_capture_names(db_path: Path) -> set[str]:
    connection = sqlite3.connect(db_path)
    try:
        rows = connection.execute("SELECT source_file FROM flow_imports").fetchall()
    finally:
        connection.close()
    return {str(row[0]) for row in rows if row and row[0]}


def prune_captures(
    *,
    capture_dir: Path,
    db_path: Path,
    retention_days: int,
    apply: bool,
) -> PruneResult:
    if retention_days < 1:
        raise ValueError("retention_days must be at least 1")
    if not capture_dir.is_dir():
        raise FileNotFoundError(f"Capture directory not found: {capture_dir}")
    if not db_path.is_file():
        raise FileNotFoundError(f"Database not found: {db_path}")

    cutoff = datetime.now() - timedelta(days=retention_days)
    imported = imported_capture_names(db_path)
    scanned_files = 0
    candidate_files = 0
    deleted_files = 0
    deleted_bytes = 0
    skipped_recent_files = 0
    skipped_unimported_files = 0
    skipped_non_capture_files = 0
    errors: list[str] = []

    for path in capture_dir.iterdir():
        if not path.is_file():
            continue
        scanned_files += 1
        if not COMPLETED_CAPTURE_PATTERN.fullmatch(path.name):
            skipped_non_capture_files += 1
            continue
        try:
            stat = path.stat()
        except FileNotFoundError:
            continue
        if datetime.fromtimestamp(stat.st_mtime) >= cutoff:
            skipped_recent_files += 1
            continue
        if path.name not in imported:
            skipped_unimported_files += 1
            continue

        candidate_files += 1
        if not apply:
            deleted_bytes += stat.st_size
            continue

        try:
            path.unlink()
        except FileNotFoundError:
            continue
        except OSError as exc:
            errors.append(f"{path}: {exc}")
            continue
        deleted_files += 1
        deleted_bytes += stat.st_size

    return PruneResult(
        cutoff=cutoff,
        imported_names=len(imported),
        scanned_files=scanned_files,
        candidate_files=candidate_files,
        deleted_files=deleted_files,
        deleted_bytes=deleted_bytes,
        skipped_recent_files=skipped_recent_files,
        skipped_unimported_files=skipped_unimported_files,
        skipped_non_capture_files=skipped_non_capture_files,
        errors=tuple(errors),
    )


def print_result(result: PruneResult, *, apply: bool) -> None:
    print(f"dry_run: {str(not apply).lower()}")
    print(f"cutoff: {result.cutoff.isoformat(timespec='seconds')}")
    print(f"imported_names: {result.imported_names}")
    print(f"scanned_files: {result.scanned_files}")
    print(f"candidate_files: {result.candidate_files}")
    print(f"deleted_files: {result.deleted_files}")
    print(f"reclaimable_or_deleted_bytes: {result.deleted_bytes}")
    print(f"reclaimable_or_deleted_gib: {result.deleted_bytes / 1024 / 1024 / 1024:.2f}")
    print(f"reclaimable_or_deleted_gb: {result.deleted_bytes / 1000 / 1000 / 1000:.2f}")
    print(f"skipped_recent_files: {result.skipped_recent_files}")
    print(f"skipped_unimported_files: {result.skipped_unimported_files}")
    print(f"skipped_non_capture_files: {result.skipped_non_capture_files}")
    print(f"errors: {len(result.errors)}")
    for error in result.errors[:5]:
        print(f"error: {error}")


def main() -> int:
    args = parse_args()
    result = prune_captures(
        capture_dir=args.capture_dir,
        db_path=args.db,
        retention_days=args.retention_days,
        apply=args.apply,
    )
    print_result(result, apply=args.apply)
    return 1 if result.errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
