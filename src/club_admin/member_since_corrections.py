'''Conservative utility for correcting imported member-since dates.'''

import argparse
import csv
import re
import sqlite3
from collections import defaultdict
from contextlib import closing
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Sequence

from club_admin import audit_repository, database, member_repository
from club_admin.models import Member


SUSPECT_MEMBER_SINCE_DATES = {
    date(2025, 4, 5),
    date(2025, 4, 6),
    date(2026, 4, 5),
    date(2026, 4, 6),
}


@dataclass(frozen=True, kw_only=True)
class CorrectionRow:
    row_number: int
    raw_name: str
    member_since: date | None
    name_key: tuple[str, str] | None


@dataclass(frozen=True, kw_only=True)
class CorrectionResult:
    row: CorrectionRow
    status: str
    message: str
    member: Member | None = None


def _normalized_header(value: str) -> str:
    return " ".join(value.strip().lower().split())


def _normalized_name_part(value: str) -> str:
    return re.sub(r"[^a-z]+", "", value.lower())


def _name_words(value: str) -> list[str]:
    return re.findall(r"[A-Za-z]+", value)


def name_key_from_correction(value: str) -> tuple[str, str] | None:
    '''Return a normalized first/last key for supported correction-file names.'''
    stripped = value.strip()
    if not stripped:
        return None
    if "," in stripped:
        last_name, first_names = stripped.split(",", 1)
        first_words = _name_words(first_names)
        last_words = _name_words(last_name)
        if not first_words or not last_words:
            return None
        return _normalized_name_part(first_words[0]), _normalized_name_part(last_words[-1])

    words = _name_words(stripped)
    if len(words) < 2:
        return None
    return _normalized_name_part(words[0]), _normalized_name_part(words[-1])


def name_key_from_member(member: Member) -> tuple[str, str]:
    '''Return the normalized first/last key used for conservative matching.'''
    return _normalized_name_part(member.first_name), _normalized_name_part(member.last_name)


def parse_correction_date(value: str) -> date:
    '''Parse dates used in the Date Joined correction CSV.'''
    stripped = value.strip()
    for date_format in ("%m/%d/%y", "%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(stripped, date_format).date()
        except ValueError:
            pass
    raise ValueError(f"Unsupported date joined value: {value}")


def read_corrections(path: Path) -> list[CorrectionRow]:
    '''Read the correction CSV exported as Customer full name, Date Joined.'''
    with path.open(newline="", encoding="utf-8-sig") as source:
        reader = csv.DictReader(source)
        if reader.fieldnames is None:
            raise ValueError("Correction CSV does not contain a header row.")
        header_map = {_normalized_header(header): header for header in reader.fieldnames}
        name_header = header_map.get("customer full name")
        date_header = header_map.get("date joined")
        if name_header is None or date_header is None:
            raise ValueError("Correction CSV must include Customer full name and Date Joined.")

        corrections = []
        for row_number, row in enumerate(reader, start=2):
            raw_name = (row.get(name_header) or "").strip()
            raw_date = (row.get(date_header) or "").strip()
            if not raw_name and not raw_date:
                continue
            parsed_date = parse_correction_date(raw_date) if raw_date else None
            corrections.append(
                CorrectionRow(
                    row_number=row_number,
                    raw_name=raw_name,
                    member_since=parsed_date,
                    name_key=name_key_from_correction(raw_name),
                )
            )
    return corrections


def _members_by_name_key(members: Sequence[Member]) -> dict[tuple[str, str], list[Member]]:
    by_key: dict[tuple[str, str], list[Member]] = defaultdict(list)
    for member in members:
        by_key[name_key_from_member(member)].append(member)
    return by_key


def plan_corrections(
    members: Sequence[Member],
    corrections: Sequence[CorrectionRow],
    *,
    only_suspect_dates: bool = True,
) -> list[CorrectionResult]:
    '''Return conservative correction decisions without mutating the database.'''
    members_by_key = _members_by_name_key(members)
    correction_key_counts: dict[tuple[str, str], int] = defaultdict(int)
    for correction in corrections:
        if correction.name_key is not None:
            correction_key_counts[correction.name_key] += 1

    results = []
    for correction in corrections:
        if correction.member_since is None:
            results.append(
                CorrectionResult(
                    row=correction,
                    status="skipped",
                    message="missing date joined",
                )
            )
            continue
        if correction.name_key is None:
            results.append(
                CorrectionResult(
                    row=correction,
                    status="skipped",
                    message="could not parse name",
                )
            )
            continue
        if correction_key_counts[correction.name_key] > 1:
            results.append(
                CorrectionResult(
                    row=correction,
                    status="skipped",
                    message="duplicate correction name",
                )
            )
            continue

        matches = members_by_key.get(correction.name_key, [])
        if not matches:
            results.append(
                CorrectionResult(row=correction, status="skipped", message="no user match")
            )
            continue
        if len(matches) > 1:
            results.append(
                CorrectionResult(
                    row=correction,
                    status="skipped",
                    message=f"ambiguous user match ({len(matches)} users)",
                )
            )
            continue

        member = matches[0]
        if member.id is None:
            results.append(
                CorrectionResult(
                    row=correction,
                    status="skipped",
                    message="matched user has no database id",
                    member=member,
                )
            )
            continue
        if member.member_since == correction.member_since:
            results.append(
                CorrectionResult(
                    row=correction,
                    status="unchanged",
                    message="already correct",
                    member=member,
                )
            )
            continue
        if only_suspect_dates and member.member_since not in SUSPECT_MEMBER_SINCE_DATES:
            results.append(
                CorrectionResult(
                    row=correction,
                    status="skipped",
                    message=f"current date is not suspect ({member.member_since or 'blank'})",
                    member=member,
                )
            )
            continue

        results.append(
            CorrectionResult(
                row=correction,
                status="ready",
                message="confident match",
                member=member,
            )
        )
    return results


def apply_corrections(connection: sqlite3.Connection, results: Sequence[CorrectionResult]) -> int:
    '''Apply ready correction results and record audit-log entries.'''
    applied_count = 0
    for result in results:
        if result.status != "ready" or result.member is None or result.member.id is None:
            continue
        connection.execute(
            "UPDATE users SET member_since = ? WHERE id = ?",
            # The row date is guaranteed by the ready status in plan_corrections.
            (result.row.member_since.isoformat(), result.member.id),
        )
        audit_repository.record_field_change(
            connection,
            entity_type="user",
            entity_id=result.member.id,
            action="edit",
            field_name="member_since",
            old_value=result.member.member_since,
            new_value=result.row.member_since,
        )
        applied_count += 1
    return applied_count


def _result_line(result: CorrectionResult) -> str:
    member = result.member
    new_date = result.row.member_since.isoformat() if result.row.member_since else ""
    member_text = (
        f"user_id={member.id} {member.first_name} {member.last_name} "
        f"old={member.member_since or ''}"
        if member is not None
        else "user_id="
    )
    return (
        f"{result.status.upper():9} row={result.row.row_number} "
        f"name={result.row.raw_name!r} new={new_date} "
        f"{member_text} reason={result.message}"
    )


def _print_results(results: Sequence[CorrectionResult]) -> None:
    counts: dict[str, int] = defaultdict(int)
    for result in results:
        counts[result.status] += 1

    print(
        "Summary: "
        + ", ".join(f"{status}={counts[status]}" for status in sorted(counts))
    )
    for result in results:
        print(_result_line(result))


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Conservatively correct user member-since dates from a CSV."
    )
    parser.add_argument("corrections_csv", type=Path)
    parser.add_argument(
        "--db",
        type=Path,
        default=database.get_db_path(),
        help="SQLite database path. Defaults to the configured club admin DB.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply ready corrections. Default is dry-run only.",
    )
    parser.add_argument(
        "--include-non-suspect",
        action="store_true",
        help="Allow changes even when current member_since is not 2026-04-05/2026-04-06.",
    )
    args = parser.parse_args(argv)

    database.init_db(args.db)
    corrections = read_corrections(args.corrections_csv)
    with closing(database.connect(args.db)) as connection:
        results = plan_corrections(
            member_repository.list_members(connection),
            corrections,
            only_suspect_dates=not args.include_non_suspect,
        )
        _print_results(results)
        if args.apply:
            applied_count = apply_corrections(connection, results)
            connection.commit()
            print(f"Applied {applied_count} corrections.")
        else:
            print("Dry run only. Pass --apply to update the database.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
