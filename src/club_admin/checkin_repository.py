'''Persistence operations for club user check-ins.'''

import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, timedelta

from club_admin.models import CheckIn


@dataclass(frozen=True, kw_only=True)
class UserCheckInSummary:
    '''Aggregated check-in counts for one club user.'''

    user_id: int
    last_name: str
    first_name: str
    card_number: str
    membership: str
    checkin_count: int
    first_check_in_at: datetime
    last_check_in_at: datetime


def _datetime_to_text(value: datetime | None) -> str | None:
    return value.isoformat(timespec="seconds") if value is not None else None


def _text_to_datetime(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None


def _date_range_bounds(start_date: date, end_date: date) -> tuple[str, str]:
    start_at = datetime.combine(start_date, datetime.min.time()).isoformat(timespec="seconds")
    exclusive_end_at = datetime.combine(
        end_date + timedelta(days=1),
        datetime.min.time(),
    ).isoformat(timespec="seconds")
    return start_at, exclusive_end_at


def _ensure_user_for_checkin(connection: sqlite3.Connection, checkin: CheckIn) -> int:
    connection.execute(
        """
        INSERT INTO users (
            last_name,
            first_name,
            card_number,
            membership
        )
        VALUES (?, ?, ?, ?)
        ON CONFLICT(card_number) DO UPDATE SET
            last_name = excluded.last_name,
            first_name = excluded.first_name,
            membership = excluded.membership
        """,
        (
            checkin.last_name.strip(),
            checkin.first_name.strip(),
            checkin.card_number.strip(),
            checkin.membership.strip(),
        ),
    )
    row = connection.execute(
        "SELECT id FROM users WHERE card_number = ?",
        (checkin.card_number.strip(),),
    ).fetchone()
    if row is None:
        raise sqlite3.IntegrityError("Could not create or find user for check-in.")
    return int(row["id"])


def checkin_from_row(row: sqlite3.Row) -> CheckIn:
    '''Build a CheckIn from a SQLite row.'''
    return CheckIn(
        id=row["id"],
        user_id=row["user_id"],
        member_id=row["member_id"],
        last_name=row["last_name"],
        first_name=row["first_name"],
        card_number=row["card_number"],
        check_in_at=datetime.fromisoformat(row["check_in_at"]),
        check_out_at=_text_to_datetime(row["check_out_at"]),
        total_checkins=row["total_checkins"],
        duration=row["duration"],
        membership=row["membership"],
    )


def upsert_checkin(connection: sqlite3.Connection, checkin: CheckIn) -> None:
    '''Insert or update a check-in matched by member, card, and check-in time.'''
    user_id = _ensure_user_for_checkin(connection, checkin)
    connection.execute(
        """
        INSERT INTO checkins (
            user_id,
            member_id,
            last_name,
            first_name,
            card_number,
            check_in_at,
            check_out_at,
            total_checkins,
            duration,
            membership
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(member_id, card_number, check_in_at) DO UPDATE SET
            user_id = excluded.user_id,
            last_name = excluded.last_name,
            first_name = excluded.first_name,
            check_out_at = excluded.check_out_at,
            total_checkins = excluded.total_checkins,
            duration = excluded.duration,
            membership = excluded.membership
        """,
        (
            user_id,
            checkin.member_id,
            checkin.last_name.strip(),
            checkin.first_name.strip(),
            checkin.card_number.strip(),
            _datetime_to_text(checkin.check_in_at),
            _datetime_to_text(checkin.check_out_at),
            checkin.total_checkins,
            checkin.duration,
            checkin.membership.strip(),
        ),
    )


def list_checkins(connection: sqlite3.Connection) -> list[CheckIn]:
    '''Return check-ins, newest first.'''
    rows = connection.execute(
        """
        SELECT
            id,
            user_id,
            member_id,
            last_name,
            first_name,
            card_number,
            check_in_at,
            check_out_at,
            total_checkins,
            duration,
            membership
        FROM checkins
        ORDER BY check_in_at DESC, last_name, first_name
        """
    ).fetchall()
    return [checkin_from_row(row) for row in rows]


def list_checkins_for_user(connection: sqlite3.Connection, user_id: int) -> list[CheckIn]:
    '''Return check-ins for one user, newest first.'''
    rows = connection.execute(
        """
        SELECT
            id,
            user_id,
            member_id,
            last_name,
            first_name,
            card_number,
            check_in_at,
            check_out_at,
            total_checkins,
            duration,
            membership
        FROM checkins
        WHERE user_id = ?
        ORDER BY check_in_at DESC
        """,
        (user_id,),
    ).fetchall()
    return [checkin_from_row(row) for row in rows]


def list_checkins_for_date_range(
    connection: sqlite3.Connection,
    start_date: date,
    end_date: date,
) -> list[CheckIn]:
    '''Return check-ins in an inclusive date range, newest first.'''
    start_at, exclusive_end_at = _date_range_bounds(start_date, end_date)
    rows = connection.execute(
        """
        SELECT
            id,
            user_id,
            member_id,
            last_name,
            first_name,
            card_number,
            check_in_at,
            check_out_at,
            total_checkins,
            duration,
            membership
        FROM checkins
        WHERE check_in_at >= ? AND check_in_at < ?
        ORDER BY check_in_at DESC, last_name, first_name
        """,
        (start_at, exclusive_end_at),
    ).fetchall()
    return [checkin_from_row(row) for row in rows]


def summarize_checkins_by_user(
    connection: sqlite3.Connection,
    start_date: date,
    end_date: date,
) -> list[UserCheckInSummary]:
    '''Return check-in counts by user for an inclusive date range.'''
    start_at, exclusive_end_at = _date_range_bounds(start_date, end_date)
    rows = connection.execute(
        """
        SELECT
            c.user_id,
            u.last_name,
            u.first_name,
            u.card_number,
            u.membership,
            COUNT(*) AS checkin_count,
            MIN(c.check_in_at) AS first_check_in_at,
            MAX(c.check_in_at) AS last_check_in_at
        FROM checkins c
        JOIN users u ON u.id = c.user_id
        WHERE c.check_in_at >= ? AND c.check_in_at < ?
        GROUP BY c.user_id, u.last_name, u.first_name, u.card_number, u.membership
        ORDER BY checkin_count DESC, u.last_name, u.first_name, u.card_number
        """,
        (start_at, exclusive_end_at),
    ).fetchall()
    return [
        UserCheckInSummary(
            user_id=row["user_id"],
            last_name=row["last_name"],
            first_name=row["first_name"],
            card_number=row["card_number"],
            membership=row["membership"],
            checkin_count=row["checkin_count"],
            first_check_in_at=datetime.fromisoformat(row["first_check_in_at"]),
            last_check_in_at=datetime.fromisoformat(row["last_check_in_at"]),
        )
        for row in rows
    ]
