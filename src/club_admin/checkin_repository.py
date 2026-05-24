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
    check_in_dates: tuple[datetime, ...]


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
            first_name = excluded.first_name
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


def update_checkin_for_user(connection: sqlite3.Connection, checkin: CheckIn) -> None:
    '''Update one existing check-in row for the owning user.'''
    if checkin.id is None or checkin.user_id is None:
        raise ValueError("Check-in id and user id are required to update a check-in.")

    result = connection.execute(
        """
        UPDATE checkins
        SET
            member_id = ?,
            last_name = ?,
            first_name = ?,
            card_number = ?,
            check_in_at = ?,
            check_out_at = ?,
            total_checkins = ?,
            duration = ?,
            membership = ?
        WHERE id = ? AND user_id = ?
        """,
        (
            checkin.member_id,
            checkin.last_name.strip(),
            checkin.first_name.strip(),
            checkin.card_number.strip(),
            _datetime_to_text(checkin.check_in_at),
            _datetime_to_text(checkin.check_out_at),
            checkin.total_checkins,
            checkin.duration,
            checkin.membership.strip(),
            checkin.id,
            checkin.user_id,
        ),
    )
    if result.rowcount != 1:
        raise ValueError("Check-in was not found for this user.")


def delete_checkin_for_user(
    connection: sqlite3.Connection,
    *,
    checkin_id: int,
    user_id: int,
) -> None:
    '''Delete one check-in row for the owning user.'''
    connection.execute(
        "DELETE FROM checkins WHERE id = ? AND user_id = ?",
        (checkin_id, user_id),
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
    '''Return check-ins in an inclusive date range, sorted by user name.'''
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
        ORDER BY last_name, first_name, card_number, check_in_at DESC
        """,
        (start_at, exclusive_end_at),
    ).fetchall()
    return [checkin_from_row(row) for row in rows]


def summarize_checkins_by_user(
    connection: sqlite3.Connection,
    start_date: date,
    end_date: date,
) -> list[UserCheckInSummary]:
    '''Return check-in counts and dates by user for an inclusive date range.'''
    start_at, exclusive_end_at = _date_range_bounds(start_date, end_date)
    rows = connection.execute(
        """
        SELECT
            c.user_id,
            u.last_name,
            u.first_name,
            u.card_number,
            u.membership,
            c.check_in_at
        FROM checkins c
        JOIN users u ON u.id = c.user_id
        WHERE c.check_in_at >= ? AND c.check_in_at < ?
        ORDER BY c.check_in_at DESC, u.last_name, u.first_name, u.card_number
        """,
        (start_at, exclusive_end_at),
    ).fetchall()

    summaries_by_user: dict[int, dict[str, object]] = {}
    for row in rows:
        user_id = int(row["user_id"])
        summary = summaries_by_user.setdefault(
            user_id,
            {
                "user_id": user_id,
                "last_name": row["last_name"],
                "first_name": row["first_name"],
                "card_number": row["card_number"],
                "membership": row["membership"],
                "check_in_dates": [],
            },
        )
        check_in_dates = summary["check_in_dates"]
        assert isinstance(check_in_dates, list)
        check_in_dates.append(datetime.fromisoformat(row["check_in_at"]))

    summaries = []
    for summary in summaries_by_user.values():
        check_in_dates = summary["check_in_dates"]
        assert isinstance(check_in_dates, list)
        typed_check_in_dates = tuple(check_in_dates)
        summaries.append(
            UserCheckInSummary(
                user_id=int(summary["user_id"]),
                last_name=str(summary["last_name"]),
                first_name=str(summary["first_name"]),
                card_number=str(summary["card_number"]),
                membership=str(summary["membership"]),
                checkin_count=len(typed_check_in_dates),
                first_check_in_at=min(typed_check_in_dates),
                last_check_in_at=max(typed_check_in_dates),
                check_in_dates=typed_check_in_dates,
            )
        )

    return sorted(
        summaries,
        key=lambda summary: (
            -summary.checkin_count,
            summary.last_name,
            summary.first_name,
            summary.card_number,
        ),
    )
