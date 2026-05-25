'''Persistence operations for first-time visitor guest registrations.'''

import sqlite3
from dataclasses import dataclass
from datetime import date, datetime

from club_admin.member_repository import format_phone_number, member_from_row
from club_admin.models import GuestRegistration, Member


@dataclass(frozen=True, kw_only=True)
class GuestRegistrationRecord:
    '''A guest registration joined to its created user.'''

    registration: GuestRegistration
    member: Member


def _empty_to_none(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


def _registration_from_row(row: sqlite3.Row) -> GuestRegistration:
    return GuestRegistration(
        id=row["registration_id"],
        user_id=row["user_id"],
        visit_date=date.fromisoformat(row["visit_date"]),
        middle_name=row["middle_name"],
        other_phone=format_phone_number(row["other_phone"]),
        other_phone_type=row["other_phone_type"],
        marital_status=row["marital_status"],
        partner_name=row["partner_name"],
        guest_of_member=bool(row["guest_of_member"]),
        member_name=row["member_name"],
        heard_about=row["heard_about"],
        newsletter_opt_out=bool(row["newsletter_opt_out"]),
        created_at=datetime.fromisoformat(row["created_at"]) if row["created_at"] else None,
    )


def _record_from_row(row: sqlite3.Row) -> GuestRegistrationRecord:
    return GuestRegistrationRecord(
        registration=_registration_from_row(row),
        member=member_from_row(row),
    )


def insert_guest_registration(
    connection: sqlite3.Connection,
    registration: GuestRegistration,
) -> int:
    '''Insert one first-time visitor registration and return its ID.'''
    if registration.user_id is None:
        raise ValueError("registration.user_id is required.")

    cursor = connection.execute(
        """
        INSERT INTO guest_registrations (
            user_id,
            visit_date,
            middle_name,
            other_phone,
            other_phone_type,
            marital_status,
            partner_name,
            guest_of_member,
            member_name,
            heard_about,
            newsletter_opt_out
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            registration.user_id,
            registration.visit_date.isoformat(),
            _empty_to_none(registration.middle_name),
            format_phone_number(registration.other_phone),
            _empty_to_none(registration.other_phone_type),
            _empty_to_none(registration.marital_status),
            _empty_to_none(registration.partner_name),
            1 if registration.guest_of_member else 0,
            _empty_to_none(registration.member_name),
            _empty_to_none(registration.heard_about),
            1 if registration.newsletter_opt_out else 0,
        ),
    )
    return int(cursor.lastrowid)


def _guest_registration_select_sql() -> str:
    return """
        SELECT
            g.id AS registration_id,
            g.user_id,
            g.visit_date,
            g.middle_name,
            g.other_phone,
            g.other_phone_type,
            g.marital_status,
            g.partner_name,
            g.guest_of_member,
            g.member_name,
            g.heard_about,
            g.newsletter_opt_out,
            g.created_at,
            u.id,
            u.last_name,
            u.first_name,
            u.nickname,
            u.card_number,
            u.membership,
            u.member_since,
            u.date_of_birth,
            u.address,
            u.address2,
            u.city,
            u.state,
            u.zip,
            u.phone,
            u.email,
            u.work_phone,
            u.cell_phone
        FROM guest_registrations g
        JOIN users u ON u.id = g.user_id
    """


def list_guest_registration_records(
    connection: sqlite3.Connection,
    *,
    limit: int = 50,
) -> list[GuestRegistrationRecord]:
    '''Return recent first-time visitor registrations.'''
    rows = connection.execute(
        _guest_registration_select_sql()
        + """
        ORDER BY g.created_at DESC, g.id DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return [_record_from_row(row) for row in rows]


def get_guest_registration_record(
    connection: sqlite3.Connection,
    registration_id: int,
) -> GuestRegistrationRecord | None:
    '''Return one first-time visitor registration.'''
    row = connection.execute(
        _guest_registration_select_sql()
        + """
        WHERE g.id = ?
        """,
        (registration_id,),
    ).fetchone()
    return _record_from_row(row) if row is not None else None
