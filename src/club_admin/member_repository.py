'''Persistence operations for club users.'''

import sqlite3
import re

from club_admin.models import Member


def _empty_to_none(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


def normalize_phone(value: str | None) -> str:
    '''Return only digits from a phone value for matching.'''
    return re.sub(r"\D+", "", value or "")


def normalize_initials(value: str | None) -> str:
    '''Return uppercase letters from an initials value.'''
    return re.sub(r"[^A-Za-z]+", "", value or "").upper()


def member_initials(member: Member) -> str:
    '''Return first-name/last-name initials for a member.'''
    first_initial = member.first_name[:1].upper()
    last_initial = member.last_name[:1].upper()
    return f"{first_initial}{last_initial}"


def member_from_row(row: sqlite3.Row) -> Member:
    '''Build a Member from a SQLite row.'''
    return Member(
        id=row["id"],
        last_name=row["last_name"],
        first_name=row["first_name"],
        card_number=row["card_number"],
        membership=row["membership"],
        address=row["address"],
        address2=row["address2"],
        city=row["city"],
        state=row["state"],
        zip=row["zip"],
        phone=row["phone"],
        email=row["email"],
        work_phone=row["work_phone"],
        cell_phone=row["cell_phone"],
    )


def upsert_member(connection: sqlite3.Connection, member: Member) -> None:
    '''Insert or update a club user matched by card number.'''
    connection.execute(
        """
        INSERT INTO users (
            last_name,
            first_name,
            card_number,
            membership,
            address,
            address2,
            city,
            state,
            zip,
            phone,
            email,
            work_phone,
            cell_phone
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(card_number) DO UPDATE SET
            last_name = excluded.last_name,
            first_name = excluded.first_name,
            membership = excluded.membership,
            address = excluded.address,
            address2 = excluded.address2,
            city = excluded.city,
            state = excluded.state,
            zip = excluded.zip,
            phone = excluded.phone,
            email = excluded.email,
            work_phone = excluded.work_phone,
            cell_phone = excluded.cell_phone
        """,
        (
            member.last_name.strip(),
            member.first_name.strip(),
            member.card_number.strip(),
            member.membership.strip(),
            _empty_to_none(member.address),
            _empty_to_none(member.address2),
            _empty_to_none(member.city),
            _empty_to_none(member.state),
            _empty_to_none(member.zip),
            _empty_to_none(member.phone),
            _empty_to_none(member.email),
            _empty_to_none(member.work_phone),
            _empty_to_none(member.cell_phone),
        ),
    )


def list_members(connection: sqlite3.Connection) -> list[Member]:
    '''Return all club users, sorted for roster display.'''
    rows = connection.execute(
        """
        SELECT
            id,
            last_name,
            first_name,
            card_number,
            membership,
            address,
            address2,
            city,
            state,
            zip,
            phone,
            email,
            work_phone,
            cell_phone
        FROM users
        ORDER BY last_name, first_name, card_number
        """
    ).fetchall()
    return [member_from_row(row) for row in rows]


def get_member(connection: sqlite3.Connection, member_id: int) -> Member | None:
    '''Return one club user by database ID.'''
    row = connection.execute(
        """
        SELECT
            id,
            last_name,
            first_name,
            card_number,
            membership,
            address,
            address2,
            city,
            state,
            zip,
            phone,
            email,
            work_phone,
            cell_phone
        FROM users
        WHERE id = ?
        """,
        (member_id,),
    ).fetchone()
    return member_from_row(row) if row is not None else None


def find_member_by_phone(connection: sqlite3.Connection, phone: str) -> Member | None:
    '''Return a member when exactly one phone/work/cell field matches.'''
    target_digits = normalize_phone(phone)
    if len(target_digits) < 7:
        return None

    matches: list[Member] = []
    for member in list_members(connection):
        member_numbers = {
            normalize_phone(member.phone),
            normalize_phone(member.work_phone),
            normalize_phone(member.cell_phone),
        }
        if target_digits in member_numbers:
            matches.append(member)

    return matches[0] if len(matches) == 1 else None


def find_member_by_phone_and_initials(
    connection: sqlite3.Connection,
    phone: str,
    initials: str,
) -> Member | None:
    '''Return a member when phone plus first/last initials match exactly one user.'''
    target_digits = normalize_phone(phone)
    target_initials = normalize_initials(initials)
    if len(target_digits) < 7:
        return None
    if len(target_initials) != 2:
        return None

    matches: list[Member] = []
    for member in list_members(connection):
        member_numbers = {
            normalize_phone(member.phone),
            normalize_phone(member.work_phone),
            normalize_phone(member.cell_phone),
        }
        if target_digits in member_numbers and member_initials(member) == target_initials:
            matches.append(member)

    return matches[0] if len(matches) == 1 else None


def update_member(connection: sqlite3.Connection, member: Member) -> None:
    '''Update all editable fields for an existing club user.'''
    if member.id is None:
        raise ValueError("member.id is required for update.")

    connection.execute(
        """
        UPDATE users
        SET
            last_name = ?,
            first_name = ?,
            card_number = ?,
            membership = ?,
            address = ?,
            address2 = ?,
            city = ?,
            state = ?,
            zip = ?,
            phone = ?,
            email = ?,
            work_phone = ?,
            cell_phone = ?
        WHERE id = ?
        """,
        (
            member.last_name.strip(),
            member.first_name.strip(),
            member.card_number.strip(),
            member.membership.strip(),
            _empty_to_none(member.address),
            _empty_to_none(member.address2),
            _empty_to_none(member.city),
            _empty_to_none(member.state),
            _empty_to_none(member.zip),
            _empty_to_none(member.phone),
            _empty_to_none(member.email),
            _empty_to_none(member.work_phone),
            _empty_to_none(member.cell_phone),
            member.id,
        ),
    )
