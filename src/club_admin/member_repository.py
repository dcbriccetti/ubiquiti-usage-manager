'''Persistence operations for club users.'''

import sqlite3
import re
from dataclasses import dataclass
from datetime import date, datetime

from club_admin.models import Member


@dataclass(frozen=True, kw_only=True)
class MemberReportRow:
    '''One row for the admin users report.'''

    member: Member
    address: str
    address_lines: tuple[str, ...]
    checkin_count: int | None
    last_check_in_at: datetime | None
    document_count: int = 0


def _empty_to_none(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


def _collapse_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def _date_to_text(value: date | None) -> str | None:
    return value.isoformat() if value is not None else None


def _text_to_date(value: str | None) -> date | None:
    return date.fromisoformat(value) if value else None


def _years_before(value: date, years: int) -> date:
    try:
        return value.replace(year=value.year - years)
    except ValueError:
        return date(value.year - years, 2, 28)


def _member_visit_period_start(membership: str, as_of_date: date) -> date | None:
    if membership == "AANR Member":
        return _years_before(as_of_date, 1)
    if membership == "Visitor":
        return _years_before(as_of_date, 2)
    return None


def normalize_phone(value: str | None) -> str:
    '''Return only digits from a phone value for matching.'''
    digits = re.sub(r"\D+", "", value or "")
    if len(digits) == 11 and digits.startswith("1"):
        return digits[1:]
    return digits


def format_phone_number(value: str | None) -> str | None:
    '''Return a display-friendly phone number when the value looks domestic.'''
    stripped_value = _empty_to_none(value)
    if stripped_value is None:
        return None

    extension = ""
    base_value = stripped_value
    extension_match = re.search(
        r"(?i)\s*(?:ext\.?|extension|x)\s*(\d+)\s*$",
        stripped_value,
    )
    if extension_match:
        extension = f" x{extension_match.group(1)}"
        base_value = stripped_value[: extension_match.start()]

    digits = normalize_phone(base_value)
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}{extension}"
    if len(digits) == 7:
        return f"{digits[:3]}-{digits[3:]}{extension}"
    return _collapse_whitespace(stripped_value)


def _phone_to_none(value: str | None) -> str | None:
    return format_phone_number(value)


def normalize_name_token(value: str | None) -> str:
    '''Return uppercase letters from a name/check-in identity value.'''
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
        nickname=row["nickname"],
        card_number=row["card_number"],
        membership=row["membership"],
        member_since=_text_to_date(row["member_since"]),
        date_of_birth=_text_to_date(row["date_of_birth"]),
        address=row["address"],
        address2=row["address2"],
        city=row["city"],
        state=row["state"],
        zip=row["zip"],
        phone=format_phone_number(row["phone"]),
        email=row["email"],
        work_phone=format_phone_number(row["work_phone"]),
        cell_phone=format_phone_number(row["cell_phone"]),
    )


def format_member_address(member: Member) -> str:
    '''Return a compact single-line address for report display.'''
    return ", ".join(format_member_address_lines(member))


def format_member_address_lines(member: Member) -> tuple[str, ...]:
    '''Return address display lines for the admin users report.'''
    street_parts = [part for part in (member.address, member.address2) if part]
    city_state_zip = " ".join(part for part in (member.city, member.state, member.zip) if part)
    parts: list[str] = [*street_parts]
    if city_state_zip:
        parts.append(city_state_zip)
    return tuple(parts)


def upsert_member(connection: sqlite3.Connection, member: Member) -> None:
    '''Insert or update a club user matched by card number.'''
    connection.execute(
        """
        INSERT INTO users (
            last_name,
            first_name,
            nickname,
            card_number,
            membership,
            member_since,
            date_of_birth,
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
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(card_number) DO UPDATE SET
            last_name = excluded.last_name,
            first_name = excluded.first_name,
            nickname = excluded.nickname,
            membership = excluded.membership,
            member_since = excluded.member_since,
            date_of_birth = excluded.date_of_birth,
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
            _empty_to_none(member.nickname),
            member.card_number.strip(),
            member.membership.strip(),
            _date_to_text(member.member_since),
            _date_to_text(member.date_of_birth),
            _empty_to_none(member.address),
            _empty_to_none(member.address2),
            _empty_to_none(member.city),
            _empty_to_none(member.state),
            _empty_to_none(member.zip),
            _phone_to_none(member.phone),
            _empty_to_none(member.email),
            _phone_to_none(member.work_phone),
            _phone_to_none(member.cell_phone),
        ),
    )


def insert_member(connection: sqlite3.Connection, member: Member) -> int:
    '''Insert a new club user and return its database ID.'''
    cursor = connection.execute(
        """
        INSERT INTO users (
            last_name,
            first_name,
            nickname,
            card_number,
            membership,
            member_since,
            date_of_birth,
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
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            member.last_name.strip(),
            member.first_name.strip(),
            _empty_to_none(member.nickname),
            member.card_number.strip(),
            member.membership.strip(),
            _date_to_text(member.member_since),
            _date_to_text(member.date_of_birth),
            _empty_to_none(member.address),
            _empty_to_none(member.address2),
            _empty_to_none(member.city),
            _empty_to_none(member.state),
            _empty_to_none(member.zip),
            _phone_to_none(member.phone),
            _empty_to_none(member.email),
            _phone_to_none(member.work_phone),
            _phone_to_none(member.cell_phone),
        ),
    )
    return int(cursor.lastrowid)


def list_members(connection: sqlite3.Connection) -> list[Member]:
    '''Return all club users, sorted for roster display.'''
    rows = connection.execute(
        """
        SELECT
            id,
            last_name,
            first_name,
            nickname,
            card_number,
            membership,
            member_since,
            date_of_birth,
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


def list_members_checked_in_for_date_range(
    connection: sqlite3.Connection,
    start_date: date,
    end_date: date,
) -> list[Member]:
    '''Return distinct users who checked in during the date range.'''
    start_at = datetime.combine(start_date, datetime.min.time()).isoformat(
        timespec="seconds"
    )
    exclusive_end_at = datetime.combine(
        end_date + date.resolution,
        datetime.min.time(),
    ).isoformat(timespec="seconds")
    rows = connection.execute(
        """
        SELECT DISTINCT
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
        FROM users u
        INNER JOIN checkins c ON c.user_id = u.id
        WHERE c.check_in_at >= ? AND c.check_in_at < ?
        ORDER BY u.last_name, u.first_name, u.card_number
        """,
        (start_at, exclusive_end_at),
    ).fetchall()
    return [member_from_row(row) for row in rows]


def list_member_report_rows(
    connection: sqlite3.Connection,
    *,
    as_of_date: date | None = None,
) -> list[MemberReportRow]:
    '''Return all users with membership-specific visit counts for the users report.'''
    period_end_date = as_of_date or date.today()
    one_year_start = datetime.combine(
        _years_before(period_end_date, 1),
        datetime.min.time(),
    ).isoformat(timespec="seconds")
    two_year_start = datetime.combine(
        _years_before(period_end_date, 2),
        datetime.min.time(),
    ).isoformat(timespec="seconds")
    rows = connection.execute(
        """
        SELECT
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
            u.cell_phone,
            COUNT(
                CASE
                    WHEN u.membership = 'AANR Member' AND c.check_in_at >= ? THEN 1
                    WHEN u.membership = 'Visitor' AND c.check_in_at >= ? THEN 1
                END
            ) AS checkin_count,
            MAX(c.check_in_at) AS last_check_in_at
        FROM users u
        LEFT JOIN checkins c ON c.user_id = u.id
        GROUP BY
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
        ORDER BY u.last_name, u.first_name, u.card_number
        """,
        (one_year_start, two_year_start),
    ).fetchall()
    report_rows: list[MemberReportRow] = []
    for row in rows:
        member = member_from_row(row)
        report_rows.append(
            MemberReportRow(
                member=member,
                address=format_member_address(member),
                address_lines=format_member_address_lines(member),
                checkin_count=(
                    int(row["checkin_count"])
                    if _member_visit_period_start(member.membership, period_end_date) is not None
                    else None
                ),
                last_check_in_at=(
                    datetime.fromisoformat(row["last_check_in_at"])
                    if row["last_check_in_at"]
                    else None
                ),
            )
        )
    return report_rows


def get_member(connection: sqlite3.Connection, member_id: int) -> Member | None:
    '''Return one club user by database ID.'''
    row = connection.execute(
        """
        SELECT
            id,
            last_name,
            first_name,
            nickname,
            card_number,
            membership,
            member_since,
            date_of_birth,
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


def get_member_by_card_number(connection: sqlite3.Connection, card_number: str) -> Member | None:
    '''Return one club user by card number.'''
    row = connection.execute(
        """
        SELECT
            id,
            last_name,
            first_name,
            nickname,
            card_number,
            membership,
            member_since,
            date_of_birth,
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
        WHERE card_number = ?
        """,
        (card_number,),
    ).fetchone()
    return member_from_row(row) if row is not None else None


def largest_numeric_card_number(connection: sqlite3.Connection) -> int | None:
    '''Return the largest all-digit card number, ignoring nonnumeric card IDs.'''
    row = connection.execute(
        """
        SELECT MAX(CAST(card_number AS INTEGER)) AS largest_card_number
        FROM users
        WHERE card_number <> ''
          AND card_number NOT GLOB '*[^0-9]*'
        """
    ).fetchone()
    value = row["largest_card_number"] if row is not None else None
    return int(value) if value is not None else None


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
    '''Return a member when phone plus initials or first name match exactly one user.'''
    target_digits = normalize_phone(phone)
    target_identity = normalize_name_token(initials)
    if len(target_digits) < 7:
        return None
    if not target_identity:
        return None

    matches: list[Member] = []
    for member in list_members(connection):
        member_numbers = {
            normalize_phone(member.phone),
            normalize_phone(member.work_phone),
            normalize_phone(member.cell_phone),
        }
        identity_matches = (
            member_initials(member) == target_identity
            or normalize_name_token(member.first_name) == target_identity
            or normalize_name_token(member.nickname) == target_identity
        )
        if target_digits in member_numbers and identity_matches:
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
            nickname = ?,
            card_number = ?,
            membership = ?,
            member_since = ?,
            date_of_birth = ?,
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
            _empty_to_none(member.nickname),
            member.card_number.strip(),
            member.membership.strip(),
            _date_to_text(member.member_since),
            _date_to_text(member.date_of_birth),
            _empty_to_none(member.address),
            _empty_to_none(member.address2),
            _empty_to_none(member.city),
            _empty_to_none(member.state),
            _empty_to_none(member.zip),
            _phone_to_none(member.phone),
            _empty_to_none(member.email),
            _phone_to_none(member.work_phone),
            _phone_to_none(member.cell_phone),
            member.id,
        ),
    )
