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


def _screening_status_to_text(value: str | None) -> str | None:
    if value in {"pending", "safe", "banned"}:
        return value
    return None


def _row_value(row: sqlite3.Row, key: str) -> str | None:
    try:
        return row[key]
    except (IndexError, KeyError):
        return None


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


def _normalize_identity_name(value: str | None) -> str:
    token = normalize_name_token(value)
    return "" if token in {"NONE", "NA", "NAN", "UNKNOWN"} else token


def member_initials(member: Member) -> str:
    '''Return first-name/last-name initials for a member.'''
    first_name = _normalize_identity_name(member.first_name)
    last_name = _normalize_identity_name(member.last_name)
    return f"{first_name[:1]}{last_name[:1]}"


def _member_identity_tokens(member: Member) -> set[str]:
    first_name = _normalize_identity_name(member.first_name)
    last_name = _normalize_identity_name(member.last_name)
    nickname = _normalize_identity_name(member.nickname)
    tokens = {token for token in {first_name, nickname} if token}
    if not first_name and last_name:
        tokens.add(last_name)

    initials = member_initials(member)
    if initials:
        tokens.add(initials)
    if nickname and last_name:
        tokens.add(f"{nickname[:1]}{last_name[:1]}")
    elif nickname:
        tokens.add(nickname[:1])
    return tokens


def _member_identity_key(member: Member) -> tuple[str, str, date | None]:
    return (
        _normalize_identity_name(member.first_name),
        _normalize_identity_name(member.last_name),
        member.date_of_birth,
    )


def _membership_rank(member: Member) -> int:
    ranks = {
        "Full Member": 0,
        "Associate Member": 1,
        "AANR Member": 2,
        "Visitor": 3,
    }
    return ranks.get(member.membership, 4)


def _matching_member_checkin_sort_key(
    connection: sqlite3.Connection,
    member: Member,
) -> tuple[int, int, float, int]:
    row = connection.execute(
        """
        SELECT COUNT(*) AS checkin_count, MAX(check_in_at) AS last_check_in_at
        FROM checkins
        WHERE user_id = ?
        """,
        (member.id,),
    ).fetchone()
    checkin_count = int(row["checkin_count"] or 0) if row is not None else 0
    last_check_in_at = str(row["last_check_in_at"] or "") if row is not None else ""
    last_check_in_timestamp = (
        datetime.fromisoformat(last_check_in_at).timestamp()
        if last_check_in_at
        else 0.0
    )
    return (
        _membership_rank(member),
        -checkin_count,
        -last_check_in_timestamp,
        int(member.id or 0),
    )


def _resolve_duplicate_identity_match(
    connection: sqlite3.Connection,
    matches: list[Member],
) -> Member | None:
    identity_keys = {_member_identity_key(member) for member in matches}
    if len(identity_keys) != 1:
        return None
    return sorted(
        matches,
        key=lambda member: _matching_member_checkin_sort_key(connection, member),
    )[0]


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
        mailing_address=_row_value(row, "mailing_address"),
        mailing_address2=_row_value(row, "mailing_address2"),
        mailing_city=_row_value(row, "mailing_city"),
        mailing_state=_row_value(row, "mailing_state"),
        mailing_zip=_row_value(row, "mailing_zip"),
        phone=format_phone_number(row["phone"]),
        email=row["email"],
        work_phone=format_phone_number(row["work_phone"]),
        cell_phone=format_phone_number(row["cell_phone"]),
        screening_status=_screening_status_to_text(row["screening_status"]),
        gender=_row_value(row, "gender"),
        occupation=_row_value(row, "occupation"),
        driver_license_number=_row_value(row, "driver_license_number"),
        driver_license_state=_row_value(row, "driver_license_state"),
        driver_license_expires=_text_to_date(_row_value(row, "driver_license_expires")),
        emergency_contact_name=_row_value(row, "emergency_contact_name"),
        emergency_contact_relationship=_row_value(
            row,
            "emergency_contact_relationship",
        ),
        emergency_contact_phone=format_phone_number(
            _row_value(row, "emergency_contact_phone")
        ),
        aanr_number=_row_value(row, "aanr_number"),
        other_club_name=_row_value(row, "other_club_name"),
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
            mailing_address,
            mailing_address2,
            mailing_city,
            mailing_state,
            mailing_zip,
            phone,
            email,
            work_phone,
            cell_phone,
            screening_status
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            mailing_address = excluded.mailing_address,
            mailing_address2 = excluded.mailing_address2,
            mailing_city = excluded.mailing_city,
            mailing_state = excluded.mailing_state,
            mailing_zip = excluded.mailing_zip,
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
            _empty_to_none(member.mailing_address),
            _empty_to_none(member.mailing_address2),
            _empty_to_none(member.mailing_city),
            _empty_to_none(member.mailing_state),
            _empty_to_none(member.mailing_zip),
            _phone_to_none(member.phone),
            _empty_to_none(member.email),
            _phone_to_none(member.work_phone),
            _phone_to_none(member.cell_phone),
            _screening_status_to_text(member.screening_status),
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
            mailing_address,
            mailing_address2,
            mailing_city,
            mailing_state,
            mailing_zip,
            phone,
            email,
            work_phone,
            cell_phone,
            screening_status
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            _empty_to_none(member.mailing_address),
            _empty_to_none(member.mailing_address2),
            _empty_to_none(member.mailing_city),
            _empty_to_none(member.mailing_state),
            _empty_to_none(member.mailing_zip),
            _phone_to_none(member.phone),
            _empty_to_none(member.email),
            _phone_to_none(member.work_phone),
            _phone_to_none(member.cell_phone),
            _screening_status_to_text(member.screening_status),
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
            mailing_address,
            mailing_address2,
            mailing_city,
            mailing_state,
            mailing_zip,
            phone,
            email,
            work_phone,
            cell_phone,
            screening_status
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
            u.mailing_address,
            u.mailing_address2,
            u.mailing_city,
            u.mailing_state,
            u.mailing_zip,
            u.phone,
            u.email,
            u.work_phone,
            u.cell_phone,
            u.screening_status,
            u.gender,
            u.occupation,
            u.driver_license_number,
            u.driver_license_state,
            u.driver_license_expires,
            u.emergency_contact_name,
            u.emergency_contact_relationship,
            u.emergency_contact_phone,
            u.aanr_number,
            u.other_club_name
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
            u.mailing_address,
            u.mailing_address2,
            u.mailing_city,
            u.mailing_state,
            u.mailing_zip,
            u.phone,
            u.email,
            u.work_phone,
            u.cell_phone,
            u.screening_status,
            u.gender,
            u.occupation,
            u.driver_license_number,
            u.driver_license_state,
            u.driver_license_expires,
            u.emergency_contact_name,
            u.emergency_contact_relationship,
            u.emergency_contact_phone,
            u.aanr_number,
            u.other_club_name,
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
            u.mailing_address,
            u.mailing_address2,
            u.mailing_city,
            u.mailing_state,
            u.mailing_zip,
            u.phone,
            u.email,
            u.work_phone,
            u.cell_phone,
            u.screening_status,
            u.gender,
            u.occupation,
            u.driver_license_number,
            u.driver_license_state,
            u.driver_license_expires,
            u.emergency_contact_name,
            u.emergency_contact_relationship,
            u.emergency_contact_phone,
            u.aanr_number,
            u.other_club_name
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
            mailing_address,
            mailing_address2,
            mailing_city,
            mailing_state,
            mailing_zip,
            phone,
            email,
            work_phone,
            cell_phone,
            screening_status,
            gender,
            occupation,
            driver_license_number,
            driver_license_state,
            driver_license_expires,
            emergency_contact_name,
            emergency_contact_relationship,
            emergency_contact_phone,
            aanr_number,
            other_club_name
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
            mailing_address,
            mailing_address2,
            mailing_city,
            mailing_state,
            mailing_zip,
            phone,
            email,
            work_phone,
            cell_phone,
            screening_status,
            gender,
            occupation,
            driver_license_number,
            driver_license_state,
            driver_license_expires,
            emergency_contact_name,
            emergency_contact_relationship,
            emergency_contact_phone,
            aanr_number,
            other_club_name
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

    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        return _resolve_duplicate_identity_match(connection, matches)
    return None


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
        identity_matches = target_identity in _member_identity_tokens(member)
        if target_digits in member_numbers and identity_matches:
            matches.append(member)

    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        return _resolve_duplicate_identity_match(connection, matches)
    return None


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
            mailing_address = ?,
            mailing_address2 = ?,
            mailing_city = ?,
            mailing_state = ?,
            mailing_zip = ?,
            phone = ?,
            email = ?,
            work_phone = ?,
            cell_phone = ?,
            screening_status = ?
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
            _empty_to_none(member.mailing_address),
            _empty_to_none(member.mailing_address2),
            _empty_to_none(member.mailing_city),
            _empty_to_none(member.mailing_state),
            _empty_to_none(member.mailing_zip),
            _phone_to_none(member.phone),
            _empty_to_none(member.email),
            _phone_to_none(member.work_phone),
            _phone_to_none(member.cell_phone),
            _screening_status_to_text(member.screening_status),
            member.id,
        ),
    )


def update_member_membership_profile(connection: sqlite3.Connection, member: Member) -> None:
    '''Update membership approval fields for an existing club user.'''
    if member.id is None:
        raise ValueError("member.id is required for update.")

    connection.execute(
        """
        UPDATE users
        SET
            membership = ?,
            gender = ?,
            occupation = ?,
            driver_license_number = ?,
            driver_license_state = ?,
            driver_license_expires = ?,
            mailing_address = ?,
            mailing_address2 = ?,
            mailing_city = ?,
            mailing_state = ?,
            mailing_zip = ?,
            emergency_contact_name = ?,
            emergency_contact_relationship = ?,
            emergency_contact_phone = ?,
            aanr_number = ?,
            other_club_name = ?
        WHERE id = ?
        """,
        (
            member.membership.strip(),
            _empty_to_none(member.gender),
            _empty_to_none(member.occupation),
            _empty_to_none(member.driver_license_number),
            _empty_to_none(member.driver_license_state),
            _date_to_text(member.driver_license_expires),
            _empty_to_none(member.mailing_address),
            _empty_to_none(member.mailing_address2),
            _empty_to_none(member.mailing_city),
            _empty_to_none(member.mailing_state),
            _empty_to_none(member.mailing_zip),
            _empty_to_none(member.emergency_contact_name),
            _empty_to_none(member.emergency_contact_relationship),
            _phone_to_none(member.emergency_contact_phone),
            _empty_to_none(member.aanr_number),
            _empty_to_none(member.other_club_name),
            member.id,
        ),
    )


def update_member_screening_status(
    connection: sqlite3.Connection,
    member_id: int,
    screening_status: str | None,
) -> None:
    '''Update only a user's screening status.'''
    connection.execute(
        """
        UPDATE users
        SET screening_status = ?
        WHERE id = ?
        """,
        (_screening_status_to_text(screening_status), member_id),
    )
