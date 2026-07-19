'''Persistence operations for visitor membership applications.'''

import sqlite3
from dataclasses import dataclass
from datetime import date, datetime

from club_admin.member_repository import format_phone_number, member_from_row
from club_admin.models import Member, MembershipApplication


@dataclass(frozen=True, kw_only=True)
class MembershipApplicationRecord:
    '''A membership application joined to its visitor user.'''

    application: MembershipApplication
    member: Member


def _empty_to_none(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


def _date_to_text(value: date | None) -> str | None:
    return value.isoformat() if value is not None else None


def _text_to_date(value: str | None) -> date | None:
    return date.fromisoformat(value) if value else None


def _datetime_to_text(value: datetime | None) -> str | None:
    return value.isoformat(timespec="seconds") if value is not None else None


def _text_to_datetime(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None


def _bool_to_int(value: bool | None) -> int | None:
    if value is None:
        return None
    return 1 if value else 0


def _int_to_bool(value: int | None) -> bool | None:
    if value is None:
        return None
    return bool(value)


def _application_from_row(row: sqlite3.Row) -> MembershipApplication:
    return MembershipApplication(
        id=row["application_id"],
        user_id=row["user_id"],
        requested_membership=row["requested_membership"],
        gender=row["application_gender"],
        occupation=row["application_occupation"],
        driver_license_number=row["application_driver_license_number"],
        driver_license_state=row["application_driver_license_state"],
        driver_license_expires=_text_to_date(row["application_driver_license_expires"]),
        mailing_address=row["application_mailing_address"],
        mailing_address2=row["application_mailing_address2"],
        mailing_city=row["application_mailing_city"],
        mailing_state=row["application_mailing_state"],
        mailing_zip=row["application_mailing_zip"],
        club_news_name_permission=_int_to_bool(row["club_news_name_permission"]),
        emergency_contact_name=row["application_emergency_contact_name"],
        emergency_contact_relationship=row["application_emergency_contact_relationship"],
        emergency_contact_phone=format_phone_number(row["application_emergency_contact_phone"]),
        minor_children=row["minor_children"],
        convicted=_int_to_bool(row["convicted"]),
        conviction_explanation=row["conviction_explanation"],
        social_nudity_practiced=_int_to_bool(row["social_nudity_practiced"]),
        social_nudity_duration=row["social_nudity_duration"],
        social_nudity_experience=row["social_nudity_experience"],
        aanr_member=_int_to_bool(row["aanr_member"]),
        aanr_number=row["application_aanr_number"],
        aanr_expires=_text_to_date(row["aanr_expires"]),
        other_club_member=_int_to_bool(row["other_club_member"]),
        other_club_name=row["application_other_club_name"],
        agreement_accepted=bool(row["agreement_accepted"]),
        signed_at=_text_to_date(row["signed_at"]),
        status=row["status"],
        application_fee_received_at=_text_to_date(row["application_fee_received_at"]),
        reviewed_at=_text_to_datetime(row["reviewed_at"]),
        created_at=_text_to_datetime(row["created_at"]),
    )


def _record_from_row(row: sqlite3.Row) -> MembershipApplicationRecord:
    return MembershipApplicationRecord(
        application=_application_from_row(row),
        member=member_from_row(row),
    )


def insert_membership_application(
    connection: sqlite3.Connection,
    application: MembershipApplication,
) -> int:
    '''Insert one membership application and return its ID.'''
    cursor = connection.execute(
        """
        INSERT INTO membership_applications (
            user_id,
            requested_membership,
            gender,
            occupation,
            driver_license_number,
            driver_license_state,
            driver_license_expires,
            mailing_address,
            mailing_address2,
            mailing_city,
            mailing_state,
            mailing_zip,
            club_news_name_permission,
            emergency_contact_name,
            emergency_contact_relationship,
            emergency_contact_phone,
            minor_children,
            convicted,
            conviction_explanation,
            social_nudity_practiced,
            social_nudity_duration,
            social_nudity_experience,
            aanr_member,
            aanr_number,
            aanr_expires,
            other_club_member,
            other_club_name,
            agreement_accepted,
            signed_at,
            status,
            application_fee_received_at,
            reviewed_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            application.user_id,
            application.requested_membership,
            _empty_to_none(application.gender),
            _empty_to_none(application.occupation),
            _empty_to_none(application.driver_license_number),
            _empty_to_none(application.driver_license_state),
            _date_to_text(application.driver_license_expires),
            _empty_to_none(application.mailing_address),
            _empty_to_none(application.mailing_address2),
            _empty_to_none(application.mailing_city),
            _empty_to_none(application.mailing_state),
            _empty_to_none(application.mailing_zip),
            _bool_to_int(application.club_news_name_permission),
            _empty_to_none(application.emergency_contact_name),
            _empty_to_none(application.emergency_contact_relationship),
            format_phone_number(application.emergency_contact_phone),
            _empty_to_none(application.minor_children),
            _bool_to_int(application.convicted),
            _empty_to_none(application.conviction_explanation),
            _bool_to_int(application.social_nudity_practiced),
            _empty_to_none(application.social_nudity_duration),
            _empty_to_none(application.social_nudity_experience),
            _bool_to_int(application.aanr_member),
            _empty_to_none(application.aanr_number),
            _date_to_text(application.aanr_expires),
            _bool_to_int(application.other_club_member),
            _empty_to_none(application.other_club_name),
            1 if application.agreement_accepted else 0,
            _date_to_text(application.signed_at),
            application.status,
            _date_to_text(application.application_fee_received_at),
            _datetime_to_text(application.reviewed_at),
        ),
    )
    return int(cursor.lastrowid)


def _membership_application_select_sql() -> str:
    return """
        SELECT
            a.id AS application_id,
            a.user_id,
            a.requested_membership,
            a.gender AS application_gender,
            a.occupation AS application_occupation,
            a.driver_license_number AS application_driver_license_number,
            a.driver_license_state AS application_driver_license_state,
            a.driver_license_expires AS application_driver_license_expires,
            a.mailing_address AS application_mailing_address,
            a.mailing_address2 AS application_mailing_address2,
            a.mailing_city AS application_mailing_city,
            a.mailing_state AS application_mailing_state,
            a.mailing_zip AS application_mailing_zip,
            a.club_news_name_permission,
            a.emergency_contact_name AS application_emergency_contact_name,
            a.emergency_contact_relationship AS application_emergency_contact_relationship,
            a.emergency_contact_phone AS application_emergency_contact_phone,
            a.minor_children,
            a.convicted,
            a.conviction_explanation,
            a.social_nudity_practiced,
            a.social_nudity_duration,
            a.social_nudity_experience,
            a.aanr_member,
            a.aanr_number AS application_aanr_number,
            a.aanr_expires,
            a.other_club_member,
            a.other_club_name AS application_other_club_name,
            a.agreement_accepted,
            a.signed_at,
            a.status,
            a.application_fee_received_at,
            a.reviewed_at,
            a.created_at,
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
        FROM membership_applications a
        JOIN users u ON u.id = a.user_id
    """


def list_membership_application_records(
    connection: sqlite3.Connection,
    *,
    limit: int = 50,
    status: str | None = "pending",
) -> list[MembershipApplicationRecord]:
    '''Return recent membership applications.'''
    status_filter = "WHERE a.status = ?" if status is not None else ""
    parameters: tuple[object, ...] = (status, limit) if status is not None else (limit,)
    rows = connection.execute(
        _membership_application_select_sql()
        + f"""
        {status_filter}
        ORDER BY a.created_at DESC, a.id DESC
        LIMIT ?
        """,
        parameters,
    ).fetchall()
    return [_record_from_row(row) for row in rows]


def get_membership_application_record(
    connection: sqlite3.Connection,
    application_id: int,
) -> MembershipApplicationRecord | None:
    '''Return one membership application.'''
    row = connection.execute(
        _membership_application_select_sql()
        + """
        WHERE a.id = ?
        """,
        (application_id,),
    ).fetchone()
    return _record_from_row(row) if row is not None else None


def get_pending_membership_application_for_user(
    connection: sqlite3.Connection,
    user_id: int,
) -> MembershipApplicationRecord | None:
    '''Return the user's pending application, if one exists.'''
    row = connection.execute(
        _membership_application_select_sql()
        + """
        WHERE a.user_id = ? AND a.status = 'pending'
        ORDER BY a.created_at DESC, a.id DESC
        LIMIT 1
        """,
        (user_id,),
    ).fetchone()
    return _record_from_row(row) if row is not None else None


def mark_application_fee_received(
    connection: sqlite3.Connection,
    application_id: int,
    received_at: date,
) -> None:
    '''Record that the application fee was received.'''
    connection.execute(
        """
        UPDATE membership_applications
        SET application_fee_received_at = COALESCE(application_fee_received_at, ?)
        WHERE id = ?
        """,
        (_date_to_text(received_at), application_id),
    )


def update_membership_application(
    connection: sqlite3.Connection,
    application: MembershipApplication,
) -> None:
    '''Update editable fields for a submitted membership application.'''
    if application.id is None:
        raise ValueError("application.id is required for update.")
    connection.execute(
        """
        UPDATE membership_applications
        SET
            requested_membership = ?,
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
            club_news_name_permission = ?,
            emergency_contact_name = ?,
            emergency_contact_relationship = ?,
            emergency_contact_phone = ?,
            minor_children = ?,
            convicted = ?,
            conviction_explanation = ?,
            social_nudity_practiced = ?,
            social_nudity_duration = ?,
            social_nudity_experience = ?,
            aanr_member = ?,
            aanr_number = ?,
            aanr_expires = ?,
            other_club_member = ?,
            other_club_name = ?,
            agreement_accepted = ?
        WHERE id = ?
        """,
        (
            application.requested_membership,
            _empty_to_none(application.gender),
            _empty_to_none(application.occupation),
            _empty_to_none(application.driver_license_number),
            _empty_to_none(application.driver_license_state),
            _date_to_text(application.driver_license_expires),
            _empty_to_none(application.mailing_address),
            _empty_to_none(application.mailing_address2),
            _empty_to_none(application.mailing_city),
            _empty_to_none(application.mailing_state),
            _empty_to_none(application.mailing_zip),
            _bool_to_int(application.club_news_name_permission),
            _empty_to_none(application.emergency_contact_name),
            _empty_to_none(application.emergency_contact_relationship),
            format_phone_number(application.emergency_contact_phone),
            _empty_to_none(application.minor_children),
            _bool_to_int(application.convicted),
            _empty_to_none(application.conviction_explanation),
            _bool_to_int(application.social_nudity_practiced),
            _empty_to_none(application.social_nudity_duration),
            _empty_to_none(application.social_nudity_experience),
            _bool_to_int(application.aanr_member),
            _empty_to_none(application.aanr_number),
            _date_to_text(application.aanr_expires),
            _bool_to_int(application.other_club_member),
            _empty_to_none(application.other_club_name),
            1 if application.agreement_accepted else 0,
            application.id,
        ),
    )


def update_application_status(
    connection: sqlite3.Connection,
    application_id: int,
    *,
    status: str,
    reviewed_at: datetime,
) -> None:
    '''Set the review outcome for one membership application.'''
    connection.execute(
        """
        UPDATE membership_applications
        SET status = ?, reviewed_at = ?
        WHERE id = ?
        """,
        (status, _datetime_to_text(reviewed_at), application_id),
    )
