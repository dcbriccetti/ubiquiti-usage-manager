'''Domain models for club user management.'''

from dataclasses import dataclass
from datetime import date, datetime


@dataclass(frozen=True, kw_only=True)
class Member:
    '''A club user record.'''

    last_name: str
    first_name: str
    card_number: str
    membership: str
    member_since: date | None = None
    date_of_birth: date | None = None
    nickname: str | None = None
    address: str | None = None
    address2: str | None = None
    city: str | None = None
    state: str | None = None
    zip: str | None = None
    mailing_address: str | None = None
    mailing_address2: str | None = None
    mailing_city: str | None = None
    mailing_state: str | None = None
    mailing_zip: str | None = None
    phone: str | None = None
    email: str | None = None
    work_phone: str | None = None
    cell_phone: str | None = None
    screening_status: str | None = None
    gender: str | None = None
    occupation: str | None = None
    driver_license_number: str | None = None
    driver_license_state: str | None = None
    driver_license_expires: date | None = None
    emergency_contact_name: str | None = None
    emergency_contact_relationship: str | None = None
    emergency_contact_phone: str | None = None
    aanr_number: str | None = None
    other_club_name: str | None = None
    id: int | None = None


@dataclass(frozen=True, kw_only=True)
class CheckIn:
    '''A club user check-in event.'''

    member_id: str | None
    last_name: str
    first_name: str
    card_number: str
    check_in_at: datetime
    membership: str
    check_out_at: datetime | None = None
    total_checkins: int | None = None
    visit_number: int | None = None
    previous_check_in_at: datetime | None = None
    checkin_count: int | None = None
    duration: str | None = None
    user_id: int | None = None
    id: int | None = None


@dataclass(frozen=True, kw_only=True)
class GuestRegistration:
    '''A first-time visitor registration submitted before signing the guest form.'''

    visit_date: date
    other_phone: str | None = None
    other_phone_type: str | None = None
    marital_status: str | None = None
    partner_name: str | None = None
    guest_of_member: bool = False
    member_name: str | None = None
    heard_about: str | None = None
    newsletter_opt_out: bool = False
    user_id: int | None = None
    created_at: datetime | None = None
    id: int | None = None


@dataclass(frozen=True, kw_only=True)
class MembershipApplication:
    '''A visitor application for full or associate membership.'''

    user_id: int
    requested_membership: str
    gender: str | None = None
    occupation: str | None = None
    driver_license_number: str | None = None
    driver_license_state: str | None = None
    driver_license_expires: date | None = None
    mailing_address: str | None = None
    mailing_address2: str | None = None
    mailing_city: str | None = None
    mailing_state: str | None = None
    mailing_zip: str | None = None
    club_news_name_permission: bool | None = None
    emergency_contact_name: str | None = None
    emergency_contact_relationship: str | None = None
    emergency_contact_phone: str | None = None
    minor_children: str | None = None
    convicted: bool | None = None
    conviction_explanation: str | None = None
    social_nudity_practiced: bool | None = None
    social_nudity_duration: str | None = None
    social_nudity_experience: str | None = None
    aanr_member: bool | None = None
    aanr_number: str | None = None
    aanr_expires: date | None = None
    other_club_member: bool | None = None
    other_club_name: str | None = None
    agreement_accepted: bool = False
    signed_at: date | None = None
    status: str = "pending"
    application_fee_received_at: date | None = None
    reviewed_at: datetime | None = None
    created_at: datetime | None = None
    id: int | None = None


@dataclass(frozen=True, kw_only=True)
class UserNote:
    '''One admin note attached to a club user.'''

    user_id: int
    summary: str
    details: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    id: int | None = None
