'''Domain models for club user management.'''

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, kw_only=True)
class Member:
    '''A club user record imported from the user roster.'''

    last_name: str
    first_name: str
    card_number: str
    membership: str
    address: str | None = None
    address2: str | None = None
    city: str | None = None
    state: str | None = None
    zip: str | None = None
    phone: str | None = None
    email: str | None = None
    work_phone: str | None = None
    cell_phone: str | None = None
    id: int | None = None


@dataclass(frozen=True, kw_only=True)
class CheckIn:
    '''A club user check-in event imported from the check-in report.'''

    member_id: str | None
    last_name: str
    first_name: str
    card_number: str
    check_in_at: datetime
    membership: str
    check_out_at: datetime | None = None
    total_checkins: int | None = None
    duration: str | None = None
    user_id: int | None = None
    id: int | None = None
