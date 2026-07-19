'''Flask app for club user management.'''

import base64
import csv
import hashlib
import hmac
import io
import json
import math
import os
import re
import sqlite3
import time
import unicodedata
from collections.abc import Callable, Iterator
from collections import Counter
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta, timezone
from functools import wraps
from pathlib import Path
from secrets import token_hex, token_urlsafe
from typing import Any, cast
from zoneinfo import ZoneInfo

from flask import (
    Flask,
    Response,
    abort,
    current_app,
    jsonify,
    make_response,
    redirect,
    render_template,
    request,
    send_file,
    session,
    stream_with_context,
    url_for,
)
from PIL import Image, ImageChops, ImageOps, UnidentifiedImageError
from werkzeug.security import check_password_hash

import config as cfg
from club_admin import audit_repository
from club_admin import checkin_events
from club_admin import checkin_repository
from club_admin import database
from club_admin import guest_form
from club_admin import guest_registration_repository
from club_admin import membership_application_repository
from club_admin import member_repository
from club_admin import user_note_repository
from club_admin import zip_repository
from club_admin.models import CheckIn, GuestRegistration, Member, MembershipApplication


EDITABLE_MEMBER_FIELDS = (
    "last_name",
    "first_name",
    "nickname",
    "card_number",
    "membership",
    "member_since",
    "date_of_birth",
    "address",
    "address2",
    "city",
    "state",
    "zip",
    "mailing_address",
    "mailing_address2",
    "mailing_city",
    "mailing_state",
    "mailing_zip",
    "phone",
    "email",
    "work_phone",
    "cell_phone",
    "screening_status",
)
MEMBERSHIP_OPTIONS = (
    "AANR Member",
    "Associate Member",
    "Full Member",
    "Visitor",
)
SCREENING_STATUS_OPTIONS = (
    ("", "No flag"),
    ("pending", "Needs review"),
    ("safe", "Safe"),
    ("banned", "Banned"),
)
SCREENING_STATUS_LABELS = dict(SCREENING_STATUS_OPTIONS)
CHECKIN_REPORT_MEMBERSHIP_BREAKDOWN = (
    ("Full Member", "Full Member", "membership-full"),
    ("Assoc.", "Associate Member", "membership-assoc"),
    ("AANR", "AANR Member", "membership-aanr"),
    ("Visitor", "Visitor", "membership-visitor"),
)
CHECKIN_SEASON_COMPARISON_GROUPS = (
    ("Members", ("Full Member", "Associate Member"), "membership-members"),
    ("Visitors", ("AANR Member", "Visitor"), "membership-visitors"),
)
CHECKIN_VISIT_NUMBER_CHART_MAX_EXACT = 9
LIVE_CHECKIN_REPEAT_WINDOW = timedelta(hours=1)
BARCODE_SECRET_SETTING_KEY = "self_checkin_barcode_secret"
KIOSK_AUTO_RETURN_SECONDS = 60
SUPPORTED_DOCUMENT_IMAGE_SUFFIXES = {".gif", ".jpeg", ".jpg", ".png", ".webp"}
DRIVER_LICENSE_DOCUMENT_NAME = "Driver License.jpg"
DRIVER_LICENSE_IMAGE_SIZE = (2026, 1152)
DRIVER_LICENSE_CROP_THRESHOLD = 24
DRIVER_LICENSE_CROP_ASPECT_FLOOR = 1.45
DRIVER_LICENSE_DARK_BACKGROUND_LUMINANCE = 100
ID_DOCUMENT_NAME_PATTERN = re.compile(
    r"^(?:drivers?\s+license|drivers?\s+licence|dl|id|identification)(?:\b|[_\-\s])",
    re.IGNORECASE,
)
BARCODE_TOKEN_VERSION = "U2"
LEGACY_BARCODE_TOKEN_VERSIONS = ("UM1",)
CLUB_DISPLAY_TIMEZONE = ZoneInfo("America/Los_Angeles")
CHECKIN_MONITOR_TOKEN_HEADER = "X-Checkin-Monitor-Token"
CHECKIN_MONITOR_MAX_LIMIT = 50
CHECKIN_MONITOR_MAX_WAIT_SECONDS = 30.0
CHECKIN_MONITOR_POLL_SECONDS = 1.0
MEMBERSHIP_APPLICATION_SESSION_KEY = "membership_application_user_id"
PUBLIC_KIOSK_ENDPOINTS = frozenset(
    {
        "guest_registration",
        "guest_registration_thanks",
        "membership_application",
        "membership_application_thanks",
        "self_checkin",
    }
)
CODE128_PATTERNS = (
    "212222", "222122", "222221", "121223", "121322", "131222", "122213", "122312",
    "132212", "221213", "221312", "231212", "112232", "122132", "122231", "113222",
    "123122", "123221", "223211", "221132", "221231", "213212", "223112", "312131",
    "311222", "321122", "321221", "312212", "322112", "322211", "212123", "212321",
    "232121", "111323", "131123", "131321", "112313", "132113", "132311", "211313",
    "231113", "231311", "112133", "112331", "132131", "113123", "113321", "133121",
    "313121", "211331", "231131", "213113", "213311", "213131", "311123", "311321",
    "331121", "312113", "312311", "332111", "314111", "221411", "431111", "111224",
    "111422", "121124", "121421", "141122", "141221", "112214", "112412", "122114",
    "122411", "142112", "142211", "241211", "221114", "413111", "241112", "134111",
    "111242", "121142", "121241", "114212", "124112", "124211", "411212", "421112",
    "421211", "212141", "214121", "412121", "111143", "111341", "131141", "114113",
    "114311", "411113", "411311", "113141", "114131", "311141", "411131", "211412",
    "211214", "211232", "2331112",
)


def _normalize_url_prefix(prefix: object) -> str:
    normalized = str(prefix or "").strip()
    if not normalized or normalized == "/":
        return ""
    return "/" + normalized.strip("/")


def _sqlite_utc_to_local(value: datetime | str | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, str):
        if not value.strip():
            return None
        try:
            parsed_value = datetime.fromisoformat(value)
        except ValueError:
            return None
    else:
        parsed_value = value
    if parsed_value.tzinfo is None:
        parsed_value = parsed_value.replace(tzinfo=timezone.utc)
    return parsed_value.astimezone(CLUB_DISPLAY_TIMEZONE)


def _format_sqlite_utc_datetime(value: datetime | str | None) -> str:
    local_value = _sqlite_utc_to_local(value)
    return local_value.strftime("%Y-%m-%d %H:%M:%S") if local_value is not None else ""


def _format_sqlite_utc_date(value: datetime | str | None) -> str:
    local_value = _sqlite_utc_to_local(value)
    return local_value.strftime("%Y-%m-%d") if local_value is not None else ""


def _format_date_entry(value: date | str | None) -> str:
    if value is None:
        return ""
    if isinstance(value, date):
        return value.strftime("%m/%d/%Y")
    parsed_value = _parse_flexible_date(str(value))
    return parsed_value.strftime("%m/%d/%Y") if parsed_value is not None else str(value)


def _display_audit_field_name(value: str | None) -> str:
    if value == "guest registration submitted":
        return "visitor registration submitted"
    return value or ""


def _datetime_to_stored_text(value: datetime | None) -> str | None:
    return value.isoformat(timespec="seconds") if value is not None else None


class UrlPrefixMiddleware:
    '''Mount this app under a reverse-proxy path prefix.'''

    def __init__(self, app: Any, prefix: str) -> None:
        self.app = app
        self.prefix = prefix

    def __call__(self, environ: dict[str, Any], start_response: Any) -> Any:
        path = str(environ.get("PATH_INFO") or "")
        if path == self.prefix:
            environ["SCRIPT_NAME"] = self.prefix
            environ["PATH_INFO"] = "/"
        elif path.startswith(f"{self.prefix}/"):
            environ["SCRIPT_NAME"] = self.prefix
            environ["PATH_INFO"] = path[len(self.prefix) :] or "/"
        return self.app(environ, start_response)


@dataclass(frozen=True, kw_only=True)
class DocumentFilenamePattern:
    pattern: str
    count: int
    examples: tuple[str, ...]


@dataclass(frozen=True, kw_only=True)
class DocumentExtensionCount:
    extension: str
    count: int


@dataclass(frozen=True, kw_only=True)
class MemberDocument:
    name: str


@dataclass(frozen=True, kw_only=True)
class MemberDocumentPreview:
    title: str
    document_name: str | None = None
    is_guest_form: bool = False


@dataclass(frozen=True, kw_only=True)
class KioskIdentityResult:
    member: Member | None
    used_barcode: bool = False


@dataclass(frozen=True, kw_only=True)
class ZipMapPoint:
    zip_code: str
    count: int
    latitude: float
    longitude: float


@dataclass(frozen=True, kw_only=True)
class ZipCount:
    zip_code: str
    count: int


@dataclass(frozen=True, kw_only=True)
class ZipMapReport:
    total_users: int
    users_with_zip: int
    configured_zip_count: int
    map_points: tuple[ZipMapPoint, ...]
    unmapped_zip_counts: tuple[ZipCount, ...]


@dataclass(frozen=True, kw_only=True)
class DateRangePreset:
    label: str
    start_date: date
    end_date: date


@dataclass(frozen=True, kw_only=True)
class CheckinChartSegment:
    label: str
    css_class: str
    count: int
    percent: float


@dataclass(frozen=True, kw_only=True)
class CheckinChartBucket:
    label: str
    group_label: str | None
    group_total: int | None
    total: int
    segments: tuple[CheckinChartSegment, ...]


@dataclass(frozen=True, kw_only=True)
class CheckinTimeChart:
    title: str
    legend: tuple[tuple[str, str], ...]
    buckets: tuple[CheckinChartBucket, ...]


@dataclass(frozen=True, kw_only=True)
class CheckinSeasonAxisLabel:
    x: float
    label: str


@dataclass(frozen=True, kw_only=True)
class CheckinSeasonYAxisLabel:
    y: float
    label: str


@dataclass(frozen=True, kw_only=True)
class CheckinSeasonPoint:
    x: float
    y: float
    bar_x: float
    bar_y: float
    bar_width: float
    bar_height: float
    value_label_y: float
    label: str
    count: int


@dataclass(frozen=True, kw_only=True)
class CheckinSeasonSeries:
    year: int
    css_class: str
    points: tuple[CheckinSeasonPoint, ...]
    total: int
    latest_label: str | None


@dataclass(frozen=True, kw_only=True)
class CheckinSeasonPanel:
    label: str
    css_class: str
    max_count: int
    scale_max_count: int
    y_axis_labels: tuple[CheckinSeasonYAxisLabel, ...]
    series: tuple[CheckinSeasonSeries, ...]


@dataclass(frozen=True, kw_only=True)
class CheckinSeasonComparisonChart:
    title: str
    previous_year: int
    current_year: int
    peak_unit: str
    aria_unit: str
    x_axis_labels: tuple[CheckinSeasonAxisLabel, ...]
    panels: tuple[CheckinSeasonPanel, ...]


@dataclass(frozen=True, kw_only=True)
class LiveCheckInResult:
    check_in_at: datetime
    recorded: bool
    blocked: bool = False


@dataclass(frozen=True, kw_only=True)
class BannedDocumentEntry:
    entry_name: str
    folder_name: str | None
    member: Member | None


@dataclass(frozen=True, kw_only=True)
class DocumentsScanReport:
    configured: bool
    readable: bool
    total_users: int
    total_document_folders: int
    total_document_files: int
    users_with_guest_form: int
    users_with_id_document: int
    users_without_guest_form: int
    extension_counts: tuple[DocumentExtensionCount, ...]
    filename_patterns: tuple[DocumentFilenamePattern, ...]
    missing_guest_form_users: tuple[Member, ...]
    banned_documents: tuple[BannedDocumentEntry, ...]
    card_folders_without_user: tuple[tuple[str, tuple[str, ...]], ...]
    user_folders_with_extra_files: tuple[tuple[Member, tuple[str, ...]], ...]
    non_folder_entries: tuple[str, ...]


class GuestRegistrationFormError(ValueError):
    '''Raised when a visitor registration submission cannot be accepted.'''


class MembershipApplicationFormError(ValueError):
    '''Raised when a membership application submission cannot be accepted.'''


class MemberFormError(ValueError):
    '''Raised when admin-edited user fields cannot be accepted.'''


class CheckInFormError(ValueError):
    '''Raised when admin-edited check-ins cannot be accepted.'''


def _parse_member_form_date(form_data: Any, field_name: str) -> date | None:
    value = form_data.get(field_name, "").strip()
    if not value:
        return None
    parsed_date = _parse_flexible_date(value)
    if parsed_date is not None:
        return parsed_date
    raise MemberFormError("Enter valid user dates as MM/DD/YYYY.")


def _screening_status_from_form(form_data: Any) -> str | None:
    value = form_data.get("screening_status", "").strip()
    if value == "":
        return None
    if value not in SCREENING_STATUS_LABELS:
        raise MemberFormError("Choose a valid screening status.")
    return value


def _screening_status_label(status: str | None) -> str:
    return SCREENING_STATUS_LABELS.get(status or "", "")


def _parse_flexible_date(value: str) -> date | None:
    try:
        return date.fromisoformat(value)
    except ValueError:
        pass
    for date_format in ("%m/%d/%Y", "%m/%d/%y", "%m-%d-%Y", "%m-%d-%y"):
        try:
            return datetime.strptime(value, date_format).date()
        except ValueError:
            pass
    return None


def _member_from_form(member: Member, form_data: Any) -> Member:
    return Member(
        id=member.id,
        last_name=form_data.get("last_name", "").strip(),
        first_name=form_data.get("first_name", "").strip(),
        nickname=form_data.get("nickname", "").strip() or None,
        card_number=member.card_number,
        membership=form_data.get("membership", "").strip(),
        member_since=_parse_member_form_date(form_data, "member_since"),
        date_of_birth=_parse_member_form_date(form_data, "date_of_birth"),
        address=form_data.get("address", "").strip() or None,
        address2=form_data.get("address2", "").strip() or None,
        city=form_data.get("city", "").strip() or None,
        state=form_data.get("state", "").strip() or None,
        zip=form_data.get("zip", "").strip() or None,
        mailing_address=form_data.get("mailing_address", "").strip() or None,
        mailing_address2=form_data.get("mailing_address2", "").strip() or None,
        mailing_city=form_data.get("mailing_city", "").strip() or None,
        mailing_state=form_data.get("mailing_state", "").strip() or None,
        mailing_zip=form_data.get("mailing_zip", "").strip() or None,
        phone=member_repository.format_phone_number(form_data.get("phone")),
        email=form_data.get("email", "").strip() or None,
        work_phone=member_repository.format_phone_number(form_data.get("work_phone")),
        cell_phone=member_repository.format_phone_number(form_data.get("cell_phone")),
        screening_status=_screening_status_from_form(form_data),
    )


def _parse_checkin_datetime(value: str, *, required: bool) -> datetime | None:
    stripped_value = value.strip()
    if not stripped_value:
        if required:
            raise CheckInFormError("Check-in date and time are required.")
        return None
    try:
        parsed_value = datetime.fromisoformat(stripped_value)
    except ValueError as error:
        raise CheckInFormError("Enter a valid check-in date and time.") from error
    return parsed_value.replace(microsecond=0)


def _member_id_for_manual_checkin(member: Member, checkins: list[CheckIn]) -> str:
    for checkin in checkins:
        if checkin.member_id:
            return checkin.member_id
    return member.card_number


def _checkin_for_member(
    member: Member,
    *,
    check_in_at: datetime,
    member_id: str,
    membership: str | None = None,
    existing_checkin: CheckIn | None = None,
) -> CheckIn:
    return CheckIn(
        id=existing_checkin.id if existing_checkin else None,
        user_id=member.id,
        member_id=member_id,
        last_name=member.last_name,
        first_name=member.first_name,
        card_number=member.card_number,
        check_in_at=check_in_at,
        check_out_at=existing_checkin.check_out_at if existing_checkin else None,
        total_checkins=existing_checkin.total_checkins if existing_checkin else None,
        duration=existing_checkin.duration if existing_checkin else None,
        membership=membership or member.membership,
    )


def _record_checkin_change(
    connection: sqlite3.Connection,
    *,
    member_id: int,
    field_name: str,
    old_value: object,
    new_value: object,
) -> None:
    audit_repository.record_field_change(
        connection,
        entity_type="user",
        entity_id=member_id,
        action="edit",
        field_name=field_name,
        old_value=old_value,
        new_value=new_value,
    )


def _member_with_approved_application(
    member: Member,
    application: MembershipApplication,
) -> Member:
    return replace(
        member,
        membership=application.requested_membership,
        gender=application.gender,
        occupation=application.occupation,
        driver_license_number=application.driver_license_number,
        driver_license_state=application.driver_license_state,
        driver_license_expires=application.driver_license_expires,
        mailing_address=application.mailing_address,
        mailing_address2=application.mailing_address2,
        mailing_city=application.mailing_city,
        mailing_state=application.mailing_state,
        mailing_zip=application.mailing_zip,
        emergency_contact_name=application.emergency_contact_name,
        emergency_contact_relationship=application.emergency_contact_relationship,
        emergency_contact_phone=application.emergency_contact_phone,
        aanr_number=application.aanr_number,
        other_club_name=application.other_club_name,
    )


def _record_member_profile_changes(
    connection: sqlite3.Connection,
    *,
    old_member: Member,
    new_member: Member,
) -> None:
    if old_member.id is None:
        raise ValueError("old_member.id is required for audit.")
    for field_name in (
        "membership",
        "gender",
        "occupation",
        "driver_license_number",
        "driver_license_state",
        "driver_license_expires",
        "mailing_address",
        "mailing_address2",
        "mailing_city",
        "mailing_state",
        "mailing_zip",
        "emergency_contact_name",
        "emergency_contact_relationship",
        "emergency_contact_phone",
        "aanr_number",
        "other_club_name",
    ):
        old_value = getattr(old_member, field_name)
        new_value = getattr(new_member, field_name)
        if old_value == new_value:
            continue
        audit_repository.record_field_change(
            connection,
            entity_type="user",
            entity_id=old_member.id,
            action="edit",
            field_name=field_name,
            old_value=old_value,
            new_value=new_value,
        )


def _visible_member_audit_entries(audit_entries: list[audit_repository.AuditLogEntry]):
    return tuple(
        entry
        for entry in audit_entries
        if not entry.field_name.startswith("check-in ")
    )


def _checkin_membership_breakdown(checkins: list[CheckIn]) -> tuple[tuple[str, int], ...]:
    counts: Counter[str] = Counter()
    seen_users: set[str] = set()
    counted_memberships = {
        membership for _, membership, _ in CHECKIN_REPORT_MEMBERSHIP_BREAKDOWN
    }
    for checkin in checkins:
        user_key = (
            f"user:{checkin.user_id}"
            if checkin.user_id is not None
            else f"card:{checkin.card_number.strip().casefold()}"
        )
        if user_key in seen_users:
            continue
        seen_users.add(user_key)
        if checkin.membership in counted_memberships:
            counts[checkin.membership] += 1
    return tuple(
        (label, counts[membership])
        for label, membership, _ in CHECKIN_REPORT_MEMBERSHIP_BREAKDOWN
    )


def _date_range_presets(today: date) -> tuple[DateRangePreset, ...]:
    yesterday = today - timedelta(days=1)
    this_week_start = today - timedelta(days=today.weekday())
    last_week_start = this_week_start - timedelta(days=7)
    this_month_start = today.replace(day=1)
    last_month_end = this_month_start - timedelta(days=1)
    last_month_start = last_month_end.replace(day=1)
    return (
        DateRangePreset(label="Today", start_date=today, end_date=today),
        DateRangePreset(label="Yesterday", start_date=yesterday, end_date=yesterday),
        DateRangePreset(label="This Week", start_date=this_week_start, end_date=today),
        DateRangePreset(
            label="Last Week",
            start_date=last_week_start,
            end_date=this_week_start - timedelta(days=1),
        ),
        DateRangePreset(label="This Month", start_date=this_month_start, end_date=today),
        DateRangePreset(
            label="Last Month",
            start_date=last_month_start,
            end_date=last_month_end,
        ),
    )


def _active_date_range_preset_label(
    presets: tuple[DateRangePreset, ...],
    start_date: date,
    end_date: date,
) -> str | None:
    for preset in presets:
        if preset.start_date == start_date and preset.end_date == end_date:
            return preset.label
    return None


def _date_range_from_request(today: date) -> tuple[date, date]:
    start_date_raw = request.args.get("start_date", today.isoformat())
    end_date_raw = request.args.get("end_date", today.isoformat())
    try:
        start_date = date.fromisoformat(start_date_raw)
        end_date = date.fromisoformat(end_date_raw)
    except ValueError:
        abort(400, "Date range must use YYYY-MM-DD dates.")

    if start_date > end_date:
        abort(400, "Start date must be on or before end date.")
    return start_date, end_date


def _date_label(value: date) -> str:
    return f"{value.strftime('%b')} {value.day}"


def _hour_label(hour: int) -> str:
    hour_12 = hour % 12 or 12
    suffix = "AM" if hour < 12 else "PM"
    return f"{hour_12} {suffix}"


def _week_label(start_date: date, end_date: date) -> str:
    if start_date == end_date:
        return _date_label(start_date)
    return f"{_date_label(start_date)}-{_date_label(end_date)}"


def _week_start(value: date) -> date:
    return value - timedelta(days=value.weekday())


def _checkin_time_chart(
    checkins: list[CheckIn],
    start_date: date,
    end_date: date,
) -> CheckinTimeChart:
    span_days = (end_date - start_date).days + 1
    bucket_counts: dict[object, Counter[str]] = {}
    bucket_labels: dict[object, str] = {}
    bucket_mode = "day"

    if span_days == 1:
        title = "Check-ins by Hour"
        bucket_mode = "hour"
        for hour in range(24):
            bucket_counts[hour] = Counter()
            bucket_labels[hour] = _hour_label(hour)
        for checkin in checkins:
            bucket_counts[checkin.check_in_at.hour][checkin.membership] += 1
    elif span_days <= 62:
        title = "Check-ins by Day"
        for day_offset in range(span_days):
            bucket_date = start_date + timedelta(days=day_offset)
            bucket_counts[bucket_date] = Counter()
            bucket_labels[bucket_date] = _date_label(bucket_date)
        for checkin in checkins:
            bucket_counts[checkin.check_in_at.date()][checkin.membership] += 1
    else:
        title = "Check-ins by Week"
        bucket_mode = "week"
        for day_offset in range(0, span_days, 7):
            bucket_start = start_date + timedelta(days=day_offset)
            bucket_end = min(bucket_start + timedelta(days=6), end_date)
            bucket_counts[bucket_start] = Counter()
            bucket_labels[bucket_start] = _week_label(bucket_start, bucket_end)
        for checkin in checkins:
            week_offset = ((checkin.check_in_at.date() - start_date).days // 7) * 7
            bucket_start = start_date + timedelta(days=week_offset)
            bucket_counts[bucket_start][checkin.membership] += 1

    bucket_items = list(bucket_counts.items())
    if bucket_mode == "day":
        bucket_items = [
            (bucket, counts)
            for bucket, counts in bucket_items
            if sum(counts.values()) > 0
        ]
    else:
        populated_indexes = [
            index
            for index, (_, counts) in enumerate(bucket_items)
            if sum(counts.values()) > 0
        ]
        if populated_indexes:
            bucket_items = bucket_items[populated_indexes[0] : populated_indexes[-1] + 1]
        else:
            bucket_items = []

    bucket_totals = {bucket: sum(counts.values()) for bucket, counts in bucket_items}
    max_total = max(bucket_totals.values(), default=0)
    day_group_totals: dict[date, int] = {}
    if bucket_mode == "day":
        for bucket, total in bucket_totals.items():
            bucket_date = cast(date, bucket)
            bucket_week_start = _week_start(bucket_date)
            day_group_totals[bucket_week_start] = (
                day_group_totals.get(bucket_week_start, 0) + total
            )
    buckets: list[CheckinChartBucket] = []
    previous_week_start: date | None = None
    for bucket, counts in bucket_items:
        group_label = None
        group_total = None
        if bucket_mode == "day":
            bucket_date = cast(date, bucket)
            bucket_week_start = _week_start(bucket_date)
            if bucket_week_start != previous_week_start:
                group_label = f"Week of {_date_label(bucket_week_start)}"
                group_total = day_group_totals[bucket_week_start]
                previous_week_start = bucket_week_start
        segment_rows: list[CheckinChartSegment] = []
        for label, membership, css_class in CHECKIN_REPORT_MEMBERSHIP_BREAKDOWN:
            count = counts[membership]
            if count and max_total:
                segment_rows.append(
                    CheckinChartSegment(
                        label=label,
                        css_class=css_class,
                        count=count,
                        percent=(count / max_total) * 100,
                    )
                )
        buckets.append(
            CheckinChartBucket(
                label=bucket_labels[bucket],
                group_label=group_label,
                group_total=group_total,
                total=bucket_totals[bucket],
                segments=tuple(segment_rows),
            )
        )

    return CheckinTimeChart(
        title=title,
        legend=tuple(
            (label, css_class)
            for label, _, css_class in CHECKIN_REPORT_MEMBERSHIP_BREAKDOWN
        ),
        buckets=tuple(buckets),
    )


def _checkin_visit_number_chart(
    visit_number_counts: tuple[tuple[int, str, int], ...],
) -> CheckinTimeChart:
    exact_counts: dict[int, Counter[str]] = {}
    overflow_counts: Counter[str] = Counter()
    for visit_number, membership, count in visit_number_counts:
        if visit_number <= CHECKIN_VISIT_NUMBER_CHART_MAX_EXACT:
            exact_counts.setdefault(visit_number, Counter())[membership] += count
        else:
            overflow_counts[membership] += count

    totals = [sum(counts.values()) for counts in exact_counts.values()]
    overflow_total = sum(overflow_counts.values())
    max_total = max([*totals, overflow_total], default=0)
    buckets = []
    for visit_number in range(1, CHECKIN_VISIT_NUMBER_CHART_MAX_EXACT + 1):
        counts = exact_counts.get(visit_number, Counter())
        total = sum(counts.values())
        if not total:
            continue
        segments = []
        if max_total:
            for label, membership, css_class in CHECKIN_REPORT_MEMBERSHIP_BREAKDOWN:
                count = counts[membership]
                if not count:
                    continue
                segments.append(
                    CheckinChartSegment(
                        label=label,
                        css_class=css_class,
                        count=count,
                        percent=(count / max_total) * 100,
                    )
                )
        buckets.append(
            CheckinChartBucket(
                label=str(visit_number),
                group_label=None,
                group_total=None,
                total=total,
                segments=tuple(segments),
            )
        )

    if overflow_total:
        overflow_segments = []
        for label, membership, css_class in CHECKIN_REPORT_MEMBERSHIP_BREAKDOWN:
            count = overflow_counts[membership]
            if not count:
                continue
            overflow_segments.append(
                CheckinChartSegment(
                    label=label,
                    css_class=css_class,
                    count=count,
                    percent=(count / max_total) * 100,
                )
            )
        buckets.append(
            CheckinChartBucket(
                label=f"{CHECKIN_VISIT_NUMBER_CHART_MAX_EXACT + 1}+",
                group_label=None,
                group_total=None,
                total=overflow_total,
                segments=tuple(overflow_segments),
            )
        )

    return CheckinTimeChart(
        title="Check-ins by Check-in Number",
        legend=tuple(
            (label, css_class)
            for label, _, css_class in CHECKIN_REPORT_MEMBERSHIP_BREAKDOWN
        ),
        buckets=tuple(buckets) if max_total else (),
    )


def _season_date_range(year: int) -> tuple[date, date]:
    return date(year, 4, 1), date(year, 10, 31)


def _season_month_buckets(year: int, *, today: date) -> tuple[date, ...]:
    season_start, season_end = _season_date_range(year)
    if year == today.year:
        if today < season_start:
            return ()
        month_end = min(today, season_end).month
    else:
        month_end = season_end.month
    return tuple(date(year, month, 1) for month in range(season_start.month, month_end + 1))


def _season_x_axis_labels(bucket_count: int, labels: tuple[str, ...]) -> tuple[CheckinSeasonAxisLabel, ...]:
    if not labels:
        return ()
    return tuple(
        CheckinSeasonAxisLabel(
            x=((index + 0.5) / bucket_count) * 100,
            label=label,
        )
        for index, label in enumerate(labels[:bucket_count])
    )


def _nice_season_tick_step(max_count: int) -> int:
    if max_count <= 3:
        return 1
    rough_step = max_count / 3
    magnitude = 10 ** math.floor(math.log10(rough_step))
    for multiplier in (1, 2, 5, 10):
        step = multiplier * magnitude
        if step >= rough_step:
            return int(step)
    return int(10 * magnitude)


def _season_y_axis_labels(max_count: int) -> tuple[int, tuple[CheckinSeasonYAxisLabel, ...]]:
    if max_count <= 0:
        return 0, ()
    step = _nice_season_tick_step(max_count)
    scale_max_count = int(math.ceil(max_count / step) * step)
    labels = tuple(
        CheckinSeasonYAxisLabel(
            y=100 - ((tick / scale_max_count) * 100),
            label=str(tick),
        )
        for tick in range(scale_max_count, -1, -step)
    )
    return scale_max_count, labels


def _checkin_season_comparison_chart(
    checkins: list[CheckIn],
    *,
    today: date,
    by_month: bool = False,
) -> CheckinSeasonComparisonChart:
    current_year = today.year
    previous_year = current_year - 1
    comparison_years = (previous_year, current_year)
    season_ranges = {
        year: _season_date_range(year)
        for year in comparison_years
    }
    current_season_start, current_season_end = season_ranges[current_year]
    season_ranges[current_year] = (
        current_season_start,
        min(today, current_season_end),
    )
    bucket_dates_by_year: dict[int, set[date]] = {
        year: set()
        for year in comparison_years
    }
    counts: dict[int, dict[str, Counter[date]]] = {
        year: {
            membership: Counter()
            for _, membership, _ in CHECKIN_REPORT_MEMBERSHIP_BREAKDOWN
        }
        for year in comparison_years
    }

    counted_memberships = {
        membership for _, membership, _ in CHECKIN_REPORT_MEMBERSHIP_BREAKDOWN
    }
    for checkin in checkins:
        checkin_date = checkin.check_in_at.date()
        checkin_year = checkin_date.year
        if checkin_year not in season_ranges or checkin.membership not in counted_memberships:
            continue
        season_start, season_end = season_ranges[checkin_year]
        if season_start <= checkin_date <= season_end:
            bucket_date = (
                date(checkin_year, checkin_date.month, 1)
                if by_month
                else checkin_date
            )
            bucket_dates_by_year[checkin_year].add(bucket_date)
            counts[checkin_year][checkin.membership][bucket_date] += 1

    buckets_by_year = {
        year: (
            _season_month_buckets(year, today=today)
            if by_month
            else tuple(sorted(bucket_dates))
        )
        for year, bucket_dates in bucket_dates_by_year.items()
    }
    max_bucket_count = max(
        (len(bucket_dates) for bucket_dates in buckets_by_year.values()),
        default=0,
    )
    x_axis_labels = _season_x_axis_labels(
        max_bucket_count,
        tuple(date(2000, month, 1).strftime("%b") for month in range(4, 11))
        if by_month
        else (),
    )

    panels: list[CheckinSeasonPanel] = []
    for label, memberships, css_class in CHECKIN_SEASON_COMPARISON_GROUPS:
        counts_by_year: dict[int, list[tuple[date, int]]] = {}
        max_count = 0
        for year in comparison_years:
            year_points: list[tuple[date, int]] = []
            for bucket_date in buckets_by_year[year]:
                count = sum(
                    counts[year][membership][bucket_date]
                    for membership in memberships
                )
                year_points.append((bucket_date, count))
                max_count = max(max_count, count)
            counts_by_year[year] = year_points

        scale_max_count, y_axis_labels = _season_y_axis_labels(max_count)
        series_rows: list[CheckinSeasonSeries] = []
        year_count = len(comparison_years)
        point_gap = 0.6
        for year_index, year in enumerate(comparison_years):
            year_points = counts_by_year[year]
            chart_points: list[CheckinSeasonPoint] = []
            for index, (bucket_date, count) in enumerate(year_points):
                group_width = 100 / max_bucket_count if max_bucket_count else 100
                lane_width = max(0.35, (group_width - point_gap) / year_count)
                lane_x = (index * group_width) + (point_gap / 2) + (year_index * lane_width)
                point_height = 0.0 if scale_max_count <= 0 else (count / scale_max_count) * 100
                x = lane_x + (lane_width / 2)
                y = 100 - point_height
                chart_points.append(
                    CheckinSeasonPoint(
                        x=x,
                        y=y,
                        bar_x=lane_x,
                        bar_y=y,
                        bar_width=lane_width,
                        bar_height=point_height,
                        value_label_y=max(4.0, y - 2.0),
                        label=(
                            bucket_date.strftime("%b")
                            if by_month
                            else _date_label(bucket_date)
                        ),
                        count=count,
                    )
                )
            series_rows.append(
                CheckinSeasonSeries(
                    year=year,
                    css_class="season-current" if year == current_year else "season-previous",
                    points=tuple(chart_points),
                    total=sum(point.count for point in chart_points),
                    latest_label=chart_points[-1].label if chart_points else None,
                )
            )
        panels.append(
            CheckinSeasonPanel(
                label=label,
                css_class=css_class,
                max_count=max_count,
                scale_max_count=scale_max_count,
                y_axis_labels=y_axis_labels,
                series=tuple(series_rows),
            )
        )

    return CheckinSeasonComparisonChart(
        title="Season Comparison by Month" if by_month else "Season Comparison by Operating Day",
        previous_year=previous_year,
        current_year=current_year,
        peak_unit="month" if by_month else "day",
        aria_unit="month" if by_month else "operating day",
        x_axis_labels=x_axis_labels,
        panels=tuple(panels),
    )


def _checkins_count_text(checkins: list[CheckIn]) -> str:
    count = len(checkins)
    return f"{count} {'check-in' if count == 1 else 'check-ins'}"


def _checkins_report_context(
    connection: sqlite3.Connection,
    start_date: date,
    end_date: date,
    *,
    today: date,
) -> dict[str, object]:
    checkins = checkin_repository.list_checkins_for_date_range(
        connection,
        start_date,
        end_date,
    )
    previous_season_start, _ = _season_date_range(today.year - 1)
    _, current_season_end = _season_date_range(today.year)
    season_checkins = checkin_repository.list_checkins_for_date_range(
        connection,
        previous_season_start,
        min(today, current_season_end),
    )
    visit_number_counts = checkin_repository.count_visit_numbers_for_date_range(
        connection,
        start_date,
        end_date,
    )
    notes_by_user_id = user_note_repository.list_user_notes_by_user_ids(
        connection,
        {checkin.user_id for checkin in checkins if checkin.user_id is not None},
    )
    return {
        "checkins": checkins,
        "count_text": _checkins_count_text(checkins),
        "membership_breakdown": _checkin_membership_breakdown(checkins),
        "time_chart": _checkin_time_chart(checkins, start_date, end_date),
        "visit_number_chart": _checkin_visit_number_chart(visit_number_counts),
        "season_comparison_chart": _checkin_season_comparison_chart(
            season_checkins,
            today=today,
        ),
        "monthly_season_comparison_chart": _checkin_season_comparison_chart(
            season_checkins,
            today=today,
            by_month=True,
        ),
        "notes_by_user_id": notes_by_user_id,
        "start_date": start_date,
        "end_date": end_date,
    }


def _checkins_report_stream_payload(
    connection: sqlite3.Connection,
    start_date: date,
    end_date: date,
    *,
    today: date,
) -> dict[str, str]:
    context = _checkins_report_context(connection, start_date, end_date, today=today)
    return {
        "count_text": str(context["count_text"]),
        "membership_breakdown_html": render_template(
            "club_admin/_checkins_membership_breakdown.html",
            **context,
        ),
        "time_chart_html": render_template(
            "club_admin/_checkins_chart.html",
            chart=context["time_chart"],
            chart_extra_class="",
            legend_label="Membership type legend",
        ),
        "visit_number_chart_html": render_template(
            "club_admin/_checkins_chart.html",
            chart=context["visit_number_chart"],
            chart_extra_class="checkins-visit-number-chart",
            legend_label="Check-in group legend",
        ),
        "season_comparison_chart_html": render_template(
            "club_admin/_checkins_season_comparison.html",
            chart=context["season_comparison_chart"],
        ),
        "monthly_season_comparison_chart_html": render_template(
            "club_admin/_checkins_season_comparison.html",
            chart=context["monthly_season_comparison_chart"],
        ),
        "rows_html": render_template(
            "club_admin/_checkins_report_rows.html",
            **context,
        ),
    }


def _checkins_report_event_stream(
    connection_context: Callable[[], Iterator[sqlite3.Connection]],
    start_date: date,
    end_date: date,
    *,
    today: date,
) -> Iterator[str]:
    last_version = checkin_events.current_checkins_version()
    while True:
        with connection_context() as connection:
            payload = _checkins_report_stream_payload(
                connection,
                start_date,
                end_date,
                today=today,
            )
        yield f"data: {json.dumps(payload)}\n\n"

        deadline = time.monotonic() + 30.0
        while True:
            timeout_seconds = min(1.0, max(0.0, deadline - time.monotonic()))
            next_version = checkin_events.wait_for_checkins_change(
                last_version,
                timeout_seconds,
            )
            if next_version != last_version:
                last_version = next_version
                break
            if time.monotonic() >= deadline:
                yield ": keepalive\n\n"
                deadline = time.monotonic() + 30.0


def _collapsed_text(value: str | None) -> str:
    return " ".join(str(value or "").strip().split())


def _visitor_text_or_none(form_data: Any, field_name: str) -> str | None:
    value = _collapsed_text(form_data.get(field_name, ""))
    return value or None


def _titlecase_text(value: str | None) -> str | None:
    cleaned_value = _collapsed_text(value)
    if not cleaned_value:
        return None
    if cleaned_value.islower() or cleaned_value.isupper():
        return cleaned_value.title()
    return cleaned_value


def _visitor_title_text_or_none(form_data: Any, field_name: str) -> str | None:
    return _titlecase_text(form_data.get(field_name, ""))


def _visitor_state_or_none(form_data: Any) -> str | None:
    value = _collapsed_text(form_data.get("state", ""))
    return value.upper() if value else None


def _visitor_zip_or_none(form_data: Any) -> str | None:
    value = _collapsed_text(form_data.get("zip", ""))
    digits = re.sub(r"\D+", "", value)
    if len(digits) == 9:
        return f"{digits[:5]}-{digits[5:]}"
    if len(digits) >= 5:
        return digits[:5]
    return value or None


def _visitor_bool(form_data: Any, field_name: str) -> bool:
    return form_data.get(field_name) in {"1", "true", "yes", "on"}


def _visitor_choice(
    form_data: Any,
    field_name: str,
    allowed_values: set[str],
) -> str | None:
    value = form_data.get(field_name, "").strip()
    return value if value in allowed_values else None


def _membership_choice(
    form_data: Any,
    field_name: str,
    allowed_values: set[str],
    message: str,
) -> str:
    value = form_data.get(field_name, "").strip()
    if value in allowed_values:
        return value
    raise MembershipApplicationFormError(message)


def _membership_optional_choice(
    form_data: Any,
    field_name: str,
    allowed_values: set[str],
) -> str | None:
    value = form_data.get(field_name, "").strip()
    return value if value in allowed_values else None


def _membership_bool_choice(
    form_data: Any,
    field_name: str,
    message: str,
) -> bool:
    value = form_data.get(field_name, "").strip()
    if value == "yes":
        return True
    if value == "no":
        return False
    raise MembershipApplicationFormError(message)


def _membership_optional_bool_choice(form_data: Any, field_name: str) -> bool | None:
    value = form_data.get(field_name, "").strip()
    if value == "yes":
        return True
    if value == "no":
        return False
    return None


def _parse_membership_application_date(
    form_data: Any,
    field_name: str,
    *,
    required: bool = False,
    message: str = "Enter valid dates.",
) -> date | None:
    value = form_data.get(field_name, "").strip()
    if not value:
        if required:
            raise MembershipApplicationFormError(message)
        return None
    parsed_date = _parse_flexible_date(value)
    if parsed_date is None:
        raise MembershipApplicationFormError(message)
    return parsed_date


def _membership_application_from_form(
    form_data: Any,
    *,
    user_id: int,
) -> MembershipApplication:
    requested_membership = _membership_choice(
        form_data,
        "requested_membership",
        {"Full Member", "Associate Member"},
        "Choose a membership type.",
    )
    convicted = _membership_bool_choice(
        form_data,
        "convicted",
        "Answer the conviction question.",
    )
    conviction_explanation = _visitor_text_or_none(form_data, "conviction_explanation")
    if convicted and not conviction_explanation:
        raise MembershipApplicationFormError("Explain the conviction answer.")
    gender = _membership_choice(
        form_data,
        "gender",
        {"female", "male", "non_binary", "self_described", "prefer_not_to_say"},
        "Choose a gender option.",
    )
    occupation = _membership_required_text(
        form_data,
        "occupation",
        "Occupation is required.",
        titlecase=True,
    )
    driver_license_number = _membership_required_text(
        form_data,
        "driver_license_number",
        "Driver license number is required.",
    )
    driver_license_state = _visitor_state_or_none_for_field(
        form_data,
        "driver_license_state",
    )
    if not driver_license_state:
        raise MembershipApplicationFormError("Driver license state is required.")
    driver_license_expires = _parse_membership_application_date(
        form_data,
        "driver_license_expires",
        required=True,
        message="Driver license expiration must use MM/DD/YYYY.",
    )
    club_news_name_permission = _membership_bool_choice(
        form_data,
        "club_news_name_permission",
        "Answer the club news name permission question.",
    )
    social_nudity_practiced = _membership_optional_bool_choice(
        form_data,
        "social_nudity_practiced",
    )
    aanr_member = _membership_bool_choice(
        form_data,
        "aanr_member",
        "Answer the AANR membership question.",
    )
    other_club_member = _membership_bool_choice(
        form_data,
        "other_club_member",
        "Answer the other nudist club membership question.",
    )
    emergency_contact_name = _membership_required_text(
        form_data,
        "emergency_contact_name",
        "Emergency contact name is required.",
        titlecase=True,
    )
    emergency_contact_relationship = _membership_required_text(
        form_data,
        "emergency_contact_relationship",
        "Emergency contact relationship is required.",
        titlecase=True,
    )
    emergency_contact_phone = member_repository.format_phone_number(
        _membership_required_text(
            form_data,
            "emergency_contact_phone",
            "Emergency contact phone is required.",
        )
    )
    return MembershipApplication(
        user_id=user_id,
        requested_membership=requested_membership,
        gender=gender,
        occupation=occupation,
        driver_license_number=driver_license_number,
        driver_license_state=driver_license_state,
        driver_license_expires=driver_license_expires,
        mailing_address=_visitor_text_or_none(form_data, "mailing_address"),
        mailing_address2=_visitor_text_or_none(form_data, "mailing_address2"),
        mailing_city=_visitor_title_text_or_none(form_data, "mailing_city"),
        mailing_state=_visitor_state_or_none_for_field(form_data, "mailing_state"),
        mailing_zip=_visitor_text_or_none(form_data, "mailing_zip"),
        club_news_name_permission=club_news_name_permission,
        emergency_contact_name=emergency_contact_name,
        emergency_contact_relationship=emergency_contact_relationship,
        emergency_contact_phone=emergency_contact_phone,
        minor_children=_visitor_text_or_none(form_data, "minor_children"),
        convicted=convicted,
        conviction_explanation=conviction_explanation,
        social_nudity_practiced=social_nudity_practiced,
        social_nudity_duration=_visitor_text_or_none(
            form_data,
            "social_nudity_duration",
        ),
        social_nudity_experience=_visitor_text_or_none(
            form_data, "social_nudity_experience"
        ),
        aanr_member=aanr_member,
        aanr_number=_visitor_text_or_none(form_data, "aanr_number"),
        aanr_expires=_parse_membership_application_date(
            form_data,
            "aanr_expires",
            message="AANR expiration must use MM/DD/YYYY.",
        ),
        other_club_member=other_club_member,
        other_club_name=_visitor_title_text_or_none(form_data, "other_club_name"),
        agreement_accepted=True,
        signed_at=None,
    )


def _membership_application_form_data(
    application: MembershipApplication,
) -> dict[str, object]:
    return {
        "requested_membership": application.requested_membership,
        "gender": application.gender or "",
        "occupation": application.occupation or "",
        "driver_license_number": application.driver_license_number or "",
        "driver_license_state": application.driver_license_state or "",
        "driver_license_expires": application.driver_license_expires,
        "mailing_address": application.mailing_address or "",
        "mailing_address2": application.mailing_address2 or "",
        "mailing_city": application.mailing_city or "",
        "mailing_state": application.mailing_state or "",
        "mailing_zip": application.mailing_zip or "",
        "club_news_name_permission": (
            "yes" if application.club_news_name_permission else "no"
        ),
        "emergency_contact_name": application.emergency_contact_name or "",
        "emergency_contact_relationship": (
            application.emergency_contact_relationship or ""
        ),
        "emergency_contact_phone": application.emergency_contact_phone or "",
        "minor_children": application.minor_children or "",
        "convicted": "yes" if application.convicted else "no",
        "conviction_explanation": application.conviction_explanation or "",
        "social_nudity_practiced": (
            ""
            if application.social_nudity_practiced is None
            else "yes"
            if application.social_nudity_practiced
            else "no"
        ),
        "social_nudity_duration": application.social_nudity_duration or "",
        "aanr_member": "yes" if application.aanr_member else "no",
        "aanr_number": application.aanr_number or "",
        "aanr_expires": application.aanr_expires,
        "other_club_member": "yes" if application.other_club_member else "no",
        "other_club_name": application.other_club_name or "",
    }


def _visitor_state_or_none_for_field(form_data: Any, field_name: str) -> str | None:
    value = _collapsed_text(form_data.get(field_name, ""))
    return value.upper() if value else None


def _membership_required_text(
    form_data: Any,
    field_name: str,
    message: str,
    *,
    titlecase: bool = False,
) -> str:
    value = (
        _visitor_title_text_or_none(form_data, field_name)
        if titlecase
        else _visitor_text_or_none(form_data, field_name)
    )
    if value:
        return value
    raise MembershipApplicationFormError(message)


def _members_csv(roster: list[member_repository.MemberReportRow]) -> str:
    output = io.StringIO(newline="")
    writer = csv.writer(output)
    writer.writerow(
        (
            "User ID",
            "Card Number",
            "Last Name",
            "First Name",
            "Nickname",
            "Membership",
            "Screening Status",
            "First Visit",
            "Last Visit",
            "Date of Birth",
            "Address",
            "Address 2",
            "City",
            "State",
            "ZIP",
            "Phone",
            "Email",
            "Work Phone",
            "Cell Phone",
            "Documents",
            "Visits In Period",
        )
    )
    for row in roster:
        member = row.member
        last_visit = row.last_check_in_at.date() if row.last_check_in_at else None
        writer.writerow(
            (
                member.id or "",
                member.card_number,
                member.last_name,
                member.first_name,
                member.nickname or "",
                member.membership,
                _screening_status_label(member.screening_status),
                member.member_since or "",
                last_visit or "",
                member.date_of_birth or "",
                member.address or "",
                member.address2 or "",
                member.city or "",
                member.state or "",
                member.zip or "",
                member.phone or "",
                member.email or "",
                member.work_phone or "",
                member.cell_phone or "",
                row.document_count,
                "" if row.checkin_count is None else row.checkin_count,
            )
        )
    return output.getvalue()


def _parse_visitor_visit_date(form_data: Any) -> date:
    value = form_data.get("visit_date", "").strip()
    if not value:
        return date.today()
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise GuestRegistrationFormError("Visit date must use YYYY-MM-DD.")


def _parse_visitor_date_of_birth(form_data: Any) -> date | None:
    value = form_data.get("date_of_birth", "").strip()
    if not value:
        return None
    parsed_date = _parse_flexible_date(value)
    if parsed_date is None:
        raise GuestRegistrationFormError("Date of birth must use MM/DD/YYYY.")
    return parsed_date


def _guest_registration_validation_message(
    member: Member,
    registration: GuestRegistration,
) -> str | None:
    if not member.first_name or not member.last_name:
        return "First and last name are required."
    if not member.cell_phone and not member.phone and not member.email:
        return "Phone or email is required."
    if member.date_of_birth is None:
        return "Date of birth is required."
    if not member.address or not member.city or not member.state or not member.zip:
        return "Street address, city, state, and zip code are required."
    if not registration.marital_status:
        return "Marital status is required."
    if not registration.heard_about:
        return "How you heard about us is required."
    return None


def _guest_registration_from_form(
    form_data: Any,
    *,
    card_number: str,
) -> tuple[Member, GuestRegistration]:
    other_phone = member_repository.format_phone_number(form_data.get("other_phone"))
    other_phone_type = _visitor_choice(form_data, "other_phone_type", {"home", "work", "other"})
    phone = other_phone if other_phone_type != "work" else None
    work_phone = other_phone if other_phone_type == "work" else None

    member = Member(
        last_name=_visitor_title_text_or_none(form_data, "last_name") or "",
        first_name=_visitor_title_text_or_none(form_data, "first_name") or "",
        nickname=_visitor_title_text_or_none(form_data, "nickname"),
        card_number=card_number,
        membership="Visitor",
        date_of_birth=_parse_visitor_date_of_birth(form_data),
        address=_visitor_title_text_or_none(form_data, "address"),
        city=_visitor_title_text_or_none(form_data, "city"),
        state=_visitor_state_or_none(form_data),
        zip=_visitor_zip_or_none(form_data),
        phone=phone,
        email=_visitor_text_or_none(form_data, "email"),
        work_phone=work_phone,
        cell_phone=member_repository.format_phone_number(form_data.get("cell_phone")),
    )
    registration = GuestRegistration(
        visit_date=_parse_visitor_visit_date(form_data),
        other_phone=other_phone,
        other_phone_type=other_phone_type,
        marital_status=_visitor_choice(
            form_data,
            "marital_status",
            {"single", "married", "recognized_couple"},
        ),
        partner_name=_visitor_title_text_or_none(form_data, "partner_name"),
        guest_of_member=_visitor_bool(form_data, "guest_of_member"),
        member_name=_visitor_title_text_or_none(form_data, "member_name"),
        heard_about=_visitor_text_or_none(form_data, "heard_about"),
        newsletter_opt_out=_visitor_bool(form_data, "newsletter_opt_out"),
    )
    return member, registration


def _guest_registration_identity_token(value: str | None) -> str:
    return member_repository.normalize_name_token(value)


def _guest_registration_contact_tokens(member: Member) -> set[str]:
    tokens = {
        member_repository.normalize_phone(member.phone),
        member_repository.normalize_phone(member.work_phone),
        member_repository.normalize_phone(member.cell_phone),
    }
    if member.email:
        tokens.add(member.email.strip().lower())
    tokens.discard("")
    return tokens


def _matching_guest_registration_member(
    connection: sqlite3.Connection,
    member: Member,
) -> Member | None:
    if member.date_of_birth is None:
        return None
    first_name = _guest_registration_identity_token(member.first_name)
    last_name = _guest_registration_identity_token(member.last_name)
    contact_tokens = _guest_registration_contact_tokens(member)
    if not first_name or not last_name or not contact_tokens:
        return None

    matches = [
        candidate
        for candidate in member_repository.list_members(connection)
        if candidate.membership == "Visitor"
        and candidate.date_of_birth == member.date_of_birth
        and _guest_registration_identity_token(candidate.first_name) == first_name
        and _guest_registration_identity_token(candidate.last_name) == last_name
        and bool(contact_tokens & _guest_registration_contact_tokens(candidate))
    ]
    if not matches:
        return None
    return sorted(matches, key=lambda candidate: int(candidate.id or 0))[0]


def _guest_registration_exists_for_visit(
    connection: sqlite3.Connection,
    *,
    user_id: int,
    visit_date: date,
) -> bool:
    row = connection.execute(
        """
        SELECT 1
        FROM guest_registrations
        WHERE user_id = ? AND visit_date = ?
        LIMIT 1
        """,
        (user_id, visit_date.isoformat()),
    ).fetchone()
    return row is not None


def _guest_registration_exists(
    connection: sqlite3.Connection,
    *,
    user_id: int,
) -> bool:
    row = connection.execute(
        "SELECT 1 FROM guest_registrations WHERE user_id = ? LIMIT 1",
        (user_id,),
    ).fetchone()
    return row is not None


def _membership_application_form_spec() -> guest_form.GuestFormSpec:
    try:
        return guest_form.load_required_form_spec(
            current_app.config[
                "USER_MANAGEMENT_MEMBERSHIP_APPLICATION_DEFINITION_PATH"
            ]
        )
    except guest_form.FormDefinitionError as error:
        current_app.logger.error("Membership application unavailable: %s", error)
        abort(
            503,
            "Membership applications are temporarily unavailable. Please see the front desk.",
        )


def _safe_next_url(next_url: str | None) -> str:
    if next_url and next_url.startswith("/") and not next_url.startswith("//"):
        return next_url
    return url_for("members")


def _bearer_token() -> str:
    authorization = request.headers.get("Authorization", "").strip()
    prefix = "Bearer "
    if authorization.startswith(prefix):
        return authorization[len(prefix) :].strip()
    return ""


def _checkin_monitor_token_from_request() -> str:
    return request.headers.get(CHECKIN_MONITOR_TOKEN_HEADER, "").strip() or _bearer_token()


def _request_has_checkin_monitor_access() -> bool:
    if session.get("user_management_admin_authenticated") is True:
        return True
    configured_token = str(
        current_app.config.get("USER_MANAGEMENT_CHECKIN_MONITOR_TOKEN", "")
    ).strip()
    request_token = _checkin_monitor_token_from_request()
    return bool(configured_token and request_token) and hmac.compare_digest(
        configured_token,
        request_token,
    )


def _checkin_monitor_display_name(checkin: CheckIn) -> str:
    first_or_nickname = checkin.first_name.strip()
    last_name = checkin.last_name.strip()
    return " ".join(
        part for part in (first_or_nickname, last_name) if part
    ).strip()


def _checkin_monitor_payload(checkin: CheckIn) -> dict[str, object]:
    previous_visit_date = (
        checkin.previous_check_in_at.date().isoformat()
        if checkin.previous_check_in_at is not None
        else None
    )
    return {
        "id": checkin.id,
        "user_id": checkin.user_id,
        "display_name": _checkin_monitor_display_name(checkin),
        "membership": checkin.membership,
        "previous_visit_date": previous_visit_date,
        "checkin_count": checkin.checkin_count,
        "check_in_at": _datetime_to_stored_text(checkin.check_in_at),
        "check_in_at_local": _format_sqlite_utc_datetime(checkin.check_in_at),
    }


def _guest_form_path_for_member(member: Member, documents_dir: str) -> Path | None:
    if not documents_dir.strip():
        return None
    base_dir = Path(documents_dir).expanduser().resolve(strict=False)
    card_dir = _card_document_dir(member, base_dir)
    if card_dir is None or not card_dir.is_dir():
        return None
    candidate = _first_guest_form_image(card_dir)
    if candidate is None:
        return None
    candidate = candidate.resolve(strict=False)
    try:
        candidate.relative_to(base_dir)
    except ValueError:
        return None
    return candidate if candidate.is_file() else None


def _card_document_dir(member: Member, base_dir: Path) -> Path | None:
    return _case_insensitive_child_path(base_dir, member.card_number.strip().strip("'").strip())


def _member_document_path(member: Member, documents_dir: str, document_name: str) -> Path | None:
    if not documents_dir.strip() or not _is_safe_document_entry_name(document_name):
        return None
    base_dir = Path(documents_dir).expanduser().resolve(strict=False)
    card_dir = _card_document_dir(member, base_dir)
    if card_dir is None or not card_dir.is_dir():
        return None
    candidate = _case_insensitive_child_path(card_dir, document_name)
    if candidate is None:
        return None
    candidate = candidate.resolve(strict=False)
    try:
        candidate.relative_to(base_dir)
    except ValueError:
        return None
    return candidate if candidate.is_file() else None


def _member_document_upload_path(
    member: Member,
    documents_dir: str,
    document_name: str,
) -> Path | None:
    card_folder_name = member.card_number.strip().strip("'").strip()
    if (
        not documents_dir.strip()
        or not card_folder_name
        or not _is_safe_document_entry_name(card_folder_name)
        or not _is_safe_document_entry_name(document_name)
    ):
        return None
    base_dir = Path(documents_dir).expanduser().resolve(strict=False)
    card_dir = base_dir / card_folder_name
    resolved_card_dir = card_dir.resolve(strict=False)
    try:
        resolved_card_dir.relative_to(base_dir)
    except ValueError:
        return None

    candidate = resolved_card_dir / document_name
    if not candidate.exists():
        return candidate

    stem = candidate.stem
    suffix = candidate.suffix
    for index in range(2, 1000):
        deduped_candidate = resolved_card_dir / f"{stem} {index}{suffix}"
        if not deduped_candidate.exists():
            return deduped_candidate
    return None


def _uploaded_document_name(filename: str) -> str | None:
    document_name = filename.replace("\\", "/").rsplit("/", 1)[-1].strip()
    document_name = unicodedata.normalize("NFKC", document_name)
    document_name = "".join(character for character in document_name if character.isprintable()).strip()
    return document_name if _is_safe_document_entry_name(document_name) else None


def _member_document_names(
    member: Member,
    documents_dir: str,
    *,
    include_guest_form: bool,
) -> tuple[str, ...]:
    if not documents_dir.strip():
        return ()
    base_dir = Path(documents_dir).expanduser().resolve(strict=False)
    card_dir = _card_document_dir(member, base_dir)
    if card_dir is None or not card_dir.is_dir():
        return ()

    guest_form_path = _first_guest_form_image(card_dir)
    resolved_guest_form_path = guest_form_path.resolve(strict=False) if guest_form_path else None
    document_names: list[str] = []
    for child in _folder_entry_paths(card_dir):
        if not child.is_file():
            continue
        if not include_guest_form and resolved_guest_form_path == child.resolve(strict=False):
            continue
        document_names.append(child.name)
    return tuple(document_names)


def _id_document_name_for_member(member: Member, documents_dir: str) -> str | None:
    managed_document_name: str | None = None
    fallback_document_name: str | None = None
    for document_name in _member_document_names(
        member,
        documents_dir,
        include_guest_form=False,
    ):
        if not _is_document_image_name(document_name):
            continue
        if document_name.casefold() == DRIVER_LICENSE_DOCUMENT_NAME.casefold():
            managed_document_name = document_name
            continue
        normalized_stem = _normalized_filename(Path(document_name).stem)
        normalized_stem = normalized_stem.replace("'", "").replace("’", "")
        if ID_DOCUMENT_NAME_PATTERN.match(normalized_stem):
            fallback_document_name = fallback_document_name or document_name
    return managed_document_name or fallback_document_name


def _member_document_preview(member: Member, documents_dir: str) -> MemberDocumentPreview | None:
    if _guest_form_path_for_member(member, documents_dir) is not None:
        return MemberDocumentPreview(title="Visitor Form", is_guest_form=True)
    id_document_name = _id_document_name_for_member(member, documents_dir)
    if id_document_name is None:
        return None
    return MemberDocumentPreview(title="Driver License", document_name=id_document_name)


def _id_document_storage_path(member: Member, documents_dir: str) -> Path | None:
    card_folder_name = member.card_number.strip().strip("'").strip()
    if (
        not documents_dir.strip()
        or not card_folder_name
        or not _is_safe_document_entry_name(card_folder_name)
    ):
        return None
    base_dir = Path(documents_dir).expanduser().resolve(strict=False)
    card_dir = base_dir / card_folder_name
    resolved_card_dir = card_dir.resolve(strict=False)
    try:
        resolved_card_dir.relative_to(base_dir)
    except ValueError:
        return None
    return resolved_card_dir / DRIVER_LICENSE_DOCUMENT_NAME


def _first_content_cluster_end(
    values: list[int],
    total_length: int,
    *,
    max_blank_gap_ratio: float,
) -> int | None:
    if not values:
        return None
    max_blank_gap = max(24, int(total_length * max_blank_gap_ratio))
    min_cluster_extent = max(80, int(total_length * 0.08))
    cluster_start = values[0]
    cluster_end = values[0]
    for value in values[1:]:
        if value - cluster_end > max_blank_gap and cluster_end - cluster_start + 1 >= min_cluster_extent:
            return cluster_end + 1
        cluster_end = value
    return cluster_end + 1


def _rgb_luminance(rgb: tuple[int, int, int]) -> float:
    return (0.2126 * rgb[0]) + (0.7152 * rgb[1]) + (0.0722 * rgb[2])


def _sample_driver_license_background(image: Image.Image) -> tuple[int, int, int]:
    width, height = image.size
    if width <= 0 or height <= 0:
        return (255, 255, 255)

    edge_x = max(8, int(width * 0.04))
    edge_y = max(8, int(height * 0.04))
    step = max(1, min(width, height) // 120)
    pixels = image.load()
    samples: list[tuple[int, int, int]] = []

    for y in range(max(0, height - edge_y), height, step):
        for x in range(0, width, step):
            samples.append(pixels[x, y])
    for x in range(max(0, width - edge_x), width, step):
        for y in range(0, height, step):
            samples.append(pixels[x, y])

    if not samples:
        return (255, 255, 255)

    return tuple(
        sorted(sample[channel] for sample in samples)[len(samples) // 2]
        for channel in range(3)
    )


def _driver_license_crop_mask(image: Image.Image) -> Image.Image:
    rgb_image = image.convert("RGB")
    background_color = _sample_driver_license_background(rgb_image)
    if _rgb_luminance(background_color) < DRIVER_LICENSE_DARK_BACKGROUND_LUMINANCE:
        background = Image.new("RGB", rgb_image.size, background_color)
    else:
        background = Image.new("RGB", rgb_image.size, "WHITE")
    difference = ImageChops.difference(rgb_image, background).convert("L")
    return difference.point(
        lambda value: 255 if value > DRIVER_LICENSE_CROP_THRESHOLD else 0
    )


def _image_content_bbox(image: Image.Image) -> tuple[int, int, int, int] | None:
    rgb_image = image.convert("RGB")
    mask = _driver_license_crop_mask(rgb_image)
    mask_pixels = mask.load()
    width, height = mask.size
    ignore_x = max(12, int(width * 0.025))
    ignore_y = max(20, int(height * 0.05))
    row_x_start = min(ignore_x, width)
    row_x_end = max(row_x_start, width - ignore_x)
    column_y_start = min(ignore_y, height)
    column_y_end = max(column_y_start, height - ignore_y)
    row_width = max(1, row_x_end - row_x_start)
    column_height = max(1, column_y_end - column_y_start)
    min_row_pixels = max(5, int(row_width * 0.006))
    min_column_pixels = max(12, int(column_height * 0.012))
    content_rows = [
        y
        for y in range(max(0, height - ignore_y))
        if sum(1 for x in range(row_x_start, row_x_end) if mask_pixels[x, y]) >= min_row_pixels
    ]
    content_columns = [
        x
        for x in range(max(0, width - ignore_x))
        if sum(1 for y in range(column_y_start, column_y_end) if mask_pixels[x, y]) >= min_column_pixels
    ]
    if not content_rows or not content_columns:
        return None

    left = 0
    top = 0
    right = _first_content_cluster_end(
        content_columns,
        width,
        max_blank_gap_ratio=0.02,
    )
    bottom = _first_content_cluster_end(
        content_rows,
        height,
        max_blank_gap_ratio=0.04,
    )
    if right is None or bottom is None:
        return None
    aspect_bottom = int((right - left) / DRIVER_LICENSE_CROP_ASPECT_FLOOR)
    bottom = max(bottom, aspect_bottom)
    horizontal_padding = max(12, int((right - left) * 0.04))
    vertical_padding = max(12, int((bottom - top) * 0.04))
    return (
        max(0, left - horizontal_padding),
        max(0, top - vertical_padding),
        min(image.width, right + horizontal_padding),
        min(image.height, bottom + vertical_padding),
    )


def _prepare_driver_license_image(image: Image.Image) -> Image.Image:
    normalized = ImageOps.exif_transpose(image)
    if normalized.mode not in {"RGB", "L"}:
        normalized = normalized.convert("RGBA")
        background = Image.new("RGBA", normalized.size, "WHITE")
        background.alpha_composite(normalized)
        normalized = background.convert("RGB")
    else:
        normalized = normalized.convert("RGB")

    content_bbox = _image_content_bbox(normalized)
    if content_bbox is not None:
        normalized = normalized.crop(content_bbox)

    contained = ImageOps.contain(
        normalized,
        DRIVER_LICENSE_IMAGE_SIZE,
        method=Image.Resampling.LANCZOS,
    )
    canvas = Image.new("RGB", DRIVER_LICENSE_IMAGE_SIZE, "WHITE")
    canvas.paste(
        contained,
        (
            (DRIVER_LICENSE_IMAGE_SIZE[0] - contained.width) // 2,
            (DRIVER_LICENSE_IMAGE_SIZE[1] - contained.height) // 2,
        ),
    )
    return canvas


def _save_driver_license_image(uploaded_file: Any, destination_path: Path) -> None:
    try:
        with Image.open(uploaded_file.stream) as image:
            prepared = _prepare_driver_license_image(image)
            destination_path.parent.mkdir(parents=True, exist_ok=True)
            prepared.save(destination_path, format="JPEG", quality=92, optimize=True)
    except (OSError, UnidentifiedImageError) as error:
        abort(400, f"Could not read that image: {error}")


def _document_counts_by_member(members: list[Member], documents_dir: str) -> dict[int, int]:
    counts: dict[int, int] = {}
    for member in members:
        if member.id is None:
            continue
        counts[member.id] = len(
            _member_document_names(
                member,
                documents_dir,
                include_guest_form=True,
            )
        )
    return counts


def _zip_map_report(members: list[Member], coordinates: Any) -> ZipMapReport:
    zip_counts: Counter[str] = Counter()
    for member in members:
        zip_code = _normalized_zip_code(member.zip)
        if zip_code:
            zip_counts[zip_code] += 1

    coordinate_lookup = _normalized_zip_coordinates(coordinates)
    map_points: list[ZipMapPoint] = []
    unmapped_zip_counts: list[ZipCount] = []
    for zip_code, count in sorted(zip_counts.items()):
        coordinate = coordinate_lookup.get(zip_code)
        if coordinate is None:
            unmapped_zip_counts.append(ZipCount(zip_code=zip_code, count=count))
            continue
        map_points.append(
            ZipMapPoint(
                zip_code=zip_code,
                count=count,
                latitude=coordinate[0],
                longitude=coordinate[1],
            )
        )

    return ZipMapReport(
        total_users=len(members),
        users_with_zip=sum(zip_counts.values()),
        configured_zip_count=len(coordinate_lookup),
        map_points=tuple(map_points),
        unmapped_zip_counts=tuple(unmapped_zip_counts),
    )


def _normalized_zip_code(value: str | None) -> str:
    match = re.search(r"\d{5}", value or "")
    return match.group(0) if match else ""


def _normalized_zip_coordinates(coordinates: Any) -> dict[str, tuple[float, float]]:
    if not isinstance(coordinates, dict):
        return {}
    normalized: dict[str, tuple[float, float]] = {}
    for raw_zip_code, raw_coordinate in coordinates.items():
        zip_code = _normalized_zip_code(str(raw_zip_code))
        coordinate = _parsed_coordinate(raw_coordinate)
        if zip_code and coordinate is not None:
            normalized[zip_code] = coordinate
    return normalized


def _parsed_coordinate(raw_coordinate: Any) -> tuple[float, float] | None:
    try:
        if isinstance(raw_coordinate, dict):
            latitude = float(raw_coordinate["lat"])
            longitude = float(raw_coordinate["lon"])
        else:
            latitude = float(raw_coordinate[0])
            longitude = float(raw_coordinate[1])
    except (KeyError, TypeError, ValueError, IndexError):
        return None
    if not (-90 <= latitude <= 90 and -180 <= longitude <= 180):
        return None
    return (latitude, longitude)


def _document_image_path(
    documents_dir: str,
    entry_name: str,
    folder_name: str | None = None,
) -> Path | None:
    if not documents_dir.strip() or not _is_safe_document_entry_name(entry_name):
        return None
    if not _is_document_image_name(entry_name):
        return None

    base_dir = Path(documents_dir).expanduser().resolve(strict=False)
    if folder_name:
        if not _is_safe_document_entry_name(folder_name):
            return None
        parent = _case_insensitive_child_path(base_dir, folder_name)
        if parent is None or not parent.is_dir():
            return None
    else:
        parent = base_dir

    candidate = _case_insensitive_child_path(parent, entry_name)
    if candidate is None:
        return None
    candidate = candidate.resolve(strict=False)
    try:
        candidate.relative_to(base_dir)
    except ValueError:
        return None
    return candidate if candidate.is_file() else None


def _is_document_image_name(entry_name: str) -> bool:
    return not entry_name.endswith("/") and Path(entry_name).suffix.casefold() in (
        SUPPORTED_DOCUMENT_IMAGE_SUFFIXES
    )


def _is_safe_document_entry_name(entry_name: str) -> bool:
    return (
        bool(entry_name)
        and entry_name not in {".", ".."}
        and Path(entry_name).name == entry_name
        and "/" not in entry_name
        and "\\" not in entry_name
    )


def _first_guest_form_image(card_dir: Path) -> Path | None:
    try:
        children = sorted(card_dir.iterdir(), key=lambda path: _normalized_filename(path.name))
    except OSError:
        return None
    for child in children:
        if not child.is_file():
            continue
        normalized_name = _normalized_filename(child.name)
        if normalized_name.startswith("guest form") and child.suffix.casefold() in {".jpg", ".jpeg"}:
            return child
    return None


def _extra_document_names(card_dir: Path, guest_form_path: Path | None) -> tuple[str, ...]:
    children = _folder_entry_paths(card_dir)
    extra_names: list[str] = []
    resolved_guest_form_path = guest_form_path.resolve(strict=False) if guest_form_path else None
    for child in children:
        resolved_child = child.resolve(strict=False)
        if resolved_guest_form_path is not None and resolved_child == resolved_guest_form_path:
            continue
        if child.is_file() and child.name.casefold() == DRIVER_LICENSE_DOCUMENT_NAME.casefold():
            continue
        if child.is_file() and _is_banned_document_name(child.name):
            continue
        extra_names.append(child.name + ("/" if child.is_dir() else ""))
    return tuple(extra_names)


def _is_banned_document_name(entry_name: str) -> bool:
    normalized_name = _normalized_filename(entry_name).casefold()
    return normalized_name.startswith("pink card") or normalized_name.startswith("banned")


def _folder_entry_names(folder: Path) -> tuple[str, ...]:
    return tuple(child.name + ("/" if child.is_dir() else "") for child in _folder_entry_paths(folder))


def _folder_entry_paths(folder: Path) -> tuple[Path, ...]:
    try:
        return tuple(
            sorted(
                (
                    child
                    for child in folder.iterdir()
                    if not _is_ignored_document_entry(child.name)
                ),
                key=lambda path: _normalized_filename(path.name),
            )
        )
    except OSError:
        return ()


def _is_ignored_document_entry(entry_name: str) -> bool:
    return _normalized_filename(entry_name).startswith(".ds")


def _filename_analysis(file_names: list[str]) -> tuple[
    tuple[DocumentExtensionCount, ...],
    tuple[DocumentFilenamePattern, ...],
]:
    extension_counter = Counter(_filename_extension(name) for name in file_names)
    pattern_counter = Counter(_filename_pattern(name) for name in file_names)
    pattern_examples: dict[str, list[str]] = {}
    pattern_example_names: dict[str, set[str]] = {}
    for file_name in sorted(file_names, key=_normalized_filename):
        pattern = _filename_pattern(file_name)
        pattern_examples.setdefault(pattern, [])
        pattern_example_names.setdefault(pattern, set())
        normalized_name = _normalized_filename(file_name).casefold()
        if normalized_name in pattern_example_names[pattern]:
            continue
        if len(pattern_examples[pattern]) < 3:
            pattern_examples[pattern].append(file_name)
            pattern_example_names[pattern].add(normalized_name)

    extension_counts = tuple(
        DocumentExtensionCount(extension=extension, count=count)
        for extension, count in sorted(
            extension_counter.items(),
            key=lambda item: (-item[1], item[0]),
        )
    )
    filename_patterns = tuple(
        DocumentFilenamePattern(
            pattern=pattern,
            count=count,
            examples=tuple(pattern_examples[pattern]),
        )
        for pattern, count in sorted(
            pattern_counter.items(),
            key=lambda item: (-item[1], item[0]),
        )
    )
    return extension_counts, filename_patterns


def _filename_extension(file_name: str) -> str:
    suffix = Path(file_name).suffix.casefold()
    return suffix or "(none)"


def _filename_pattern(file_name: str) -> str:
    path = Path(file_name)
    suffix = path.suffix.casefold()
    stem = path.name[: -len(path.suffix)] if path.suffix else path.name
    normalized_stem = " ".join(unicodedata.normalize("NFC", stem).split())
    guest_form_match = re.match(
        r"(?i)^guest\s+form(?P<separator>[_\-\s]+)?(?P<detail>.*)$",
        normalized_stem,
    )
    if guest_form_match is not None:
        separator = guest_form_match.group("separator") or ""
        detail = guest_form_match.group("detail").strip(" _-")
        if not detail:
            return f"Guest Form{suffix}"
        if "_" in separator:
            return f"Guest Form_<text>{suffix}"
        if "-" in separator:
            return f"Guest Form-<text>{suffix}"
        return f"Guest Form <text>{suffix}"

    if "_" in normalized_stem:
        prefix = normalized_stem.split("_", 1)[0].strip()
        if prefix:
            return f"{prefix}_<text>{suffix}"

    return re.sub(r"\d+", "<number>", normalized_stem) + suffix


def _scan_documents_directory(members: list[Member], documents_dir: str) -> DocumentsScanReport:
    empty_extension_counts, empty_filename_patterns = _filename_analysis([])
    if not documents_dir.strip():
        return DocumentsScanReport(
            configured=False,
            readable=False,
            total_users=len(members),
            total_document_folders=0,
            total_document_files=0,
            users_with_guest_form=0,
            users_with_id_document=0,
            users_without_guest_form=len(members),
            extension_counts=empty_extension_counts,
            filename_patterns=empty_filename_patterns,
            missing_guest_form_users=tuple(members),
            banned_documents=(),
            card_folders_without_user=(),
            user_folders_with_extra_files=(),
            non_folder_entries=(),
        )

    base_dir = Path(documents_dir).expanduser().resolve(strict=False)
    if not base_dir.is_dir():
        return DocumentsScanReport(
            configured=True,
            readable=False,
            total_users=len(members),
            total_document_folders=0,
            total_document_files=0,
            users_with_guest_form=0,
            users_with_id_document=0,
            users_without_guest_form=len(members),
            extension_counts=empty_extension_counts,
            filename_patterns=empty_filename_patterns,
            missing_guest_form_users=tuple(members),
            banned_documents=(),
            card_folders_without_user=(),
            user_folders_with_extra_files=(),
            non_folder_entries=(),
        )

    members_by_card = {
        _normalized_filename(member.card_number.strip().strip("'").strip()): member
        for member in members
    }
    user_cards_with_guest_form: set[str] = set()
    user_cards_with_id_document: set[str] = set()
    banned_documents: list[BannedDocumentEntry] = []
    user_folders_with_extra_files: list[tuple[Member, tuple[str, ...]]] = []
    card_folders_without_user: list[tuple[str, tuple[str, ...]]] = []
    non_folder_entries: list[str] = []
    document_file_names: list[str] = []
    document_folder_count = 0

    try:
        children = sorted(base_dir.iterdir(), key=lambda path: _normalized_filename(path.name))
    except OSError:
        return DocumentsScanReport(
            configured=True,
            readable=False,
            total_users=len(members),
            total_document_folders=0,
            total_document_files=0,
            users_with_guest_form=0,
            users_with_id_document=0,
            users_without_guest_form=len(members),
            extension_counts=empty_extension_counts,
            filename_patterns=empty_filename_patterns,
            missing_guest_form_users=tuple(members),
            banned_documents=(),
            card_folders_without_user=(),
            user_folders_with_extra_files=(),
            non_folder_entries=(),
        )

    for child in children:
        if _is_ignored_document_entry(child.name):
            continue
        if not child.is_dir():
            non_folder_entries.append(child.name)
            if child.is_file():
                document_file_names.append(child.name)
                if _is_banned_document_name(child.name):
                    banned_documents.append(
                        BannedDocumentEntry(
                            entry_name=child.name,
                            folder_name=None,
                            member=None,
                        )
                    )
            continue

        document_folder_count += 1
        folder_children = _folder_entry_paths(child)
        document_file_names.extend(folder_child.name for folder_child in folder_children if folder_child.is_file())
        normalized_card = _normalized_filename(child.name)
        member = members_by_card.get(normalized_card)
        for folder_child in folder_children:
            if not folder_child.is_file() or not _is_banned_document_name(folder_child.name):
                continue
            banned_documents.append(
                BannedDocumentEntry(
                    entry_name=folder_child.name,
                    folder_name=child.name,
                    member=member,
                )
            )
        if member is None:
            card_folders_without_user.append((child.name, _folder_entry_names(child)))
            continue

        guest_form_path = _first_guest_form_image(child)
        if guest_form_path is not None:
            user_cards_with_guest_form.add(normalized_card)
        if _id_document_name_for_member(member, documents_dir) is not None:
            user_cards_with_id_document.add(normalized_card)

        extra_names = _extra_document_names(child, guest_form_path)
        if extra_names:
            user_folders_with_extra_files.append((member, extra_names))

    missing_guest_form_users = tuple(
        member
        for member in members
        if _normalized_filename(member.card_number.strip().strip("'").strip())
        not in user_cards_with_guest_form
    )
    extension_counts, filename_patterns = _filename_analysis(document_file_names)

    return DocumentsScanReport(
        configured=True,
        readable=True,
        total_users=len(members),
        total_document_folders=document_folder_count,
        total_document_files=len(document_file_names),
        users_with_guest_form=len(user_cards_with_guest_form),
        users_with_id_document=len(user_cards_with_id_document),
        users_without_guest_form=len(missing_guest_form_users),
        extension_counts=extension_counts,
        filename_patterns=filename_patterns,
        missing_guest_form_users=missing_guest_form_users,
        banned_documents=tuple(banned_documents),
        card_folders_without_user=tuple(card_folders_without_user),
        user_folders_with_extra_files=tuple(user_folders_with_extra_files),
        non_folder_entries=tuple(non_folder_entries),
    )


def _case_insensitive_child_path(parent: Path, child_name: str) -> Path | None:
    exact_path = parent / child_name
    if exact_path.exists():
        return exact_path
    try:
        children = list(parent.iterdir())
    except OSError:
        return None
    normalized_child_name = _normalized_filename(child_name)
    for child in children:
        if _normalized_filename(child.name) == normalized_child_name:
            return child
    return None


def _normalized_filename(value: str) -> str:
    return " ".join(unicodedata.normalize("NFC", value).casefold().split())


def _generate_guest_card_number(connection: sqlite3.Connection) -> str:
    largest_card_number = member_repository.largest_numeric_card_number(connection)
    return str((largest_card_number or 0) + 1)


def _barcode_secret_bytes(secret_key: object) -> bytes:
    return str(secret_key or "").encode("utf-8")


def _barcode_signature(card_number: str, secret_key: object, byte_count: int = 6) -> str:
    digest = hmac.new(
        _barcode_secret_bytes(secret_key),
        card_number.encode("utf-8"),
        hashlib.sha256,
    ).digest()
    return base64.urlsafe_b64encode(digest[:byte_count]).decode("ascii").rstrip("=")


def _barcode_token_for_card_number(
    card_number: str,
    secret_key: object,
    version: str = BARCODE_TOKEN_VERSION,
) -> str:
    signature_byte_count = 9 if version in LEGACY_BARCODE_TOKEN_VERSIONS else 6
    signature = _barcode_signature(card_number, secret_key, signature_byte_count)
    return f"{version}:{signature}"


def _barcode_print_display_name(member: Member) -> str:
    'Return the short name shown on a printed check-in barcode.'
    first_or_nickname = (member.nickname or member.first_name).strip()
    last_initial = member.last_name.strip()[:1]
    if first_or_nickname and last_initial:
        return f"{first_or_nickname} {last_initial.upper()}."
    return first_or_nickname or last_initial.upper()


def _configured_barcode_secret() -> str:
    return (
        os.getenv("USER_MANAGEMENT_BARCODE_SECRET", "").strip()
        or str(getattr(cfg, "USER_MANAGEMENT_BARCODE_SECRET", "")).strip()
        or os.getenv("USER_MANAGEMENT_SESSION_SECRET", "").strip()
        or str(getattr(cfg, "USER_MANAGEMENT_SESSION_SECRET", "")).strip()
    )


def _barcode_secret_for_connection(
    connection: sqlite3.Connection,
    configured_secret: object = "",
) -> str:
    configured_secret_text = str(configured_secret or "").strip()
    if configured_secret_text:
        return configured_secret_text

    row = connection.execute(
        "SELECT value FROM app_settings WHERE key = ?",
        (BARCODE_SECRET_SETTING_KEY,),
    ).fetchone()
    if row is not None:
        return str(row["value"])

    generated_secret = token_urlsafe(32)
    connection.execute(
        """
        INSERT INTO app_settings (key, value)
        VALUES (?, ?)
        """,
        (BARCODE_SECRET_SETTING_KEY, generated_secret),
    )
    return generated_secret


def _member_from_barcode_token(
    connection: sqlite3.Connection,
    token: str,
    secret_key: object,
) -> Member | None:
    normalized_token = token.strip()
    parts = normalized_token.split(":")
    accepted_versions = (BARCODE_TOKEN_VERSION, *LEGACY_BARCODE_TOKEN_VERSIONS)
    if len(parts) != 2 or parts[0] not in accepted_versions:
        return None
    for member in member_repository.list_members(connection):
        if hmac.compare_digest(
            normalized_token,
            _barcode_token_for_card_number(member.card_number, secret_key, parts[0]),
        ):
            return member
    return None


def _resolve_kiosk_identity(
    connection: sqlite3.Connection,
    form_data: Any,
    *,
    barcode_secret: object,
) -> KioskIdentityResult:
    '''Resolve the shared public kiosk barcode/phone identity form.'''
    barcode_token = form_data.get("barcode_token", "").strip()
    if barcode_token:
        return KioskIdentityResult(
            member=_member_from_barcode_token(
                connection,
                barcode_token,
                barcode_secret,
            ),
            used_barcode=True,
        )

    return KioskIdentityResult(
        member=member_repository.find_member_by_phone_and_initials(
            connection,
            form_data.get("phone", "").strip(),
            form_data.get("initials", "").strip(),
        ),
        used_barcode=False,
    )


def _code128b_svg(value: str) -> str:
    if not value or any(not 32 <= ord(character) <= 127 for character in value):
        raise ValueError("Code 128B barcodes require printable ASCII text.")
    codes = [104, *(ord(character) - 32 for character in value)]
    weighted_data_sum = sum(
        index * code
        for index, code in enumerate(codes[1:], start=1)
    )
    checksum = (codes[0] + weighted_data_sum) % 103
    codes.extend((checksum, 106))

    bar_width = 2
    height = 84
    quiet_zone = 20
    x_position = quiet_zone
    rects: list[str] = []
    for code in codes:
        pattern = CODE128_PATTERNS[code]
        draw_bar = True
        for width_text in pattern:
            width = int(width_text) * bar_width
            if draw_bar:
                rects.append(f'<rect x="{x_position}" y="0" width="{width}" height="{height}"/>')
            x_position += width
            draw_bar = not draw_bar
    svg_width = x_position + quiet_zone
    return (
        f'<svg class="checkin-barcode" role="img" aria-label="Self check-in barcode" '
        f'shape-rendering="crispEdges" viewBox="0 0 {svg_width} {height}" '
        f'xmlns="http://www.w3.org/2000/svg">'
        f'<rect width="{svg_width}" height="{height}" fill="#fff"/>'
        f'<g fill="#000">{"".join(rects)}</g>'
        "</svg>"
    )


def _record_self_checkin(
    connection: sqlite3.Connection,
    member: Member,
) -> LiveCheckInResult:
    check_in_at = datetime.now().replace(microsecond=0)
    if member.screening_status == "banned":
        if member.id is not None:
            audit_repository.record_field_change(
                connection,
                entity_type="user",
                entity_id=member.id,
                action="edit",
                field_name="blocked check-in attempted",
                old_value=None,
                new_value=check_in_at,
            )
        return LiveCheckInResult(
            check_in_at=check_in_at,
            recorded=False,
            blocked=True,
        )

    if member.id is not None:
        recent_checkin = checkin_repository.latest_checkin_for_user_between(
            connection,
            user_id=member.id,
            start_at=check_in_at - LIVE_CHECKIN_REPEAT_WINDOW,
            end_at=check_in_at,
        )
        if recent_checkin is not None:
            return LiveCheckInResult(
                check_in_at=recent_checkin.check_in_at,
                recorded=False,
            )

    checkin_repository.upsert_checkin(
        connection,
        CheckIn(
            user_id=member.id,
            member_id=str(member.id),
            last_name=member.last_name,
            first_name=member.first_name,
            card_number=member.card_number,
            check_in_at=check_in_at,
            membership=member.membership,
        ),
    )
    return LiveCheckInResult(check_in_at=check_in_at, recorded=True)


def create_app(db_path: Path | None = None) -> Flask:
    '''Create the club user management app.'''
    database.init_db(db_path)
    flask_app = Flask(
        __name__,
        template_folder="templates",
        static_folder="static",
    )
    flask_app.config["CLUB_ADMIN_DB_PATH"] = str(db_path or database.get_db_path())
    flask_app.config["USER_MANAGEMENT_ADMIN_PASSWORD_HASH"] = str(
        getattr(cfg, "USER_MANAGEMENT_ADMIN_PASSWORD_HASH", "")
    ).strip()
    flask_app.config["USER_MANAGEMENT_CHECKIN_MONITOR_TOKEN"] = (
        os.getenv("USER_MANAGEMENT_CHECKIN_MONITOR_TOKEN", "").strip()
        or str(getattr(cfg, "USER_MANAGEMENT_CHECKIN_MONITOR_TOKEN", "")).strip()
    )
    flask_app.config["USER_MANAGEMENT_BARCODE_SECRET"] = _configured_barcode_secret()
    flask_app.config["USER_MANAGEMENT_DOCUMENTS_DIR"] = str(
        getattr(cfg, "USER_MANAGEMENT_DOCUMENTS_DIR", "")
    ).strip()
    flask_app.config["USER_MANAGEMENT_GUEST_FORM_DEFINITION_PATH"] = str(
        getattr(cfg, "USER_MANAGEMENT_GUEST_FORM_DEFINITION_PATH", "")
    ).strip()
    flask_app.config["USER_MANAGEMENT_MEMBERSHIP_APPLICATION_DEFINITION_PATH"] = str(
        getattr(cfg, "USER_MANAGEMENT_MEMBERSHIP_APPLICATION_DEFINITION_PATH", "")
    ).strip()
    flask_app.config["USER_MANAGEMENT_ZIP_COORDINATES"] = getattr(
        cfg,
        "USER_MANAGEMENT_ZIP_COORDINATES",
        {},
    )
    flask_app.secret_key = (
        str(getattr(cfg, "USER_MANAGEMENT_SESSION_SECRET", "")).strip()
        or os.getenv("USER_MANAGEMENT_SESSION_SECRET", "").strip()
        or token_hex(32)
    )
    url_prefix = _normalize_url_prefix(
        os.getenv("USER_MANAGEMENT_URL_PREFIX")
        or getattr(cfg, "USER_MANAGEMENT_URL_PREFIX", "")
    )
    flask_app.config["USER_MANAGEMENT_URL_PREFIX"] = url_prefix
    if url_prefix:
        flask_app.wsgi_app = UrlPrefixMiddleware(flask_app.wsgi_app, url_prefix)

    flask_app.add_template_filter(_format_sqlite_utc_datetime, "local_sqlite_datetime")
    flask_app.add_template_filter(_format_sqlite_utc_date, "local_sqlite_date")
    flask_app.add_template_filter(_format_date_entry, "date_entry")
    flask_app.add_template_filter(_display_audit_field_name, "audit_field_name")

    def require_admin(view):
        @wraps(view)
        def wrapped(*args, **kwargs):
            if session.get("user_management_admin_authenticated") is True:
                return view(*args, **kwargs)
            next_url = f"{request.script_root}{request.full_path}".rstrip("?")
            return redirect(url_for("admin_login", next=next_url))

        return wrapped

    public_endpoints = {
        "static",
        "index",
        "guest_registration",
        "guest_registration_thanks",
        "membership_application",
        "membership_application_thanks",
        "admin_login",
        "admin_logout",
        "checkins_latest_api",
        "self_checkin",
    }

    @flask_app.before_request
    def require_admin_for_private_routes():
        if request.endpoint in public_endpoints:
            return None
        if session.get("user_management_admin_authenticated") is True:
            return None
        next_url = f"{request.script_root}{request.full_path}".rstrip("?")
        return redirect(url_for("admin_login", next=next_url))

    @flask_app.after_request
    def prevent_public_kiosk_cache(response):
        if request.endpoint in PUBLIC_KIOSK_ENDPOINTS:
            response.headers["Cache-Control"] = "no-store, max-age=0"
            response.headers["Pragma"] = "no-cache"
            response.headers["Expires"] = "0"
        return response

    @flask_app.context_processor
    def inject_app_title() -> dict[str, object]:
        organization_name = str(cfg.USER_MANAGEMENT_ORGANIZATION_NAME).strip()
        return {
            "organization_name": organization_name,
            "app_title": f"{organization_name} User Management",
            "is_document_image": _is_document_image_name,
            "screening_status_label": _screening_status_label,
            "screening_status_options": SCREENING_STATUS_OPTIONS,
        }

    @contextmanager
    def open_connection() -> Iterator[sqlite3.Connection]:
        connection = database.connect(Path(flask_app.config["CLUB_ADMIN_DB_PATH"]))
        try:
            yield connection
        finally:
            connection.close()

    @flask_app.route("/")
    def index():
        return redirect(url_for("self_checkin"))

    @flask_app.route("/guest-registration", methods=["GET", "POST"])
    def guest_registration():
        if request.method == "POST":
            try:
                member, registration = _guest_registration_from_form(
                    request.form,
                    card_number="",
                )
            except GuestRegistrationFormError as exc:
                return render_template(
                    "club_admin/guest_registration.html",
                    today=date.today(),
                    message=str(exc),
                    form_data=request.form,
                ), 400

            validation_message = _guest_registration_validation_message(member, registration)
            if validation_message is not None:
                return render_template(
                    "club_admin/guest_registration.html",
                    today=date.today(),
                    message=validation_message,
                    form_data=request.form,
                ), 400

            with open_connection() as connection:
                connection.execute("BEGIN IMMEDIATE")
                existing_member = _matching_guest_registration_member(connection, member)
                if existing_member is None:
                    card_number = _generate_guest_card_number(connection)
                    member = replace(
                        member,
                        card_number=card_number,
                        screening_status="pending",
                    )
                    member_id = member_repository.insert_member(connection, member)
                    member = replace(member, id=member_id)
                else:
                    member_id = cast(int, existing_member.id)
                    if _guest_registration_exists(connection, user_id=member_id):
                        connection.rollback()
                        return render_template(
                            "club_admin/guest_registration.html",
                            today=date.today(),
                            message=(
                                "You are already registered. Please use Self Check-in or "
                                "see the front desk for help."
                            ),
                            form_data=request.form,
                        ), 409
                    member = replace(
                        member,
                        id=member_id,
                        card_number=existing_member.card_number,
                        membership=existing_member.membership,
                        member_since=existing_member.member_since,
                        screening_status=existing_member.screening_status,
                        address2=existing_member.address2,
                        mailing_address=existing_member.mailing_address,
                        mailing_address2=existing_member.mailing_address2,
                        mailing_city=existing_member.mailing_city,
                        mailing_state=existing_member.mailing_state,
                        mailing_zip=existing_member.mailing_zip,
                    )
                    member_repository.update_member(connection, member)

                if not _guest_registration_exists_for_visit(
                    connection,
                    user_id=member_id,
                    visit_date=registration.visit_date,
                ):
                    guest_registration_repository.insert_guest_registration(
                        connection,
                        replace(registration, user_id=member_id),
                    )
                    audit_repository.record_field_change(
                        connection,
                        entity_type="user",
                        entity_id=member_id,
                        action="edit",
                        field_name="guest registration submitted",
                        old_value=None,
                        new_value=registration.visit_date,
                    )
                checkin_result = _record_self_checkin(connection, member)
                if checkin_result.recorded:
                    _record_checkin_change(
                        connection,
                        member_id=member_id,
                        field_name="check-in added",
                        old_value=None,
                        new_value=checkin_result.check_in_at,
                    )
                connection.commit()
                if checkin_result.recorded:
                    checkin_events.notify_checkins_changed()
            return redirect(url_for("guest_registration_thanks"))

        return render_template(
            "club_admin/guest_registration.html",
            today=date.today(),
            form_data={},
        )

    @flask_app.route("/guest-registration/thanks")
    def guest_registration_thanks():
        response = make_response(
            render_template(
                "club_admin/guest_registration_thanks.html",
                auto_return_seconds=KIOSK_AUTO_RETURN_SECONDS,
                auto_return_delay_ms=KIOSK_AUTO_RETURN_SECONDS * 1000,
            )
        )
        response.headers["Refresh"] = (
            f"{KIOSK_AUTO_RETURN_SECONDS}; url={url_for('self_checkin')}"
        )
        return response

    @flask_app.route("/membership-application", methods=["GET", "POST"])
    def membership_application():
        form_spec = _membership_application_form_spec()
        message = ""
        member: Member | None = None
        form_data: Any = {}
        if request.method == "POST":
            action = request.form.get("action", "identify").strip()
            if action == "cancel":
                session.pop(MEMBERSHIP_APPLICATION_SESSION_KEY, None)
                return redirect(url_for("self_checkin"))
            if action == "submit":
                form_data = request.form
                raw_user_id = session.get(MEMBERSHIP_APPLICATION_SESSION_KEY)
                try:
                    user_id = int(raw_user_id)
                except (TypeError, ValueError):
                    session.pop(MEMBERSHIP_APPLICATION_SESSION_KEY, None)
                    return redirect(url_for("membership_application"))
                with open_connection() as connection:
                    connection.execute("BEGIN IMMEDIATE")
                    member = member_repository.get_member(connection, user_id)
                    if member is None:
                        session.pop(MEMBERSHIP_APPLICATION_SESSION_KEY, None)
                        return redirect(url_for("membership_application"))
                    try:
                        application = _membership_application_from_form(
                            request.form,
                            user_id=user_id,
                        )
                    except MembershipApplicationFormError as error:
                        message = str(error)
                    else:
                        pending_application = (
                            membership_application_repository.get_pending_membership_application_for_user(
                                connection,
                                user_id,
                            )
                        )
                        if pending_application is not None:
                            message = (
                                "You already have a pending membership application. "
                                "Please see the front desk for help."
                            )
                        else:
                            application_id = membership_application_repository.insert_membership_application(
                                connection,
                                application,
                            )
                            audit_repository.record_field_change(
                                connection,
                                entity_type="user",
                                entity_id=user_id,
                                action="edit",
                                field_name="membership application submitted",
                                old_value=None,
                                new_value=application_id,
                            )
                            connection.commit()
                            session.pop(MEMBERSHIP_APPLICATION_SESSION_KEY, None)
                            return redirect(url_for("membership_application_thanks"))
            else:
                with open_connection() as connection:
                    barcode_secret = _barcode_secret_for_connection(
                        connection,
                        flask_app.config["USER_MANAGEMENT_BARCODE_SECRET"],
                    )
                    identity_result = _resolve_kiosk_identity(
                        connection,
                        request.form,
                        barcode_secret=barcode_secret,
                    )
                    member = identity_result.member
                    connection.commit()
                if member is None:
                    message = "No matching user was found. Please check your barcode, phone number, and initials or first name."
                elif member.membership != "Visitor":
                    member = None
                    message = "Please see the front desk."
                else:
                    session[MEMBERSHIP_APPLICATION_SESSION_KEY] = member.id
        elif session.get(MEMBERSHIP_APPLICATION_SESSION_KEY) is not None:
            with open_connection() as connection:
                try:
                    member_id = int(session[MEMBERSHIP_APPLICATION_SESSION_KEY])
                except (TypeError, ValueError):
                    member = None
                else:
                    member = member_repository.get_member(connection, member_id)
            if member is None:
                session.pop(MEMBERSHIP_APPLICATION_SESSION_KEY, None)

        response = make_response(
            render_template(
                "club_admin/membership_application.html",
                member=member,
                message=message,
                form_data=form_data,
                today=date.today(),
                form_spec=form_spec,
                auto_return_seconds=KIOSK_AUTO_RETURN_SECONDS,
                auto_return_delay_ms=KIOSK_AUTO_RETURN_SECONDS * 1000,
            )
        )
        if message and member is None:
            response.headers["Refresh"] = (
                f"{KIOSK_AUTO_RETURN_SECONDS}; url={url_for('self_checkin')}"
            )
        return response

    @flask_app.route("/membership-application/thanks")
    def membership_application_thanks():
        response = make_response(
            render_template(
                "club_admin/membership_application_thanks.html",
                auto_return_seconds=KIOSK_AUTO_RETURN_SECONDS,
                auto_return_delay_ms=KIOSK_AUTO_RETURN_SECONDS * 1000,
            )
        )
        response.headers["Refresh"] = (
            f"{KIOSK_AUTO_RETURN_SECONDS}; url={url_for('self_checkin')}"
        )
        return response

    @flask_app.route("/admin/login", methods=["GET", "POST"])
    def admin_login():
        next_url = _safe_next_url(request.values.get("next"))
        password_hash = flask_app.config["USER_MANAGEMENT_ADMIN_PASSWORD_HASH"]
        message = ""
        if not password_hash:
            message = "Admin access is not configured."
        elif request.method == "POST":
            password = request.form.get("password", "")
            if check_password_hash(password_hash, password):
                session["user_management_admin_authenticated"] = True
                return redirect(next_url)
            message = "Password was not accepted."

        return render_template(
            "club_admin/admin_login.html",
            message=message,
            next_url=next_url,
            auth_configured=bool(password_hash),
        ), (200 if password_hash else 503)

    @flask_app.post("/admin/logout")
    def admin_logout():
        session.pop("user_management_admin_authenticated", None)
        return redirect(url_for("self_checkin"))

    @flask_app.route("/members")
    @require_admin
    def members():
        with open_connection() as connection:
            roster = member_repository.list_member_report_rows(connection)
        document_counts = _document_counts_by_member(
            [row.member for row in roster],
            flask_app.config["USER_MANAGEMENT_DOCUMENTS_DIR"],
        )
        roster = [
            replace(row, document_count=document_counts.get(row.member.id or 0, 0))
            for row in roster
        ]
        return render_template(
            "club_admin/members.html",
            members=roster,
            checkin_message=request.args.get("checked_in", "").strip(),
        )

    @flask_app.route("/members/export.csv")
    @require_admin
    def export_members_csv():
        with open_connection() as connection:
            roster = member_repository.list_member_report_rows(connection)
        document_counts = _document_counts_by_member(
            [row.member for row in roster],
            flask_app.config["USER_MANAGEMENT_DOCUMENTS_DIR"],
        )
        roster = [
            replace(row, document_count=document_counts.get(row.member.id or 0, 0))
            for row in roster
        ]
        response = make_response(_members_csv(roster))
        response.headers["Content-Type"] = "text/csv; charset=utf-8"
        response.headers["Content-Disposition"] = (
            f"attachment; filename=users-{date.today().isoformat()}.csv"
        )
        return response

    @flask_app.post("/members/check-ins")
    @require_admin
    def check_in_members():
        selected_member_ids: list[int] = []
        for raw_member_id in request.form.getlist("member_ids"):
            try:
                member_id = int(raw_member_id)
            except ValueError:
                continue
            if member_id not in selected_member_ids:
                selected_member_ids.append(member_id)

        if not selected_member_ids:
            return redirect(url_for("members", checked_in="Select at least one user to check in."))

        checked_in_members: list[Member] = []
        recent_repeat_members: list[Member] = []
        blocked_members: list[Member] = []
        with open_connection() as connection:
            for member_id in selected_member_ids:
                member = member_repository.get_member(connection, member_id)
                if member is None:
                    abort(404)
                checkin_result = _record_self_checkin(connection, member)
                if checkin_result.blocked:
                    blocked_members.append(member)
                elif checkin_result.recorded:
                    _record_checkin_change(
                        connection,
                        member_id=member_id,
                        field_name="check-in added",
                        old_value=None,
                        new_value=checkin_result.check_in_at,
                    )
                    checked_in_members.append(member)
                else:
                    recent_repeat_members.append(member)
            connection.commit()
        if checked_in_members:
            checkin_events.notify_checkins_changed()

        if len(checked_in_members) == 1 and not recent_repeat_members and not blocked_members:
            member = checked_in_members[0]
            checked_in_message = f"Checked in {member.first_name} {member.last_name}."
        else:
            message_parts = []
            if checked_in_members:
                message_parts.append(f"Checked in {len(checked_in_members)} users.")
            if (checked_in_members or blocked_members) and recent_repeat_members:
                message_parts.append(
                    f"Ignored {len(recent_repeat_members)} recent repeat "
                    f"{'check-in' if len(recent_repeat_members) == 1 else 'check-ins'}."
                )
            if blocked_members:
                blocked_count = len(blocked_members)
                message_parts.append(
                    "1 selected user is banned and was not checked in."
                    if blocked_count == 1
                    else f"{blocked_count} selected users are banned and were not checked in."
                )
            if not message_parts and len(recent_repeat_members) == 1:
                checked_in_message = "Already checked in within the past hour."
            elif not message_parts:
                checked_in_message = (
                    f"{len(recent_repeat_members)} users were already checked in "
                    "within the past hour."
                )
            else:
                checked_in_message = " ".join(part.strip() for part in message_parts)
        return redirect(url_for("members", checked_in=checked_in_message))

    @flask_app.route("/members/map")
    @require_admin
    def members_map():
        today = date.today()
        start_date, end_date = _date_range_from_request(today)
        date_presets = _date_range_presets(today)
        with open_connection() as connection:
            roster = member_repository.list_members_checked_in_for_date_range(
                connection,
                start_date,
                end_date,
            )
            stored_coordinates = zip_repository.list_zip_coordinates(connection)
        coordinates = {
            **_normalized_zip_coordinates(
                flask_app.config["USER_MANAGEMENT_ZIP_COORDINATES"],
            ),
            **stored_coordinates,
        }
        report = _zip_map_report(
            roster,
            coordinates,
        )
        map_points = [
            {
                "zip_code": point.zip_code,
                "count": point.count,
                "latitude": point.latitude,
                "longitude": point.longitude,
            }
            for point in report.map_points
        ]
        lookup_zips = [
            {
                "zip_code": zip_count.zip_code,
                "count": zip_count.count,
            }
            for zip_count in report.unmapped_zip_counts
        ]
        return render_template(
            "club_admin/members_map.html",
            report=report,
            map_points=map_points,
            lookup_zips=lookup_zips,
            date_presets=date_presets,
            active_date_preset_label=_active_date_range_preset_label(
                date_presets,
                start_date,
                end_date,
            ),
            start_date=start_date,
            end_date=end_date,
        )

    @flask_app.post("/members/map/zip-coordinates")
    @require_admin
    def save_zip_coordinates():
        payload = request.get_json(silent=True) or {}
        raw_coordinates = payload.get("coordinates")
        if not isinstance(raw_coordinates, list):
            abort(400, "Expected a coordinates list.")

        coordinates: list[zip_repository.ZipCoordinate] = []
        try:
            for raw_coordinate in raw_coordinates:
                if not isinstance(raw_coordinate, dict):
                    raise ValueError("Each coordinate must be an object.")
                coordinates.append(
                    zip_repository.coordinate_from_values(
                        raw_coordinate.get("zip_code", ""),
                        raw_coordinate.get("latitude", ""),
                        raw_coordinate.get("longitude", ""),
                    )
                )
        except ValueError as error:
            abort(400, str(error))

        with open_connection() as connection:
            saved_count = zip_repository.upsert_zip_coordinates(connection, coordinates)
            connection.commit()

        return jsonify({"saved": saved_count})

    @flask_app.route("/members/<int:member_id>")
    @require_admin
    def member_detail(member_id: int):
        with open_connection() as connection:
            member = member_repository.get_member(connection, member_id)
            if member is None:
                abort(404)
            checkins = checkin_repository.list_checkins_for_user(connection, member_id)
            audit_entries = audit_repository.list_audit_log_for_entity(
                connection,
                entity_type="user",
                entity_id=member_id,
            )
            notes = user_note_repository.list_user_notes(connection, member_id)
        document_preview = _member_document_preview(
            member,
            flask_app.config["USER_MANAGEMENT_DOCUMENTS_DIR"],
        )
        other_documents = tuple(
            MemberDocument(name=name)
            for name in _member_document_names(
                member,
                flask_app.config["USER_MANAGEMENT_DOCUMENTS_DIR"],
                include_guest_form=False,
            )
            if document_preview is None
            or document_preview.document_name is None
            or name.casefold() != document_preview.document_name.casefold()
        )

        return render_template(
            "club_admin/member_detail.html",
            member=member,
            checkins=checkins,
            notes=notes,
            audit_entries=_visible_member_audit_entries(audit_entries),
            document_preview=document_preview,
            other_documents=other_documents,
        )

    @flask_app.route("/members/<int:member_id>/checkin-barcode/print")
    @require_admin
    def member_checkin_barcode_print(member_id: int):
        with open_connection() as connection:
            member = member_repository.get_member(connection, member_id)
            if member is None:
                abort(404)
            barcode_secret = _barcode_secret_for_connection(
                connection,
                flask_app.config["USER_MANAGEMENT_BARCODE_SECRET"],
            )
            token = _barcode_token_for_card_number(member.card_number, barcode_secret)
            barcode_svg = _code128b_svg(token)
            connection.commit()

        return render_template(
            "club_admin/checkin_barcode_print.html",
            display_name=_barcode_print_display_name(member),
            barcode_svg=barcode_svg,
        )

    @flask_app.post("/members/<int:member_id>/notes")
    @require_admin
    def add_member_note(member_id: int):
        with open_connection() as connection:
            member = member_repository.get_member(connection, member_id)
            if member is None:
                abort(404)
            try:
                note = user_note_repository.note_from_values(
                    user_id=member_id,
                    summary=request.form.get("summary", ""),
                    details=request.form.get("details", ""),
                )
            except ValueError as error:
                abort(400, str(error))
            user_note_repository.add_user_note(connection, note)
            audit_repository.record_field_change(
                connection,
                entity_type="user",
                entity_id=member_id,
                action="edit",
                field_name="note added",
                old_value=None,
                new_value=note.summary,
            )
            connection.commit()
        return redirect(url_for("member_detail", member_id=member_id))

    @flask_app.post("/members/<int:member_id>/notes/<int:note_id>/edit")
    @require_admin
    def edit_member_note(member_id: int, note_id: int):
        with open_connection() as connection:
            member = member_repository.get_member(connection, member_id)
            if member is None:
                abort(404)
            try:
                note = user_note_repository.note_from_values(
                    user_id=member_id,
                    summary=request.form.get("summary", ""),
                    details=request.form.get("details", ""),
                )
            except ValueError as error:
                abort(400, str(error))
            note = replace(note, id=note_id)
            existing_note = user_note_repository.get_user_note(
                connection,
                note_id=note_id,
                user_id=member_id,
            )
            if existing_note is None:
                abort(404)
            summary_changed = existing_note.summary != note.summary
            details_changed = existing_note.details != note.details
            if not user_note_repository.update_user_note(connection, note):
                abort(404)
            if summary_changed:
                audit_repository.record_field_change(
                    connection,
                    entity_type="user",
                    entity_id=member_id,
                    action="edit",
                    field_name="note edited",
                    old_value=existing_note.summary,
                    new_value=note.summary,
                )
            elif details_changed:
                audit_repository.record_field_change(
                    connection,
                    entity_type="user",
                    entity_id=member_id,
                    action="edit",
                    field_name="note details edited",
                    old_value=None,
                    new_value=note.summary,
                )
            connection.commit()
        return redirect(url_for("member_detail", member_id=member_id))

    @flask_app.post("/members/<int:member_id>/notes/<int:note_id>/delete")
    @require_admin
    def delete_member_note(member_id: int, note_id: int):
        with open_connection() as connection:
            member = member_repository.get_member(connection, member_id)
            if member is None:
                abort(404)
            existing_note = user_note_repository.get_user_note(
                connection,
                note_id=note_id,
                user_id=member_id,
            )
            if existing_note is None:
                abort(404)
            if not user_note_repository.delete_user_note(
                connection,
                note_id=note_id,
                user_id=member_id,
            ):
                abort(404)
            audit_repository.record_field_change(
                connection,
                entity_type="user",
                entity_id=member_id,
                action="edit",
                field_name="note deleted",
                old_value=existing_note.summary,
                new_value=None,
            )
            connection.commit()
        return redirect(url_for("member_detail", member_id=member_id))

    @flask_app.route("/changes")
    @require_admin
    def recent_changes():
        with open_connection() as connection:
            changes = audit_repository.list_recent_audit_log(connection)
        return render_template("club_admin/recent_changes.html", changes=changes)

    @flask_app.route("/members/<int:member_id>/guest-form.jpg")
    @require_admin
    def member_guest_form(member_id: int):
        with open_connection() as connection:
            member = member_repository.get_member(connection, member_id)
            if member is None:
                abort(404)

        guest_form_path = _guest_form_path_for_member(
            member,
            flask_app.config["USER_MANAGEMENT_DOCUMENTS_DIR"],
        )
        if guest_form_path is None:
            abort(404)
        return send_file(guest_form_path, mimetype="image/jpeg", conditional=True)

    @flask_app.route("/members/<int:member_id>/document")
    @require_admin
    def member_document(member_id: int):
        with open_connection() as connection:
            member = member_repository.get_member(connection, member_id)
            if member is None:
                abort(404)

        document_path = _member_document_path(
            member,
            flask_app.config["USER_MANAGEMENT_DOCUMENTS_DIR"],
            request.args.get("name", "").strip(),
        )
        if document_path is None:
            abort(404)
        return send_file(document_path, conditional=True)

    @flask_app.post("/members/<int:member_id>/documents")
    @require_admin
    def upload_member_document(member_id: int):
        uploaded_file = request.files.get("member_document")
        if uploaded_file is None or not uploaded_file.filename:
            abort(400, "Document file is required.")

        with open_connection() as connection:
            member = member_repository.get_member(connection, member_id)
            if member is None:
                abort(404)

        document_name = _uploaded_document_name(uploaded_file.filename)
        if document_name is None:
            abort(400, "The document filename is not valid.")
        destination_path = _member_document_upload_path(
            member,
            flask_app.config["USER_MANAGEMENT_DOCUMENTS_DIR"],
            document_name,
        )
        if destination_path is None:
            abort(400, "The document could not be stored for this user.")

        destination_path.parent.mkdir(parents=True, exist_ok=True)
        uploaded_file.save(destination_path)
        with open_connection() as connection:
            audit_repository.record_field_change(
                connection,
                entity_type="user",
                entity_id=member_id,
                action="edit",
                field_name="document attached",
                old_value=None,
                new_value=destination_path.name,
            )
            connection.commit()
        return redirect(url_for("member_detail", member_id=member_id))

    @flask_app.route("/members/<int:member_id>/edit", methods=["GET", "POST"])
    @require_admin
    def edit_member(member_id: int):
        with open_connection() as connection:
            member = member_repository.get_member(connection, member_id)
            if member is None:
                abort(404)

            if request.method == "POST":
                try:
                    updated_member = _member_from_form(member, request.form)
                except MemberFormError as error:
                    abort(400, str(error))

                if not updated_member.last_name or not updated_member.first_name:
                    abort(400, "First and last name are required.")
                if not updated_member.membership:
                    abort(400, "Membership is required.")
                if updated_member.membership not in MEMBERSHIP_OPTIONS:
                    abort(400, "Choose a valid membership.")

                try:
                    member_repository.update_member(connection, updated_member)
                    for field_name in EDITABLE_MEMBER_FIELDS:
                        old_value = getattr(member, field_name)
                        new_value = getattr(updated_member, field_name)
                        if old_value != new_value:
                            audit_repository.record_field_change(
                                connection,
                                entity_type="user",
                                entity_id=member_id,
                                action="edit",
                                field_name=field_name,
                                old_value=old_value,
                                new_value=new_value,
                            )
                    connection.commit()
                except sqlite3.IntegrityError as error:
                    connection.rollback()
                    error_message = str(error)
                    if "users.card_number" in error_message:
                        abort(400, "Another user already has that card number.")
                    abort(400, "Could not save the user.")
                return redirect(url_for("member_detail", member_id=member_id))

        return render_template(
            "club_admin/member_edit.html",
            member=member,
            membership_options=MEMBERSHIP_OPTIONS,
        )

    @flask_app.route("/members/<int:member_id>/checkins/edit", methods=["GET", "POST"])
    @require_admin
    def edit_member_checkins(member_id: int):
        with open_connection() as connection:
            member = member_repository.get_member(connection, member_id)
            if member is None:
                abort(404)
            checkins = checkin_repository.list_checkins_for_user(connection, member_id)

            if request.method == "POST":
                try:
                    checkins_changed = False
                    if request.form.get("checkin_action") == "delete_selected":
                        deleted_checkin_ids = {
                            checkin.id
                            for checkin in checkins
                            if checkin.id is not None
                            and request.form.get(f"delete_checkin_{checkin.id}") == "1"
                        }
                        for checkin_id in deleted_checkin_ids:
                            deleted_checkin = next(
                                checkin for checkin in checkins if checkin.id == checkin_id
                            )
                            checkin_repository.delete_checkin_for_user(
                                connection,
                                checkin_id=checkin_id,
                                user_id=member_id,
                            )
                            _record_checkin_change(
                                connection,
                                member_id=member_id,
                                field_name="check-in deleted",
                                old_value=deleted_checkin.check_in_at,
                                new_value=None,
                            )
                        checkins_changed = bool(deleted_checkin_ids)
                        connection.commit()
                        if checkins_changed:
                            checkin_events.notify_checkins_changed()
                        return redirect(url_for("edit_member_checkins", member_id=member_id))

                    edited_checkins = []
                    for checkin in checkins:
                        if checkin.id is None:
                            continue
                        check_in_at = _parse_checkin_datetime(
                            request.form.get(f"checkin_{checkin.id}_check_in_at", ""),
                            required=True,
                        )
                        assert check_in_at is not None
                        membership = request.form.get(
                            f"checkin_{checkin.id}_membership",
                            "",
                        ).strip()
                        if membership not in MEMBERSHIP_OPTIONS:
                            raise CheckInFormError("Choose a valid membership.")
                        edited_checkins.append(
                            _checkin_for_member(
                                member,
                                check_in_at=check_in_at,
                                member_id=checkin.member_id or member.card_number,
                                membership=membership,
                                existing_checkin=checkin,
                            )
                        )

                    new_check_in_at = _parse_checkin_datetime(
                        request.form.get("new_checkin_at", ""),
                        required=False,
                    )
                    new_checkin = (
                        _checkin_for_member(
                            member,
                            check_in_at=new_check_in_at,
                            member_id=_member_id_for_manual_checkin(member, checkins),
                        )
                        if new_check_in_at is not None
                        else None
                    )

                    for edited_checkin in edited_checkins:
                        checkin_repository.update_checkin_for_user(connection, edited_checkin)
                        original_checkin = next(
                            checkin for checkin in checkins if checkin.id == edited_checkin.id
                        )
                        if original_checkin.check_in_at != edited_checkin.check_in_at:
                            checkins_changed = True
                            _record_checkin_change(
                                connection,
                                member_id=member_id,
                                field_name="check-in edited",
                                old_value=original_checkin.check_in_at,
                                new_value=edited_checkin.check_in_at,
                            )
                        if original_checkin.membership != edited_checkin.membership:
                            checkins_changed = True
                            _record_checkin_change(
                                connection,
                                member_id=member_id,
                                field_name="check-in membership edited",
                                old_value=original_checkin.membership,
                                new_value=edited_checkin.membership,
                            )
                    if new_checkin is not None:
                        checkin_repository.upsert_checkin(connection, new_checkin)
                        checkins_changed = True
                        _record_checkin_change(
                            connection,
                            member_id=member_id,
                            field_name="check-in added",
                            old_value=None,
                            new_value=new_checkin.check_in_at,
                        )
                    connection.commit()
                    if checkins_changed:
                        checkin_events.notify_checkins_changed()
                except CheckInFormError as error:
                    abort(400, str(error))
                except sqlite3.IntegrityError as error:
                    connection.rollback()
                    if "checkins" in str(error):
                        abort(400, "A check-in already exists for that date and time.")
                    abort(400, "Could not save the check-ins.")
                return redirect(url_for("member_detail", member_id=member_id))

        return render_template(
            "club_admin/member_checkins_edit.html",
            member=member,
            checkins=checkins,
            membership_options=MEMBERSHIP_OPTIONS,
        )

    @flask_app.route("/checkins/report")
    @require_admin
    def checkins_report():
        today = date.today()
        start_date, end_date = _date_range_from_request(today)
        date_presets = _date_range_presets(today)

        with open_connection() as connection:
            report_context = _checkins_report_context(
                connection,
                start_date,
                end_date,
                today=today,
            )

        return render_template(
            "club_admin/checkins_report.html",
            date_presets=date_presets,
            active_date_preset_label=_active_date_range_preset_label(
                date_presets,
                start_date,
                end_date,
            ),
            **report_context,
        )

    @flask_app.route("/checkins/charts")
    @require_admin
    def checkins_charts():
        today = date.today()
        start_date, end_date = _date_range_from_request(today)
        date_presets = _date_range_presets(today)

        with open_connection() as connection:
            report_context = _checkins_report_context(
                connection,
                start_date,
                end_date,
                today=today,
            )

        return render_template(
            "club_admin/checkins_charts.html",
            date_presets=date_presets,
            active_date_preset_label=_active_date_range_preset_label(
                date_presets,
                start_date,
                end_date,
            ),
            **report_context,
        )

    @flask_app.route("/checkins/report/stream")
    @require_admin
    def checkins_report_stream():
        today = date.today()
        start_date, end_date = _date_range_from_request(today)
        response = Response(
            stream_with_context(
                _checkins_report_event_stream(
                    open_connection,
                    start_date,
                    end_date,
                    today=today,
                )
            ),
            mimetype="text/event-stream",
        )
        response.headers["Cache-Control"] = "no-cache"
        response.headers["X-Accel-Buffering"] = "no"
        return response

    @flask_app.route("/api/checkins/latest")
    def checkins_latest_api():
        if not _request_has_checkin_monitor_access():
            return jsonify({"error": "unauthorized"}), 401

        after_raw = request.args.get("after")
        limit_raw = request.args.get("limit", "")
        wait_raw = request.args.get("wait", "")
        try:
            after_id = int(after_raw) if after_raw is not None else None
            requested_limit = int(limit_raw) if limit_raw else 20
            requested_wait = float(wait_raw) if wait_raw else 0.0
        except ValueError:
            return jsonify({"error": "after, limit, and wait must be numbers"}), 400
        if after_id is not None and after_id < 0:
            return jsonify({"error": "after must be zero or greater"}), 400
        if requested_limit <= 0:
            return jsonify({"error": "limit must be greater than zero"}), 400
        if requested_wait < 0:
            return jsonify({"error": "wait must be zero or greater"}), 400
        limit = min(requested_limit, CHECKIN_MONITOR_MAX_LIMIT)
        wait_seconds = min(requested_wait, CHECKIN_MONITOR_MAX_WAIT_SECONDS)

        deadline = time.monotonic() + wait_seconds

        while True:
            with open_connection() as connection:
                database_latest_id = checkin_repository.latest_checkin_id(connection) or 0
                if after_id is None:
                    latest_id = database_latest_id
                    checkins: list[CheckIn] = []
                    has_more = False
                else:
                    checkins = checkin_repository.list_checkins_after_id(
                        connection,
                        after_id=after_id,
                        limit=limit,
                    )
                    if checkins and checkins[-1].id is not None:
                        latest_id = int(checkins[-1].id)
                        has_more = latest_id < database_latest_id
                    else:
                        latest_id = min(after_id, database_latest_id)
                        has_more = False

            if checkins or after_id is None or time.monotonic() >= deadline:
                break
            sleep_seconds = min(
                CHECKIN_MONITOR_POLL_SECONDS,
                max(0.0, deadline - time.monotonic()),
            )
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)

        response = jsonify(
            {
                "latest_id": latest_id,
                "has_more": has_more,
                "checkins": [
                    _checkin_monitor_payload(checkin) for checkin in checkins
                ],
            }
        )
        response.headers["Cache-Control"] = "no-store, max-age=0"
        return response

    @flask_app.route("/documents/report")
    @require_admin
    def documents_report():
        with open_connection() as connection:
            roster = member_repository.list_members(connection)
        report = _scan_documents_directory(
            roster,
            flask_app.config["USER_MANAGEMENT_DOCUMENTS_DIR"],
        )
        return render_template("club_admin/documents_report.html", report=report)

    @flask_app.route("/documents/image")
    @require_admin
    def document_image():
        image_path = _document_image_path(
            flask_app.config["USER_MANAGEMENT_DOCUMENTS_DIR"],
            request.args.get("name", "").strip(),
            request.args.get("folder", "").strip() or None,
        )
        if image_path is None:
            abort(404)
        return send_file(image_path, conditional=True)

    @flask_app.route("/guest-registrations")
    @require_admin
    def guest_registrations():
        with open_connection() as connection:
            records = guest_registration_repository.list_guest_registration_records(connection)
        latest_record = records[0] if records else None
        return render_template(
            "club_admin/guest_registrations.html",
            records=records,
            latest_registration_id=(
                latest_record.registration.id if latest_record is not None else 0
            ),
        )

    @flask_app.route("/guest-registrations/recent")
    @require_admin
    def recent_guest_registrations():
        with open_connection() as connection:
            records = guest_registration_repository.list_guest_registration_records(connection)
        latest_record = records[0] if records else None
        latest_registration_id = (
            latest_record.registration.id if latest_record is not None else 0
        )
        latest_guest_name = (
            f"{latest_record.member.first_name} {latest_record.member.last_name}"
            if latest_record is not None
            else ""
        )
        return jsonify(
            {
                "count": len(records),
                "latest_guest_name": latest_guest_name,
                "latest_registration_id": latest_registration_id,
                "registration_ids": [
                    record.registration.id
                    for record in records
                    if record.registration.id is not None
                ],
                "rows_html": render_template(
                    "club_admin/_guest_registration_rows.html",
                    records=records,
                ),
            }
        )

    @flask_app.post("/guest-registrations/<int:registration_id>/mark-safe")
    @require_admin
    def mark_guest_registration_safe(registration_id: int):
        with open_connection() as connection:
            record = guest_registration_repository.get_guest_registration_record(
                connection,
                registration_id,
            )
            if record is None:
                abort(404)
            member_id = record.member.id
            if member_id is None:
                abort(404)
            old_status = record.member.screening_status
            if old_status == "banned":
                abort(400, "Banned users must be changed from the user edit page.")
            if old_status != "safe":
                member_repository.update_member_screening_status(
                    connection,
                    member_id,
                    "safe",
                )
                audit_repository.record_field_change(
                    connection,
                    entity_type="user",
                    entity_id=member_id,
                    action="edit",
                    field_name="screening_status",
                    old_value=old_status,
                    new_value="safe",
                )
            connection.commit()
        return redirect(url_for("guest_registrations"))

    @flask_app.route("/guest-registrations/<int:registration_id>/form")
    @require_admin
    def filled_guest_registration_form(registration_id: int):
        with open_connection() as connection:
            record = guest_registration_repository.get_guest_registration_record(
                connection,
                registration_id,
            )
        if record is None:
            abort(404)
        return render_template(
            "club_admin/filled_guest_registration_form.html",
            record=record,
            id_document_name=_id_document_name_for_member(
                record.member,
                flask_app.config["USER_MANAGEMENT_DOCUMENTS_DIR"],
            ),
            form_spec=guest_form.load_guest_form_spec(
                flask_app.config["USER_MANAGEMENT_GUEST_FORM_DEFINITION_PATH"]
            ),
        )

    @flask_app.route("/membership-applications")
    @require_admin
    def membership_applications():
        with open_connection() as connection:
            records = membership_application_repository.list_membership_application_records(
                connection
            )
        return render_template(
            "club_admin/membership_applications.html",
            records=records,
            status_message=request.args.get("message", "").strip(),
        )

    @flask_app.post("/membership-applications/<int:application_id>/fee-received")
    @require_admin
    def mark_membership_application_fee_received(application_id: int):
        with open_connection() as connection:
            record = membership_application_repository.get_membership_application_record(
                connection,
                application_id,
            )
            if record is None:
                abort(404)
            if record.application.status != "pending":
                abort(400, "Only pending applications can be updated.")
            membership_application_repository.mark_application_fee_received(
                connection,
                application_id,
                date.today(),
            )
            connection.commit()
        return redirect(url_for("membership_applications", message="Application fee recorded."))

    @flask_app.route("/membership-applications/<int:application_id>/edit", methods=["GET", "POST"])
    @require_admin
    def edit_membership_application(application_id: int):
        form_spec = _membership_application_form_spec()
        message = ""
        with open_connection() as connection:
            record = membership_application_repository.get_membership_application_record(
                connection,
                application_id,
            )
        if record is None:
            abort(404)
        if record.application.status != "pending":
            abort(400, "Only pending applications can be edited.")

        form_data: Any = _membership_application_form_data(record.application)
        if request.method == "POST":
            form_data = request.form
            try:
                updated_application = _membership_application_from_form(
                    request.form,
                    user_id=record.member.id,
                )
            except MembershipApplicationFormError as error:
                message = str(error)
            else:
                updated_application = replace(
                    updated_application,
                    id=record.application.id,
                    status=record.application.status,
                    application_fee_received_at=(
                        record.application.application_fee_received_at
                    ),
                    reviewed_at=record.application.reviewed_at,
                    created_at=record.application.created_at,
                    signed_at=record.application.signed_at,
                )
                with open_connection() as connection:
                    membership_application_repository.update_membership_application(
                        connection,
                        updated_application,
                    )
                    audit_repository.record_field_change(
                        connection,
                        entity_type="user",
                        entity_id=record.member.id,
                        action="edit",
                        field_name="membership application edited",
                        old_value=record.application.id,
                        new_value=record.application.id,
                    )
                    connection.commit()
                return redirect(
                    url_for(
                        "membership_applications",
                        message="Application updated.",
                    )
                )

        return render_template(
            "club_admin/membership_application.html",
            member=record.member,
            message=message,
            form_data=form_data,
            today=date.today(),
            form_action=url_for(
                "edit_membership_application",
                application_id=application_id,
            ),
            submit_label="Save Application",
            cancel_url=url_for("membership_applications"),
            form_spec=form_spec,
            auto_return_seconds=KIOSK_AUTO_RETURN_SECONDS,
            auto_return_delay_ms=KIOSK_AUTO_RETURN_SECONDS * 1000,
        )

    @flask_app.post("/membership-applications/<int:application_id>/approve")
    @require_admin
    def approve_membership_application(application_id: int):
        reviewed_at = datetime.now()
        with open_connection() as connection:
            record = membership_application_repository.get_membership_application_record(
                connection,
                application_id,
            )
            if record is None:
                abort(404)
            if record.application.status != "pending":
                abort(400, "Only pending applications can be approved.")
            approved_member = _member_with_approved_application(
                record.member,
                record.application,
            )
            _record_member_profile_changes(
                connection,
                old_member=record.member,
                new_member=approved_member,
            )
            member_repository.update_member_membership_profile(connection, approved_member)
            membership_application_repository.update_application_status(
                connection,
                application_id,
                status="approved",
                reviewed_at=reviewed_at,
            )
            connection.commit()
        return redirect(url_for("membership_applications", message="Application approved."))

    @flask_app.post("/membership-applications/<int:application_id>/decline")
    @require_admin
    def decline_membership_application(application_id: int):
        with open_connection() as connection:
            record = membership_application_repository.get_membership_application_record(
                connection,
                application_id,
            )
            if record is None:
                abort(404)
            if record.application.status != "pending":
                abort(400, "Only pending applications can be declined.")
            membership_application_repository.update_application_status(
                connection,
                application_id,
                status="declined",
                reviewed_at=datetime.now(),
            )
            connection.commit()
        return redirect(url_for("membership_applications", message="Application declined."))

    @flask_app.route("/membership-applications/<int:application_id>/form")
    @require_admin
    def filled_membership_application_form(application_id: int):
        form_spec = _membership_application_form_spec()
        with open_connection() as connection:
            record = membership_application_repository.get_membership_application_record(
                connection,
                application_id,
            )
            latest_registration = (
                guest_registration_repository.get_latest_guest_registration_for_user(
                    connection,
                    record.member.id,
                )
                if record is not None
                else None
            )
        if record is None:
            abort(404)
        return render_template(
            "club_admin/filled_membership_application_form.html",
            record=record,
            heard_about=(
                latest_registration.registration.heard_about
                if latest_registration is not None
                else None
            ),
            form_spec=form_spec,
        )

    @flask_app.post("/guest-registrations/<int:registration_id>/driver-license")
    @require_admin
    def upload_guest_registration_driver_license(registration_id: int):
        uploaded_file = request.files.get("driver_license")
        if uploaded_file is None or not uploaded_file.filename:
            abort(400, "Driver license image is required.")
        with open_connection() as connection:
            record = guest_registration_repository.get_guest_registration_record(
                connection,
                registration_id,
            )
        if record is None:
            abort(404)
        destination_path = _id_document_storage_path(
            record.member,
            flask_app.config["USER_MANAGEMENT_DOCUMENTS_DIR"],
        )
        if destination_path is None:
            abort(400, "Document storage is not configured.")
        _save_driver_license_image(uploaded_file, destination_path)
        with open_connection() as connection:
            audit_repository.record_field_change(
                connection,
                entity_type="user",
                entity_id=record.member.id,
                action="edit",
                field_name="driver license uploaded",
                old_value=None,
                new_value=destination_path.name,
            )
            connection.commit()
        return redirect(url_for("filled_guest_registration_form", registration_id=registration_id))

    @flask_app.route("/self-checkin", methods=["GET", "POST"])
    def self_checkin():
        message = ""
        checkin_success = False
        checkin_blocked = False
        barcode_svg = ""
        barcode_show_default = True
        if request.method == "POST":
            member = None
            checkin_result: LiveCheckInResult | None = None
            with open_connection() as connection:
                barcode_secret = _barcode_secret_for_connection(
                    connection,
                    flask_app.config["USER_MANAGEMENT_BARCODE_SECRET"],
                )
                identity_result = _resolve_kiosk_identity(
                    connection,
                    request.form,
                    barcode_secret=barcode_secret,
                )
                member = identity_result.member
                if member is not None:
                    checkin_result = _record_self_checkin(connection, member)
                    checkin_blocked = checkin_result.blocked
                    if not checkin_blocked:
                        token = _barcode_token_for_card_number(
                            member.card_number,
                            barcode_secret,
                        )
                        barcode_svg = _code128b_svg(token)
                        barcode_show_default = not identity_result.used_barcode
                    connection.commit()
                    if checkin_result.recorded:
                        checkin_events.notify_checkins_changed()
            checkin_success = member is not None and not checkin_blocked
            if checkin_blocked:
                message = "Please see the front desk."
            else:
                message = (
                    (
                        "Check-in recorded."
                        if checkin_result is not None and checkin_result.recorded
                        else "Already checked in within the past hour."
                    )
                    if checkin_success
                    else "No matching user was found. Please check your barcode, phone number, and initials or first name."
                )

        response = make_response(
            render_template(
                "club_admin/self_checkin.html",
                message=message,
                checkin_success=checkin_success,
                checkin_blocked=checkin_blocked,
                barcode_svg=barcode_svg,
                barcode_show_default=barcode_show_default,
                auto_return_seconds=KIOSK_AUTO_RETURN_SECONDS,
                auto_return_delay_ms=KIOSK_AUTO_RETURN_SECONDS * 1000,
            )
        )
        if message or barcode_svg:
            response.headers["Refresh"] = (
                f"{KIOSK_AUTO_RETURN_SECONDS}; url={url_for('self_checkin')}"
            )
        return response

    return flask_app


if __name__ == "__main__":
    app = create_app()
    flask_debug = os.getenv("FLASK_DEBUG", "").strip().lower() in {"1", "true", "yes", "on"}
    flask_host = os.getenv("CLUB_ADMIN_HOST", "127.0.0.1").strip() or "127.0.0.1"
    app.run(debug=flask_debug, use_reloader=flask_debug, host=flask_host, port=5052)
