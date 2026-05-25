'''Flask app for club user management.'''

import base64
import hashlib
import hmac
import io
import os
import re
import sqlite3
import unicodedata
from collections.abc import Iterator
from collections import Counter
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta
from functools import wraps
from pathlib import Path
from secrets import token_hex, token_urlsafe
from typing import Any

from flask import (
    Flask,
    abort,
    jsonify,
    make_response,
    redirect,
    render_template,
    request,
    send_file,
    session,
    url_for,
)
from PIL import Image, ImageChops, ImageOps, UnidentifiedImageError
from werkzeug.security import check_password_hash

import config as cfg
from club_admin import audit_repository
from club_admin import checkin_repository
from club_admin import csv_import
from club_admin import database
from club_admin import guest_form
from club_admin import guest_registration_repository
from club_admin import member_repository
from club_admin import zip_repository
from club_admin.models import CheckIn, GuestRegistration, Member


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
    "phone",
    "email",
    "work_phone",
    "cell_phone",
)
MEMBERSHIP_OPTIONS = (
    "AANR Member",
    "Associate Member",
    "Full Member",
    "Visitor",
)
BARCODE_SECRET_SETTING_KEY = "self_checkin_barcode_secret"
KIOSK_AUTO_RETURN_SECONDS = 60
SUPPORTED_DOCUMENT_IMAGE_SUFFIXES = {".gif", ".jpeg", ".jpg", ".png", ".webp"}
DRIVER_LICENSE_DOCUMENT_NAME = "Driver License.jpg"
DRIVER_LICENSE_IMAGE_SIZE = (2026, 1152)
DRIVER_LICENSE_CROP_THRESHOLD = 24
DRIVER_LICENSE_CROP_ASPECT_FLOOR = 1.45
ID_DOCUMENT_NAME_PATTERN = re.compile(
    r"^(?:drivers?\s+license|drivers?\s+licence|dl|id|identification)(?:\b|[_\-\s])",
    re.IGNORECASE,
)
BARCODE_TOKEN_VERSION = "UM1"
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
class DocumentsScanReport:
    configured: bool
    readable: bool
    total_users: int
    total_document_files: int
    users_with_guest_form: int
    users_without_guest_form: int
    extension_counts: tuple[DocumentExtensionCount, ...]
    filename_patterns: tuple[DocumentFilenamePattern, ...]
    missing_guest_form_users: tuple[Member, ...]
    card_folders_without_user: tuple[tuple[str, tuple[str, ...]], ...]
    user_folders_with_extra_files: tuple[tuple[Member, tuple[str, ...]], ...]
    non_folder_entries: tuple[str, ...]


class GuestRegistrationFormError(ValueError):
    '''Raised when a visitor registration submission cannot be accepted.'''


class MemberFormError(ValueError):
    '''Raised when admin-edited user fields cannot be accepted.'''


class CheckInFormError(ValueError):
    '''Raised when admin-edited check-ins cannot be accepted.'''


def _parse_member_form_date(form_data: Any, field_name: str) -> date | None:
    value = form_data.get(field_name, "").strip()
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise MemberFormError("Enter valid user dates.") from error


def _member_from_form(member_id: int, form_data: Any) -> Member:
    return Member(
        id=member_id,
        last_name=form_data.get("last_name", "").strip(),
        first_name=form_data.get("first_name", "").strip(),
        nickname=form_data.get("nickname", "").strip() or None,
        card_number=form_data.get("card_number", "").strip(),
        membership=form_data.get("membership", "").strip(),
        member_since=_parse_member_form_date(form_data, "member_since"),
        date_of_birth=_parse_member_form_date(form_data, "date_of_birth"),
        address=form_data.get("address", "").strip() or None,
        address2=form_data.get("address2", "").strip() or None,
        city=form_data.get("city", "").strip() or None,
        state=form_data.get("state", "").strip() or None,
        zip=form_data.get("zip", "").strip() or None,
        phone=form_data.get("phone", "").strip() or None,
        email=form_data.get("email", "").strip() or None,
        work_phone=form_data.get("work_phone", "").strip() or None,
        cell_phone=form_data.get("cell_phone", "").strip() or None,
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
        membership=member.membership,
    )


def _record_checkin_change(
    connection: sqlite3.Connection,
    *,
    member_id: int,
    field_name: str,
    old_value: datetime | None,
    new_value: datetime | None,
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


def _clear_user_import_data(connection: sqlite3.Connection) -> None:
    connection.execute("DELETE FROM guest_registrations")
    connection.execute("DELETE FROM checkins")
    connection.execute("DELETE FROM audit_log WHERE entity_type = 'user'")
    connection.execute("DELETE FROM users")


def _visitor_text_or_none(form_data: Any, field_name: str) -> str | None:
    return form_data.get(field_name, "").strip() or None


def _visitor_bool(form_data: Any, field_name: str) -> bool:
    return form_data.get(field_name) in {"1", "true", "yes", "on"}


def _visitor_choice(
    form_data: Any,
    field_name: str,
    allowed_values: set[str],
) -> str | None:
    value = form_data.get(field_name, "").strip()
    return value if value in allowed_values else None


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
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise GuestRegistrationFormError("Date of birth must use YYYY-MM-DD.")


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
    return None


def _guest_registration_from_form(
    form_data: Any,
    *,
    card_number: str,
) -> tuple[Member, GuestRegistration]:
    other_phone = _visitor_text_or_none(form_data, "other_phone")
    other_phone_type = _visitor_choice(form_data, "other_phone_type", {"home", "work", "other"})
    phone = other_phone if other_phone_type != "work" else None
    work_phone = other_phone if other_phone_type == "work" else None

    member = Member(
        last_name=form_data.get("last_name", "").strip(),
        first_name=form_data.get("first_name", "").strip(),
        nickname=_visitor_text_or_none(form_data, "nickname"),
        card_number=card_number,
        membership="Visitor",
        date_of_birth=_parse_visitor_date_of_birth(form_data),
        address=_visitor_text_or_none(form_data, "address"),
        city=_visitor_text_or_none(form_data, "city"),
        state=_visitor_text_or_none(form_data, "state"),
        zip=_visitor_text_or_none(form_data, "zip"),
        phone=phone,
        email=_visitor_text_or_none(form_data, "email"),
        work_phone=work_phone,
        cell_phone=_visitor_text_or_none(form_data, "cell_phone"),
    )
    registration = GuestRegistration(
        visit_date=_parse_visitor_visit_date(form_data),
        middle_name=_visitor_text_or_none(form_data, "middle_name"),
        other_phone=other_phone,
        other_phone_type=other_phone_type,
        marital_status=_visitor_choice(
            form_data,
            "marital_status",
            {"single", "married", "recognized_couple"},
        ),
        partner_name=_visitor_text_or_none(form_data, "partner_name"),
        guest_of_member=_visitor_bool(form_data, "guest_of_member"),
        member_name=_visitor_text_or_none(form_data, "member_name"),
        heard_about=_visitor_text_or_none(form_data, "heard_about"),
        newsletter_opt_out=_visitor_bool(form_data, "newsletter_opt_out"),
    )
    return member, registration


def _safe_next_url(next_url: str | None) -> str:
    if next_url and next_url.startswith("/") and not next_url.startswith("//"):
        return next_url
    return url_for("members")


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
        return MemberDocumentPreview(title="Guest Form", is_guest_form=True)
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


def _image_content_bbox(image: Image.Image) -> tuple[int, int, int, int] | None:
    rgb_image = image.convert("RGB")
    white_background = Image.new("RGB", rgb_image.size, "WHITE")
    difference = ImageChops.difference(rgb_image, white_background).convert("L")
    mask = difference.point(
        lambda value: 255 if value > DRIVER_LICENSE_CROP_THRESHOLD else 0
    )
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
        extra_names.append(child.name + ("/" if child.is_dir() else ""))
    return tuple(extra_names)


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
    for file_name in sorted(file_names, key=_normalized_filename):
        pattern = _filename_pattern(file_name)
        pattern_examples.setdefault(pattern, [])
        if len(pattern_examples[pattern]) < 3:
            pattern_examples[pattern].append(file_name)

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
            total_document_files=0,
            users_with_guest_form=0,
            users_without_guest_form=len(members),
            extension_counts=empty_extension_counts,
            filename_patterns=empty_filename_patterns,
            missing_guest_form_users=tuple(members),
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
            total_document_files=0,
            users_with_guest_form=0,
            users_without_guest_form=len(members),
            extension_counts=empty_extension_counts,
            filename_patterns=empty_filename_patterns,
            missing_guest_form_users=tuple(members),
            card_folders_without_user=(),
            user_folders_with_extra_files=(),
            non_folder_entries=(),
        )

    members_by_card = {
        _normalized_filename(member.card_number.strip().strip("'").strip()): member
        for member in members
    }
    user_cards_with_guest_form: set[str] = set()
    user_folders_with_extra_files: list[tuple[Member, tuple[str, ...]]] = []
    card_folders_without_user: list[tuple[str, tuple[str, ...]]] = []
    non_folder_entries: list[str] = []
    document_file_names: list[str] = []

    try:
        children = sorted(base_dir.iterdir(), key=lambda path: _normalized_filename(path.name))
    except OSError:
        return DocumentsScanReport(
            configured=True,
            readable=False,
            total_users=len(members),
            total_document_files=0,
            users_with_guest_form=0,
            users_without_guest_form=len(members),
            extension_counts=empty_extension_counts,
            filename_patterns=empty_filename_patterns,
            missing_guest_form_users=tuple(members),
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
            continue

        document_file_names.extend(
            folder_child.name for folder_child in _folder_entry_paths(child) if folder_child.is_file()
        )
        normalized_card = _normalized_filename(child.name)
        member = members_by_card.get(normalized_card)
        if member is None:
            card_folders_without_user.append((child.name, _folder_entry_names(child)))
            continue

        guest_form_path = _first_guest_form_image(child)
        if guest_form_path is not None:
            user_cards_with_guest_form.add(normalized_card)

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
        total_document_files=len(document_file_names),
        users_with_guest_form=len(user_cards_with_guest_form),
        users_without_guest_form=len(missing_guest_form_users),
        extension_counts=extension_counts,
        filename_patterns=filename_patterns,
        missing_guest_form_users=missing_guest_form_users,
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


def _barcode_signature(card_number: str, secret_key: object) -> str:
    digest = hmac.new(
        _barcode_secret_bytes(secret_key),
        card_number.encode("utf-8"),
        hashlib.sha256,
    ).digest()
    return base64.urlsafe_b64encode(digest[:9]).decode("ascii").rstrip("=")


def _barcode_token_for_card_number(card_number: str, secret_key: object) -> str:
    signature = _barcode_signature(card_number, secret_key)
    return f"{BARCODE_TOKEN_VERSION}:{signature}"


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
    parts = token.strip().split(":")
    if len(parts) != 2 or parts[0] != BARCODE_TOKEN_VERSION:
        return None
    for member in member_repository.list_members(connection):
        if hmac.compare_digest(
            token.strip(),
            _barcode_token_for_card_number(member.card_number, secret_key),
        ):
            return member
    return None


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
        f'viewBox="0 0 {svg_width} {height}" xmlns="http://www.w3.org/2000/svg">'
        f'<rect width="{svg_width}" height="{height}" fill="#fff"/>'
        f'<g fill="#111827">{"".join(rects)}</g>'
        "</svg>"
    )


def _record_self_checkin(connection: sqlite3.Connection, member: Member) -> datetime:
    check_in_at = datetime.now().replace(microsecond=0)
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
    return check_in_at


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
    flask_app.config["USER_MANAGEMENT_BARCODE_SECRET"] = _configured_barcode_secret()
    flask_app.config["USER_MANAGEMENT_DOCUMENTS_DIR"] = str(
        getattr(cfg, "USER_MANAGEMENT_DOCUMENTS_DIR", "")
    ).strip()
    flask_app.config["USER_MANAGEMENT_GUEST_FORM_DEFINITION_PATH"] = str(
        getattr(cfg, "USER_MANAGEMENT_GUEST_FORM_DEFINITION_PATH", "")
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

    def require_admin(view):
        @wraps(view)
        def wrapped(*args, **kwargs):
            if session.get("user_management_admin_authenticated") is True:
                return view(*args, **kwargs)
            next_url = f"{request.script_root}{request.full_path}".rstrip("?")
            return redirect(url_for("admin_login", next=next_url))

        return wrapped

    @flask_app.context_processor
    def inject_app_title() -> dict[str, str]:
        organization_name = str(cfg.USER_MANAGEMENT_ORGANIZATION_NAME).strip()
        return {
            "organization_name": organization_name,
            "app_title": f"{organization_name} User Management",
            "is_document_image": _is_document_image_name,
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
                card_number = _generate_guest_card_number(connection)
                member = replace(member, card_number=card_number)

                member_id = member_repository.insert_member(connection, member)
                guest_registration_repository.insert_guest_registration(
                    connection,
                    replace(registration, user_id=member_id),
                )
                connection.commit()
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
        with open_connection() as connection:
            for member_id in selected_member_ids:
                member = member_repository.get_member(connection, member_id)
                if member is None:
                    abort(404)
                check_in_at = _record_self_checkin(connection, member)
                _record_checkin_change(
                    connection,
                    member_id=member_id,
                    field_name="check-in added",
                    old_value=None,
                    new_value=check_in_at,
                )
                checked_in_members.append(member)
            connection.commit()

        if len(checked_in_members) == 1:
            member = checked_in_members[0]
            checked_in_message = f"Checked in {member.first_name} {member.last_name}."
        else:
            checked_in_message = f"Checked in {len(checked_in_members)} users."
        return redirect(url_for("members", checked_in=checked_in_message))

    @flask_app.route("/imports")
    @require_admin
    def imports():
        return render_template("club_admin/imports.html")

    @flask_app.route("/members/map")
    @require_admin
    def members_map():
        with open_connection() as connection:
            roster = member_repository.list_members(connection)
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
        )

    @flask_app.post("/members/map/zip-coordinates/import")
    @require_admin
    def import_zip_coordinates():
        csv_file = request.files.get("zip_coordinates_csv")
        if csv_file is None or not csv_file.filename:
            abort(400, "Choose a ZIP coordinate CSV file.")

        text = io.TextIOWrapper(csv_file.stream, encoding="utf-8-sig", newline="")
        try:
            coordinates = zip_repository.read_zip_coordinates_csv(text)
        except ValueError as error:
            abort(400, str(error))

        with open_connection() as connection:
            imported_count = zip_repository.upsert_zip_coordinates(connection, coordinates)
            connection.commit()

        return redirect(url_for("members_map", imported=imported_count))

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
            audit_entries=audit_entries,
            document_preview=document_preview,
            other_documents=other_documents,
        )

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
        return redirect(url_for("member_detail", member_id=member_id))

    @flask_app.route("/members/<int:member_id>/edit", methods=["GET", "POST"])
    @require_admin
    def edit_member(member_id: int):
        with open_connection() as connection:
            member = member_repository.get_member(connection, member_id)
            if member is None:
                abort(404)
            checkins = checkin_repository.list_checkins_for_user(connection, member_id)

            if request.method == "POST":
                try:
                    updated_member = _member_from_form(member_id, request.form)
                except MemberFormError as error:
                    abort(400, str(error))

                if not updated_member.last_name or not updated_member.first_name:
                    abort(400, "First and last name are required.")
                if not updated_member.card_number or not updated_member.membership:
                    abort(400, "Card number and membership are required.")
                if updated_member.membership not in MEMBERSHIP_OPTIONS:
                    abort(400, "Choose a valid membership.")

                try:
                    edited_checkins = []
                    deleted_checkin_ids = set()
                    for checkin in checkins:
                        if checkin.id is None:
                            continue
                        if request.form.get(f"delete_checkin_{checkin.id}") == "1":
                            deleted_checkin_ids.add(checkin.id)
                            continue

                        check_in_at = _parse_checkin_datetime(
                            request.form.get(f"checkin_{checkin.id}_check_in_at", ""),
                            required=True,
                        )
                        assert check_in_at is not None
                        edited_checkins.append(
                            _checkin_for_member(
                                updated_member,
                                check_in_at=check_in_at,
                                member_id=checkin.member_id or updated_member.card_number,
                                existing_checkin=checkin,
                            )
                        )

                    new_check_in_at = _parse_checkin_datetime(
                        request.form.get("new_checkin_at", ""),
                        required=False,
                    )
                    new_checkin = (
                        _checkin_for_member(
                            updated_member,
                            check_in_at=new_check_in_at,
                            member_id=_member_id_for_manual_checkin(updated_member, checkins),
                        )
                        if new_check_in_at is not None
                        else None
                    )

                    member_repository.update_member(connection, updated_member)
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
                    for edited_checkin in edited_checkins:
                        checkin_repository.update_checkin_for_user(connection, edited_checkin)
                        original_checkin = next(
                            checkin for checkin in checkins if checkin.id == edited_checkin.id
                        )
                        if original_checkin.check_in_at != edited_checkin.check_in_at:
                            _record_checkin_change(
                                connection,
                                member_id=member_id,
                                field_name="check-in edited",
                                old_value=original_checkin.check_in_at,
                                new_value=edited_checkin.check_in_at,
                            )
                    if new_checkin is not None:
                        checkin_repository.upsert_checkin(connection, new_checkin)
                        _record_checkin_change(
                            connection,
                            member_id=member_id,
                            field_name="check-in added",
                            old_value=None,
                            new_value=new_checkin.check_in_at,
                        )

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
                except CheckInFormError as error:
                    abort(400, str(error))
                except sqlite3.IntegrityError as error:
                    connection.rollback()
                    error_message = str(error)
                    if "checkins" in error_message:
                        abort(400, "A check-in already exists for that date and time.")
                    if "users.card_number" in error_message:
                        abort(400, "Another user already has that card number.")
                    abort(400, "Could not save the user or check-ins.")
                return redirect(url_for("member_detail", member_id=member_id))

        return render_template(
            "club_admin/member_edit.html",
            member=member,
            checkins=checkins,
            membership_options=MEMBERSHIP_OPTIONS,
        )

    @flask_app.route("/checkins/report")
    @require_admin
    def checkins_report():
        today = date.today()
        default_start_date = today
        start_date_raw = request.args.get("start_date", default_start_date.isoformat())
        end_date_raw = request.args.get("end_date", today.isoformat())
        try:
            start_date = date.fromisoformat(start_date_raw)
            end_date = date.fromisoformat(end_date_raw)
        except ValueError:
            abort(400, "Date range must use YYYY-MM-DD dates.")

        if start_date > end_date:
            abort(400, "Start date must be on or before end date.")

        with open_connection() as connection:
            checkins = checkin_repository.list_checkins_for_date_range(
                connection,
                start_date,
                end_date,
            )

        return render_template(
            "club_admin/checkins_report.html",
            checkins=checkins,
            start_date=start_date,
            end_date=end_date,
        )

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
        return redirect(url_for("filled_guest_registration_form", registration_id=registration_id))

    @flask_app.route("/self-checkin", methods=["GET", "POST"])
    def self_checkin():
        message = ""
        checkin_success = False
        barcode_svg = ""
        if request.method == "POST":
            barcode_token = request.form.get("barcode_token", "").strip()
            member = None
            with open_connection() as connection:
                if barcode_token:
                    barcode_secret = _barcode_secret_for_connection(
                        connection,
                        flask_app.config["USER_MANAGEMENT_BARCODE_SECRET"],
                    )
                    member = _member_from_barcode_token(
                        connection,
                        barcode_token,
                        barcode_secret,
                    )
                else:
                    phone = request.form.get("phone", "").strip()
                    initials = request.form.get("initials", "").strip()
                    member = member_repository.find_member_by_phone_and_initials(
                        connection,
                        phone,
                        initials,
                    )
                if member is not None:
                    _record_self_checkin(connection, member)
                    if not barcode_token:
                        barcode_secret = _barcode_secret_for_connection(
                            connection,
                            flask_app.config["USER_MANAGEMENT_BARCODE_SECRET"],
                        )
                        token = _barcode_token_for_card_number(
                            member.card_number,
                            barcode_secret,
                        )
                        barcode_svg = _code128b_svg(token)
                    connection.commit()
            checkin_success = member is not None
            message = (
                "Check-in recorded."
                if checkin_success
                else "No matching user was found. Please check your barcode, phone number, and initials or first name."
            )

        response = make_response(
            render_template(
                "club_admin/self_checkin.html",
                message=message,
                checkin_success=checkin_success,
                barcode_svg=barcode_svg,
                auto_return_seconds=KIOSK_AUTO_RETURN_SECONDS,
                auto_return_delay_ms=KIOSK_AUTO_RETURN_SECONDS * 1000,
            )
        )
        if message or barcode_svg:
            response.headers["Refresh"] = (
                f"{KIOSK_AUTO_RETURN_SECONDS}; url={url_for('self_checkin')}"
            )
        return response

    @flask_app.post("/members/import")
    @require_admin
    def import_members():
        csv_file = request.files.get("members_csv")
        if csv_file is None or not csv_file.filename:
            return "CSV file is required.", 400

        stream = io.StringIO(csv_file.stream.read().decode("utf-8-sig"))
        members_to_import = csv_import.read_members_csv(stream)
        with open_connection() as connection:
            if request.form.get("remove_existing_users") == "1":
                _clear_user_import_data(connection)
            for member in members_to_import:
                member_repository.upsert_member(connection, member)
            connection.commit()
        return redirect(url_for("members"))

    @flask_app.post("/checkins/import")
    @require_admin
    def import_checkins():
        csv_file = request.files.get("checkins_csv")
        if csv_file is None or not csv_file.filename:
            return "CSV file is required.", 400

        stream = io.StringIO(csv_file.stream.read().decode("utf-8-sig"))
        checkins_to_import = csv_import.read_checkins_csv(stream)
        with open_connection() as connection:
            for checkin in checkins_to_import:
                checkin_repository.upsert_checkin(connection, checkin)
            connection.commit()
        return redirect(url_for("members"))

    return flask_app


if __name__ == "__main__":
    app = create_app()
    flask_debug = os.getenv("FLASK_DEBUG", "").strip().lower() in {"1", "true", "yes", "on"}
    flask_host = os.getenv("CLUB_ADMIN_HOST", "127.0.0.1").strip() or "127.0.0.1"
    app.run(debug=flask_debug, use_reloader=flask_debug, host=flask_host, port=5052)
