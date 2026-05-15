'''Flask app for club user management.'''

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
from secrets import token_hex
from typing import Any

from flask import (
    Flask,
    abort,
    jsonify,
    redirect,
    render_template,
    request,
    send_file,
    session,
    url_for,
)
from werkzeug.security import check_password_hash

import config as cfg
from club_admin import audit_repository
from club_admin import checkin_repository
from club_admin import csv_import
from club_admin import database
from club_admin import member_repository
from club_admin import zip_repository
from club_admin.models import CheckIn, Member


EDITABLE_MEMBER_FIELDS = (
    "last_name",
    "first_name",
    "card_number",
    "membership",
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
SUPPORTED_DOCUMENT_IMAGE_SUFFIXES = {".gif", ".jpeg", ".jpg", ".png", ".webp"}


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


def _member_from_form(member_id: int, form_data: Any) -> Member:
    return Member(
        id=member_id,
        last_name=form_data.get("last_name", "").strip(),
        first_name=form_data.get("first_name", "").strip(),
        card_number=form_data.get("card_number", "").strip(),
        membership=form_data.get("membership", "").strip(),
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
    flask_app.config["USER_MANAGEMENT_DOCUMENTS_DIR"] = str(
        getattr(cfg, "USER_MANAGEMENT_DOCUMENTS_DIR", "")
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

    def require_admin(view):
        @wraps(view)
        def wrapped(*args, **kwargs):
            if session.get("user_management_admin_authenticated") is True:
                return view(*args, **kwargs)
            return redirect(url_for("admin_login", next=request.full_path.rstrip("?")))

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
        return render_template("club_admin/members.html", members=roster)

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
        guest_form_available = (
            _guest_form_path_for_member(
                member,
                flask_app.config["USER_MANAGEMENT_DOCUMENTS_DIR"],
            )
            is not None
        )
        other_documents = tuple(
            MemberDocument(name=name)
            for name in _member_document_names(
                member,
                flask_app.config["USER_MANAGEMENT_DOCUMENTS_DIR"],
                include_guest_form=False,
            )
        )

        return render_template(
            "club_admin/member_detail.html",
            member=member,
            checkins=checkins,
            audit_entries=audit_entries,
            guest_form_available=guest_form_available,
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

    @flask_app.route("/members/<int:member_id>/edit", methods=["GET", "POST"])
    @require_admin
    def edit_member(member_id: int):
        with open_connection() as connection:
            member = member_repository.get_member(connection, member_id)
            if member is None:
                abort(404)

            if request.method == "POST":
                updated_member = _member_from_form(member_id, request.form)

                if not updated_member.last_name or not updated_member.first_name:
                    abort(400, "First and last name are required.")
                if not updated_member.card_number or not updated_member.membership:
                    abort(400, "Card number and membership are required.")

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
                return redirect(url_for("member_detail", member_id=member_id))

        return render_template("club_admin/member_edit.html", member=member)

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
            summaries = checkin_repository.summarize_checkins_by_user(
                connection,
                start_date,
                end_date,
            )

        return render_template(
            "club_admin/checkins_report.html",
            summaries=summaries,
            start_date=start_date,
            end_date=end_date,
            total_checkins=sum(summary.checkin_count for summary in summaries),
        )

    @flask_app.route("/checkins/daily")
    @require_admin
    def daily_checkins_report():
        today = date.today()
        start_date_raw = request.args.get("start_date", today.isoformat())
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
            "club_admin/daily_checkins_report.html",
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

    @flask_app.route("/self-checkin", methods=["GET", "POST"])
    def self_checkin():
        message = ""
        if request.method == "POST":
            phone = request.form.get("phone", "").strip()
            initials = request.form.get("initials", "").strip()
            with open_connection() as connection:
                member = member_repository.find_member_by_phone_and_initials(
                    connection,
                    phone,
                    initials,
                )
                if member is not None:
                    checkin_repository.upsert_checkin(
                        connection,
                        CheckIn(
                            user_id=member.id,
                            member_id=str(member.id),
                            last_name=member.last_name,
                            first_name=member.first_name,
                            card_number=member.card_number,
                            check_in_at=datetime.now().replace(microsecond=0),
                            membership=member.membership,
                        ),
                    )
                    connection.commit()
            message = (
                "Check-in recorded."
                if member is not None
                else "No matching user was found. Please check your phone number and initials or first name."
            )

        return render_template(
            "club_admin/self_checkin.html",
            message=message,
        )

    @flask_app.post("/members/import")
    @require_admin
    def import_members():
        csv_file = request.files.get("members_csv")
        if csv_file is None or not csv_file.filename:
            return "CSV file is required.", 400

        stream = io.StringIO(csv_file.stream.read().decode("utf-8-sig"))
        members_to_import = csv_import.read_members_csv(stream)
        with open_connection() as connection:
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
    app.run(debug=flask_debug, use_reloader=flask_debug, host="127.0.0.1", port=5052)
