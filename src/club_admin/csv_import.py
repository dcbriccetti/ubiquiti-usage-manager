'''CSV import support for club user rosters.'''

import csv
import re
from datetime import date, datetime
from pathlib import Path
from typing import TextIO

from club_admin.models import CheckIn, Member


HEADER_MAP = {
    "last name": "last_name",
    "last": "last_name",
    "first name": "first_name",
    "first": "first_name",
    "fname": "first_name",
    "card #": "card_number",
    "card": "card_number",
    "card no": "card_number",
    "card no.": "card_number",
    "card number": "card_number",
    "membership": "membership",
    "address": "address",
    "address2": "address2",
    "city": "city",
    "state": "state",
    "zip": "zip",
    "phone": "phone",
    "email": "email",
    "work phone": "work_phone",
    "cell phone": "cell_phone",
}

CHECKIN_HEADER_MAP = {
    "member id": "member_id",
    "last name": "last_name",
    "last": "last_name",
    "first name": "first_name",
    "first": "first_name",
    "fname": "first_name",
    "card #": "card_number",
    "card": "card_number",
    "card no": "card_number",
    "card no.": "card_number",
    "card number": "card_number",
    "check-in date": "check_in_date",
    "check-in time": "check_in_time",
    "check-out time": "check_out_time",
    "total check-ins": "total_checkins",
    "duration": "duration",
    "membership": "membership",
}

REQUIRED_FIELDS = {"last_name", "first_name", "card_number", "membership"}
CHECKIN_REQUIRED_FIELDS = {
    "member_id",
    "last_name",
    "first_name",
    "card_number",
    "check_in_date",
    "check_in_time",
    "membership",
}


def _normalized_header(header: str) -> str:
    return " ".join(header.strip().lower().split())


def _lines_after_leading_blank_rows(source: TextIO) -> tuple[list[str], int]:
    lines = source.readlines()
    skipped_count = 0
    while skipped_count < len(lines) and not lines[skipped_count].strip():
        skipped_count += 1
    return lines[skipped_count:], skipped_count


def _mapped_header_fields(headers: list[str], header_map: dict[str, str]) -> set[str]:
    return {
        header_map[normalized_header]
        for header in headers
        if (normalized_header := _normalized_header(header)) in header_map
    }


def _compact_header(header: str) -> str:
    return "".join(header.strip().lower().split())


def _split_merged_header_cell(header: str) -> list[str]:
    compact_header = _compact_header(header)
    if compact_header in {"firstnamecard", "firstnamecard#", "firstnamecardnumber"}:
        return ["First Name", "Card #"]
    if compact_header in {"fnamecard", "fnamecard#", "fnamecardnumber"}:
        return ["First Name", "Card #"]
    if compact_header in {"namecard", "namecard#", "namecardnumber"}:
        return ["First Name", "Card #"]
    if compact_header in {"lastnamecard", "lastnamecard#", "lastnamecardnumber"}:
        return ["Last Name", "Card #"]
    return [header]


def _repair_merged_header_cells(headers: list[str]) -> list[str]:
    repaired_headers: list[str] = []
    for header in headers:
        repaired_headers.extend(_split_merged_header_cell(header))
    return repaired_headers


def _find_header_row(
    rows: list[list[str]],
    header_map: dict[str, str],
    required_fields: set[str],
) -> tuple[int, list[str]]:
    for index, row in enumerate(rows):
        if not any(cell.strip() for cell in row):
            continue
        repaired_row = _repair_merged_header_cells(row)
        mapped_fields = _mapped_header_fields(repaired_row, header_map)
        if required_fields.issubset(mapped_fields):
            return index, repaired_row

    preview_rows = [
        ", ".join(cell.strip() for cell in row if cell.strip())
        for row in rows[:5]
        if any(cell.strip() for cell in row)
    ]
    preview = "; ".join(preview_rows) or "no nonblank rows"
    missing_list = ", ".join(sorted(required_fields))
    raise ValueError(f"CSV header row not found. Required fields: {missing_list}. Saw: {preview}")


def _empty_to_none(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    if not stripped or stripped.upper() == "N/A":
        return None
    return stripped


def _member_text_or_none(value: str | None, *, field_name: str | None = None) -> str | None:
    stripped = _empty_to_none(value)
    if stripped and " ".join(stripped.lower().split()) in {
        "address, city ca",
        "address, city ca 12345",
    }:
        return None
    if field_name == "address" and stripped and stripped.lower() == "address":
        return None
    if field_name == "city" and stripped and stripped.lower() == "city":
        return None
    return stripped


def _split_first_name_nickname(first_name: str) -> tuple[str, str | None]:
    match = re.match(r"^(?P<first_name>.*?)\s*\((?P<nickname>[^()]*)\)\s*$", first_name)
    if not match:
        return first_name.strip(), None

    parsed_first_name = match.group("first_name").strip()
    if not parsed_first_name:
        return first_name.strip(), None

    nickname = match.group("nickname").strip()
    return parsed_first_name, nickname or None


def _is_placeholder_address_set(
    address: str | None,
    address2: str | None,
    city: str | None,
    state: str | None,
    zip_code: str | None,
) -> bool:
    if address2:
        return False
    address_text = " ".join((address or "").lower().split())
    city_state_zip = " ".join(
        part for part in ((city or "").strip(), (state or "").strip(), (zip_code or "").strip()) if part
    )
    city_state_zip_text = " ".join(city_state_zip.lower().split())
    return address_text == "address" and city_state_zip_text in {
        "city ca",
        "city ca 12345",
    }


def normalize_card_number(value: str) -> str:
    '''Strip spreadsheet text markers and whitespace from exported card numbers.'''
    return value.strip().strip("'").strip()


def parse_report_date(value: str) -> date:
    '''Parse dates used by club reports.'''
    stripped = value.strip()
    for date_format in ("%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(stripped, date_format).date()
        except ValueError:
            pass
    raise ValueError(f"Unsupported report date: {value}")


def parse_report_datetime(report_date: date, value: str | None) -> datetime | None:
    stripped = _empty_to_none(value)
    if not stripped:
        return None

    for time_format in ("%I:%M:%S %p", "%H:%M:%S", "%I:%M %p", "%H:%M"):
        try:
            parsed_time = datetime.strptime(stripped, time_format).time()
            return datetime.combine(report_date, parsed_time)
        except ValueError:
            pass

    raise ValueError(f"Unsupported report time: {value}")


def parse_optional_int(value: str | None) -> int | None:
    stripped = _empty_to_none(value)
    return int(stripped) if stripped else None


def _member_from_csv_row(row: dict[str, object], row_number: int) -> Member:
    normalized = _normalized_csv_row(row, HEADER_MAP)
    first_name, nickname = _split_first_name_nickname(normalized.get("first_name", ""))
    normalized["first_name"] = first_name

    missing_fields = [
        field_name
        for field_name in sorted(REQUIRED_FIELDS)
        if not normalized.get(field_name)
    ]
    if missing_fields:
        missing_list = ", ".join(missing_fields)
        raise ValueError(f"Row {row_number} is missing required field(s): {missing_list}")

    raw_address = _member_text_or_none(normalized.get("address"))
    raw_city = _member_text_or_none(normalized.get("city"))
    address = _member_text_or_none(normalized.get("address"), field_name="address")
    address2 = _member_text_or_none(normalized.get("address2"))
    city = _member_text_or_none(normalized.get("city"), field_name="city")
    state = _member_text_or_none(normalized.get("state"))
    zip_code = _member_text_or_none(normalized.get("zip"))
    if _is_placeholder_address_set(raw_address, address2, raw_city, state, zip_code):
        address = None
        city = None
        state = None
        zip_code = None

    return Member(
        last_name=normalized["last_name"],
        first_name=normalized["first_name"],
        nickname=nickname,
        card_number=normalize_card_number(normalized["card_number"]),
        membership=normalized["membership"],
        address=address,
        address2=address2,
        city=city,
        state=state,
        zip=zip_code,
        phone=_member_text_or_none(normalized.get("phone")),
        email=_member_text_or_none(normalized.get("email")),
        work_phone=_member_text_or_none(normalized.get("work_phone")),
        cell_phone=_member_text_or_none(normalized.get("cell_phone")),
    )


def _csv_value_has_content(value: object) -> bool:
    if value is None:
        return False
    if isinstance(value, list):
        return any(_csv_value_has_content(item) for item in value)
    return bool(str(value).strip())


def _normalized_csv_row(row: dict[str, object], header_map: dict[str, str]) -> dict[str, str]:
    normalized: dict[str, str] = {}
    for header, value in row.items():
        if header is None or value is None or isinstance(value, list):
            continue
        target_field = header_map.get(_normalized_header(header))
        if target_field:
            normalized[target_field] = str(value).strip()
    return normalized


def _checkin_from_csv_row(row: dict[str, object], row_number: int) -> CheckIn:
    normalized = _normalized_csv_row(row, CHECKIN_HEADER_MAP)
    first_name, _nickname = _split_first_name_nickname(normalized.get("first_name", ""))
    normalized["first_name"] = first_name

    missing_fields = [
        field_name
        for field_name in sorted(CHECKIN_REQUIRED_FIELDS)
        if not normalized.get(field_name)
    ]
    if missing_fields:
        missing_list = ", ".join(missing_fields)
        raise ValueError(f"Row {row_number} is missing required field(s): {missing_list}")

    check_in_date = parse_report_date(normalized["check_in_date"])
    check_in_at = parse_report_datetime(check_in_date, normalized["check_in_time"])
    if check_in_at is None:
        raise ValueError(f"Row {row_number} is missing check-in time.")

    return CheckIn(
        member_id=_empty_to_none(normalized.get("member_id")),
        last_name=normalized["last_name"],
        first_name=normalized["first_name"],
        card_number=normalize_card_number(normalized["card_number"]),
        check_in_at=check_in_at,
        check_out_at=parse_report_datetime(check_in_date, normalized.get("check_out_time")),
        total_checkins=parse_optional_int(normalized.get("total_checkins")),
        duration=_empty_to_none(normalized.get("duration")),
        membership=normalized["membership"],
    )


def _read_csv_rows(
    source: TextIO,
    header_map: dict[str, str],
    required_fields: set[str],
) -> tuple[csv.DictReader, int]:
    csv_lines, skipped_leading_rows = _lines_after_leading_blank_rows(source)
    if not csv_lines:
        raise ValueError("CSV file does not contain a header row.")

    rows = list(csv.reader(csv_lines))
    header_index, fieldnames = _find_header_row(rows, header_map, required_fields)
    data_lines = csv_lines[header_index + 1:]
    reader = csv.DictReader(data_lines, fieldnames=fieldnames)
    return reader, skipped_leading_rows + header_index + 2


def read_members_csv(source: TextIO) -> list[Member]:
    '''Read members from a CSV file object.'''
    reader, first_data_row_number = _read_csv_rows(source, HEADER_MAP, REQUIRED_FIELDS)
    members: list[Member] = []
    for row_number, row in enumerate(reader, start=first_data_row_number):
        if not any(_csv_value_has_content(value) for value in row.values()):
            continue
        members.append(_member_from_csv_row(row, row_number))
    return members


def read_members_csv_path(path: Path) -> list[Member]:
    '''Read members from a CSV path.'''
    with path.open(newline="", encoding="utf-8-sig") as source:
        return read_members_csv(source)


def read_checkins_csv(source: TextIO) -> list[CheckIn]:
    '''Read check-ins from a CSV file object.'''
    reader, first_data_row_number = _read_csv_rows(source, CHECKIN_HEADER_MAP, CHECKIN_REQUIRED_FIELDS)
    checkins: list[CheckIn] = []
    for row_number, row in enumerate(reader, start=first_data_row_number):
        if not any(_csv_value_has_content(value) for value in row.values()):
            continue
        checkins.append(_checkin_from_csv_row(row, row_number))
    return checkins


def read_checkins_csv_path(path: Path) -> list[CheckIn]:
    '''Read check-ins from a CSV path.'''
    with path.open(newline="", encoding="utf-8-sig") as source:
        return read_checkins_csv(source)
