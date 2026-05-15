'''Persistence operations for ZIP coordinate centroids.'''

import csv
import sqlite3
from dataclasses import dataclass
from typing import TextIO


@dataclass(frozen=True, kw_only=True)
class ZipCoordinate:
    zip_code: str
    latitude: float
    longitude: float


_ZIP_HEADERS = {"zip", "zipcode", "zip code", "postal code", "postal_code"}
_LATITUDE_HEADERS = {"lat", "latitude"}
_LONGITUDE_HEADERS = {"lon", "lng", "long", "longitude"}


def list_zip_coordinates(connection: sqlite3.Connection) -> dict[str, tuple[float, float]]:
    rows = connection.execute(
        """
        SELECT zip, latitude, longitude
        FROM zip_coordinates
        ORDER BY zip
        """
    ).fetchall()
    return {
        row["zip"]: (float(row["latitude"]), float(row["longitude"]))
        for row in rows
    }


def upsert_zip_coordinates(
    connection: sqlite3.Connection,
    coordinates: list[ZipCoordinate],
) -> int:
    for coordinate in coordinates:
        connection.execute(
            """
            INSERT INTO zip_coordinates (zip, latitude, longitude, updated_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(zip) DO UPDATE SET
                latitude = excluded.latitude,
                longitude = excluded.longitude,
                updated_at = CURRENT_TIMESTAMP
            """,
            (coordinate.zip_code, coordinate.latitude, coordinate.longitude),
        )
    return len(coordinates)


def read_zip_coordinates_csv(source: TextIO) -> list[ZipCoordinate]:
    reader = csv.DictReader(source)
    if not reader.fieldnames:
        raise ValueError("ZIP coordinate CSV is missing a header row.")

    field_map = _coordinate_field_map(reader.fieldnames)
    coordinates: list[ZipCoordinate] = []
    for row_number, row in enumerate(reader, start=2):
        if not any((value or "").strip() for value in row.values()):
            continue
        zip_code = _normalized_zip(row.get(field_map["zip"], ""))
        latitude = _parsed_float(row.get(field_map["latitude"], ""), "latitude", row_number)
        longitude = _parsed_float(row.get(field_map["longitude"], ""), "longitude", row_number)
        coordinates.append(
            ZipCoordinate(
                zip_code=zip_code,
                latitude=latitude,
                longitude=longitude,
            )
        )
    return coordinates


def coordinate_from_values(
    zip_code: object,
    latitude: object,
    longitude: object,
) -> ZipCoordinate:
    return ZipCoordinate(
        zip_code=_normalized_zip(zip_code),
        latitude=_parsed_float(latitude, "latitude", 1),
        longitude=_parsed_float(longitude, "longitude", 1),
    )


def _coordinate_field_map(fieldnames: list[str]) -> dict[str, str]:
    normalized = {_normalized_header(fieldname): fieldname for fieldname in fieldnames}
    zip_field = _first_matching_field(normalized, _ZIP_HEADERS)
    latitude_field = _first_matching_field(normalized, _LATITUDE_HEADERS)
    longitude_field = _first_matching_field(normalized, _LONGITUDE_HEADERS)
    missing = [
        label
        for label, field in (
            ("zip", zip_field),
            ("latitude", latitude_field),
            ("longitude", longitude_field),
        )
        if field is None
    ]
    if missing:
        raise ValueError(
            "ZIP coordinate CSV is missing required column(s): "
            + ", ".join(missing)
        )
    return {
        "zip": zip_field,
        "latitude": latitude_field,
        "longitude": longitude_field,
    }


def _first_matching_field(normalized: dict[str, str], candidates: set[str]) -> str | None:
    for candidate in candidates:
        field = normalized.get(candidate)
        if field is not None:
            return field
    return None


def _normalized_header(value: str) -> str:
    return " ".join(value.strip().replace("_", " ").lower().split())


def _normalized_zip(value: object) -> str:
    zip_code = "".join(character for character in str(value).strip() if character.isdigit())
    if len(zip_code) < 5:
        raise ValueError(f"ZIP code must contain 5 digits: {value!r}")
    return zip_code[:5]


def _parsed_float(value: object, label: str, row_number: int) -> float:
    try:
        parsed = float(str(value).strip())
    except ValueError as error:
        raise ValueError(f"Row {row_number} has invalid {label}: {value!r}") from error
    if label == "latitude" and not -90 <= parsed <= 90:
        raise ValueError(f"Row {row_number} latitude is out of range: {parsed}")
    if label == "longitude" and not -180 <= parsed <= 180:
        raise ValueError(f"Row {row_number} longitude is out of range: {parsed}")
    return parsed
