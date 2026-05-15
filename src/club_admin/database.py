'''SQLite setup for the club user management app.'''

import os
import sqlite3
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DB_PATH = PROJECT_ROOT / "data" / "club_users.db"

SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY,
    last_name TEXT NOT NULL,
    first_name TEXT NOT NULL,
    nickname TEXT,
    card_number TEXT NOT NULL UNIQUE,
    membership TEXT NOT NULL,
    address TEXT,
    address2 TEXT,
    city TEXT,
    state TEXT,
    zip TEXT,
    phone TEXT,
    email TEXT,
    work_phone TEXT,
    cell_phone TEXT,
    imported_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS ix_users_name
ON users (last_name, first_name);

CREATE TABLE IF NOT EXISTS guest_registrations (
    id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL UNIQUE REFERENCES users(id) ON DELETE RESTRICT,
    visit_date TEXT NOT NULL CHECK (visit_date GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'),
    middle_name TEXT,
    other_phone TEXT,
    other_phone_type TEXT CHECK (other_phone_type IS NULL OR other_phone_type IN ('home', 'work', 'other')),
    marital_status TEXT CHECK (marital_status IS NULL OR marital_status IN ('single', 'married', 'recognized_couple')),
    partner_name TEXT,
    guest_of_member INTEGER NOT NULL DEFAULT 0 CHECK (guest_of_member IN (0, 1)),
    member_name TEXT,
    heard_about TEXT,
    newsletter_opt_out INTEGER NOT NULL DEFAULT 0 CHECK (newsletter_opt_out IN (0, 1)),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS ix_guest_registrations_created_at
ON guest_registrations (created_at);

CREATE TABLE IF NOT EXISTS checkins (
    id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    member_id TEXT NOT NULL,
    last_name TEXT NOT NULL,
    first_name TEXT NOT NULL,
    card_number TEXT NOT NULL REFERENCES users(card_number) ON UPDATE CASCADE ON DELETE RESTRICT,
    check_in_at TEXT NOT NULL,
    check_out_at TEXT,
    total_checkins INTEGER,
    duration TEXT,
    membership TEXT NOT NULL,
    imported_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (member_id, card_number, check_in_at),
    CHECK (check_in_at GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9]'),
    CHECK (check_out_at IS NULL OR check_out_at GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9]')
);

CREATE INDEX IF NOT EXISTS ix_checkins_check_in_at
ON checkins (check_in_at);

CREATE INDEX IF NOT EXISTS ix_checkins_card_check_in_at
ON checkins (card_number, check_in_at);

CREATE TABLE IF NOT EXISTS audit_log (
    id INTEGER PRIMARY KEY,
    entity_type TEXT NOT NULL,
    entity_id INTEGER NOT NULL,
    action TEXT NOT NULL,
    field_name TEXT NOT NULL,
    old_value TEXT,
    new_value TEXT,
    changed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS ix_audit_log_entity
ON audit_log (entity_type, entity_id, changed_at);

CREATE TABLE IF NOT EXISTS zip_coordinates (
    zip TEXT PRIMARY KEY CHECK (zip GLOB '[0-9][0-9][0-9][0-9][0-9]'),
    latitude REAL NOT NULL CHECK (latitude >= -90 AND latitude <= 90),
    longitude REAL NOT NULL CHECK (longitude >= -180 AND longitude <= 180),
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"""


def get_db_path() -> Path:
    '''Return the configured club-user database path.'''
    configured_path = os.getenv("CLUB_ADMIN_DB_PATH", "").strip()
    return Path(configured_path).expanduser() if configured_path else DEFAULT_DB_PATH


def connect(db_path: Path | None = None) -> sqlite3.Connection:
    '''Open a SQLite connection with rows addressable by column name.'''
    resolved_path = db_path or get_db_path()
    resolved_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(resolved_path)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


def _table_exists(connection: sqlite3.Connection, table_name: str) -> bool:
    row = connection.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type = 'table' AND name = ?
        """,
        (table_name,),
    ).fetchone()
    return row is not None


def _validate_existing_schema(connection: sqlite3.Connection) -> None:
    dependent_tables = ("checkins", "audit_log")
    if any(_table_exists(connection, table_name) for table_name in dependent_tables):
        if not _table_exists(connection, "users"):
            raise RuntimeError(
                "Club user database is missing the users table while dependent "
                "tables still exist. Restore the database or delete it and recreate it."
            )


def _validate_foreign_keys(connection: sqlite3.Connection) -> None:
    violations = connection.execute("PRAGMA foreign_key_check").fetchall()
    if violations:
        first_violation = violations[0]
        raise RuntimeError(
            "Club user database failed foreign key validation: "
            f"table={first_violation[0]}, rowid={first_violation[1]}, "
            f"parent={first_violation[2]}, fkid={first_violation[3]}"
        )


def init_db(db_path: Path | None = None) -> None:
    '''Create club-user tables if they do not exist.'''
    connection = connect(db_path)
    try:
        _validate_existing_schema(connection)
        connection.executescript(SCHEMA_SQL)
        _validate_foreign_keys(connection)
        connection.commit()
    finally:
        connection.close()
