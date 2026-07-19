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
    member_since TEXT CHECK (member_since IS NULL OR member_since GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'),
    date_of_birth TEXT CHECK (date_of_birth IS NULL OR date_of_birth GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'),
    address TEXT,
    address2 TEXT,
    city TEXT,
    state TEXT,
    zip TEXT,
    mailing_address TEXT,
    mailing_address2 TEXT,
    mailing_city TEXT,
    mailing_state TEXT,
    mailing_zip TEXT,
    phone TEXT,
    email TEXT,
    work_phone TEXT,
    cell_phone TEXT,
    screening_status TEXT CHECK (
        screening_status IS NULL
        OR screening_status IN ('pending', 'safe', 'banned')
    ),
    gender TEXT,
    occupation TEXT,
    driver_license_number TEXT,
    driver_license_state TEXT,
    driver_license_expires TEXT CHECK (
        driver_license_expires IS NULL
        OR driver_license_expires GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'
    ),
    emergency_contact_name TEXT,
    emergency_contact_relationship TEXT,
    emergency_contact_phone TEXT,
    aanr_number TEXT,
    other_club_name TEXT,
    imported_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS ix_users_name
ON users (last_name, first_name);

CREATE TABLE IF NOT EXISTS guest_registrations (
    id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL UNIQUE REFERENCES users(id) ON DELETE RESTRICT,
    visit_date TEXT NOT NULL CHECK (visit_date GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'),
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

CREATE TABLE IF NOT EXISTS membership_applications (
    id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    requested_membership TEXT NOT NULL CHECK (requested_membership IN ('Full Member', 'Associate Member')),
    gender TEXT,
    occupation TEXT,
    driver_license_number TEXT,
    driver_license_state TEXT,
    driver_license_expires TEXT CHECK (
        driver_license_expires IS NULL
        OR driver_license_expires GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'
    ),
    mailing_address TEXT,
    mailing_address2 TEXT,
    mailing_city TEXT,
    mailing_state TEXT,
    mailing_zip TEXT,
    club_news_name_permission INTEGER CHECK (club_news_name_permission IS NULL OR club_news_name_permission IN (0, 1)),
    emergency_contact_name TEXT,
    emergency_contact_relationship TEXT,
    emergency_contact_phone TEXT,
    minor_children TEXT,
    convicted INTEGER CHECK (convicted IS NULL OR convicted IN (0, 1)),
    conviction_explanation TEXT,
    social_nudity_practiced INTEGER CHECK (social_nudity_practiced IS NULL OR social_nudity_practiced IN (0, 1)),
    social_nudity_duration TEXT,
    social_nudity_experience TEXT,
    aanr_member INTEGER CHECK (aanr_member IS NULL OR aanr_member IN (0, 1)),
    aanr_number TEXT,
    aanr_expires TEXT CHECK (
        aanr_expires IS NULL
        OR aanr_expires GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'
    ),
    other_club_member INTEGER CHECK (other_club_member IS NULL OR other_club_member IN (0, 1)),
    other_club_name TEXT,
    agreement_accepted INTEGER NOT NULL DEFAULT 0 CHECK (agreement_accepted IN (0, 1)),
    signed_at TEXT CHECK (
        signed_at IS NULL
        OR signed_at GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'
    ),
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'approved', 'declined', 'withdrawn')),
    application_fee_received_at TEXT CHECK (
        application_fee_received_at IS NULL
        OR application_fee_received_at GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'
    ),
    reviewed_at TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS ix_membership_applications_user_created
ON membership_applications (user_id, created_at);

CREATE INDEX IF NOT EXISTS ix_membership_applications_status_created
ON membership_applications (status, created_at);

CREATE UNIQUE INDEX IF NOT EXISTS ux_membership_applications_pending_user
ON membership_applications (user_id)
WHERE status = 'pending';

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

CREATE INDEX IF NOT EXISTS ix_audit_log_changed_at
ON audit_log (changed_at, id);

CREATE TABLE IF NOT EXISTS user_notes (
    id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    summary TEXT NOT NULL CHECK (length(trim(summary)) > 0 AND length(summary) <= 120),
    details TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT
);

CREATE INDEX IF NOT EXISTS ix_user_notes_user_created
ON user_notes (user_id, created_at);

CREATE TABLE IF NOT EXISTS app_settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

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


def _column_names(connection: sqlite3.Connection, table_name: str) -> set[str]:
    return {
        row["name"]
        for row in connection.execute(f"PRAGMA table_info({table_name})").fetchall()
    }


def _ensure_user_date_columns(connection: sqlite3.Connection) -> None:
    columns = _column_names(connection, "users")
    if "member_since" not in columns:
        connection.execute(
            """
            ALTER TABLE users
            ADD COLUMN member_since TEXT
            CHECK (
                member_since IS NULL
                OR member_since GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'
            )
            """
        )
    if "date_of_birth" not in columns:
        connection.execute(
            """
            ALTER TABLE users
            ADD COLUMN date_of_birth TEXT
            CHECK (
                date_of_birth IS NULL
                OR date_of_birth GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'
            )
            """
        )


def _ensure_user_screening_status_column(connection: sqlite3.Connection) -> None:
    columns = _column_names(connection, "users")
    if "screening_status" not in columns:
        connection.execute(
            """
            ALTER TABLE users
            ADD COLUMN screening_status TEXT
            CHECK (
                screening_status IS NULL
                OR screening_status IN ('pending', 'safe', 'banned')
            )
            """
        )
    if "safe" in columns:
        connection.execute(
            """
            UPDATE users
            SET screening_status = 'safe'
            WHERE screening_status IS NULL AND safe = 1
            """
        )


def _ensure_user_membership_profile_columns(connection: sqlite3.Connection) -> None:
    columns = _column_names(connection, "users")
    text_columns = (
        "gender",
        "occupation",
        "driver_license_number",
        "driver_license_state",
        "emergency_contact_name",
        "emergency_contact_relationship",
        "emergency_contact_phone",
        "aanr_number",
        "other_club_name",
        "mailing_address",
        "mailing_address2",
        "mailing_city",
        "mailing_state",
        "mailing_zip",
    )
    for column_name in text_columns:
        if column_name not in columns:
            connection.execute(f"ALTER TABLE users ADD COLUMN {column_name} TEXT")
    if "driver_license_expires" not in columns:
        connection.execute(
            """
            ALTER TABLE users
            ADD COLUMN driver_license_expires TEXT
            CHECK (
                driver_license_expires IS NULL
                OR driver_license_expires GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'
            )
            """
        )


def _ensure_membership_application_columns(connection: sqlite3.Connection) -> None:
    if not _table_exists(connection, "membership_applications"):
        return
    columns = _column_names(connection, "membership_applications")
    text_columns = (
        "mailing_address",
        "mailing_address2",
        "mailing_city",
        "mailing_state",
        "mailing_zip",
        "social_nudity_duration",
    )
    for column_name in text_columns:
        if column_name not in columns:
            connection.execute(
                f"ALTER TABLE membership_applications ADD COLUMN {column_name} TEXT"
            )
    integer_columns = (
        "social_nudity_practiced",
        "aanr_member",
        "other_club_member",
    )
    for column_name in integer_columns:
        if column_name not in columns:
            connection.execute(
                f"ALTER TABLE membership_applications ADD COLUMN {column_name} INTEGER"
            )
    if "aanr_expires" not in columns:
        connection.execute(
            """
            ALTER TABLE membership_applications
            ADD COLUMN aanr_expires TEXT
            CHECK (
                aanr_expires IS NULL
                OR aanr_expires GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'
            )
            """
        )


def _drop_guest_registration_middle_name_column(connection: sqlite3.Connection) -> None:
    if not _table_exists(connection, "guest_registrations"):
        return
    columns = _column_names(connection, "guest_registrations")
    if "middle_name" in columns:
        connection.execute("ALTER TABLE guest_registrations DROP COLUMN middle_name")


def init_db(db_path: Path | None = None) -> None:
    '''Create club-user tables if they do not exist.'''
    connection = connect(db_path)
    try:
        _validate_existing_schema(connection)
        connection.executescript(SCHEMA_SQL)
        _ensure_user_date_columns(connection)
        _ensure_user_screening_status_column(connection)
        _ensure_user_membership_profile_columns(connection)
        _ensure_membership_application_columns(connection)
        _drop_guest_registration_middle_name_column(connection)
        _validate_foreign_keys(connection)
        connection.commit()
    finally:
        connection.close()
