'''Persistence operations for user notes.'''

import sqlite3
from datetime import datetime

from club_admin.models import UserNote


MAX_NOTE_SUMMARY_LENGTH = 120


def _note_from_row(row: sqlite3.Row) -> UserNote:
    return UserNote(
        id=row["id"],
        user_id=row["user_id"],
        summary=row["summary"],
        details=row["details"],
        created_at=datetime.fromisoformat(row["created_at"]),
        updated_at=(
            datetime.fromisoformat(row["updated_at"])
            if row["updated_at"] is not None
            else None
        ),
    )


def note_from_values(
    *,
    user_id: int,
    summary: str,
    details: str,
) -> UserNote:
    '''Return a validated note from form values.'''
    stripped_summary = " ".join(summary.split())
    if not stripped_summary:
        raise ValueError("Note summary is required.")
    if len(stripped_summary) > MAX_NOTE_SUMMARY_LENGTH:
        raise ValueError(f"Note summary must be {MAX_NOTE_SUMMARY_LENGTH} characters or fewer.")
    stripped_details = details.strip()
    return UserNote(
        user_id=user_id,
        summary=stripped_summary,
        details=stripped_details or None,
    )


def add_user_note(connection: sqlite3.Connection, note: UserNote) -> int:
    '''Insert a note and return its database ID.'''
    cursor = connection.execute(
        """
        INSERT INTO user_notes (
            user_id,
            summary,
            details
        )
        VALUES (?, ?, ?)
        """,
        (
            note.user_id,
            note.summary,
            note.details,
        ),
    )
    return int(cursor.lastrowid)


def list_user_notes(connection: sqlite3.Connection, user_id: int) -> list[UserNote]:
    '''Return notes for one user, newest first.'''
    rows = connection.execute(
        """
        SELECT
            id,
            user_id,
            summary,
            details,
            created_at,
            updated_at
        FROM user_notes
        WHERE user_id = ?
        ORDER BY created_at DESC, id DESC
        """,
        (user_id,),
    ).fetchall()
    return [_note_from_row(row) for row in rows]


def get_user_note(
    connection: sqlite3.Connection,
    *,
    note_id: int,
    user_id: int,
) -> UserNote | None:
    '''Return one note owned by one user, if it exists.'''
    row = connection.execute(
        """
        SELECT
            id,
            user_id,
            summary,
            details,
            created_at,
            updated_at
        FROM user_notes
        WHERE id = ? AND user_id = ?
        """,
        (note_id, user_id),
    ).fetchone()
    return _note_from_row(row) if row is not None else None


def update_user_note(
    connection: sqlite3.Connection,
    note: UserNote,
) -> bool:
    '''Update a note owned by one user.'''
    cursor = connection.execute(
        """
        UPDATE user_notes
        SET summary = ?,
            details = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = ? AND user_id = ?
        """,
        (
            note.summary,
            note.details,
            note.id,
            note.user_id,
        ),
    )
    return cursor.rowcount > 0


def delete_user_note(
    connection: sqlite3.Connection,
    *,
    note_id: int,
    user_id: int,
) -> bool:
    '''Delete a note owned by one user.'''
    cursor = connection.execute(
        """
        DELETE FROM user_notes
        WHERE id = ? AND user_id = ?
        """,
        (note_id, user_id),
    )
    return cursor.rowcount > 0
