'''Audit log persistence for club admin changes.'''

import sqlite3
from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, kw_only=True)
class AuditLogEntry:
    '''One recorded field-level change.'''

    entity_type: str
    entity_id: int
    action: str
    field_name: str
    old_value: str | None
    new_value: str | None
    changed_at: datetime
    id: int | None = None


def _entry_from_row(row: sqlite3.Row) -> AuditLogEntry:
    return AuditLogEntry(
        id=row["id"],
        entity_type=row["entity_type"],
        entity_id=row["entity_id"],
        action=row["action"],
        field_name=row["field_name"],
        old_value=row["old_value"],
        new_value=row["new_value"],
        changed_at=datetime.fromisoformat(row["changed_at"]),
    )


def record_field_change(
    connection: sqlite3.Connection,
    *,
    entity_type: str,
    entity_id: int,
    action: str,
    field_name: str,
    old_value: object,
    new_value: object,
) -> None:
    '''Record a field-level audit entry.'''
    connection.execute(
        """
        INSERT INTO audit_log (
            entity_type,
            entity_id,
            action,
            field_name,
            old_value,
            new_value
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            entity_type,
            entity_id,
            action,
            field_name,
            None if old_value is None else str(old_value),
            None if new_value is None else str(new_value),
        ),
    )


def list_audit_log_for_entity(
    connection: sqlite3.Connection,
    *,
    entity_type: str,
    entity_id: int,
) -> list[AuditLogEntry]:
    '''Return audit entries for one entity, newest first.'''
    rows = connection.execute(
        """
        SELECT
            id,
            entity_type,
            entity_id,
            action,
            field_name,
            old_value,
            new_value,
            changed_at
        FROM audit_log
        WHERE entity_type = ? AND entity_id = ?
        ORDER BY changed_at DESC, id DESC
        """,
        (entity_type, entity_id),
    ).fetchall()
    return [_entry_from_row(row) for row in rows]
