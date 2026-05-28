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


@dataclass(frozen=True, kw_only=True)
class RecentAuditLogEntry:
    '''One recent audit entry with optional user display data.'''

    entry: AuditLogEntry
    member_first_name: str | None
    member_last_name: str | None

    @property
    def has_member(self) -> bool:
        return self.member_first_name is not None or self.member_last_name is not None

    @property
    def member_display_name(self) -> str:
        name = " ".join(
            part
            for part in (self.member_first_name, self.member_last_name)
            if part
        ).strip()
        return name or f"{self.entry.entity_type} #{self.entry.entity_id}"


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


def list_recent_audit_log(
    connection: sqlite3.Connection,
    *,
    limit: int = 100,
) -> list[RecentAuditLogEntry]:
    '''Return recent visible audit entries across users, newest first.'''
    rows = connection.execute(
        """
        SELECT
            audit_log.id,
            audit_log.entity_type,
            audit_log.entity_id,
            audit_log.action,
            audit_log.field_name,
            audit_log.old_value,
            audit_log.new_value,
            audit_log.changed_at,
            users.first_name AS member_first_name,
            users.last_name AS member_last_name
        FROM audit_log
        LEFT JOIN users
            ON audit_log.entity_type = 'user'
            AND audit_log.entity_id = users.id
        WHERE audit_log.field_name NOT IN ('card_number', 'check-in added')
        ORDER BY audit_log.changed_at DESC, audit_log.id DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return [
        RecentAuditLogEntry(
            entry=_entry_from_row(row),
            member_first_name=row["member_first_name"],
            member_last_name=row["member_last_name"],
        )
        for row in rows
    ]
