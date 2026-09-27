"""Dated guest/host associations, independent of individual check-in rows."""

from datetime import date
import sqlite3

from club_admin import audit_repository


def list_links(connection: sqlite3.Connection, start: date, end: date):
    return connection.execute("""
        SELECT l.*, h.first_name AS host_first_name, h.last_name AS host_last_name,
               g.first_name AS guest_first_name, g.last_name AS guest_last_name
        FROM visit_guest_links l
        JOIN users h ON h.id = l.host_user_id
        JOIN users g ON g.id = l.guest_user_id
        WHERE l.visit_date BETWEEN ? AND ?
        ORDER BY l.visit_date DESC, g.first_name, g.last_name, l.id
    """, (start.isoformat(), end.isoformat())).fetchall()


def guest_link(connection, guest_id: int, day: date):
    return connection.execute(
        "SELECT * FROM visit_guest_links WHERE guest_user_id = ? AND visit_date = ?",
        (guest_id, day.isoformat()),
    ).fetchone()


def record_change(connection, guest_id, old_host, new_host, day):
    for user_id in {guest_id, old_host, new_host} - {None}:
        audit_repository.record_field_change(
            connection, entity_type="user", entity_id=user_id, action="edit",
            field_name=f"guest link {day.isoformat()}",
            old_value=f"guest {guest_id}, host {old_host}" if old_host else None,
            new_value=f"guest {guest_id}, host {new_host}" if new_host else None,
        )


def save_link(connection, host_id: int, guest_id: int, day: date, *, replace=False):
    existing = guest_link(connection, guest_id, day)
    if existing and existing["host_user_id"] == host_id:
        return
    if existing and not replace:
        raise ValueError("This guest already has another host today. Please see the front desk.")
    if existing:
        connection.execute("UPDATE visit_guest_links SET host_user_id = ? WHERE id = ?",
                           (host_id, existing["id"]))
    else:
        connection.execute("""INSERT INTO visit_guest_links
            (visit_date, host_user_id, guest_user_id) VALUES (?, ?, ?)""",
            (day.isoformat(), host_id, guest_id))
    record_change(connection, guest_id, existing["host_user_id"] if existing else None, host_id, day)


def delete_link(connection, link):
    connection.execute("DELETE FROM visit_guest_links WHERE id = ?", (link["id"],))
    record_change(connection, link["guest_user_id"], link["host_user_id"], None,
                  date.fromisoformat(link["visit_date"]))


def report_links(connection, start: date, end: date):
    result = {}
    for link in list_links(connection, start, end):
        for owner, other, role in (("host", "guest", "Guest"), ("guest", "host", "Guest of")):
            key = (link[f"{owner}_user_id"], link["visit_date"])
            result.setdefault(key, []).append({
                "user_id": link[f"{other}_user_id"], "role": role,
                "name": f'{link[f"{other}_first_name"]} {link[f"{other}_last_name"]}',
                "short_name": f'{link[f"{other}_first_name"]} {link[f"{other}_last_name"][:1]}.',
            })
    return result


def guests_by_member(connection: sqlite3.Connection, start: date, end: date):
    """Count guest-days, distinct people, and hosting days from dated links only."""
    hosts = {}
    all_guests = set()
    links = list_links(connection, start, end)
    for link in links:
        host = hosts.setdefault(link['host_user_id'], {
            'id': link['host_user_id'],
            'name': f"{link['host_first_name']} {link['host_last_name'][:1]}.",
            'sort_name': (link['host_last_name'].casefold(), link['host_first_name'].casefold()),
            'guest_visits': 0, 'guest_ids': set(), 'visits': {},
        })
        host['guest_visits'] += 1
        host['guest_ids'].add(link['guest_user_id'])
        all_guests.add(link['guest_user_id'])
        host['visits'].setdefault(link['visit_date'], []).append({
            'id': link['guest_user_id'],
            'name': f"{link['guest_first_name']} {link['guest_last_name'][:1]}.",
        })
    rows = []
    for host in hosts.values():
        host['different_guests'] = len(host.pop('guest_ids'))
        host['days_hosting'] = len(host['visits'])
        host['visits'] = [{'date': day, 'guests': guests} for day, guests in host['visits'].items()]
        rows.append(host)
    rows.sort(key=lambda row: (-row['guest_visits'], row['sort_name'], row['id']))
    return {'hosts': rows, 'guest_visits': len(links), 'different_guests': len(all_guests),
            'host_count': len(rows)}
