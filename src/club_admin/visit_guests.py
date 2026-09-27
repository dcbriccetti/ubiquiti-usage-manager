"""Shared-kiosk guest linking and staff corrections."""

from contextlib import closing
from datetime import date, datetime, timedelta
from pathlib import Path
from secrets import compare_digest, token_urlsafe
import time
from zoneinfo import ZoneInfo

from flask import Blueprint, abort, current_app, jsonify, redirect, render_template, request, session, url_for

from club_admin import checkin_events, database, member_repository
from club_admin import visit_guest_repository as repository

SESSION_KEY = "visit_guest_session"
IDLE_SECONDS = 300
HOST_MEMBERSHIPS = {"Full Member", "Associate Member"}
ZONE = ZoneInfo("America/Los_Angeles")


def today():
    return datetime.now(ZONE).date()


def display_name(member):
    return f"{member.first_name} {member.last_name[:1]}." if member.last_name else member.first_name


def eligible_host(member):
    return member is not None and member.membership in HOST_MEMBERSHIPS and member.screening_status != "banned"


def start_session(member):
    session.pop(SESSION_KEY, None)
    if eligible_host(member):
        session[SESSION_KEY] = {
            "host_id": member.id, "day": today().isoformat(), "pending": [],
            "token": token_urlsafe(24), "activity": time.time(),
        }


def checked_in(connection, user_id, day):
    return connection.execute(
        "SELECT 1 FROM checkins WHERE user_id = ? AND check_in_at >= ? AND check_in_at < ? LIMIT 1",
        (user_id, f"{day.isoformat()}T00:00:00", f"{(day + timedelta(days=1)).isoformat()}T00:00:00"),
    ).fetchone() is not None


def register_routes(app, record_checkin, record_checkin_change):
    bp = Blueprint("visit_guests", __name__)

    def connection():
        return database.connect(Path(current_app.config["CLUB_ADMIN_DB_PATH"]))

    @app.before_request
    def clear_guest_identity_on_navigation():
        if request.endpoint in {"index", "self_checkin", "guest_registration", "membership_application", "admin_logout"}:
            session.pop(SESSION_KEY, None)

    @bp.route("/self-checkin/guests", methods=["GET", "POST"])
    def manage():
        state = session.get(SESSION_KEY)
        if (not state or state.get("day") != today().isoformat()
                or time.time() - state.get("activity", 0) >= IDLE_SECONDS):
            session.pop(SESSION_KEY, None)
            if request.form.get("action") == "activity":
                return jsonify(expired=True), 401
            return redirect(url_for("self_checkin"))
        if request.method == "POST" and not compare_digest(request.form.get("token", ""), state["token"]):
            abort(400, "This guest list has expired. Please check in again.")
        day = date.fromisoformat(state["day"])
        message = ""
        complete = False
        with closing(connection()) as conn:
            host = member_repository.get_member(conn, state["host_id"])
            if not eligible_host(host) or not checked_in(conn, state["host_id"], day):
                session.pop(SESSION_KEY, None)
                return redirect(url_for("self_checkin"))
            state["activity"] = time.time()
            action = request.form.get("action") if request.method == "POST" else None
            if action == "activity":
                session[SESSION_KEY] = state
                return jsonify(ok=True)
            if action == "cancel":
                session.pop(SESSION_KEY, None)
                return redirect(url_for("self_checkin"))
            if action == "add":
                guest = member_repository.find_member_by_phone_and_initials(
                    conn, request.form.get("phone", ""), request.form.get("initials", ""))
                if guest is None:
                    message = "No unique match found. Check the phone number and initials or first name, or see the front desk."
                elif guest.id == host.id:
                    message = "You are already checked in as the host. Add a different guest."
                elif guest.screening_status == "banned":
                    message = "This guest needs to see the front desk."
                else:
                    existing = repository.guest_link(conn, guest.id, day)
                    if existing and existing["host_user_id"] != host.id:
                        message = "This guest already has another host today. Please see the front desk."
                    elif existing or guest.id in state["pending"]:
                        message = "Already in your guest list."
                    elif len(state["pending"]) >= 50:
                        message = "Please save this guest list before adding more guests."
                    else:
                        state["pending"].append(guest.id)
            elif action == "remove":
                state["pending"] = [i for i in state["pending"] if str(i) != request.form.get("guest_id")]
            elif action == "done":
                try:
                    conn.execute("BEGIN IMMEDIATE")
                    # Revalidate under the write lock before saving any part of the group.
                    host = member_repository.get_member(conn, state["host_id"])
                    if not eligible_host(host) or not checked_in(conn, host.id, day) or today() != day:
                        raise ValueError("Please check in again before adding guests.")
                    guests = []
                    for guest_id in state["pending"]:
                        guest = member_repository.get_member(conn, guest_id)
                        if guest is None or guest.id == host.id or guest.screening_status == "banned":
                            raise ValueError("A guest needs front desk help. No pending guests have been saved.")
                        existing = repository.guest_link(conn, guest_id, day)
                        if existing and existing["host_user_id"] != host.id:
                            raise ValueError(f"{display_name(guest)} already has another host today. Please see the front desk.")
                        guests.append(guest)
                    for guest in guests:
                        result = record_checkin(conn, guest, same_day=True)
                        if result.blocked:
                            raise ValueError("A guest needs front desk help. No pending guests have been saved.")
                        if result.recorded:
                            record_checkin_change(conn, member_id=guest.id, field_name="check-in added",
                                                  old_value=None, new_value=result.check_in_at)
                        repository.save_link(conn, host.id, guest.id, day)
                    conn.commit()
                except ValueError as exc:
                    conn.rollback()
                    message = str(exc)
                else:
                    state["pending"] = []
                    complete = True
                    checkin_events.notify_checkins_changed()
            elif action is not None:
                abort(400)
            session[SESSION_KEY] = state
            links = [link for link in repository.list_links(conn, day, day) if link["host_user_id"] == host.id]
            saved_ids = {link["guest_user_id"] for link in links}
            guests = []
            for guest_id, saved in [(link["guest_user_id"], True) for link in links] + [(i, False) for i in state["pending"] if i not in saved_ids]:
                guest = member_repository.get_member(conn, guest_id)
                if guest is not None:
                    guests.append({"id": guest.id, "name": display_name(guest), "saved": saved,
                                   "checked_in": checked_in(conn, guest.id, day)})
        return render_template("club_admin/visit_guests.html", host_name=display_name(host), guests=guests,
                               token=state["token"], message=message, complete=complete,
                               idle_seconds=IDLE_SECONDS)

    @bp.get("/guests/report")
    def report():
        # Uses the same April–October season as the existing check-in charts.
        from club_admin.app import _season_date_range

        current_day = today()
        season_start, season_end = _season_date_range(current_day.year)
        try:
            start = date.fromisoformat(request.args.get("start_date", season_start.isoformat()))
            end = date.fromisoformat(request.args.get("end_date", season_end.isoformat()))
        except ValueError:
            abort(400, "Date range must use YYYY-MM-DD dates.")
        if start > end:
            abort(400, "Start date must be on or before end date.")
        presets = [
            {"label": "Today", "start": current_day, "end": current_day},
            {"label": "This Month", "start": current_day.replace(day=1), "end": current_day},
            {"label": "This Season", "start": season_start, "end": season_end},
        ]
        with closing(connection()) as conn:
            summary = repository.guests_by_member(conn, start, end)
        return render_template("club_admin/guests_report.html", summary=summary,
                               start_date=start, end_date=end, presets=presets)

    @bp.route("/members/<int:member_id>/visit-guests", methods=["GET", "POST"])
    def admin(member_id):
        # The app's private-route gate requires an authenticated administrator.
        session.setdefault("visit_guest_admin_token", token_urlsafe(24))
        token = session["visit_guest_admin_token"]
        message = ""
        try:
            day = date.fromisoformat(request.values.get("visit_date", today().isoformat()))
        except ValueError:
            abort(400, "Enter a valid visit date.")
        with closing(connection()) as conn:
            member = member_repository.get_member(conn, member_id)
            if member is None:
                abort(404)
            if request.method == "POST":
                if not compare_digest(request.form.get("token", ""), token):
                    abort(400)
                try:
                    conn.execute("BEGIN IMMEDIATE")
                    if request.form.get("action") == "remove":
                        link = conn.execute("SELECT * FROM visit_guest_links WHERE id = ? AND visit_date = ? AND (host_user_id = ? OR guest_user_id = ?)",
                                            (request.form.get("link_id"), day.isoformat(), member_id, member_id)).fetchone()
                        if link is None:
                            abort(404)
                        repository.delete_link(conn, link)
                    elif request.form.get("action") == "save":
                        host_id = int(request.form.get("host_id", ""))
                        guest_id = int(request.form.get("guest_id", ""))
                        host = member_repository.get_member(conn, host_id)
                        guest = member_repository.get_member(conn, guest_id)
                        if member_id not in (host_id, guest_id) or not eligible_host(host) or guest is None or host_id == guest_id:
                            raise ValueError("Choose a member host and a different guest, including this user.")
                        repository.save_link(conn, host_id, guest_id, day, replace=True)
                    else:
                        abort(400)
                    conn.commit()
                    checkin_events.notify_checkins_changed()
                    return redirect(url_for("visit_guests.admin", member_id=member_id, visit_date=day.isoformat()))
                except ValueError as exc:
                    conn.rollback()
                    message = str(exc)
            links = [r for r in repository.list_links(conn, day, day) if member_id in (r["host_user_id"], r["guest_user_id"])]
            users = member_repository.list_members(conn)
        return render_template("club_admin/visit_guests_admin.html", member=member, users=users, links=links,
                               day=day, token=token, message=message, host_memberships=HOST_MEMBERSHIPS)

    app.register_blueprint(bp)
