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

    @bp.route("/members/check-in-with-guests", methods=["GET", "POST"])
    def admin_checkin():
        # Protected by the app's admin gate; independent of public kiosk identity.
        session.setdefault("visit_guest_admin_token", token_urlsafe(24))
        token = session["visit_guest_admin_token"]
        day = today()
        host_id_text = request.values.get("host_id", "")
        selected_ids = set(request.form.getlist("guest_ids"))
        message = ""
        with closing(connection()) as conn:
            if request.method == "POST":
                if not compare_digest(request.form.get("token", ""), token):
                    abort(400)
                try:
                    conn.execute("BEGIN IMMEDIATE")
                    if request.form.get("visit_date") != day.isoformat():
                        raise ValueError("The date has changed. Review the group and submit again for today.")
                    try:
                        host_id = int(host_id_text)
                        guest_ids = sorted({int(value) for value in selected_ids})
                    except ValueError:
                        raise ValueError("Choose a member host and valid guests.") from None
                    if not guest_ids:
                        raise ValueError("Select at least one guest.")
                    host = member_repository.get_member(conn, host_id)
                    if not eligible_host(host):
                        raise ValueError("Choose a Full or Associate member who is eligible to check in.")
                    if host_id in guest_ids:
                        raise ValueError("The member host cannot also be their own guest.")
                    guests = []
                    for guest_id in guest_ids:
                        guest = member_repository.get_member(conn, guest_id)
                        if guest is None:
                            raise ValueError("A selected guest no longer exists. Review the guest list.")
                        if guest.screening_status == "banned":
                            raise ValueError(f"{guest.first_name} {guest.last_name} is banned and cannot check in.")
                        link = repository.guest_link(conn, guest_id, day)
                        if link and link["host_user_id"] != host_id:
                            raise ValueError(f"{guest.first_name} {guest.last_name} already has another host today. Correct the guest link before checking in this group.")
                        guests.append(guest)
                    # Check-ins and links commit together. Repeat submissions reuse today's records.
                    for person in [host, *guests]:
                        result = record_checkin(conn, person, same_day=True)
                        if result.blocked:
                            raise ValueError("A selected user cannot check in. No changes were saved.")
                        if result.recorded:
                            record_checkin_change(conn, member_id=person.id, field_name="check-in added",
                                                  old_value=None, new_value=result.check_in_at)
                    for guest in guests:
                        repository.save_link(conn, host_id, guest.id, day)
                    conn.commit()
                    checkin_events.notify_checkins_changed()
                    count = len(guests)
                    return redirect(url_for("members", checked_in=(
                        f"{host.first_name} {host.last_name} and {count} "
                        f"{'guest are' if count == 1 else 'guests are'} checked in today. Guest links saved."
                    )))
                except ValueError as exc:
                    conn.rollback()
                    message = str(exc) + " No check-ins or links were changed."
            users = member_repository.list_members(conn)
            selected_host = next((user for user in users if str(user.id) == host_id_text and eligible_host(user)), None)
            if selected_host is None and request.method == "GET":
                return redirect(url_for("members"))
            checked_ids = {row[0] for row in conn.execute(
                "SELECT DISTINCT user_id FROM checkins WHERE check_in_at >= ? AND check_in_at < ?",
                (f"{day.isoformat()}T00:00:00", f"{(day + timedelta(days=1)).isoformat()}T00:00:00"))}
        return render_template("club_admin/admin_group_checkin.html", users=users, host=selected_host,
                               host_id=host_id_text, selected_ids=selected_ids, checked_ids=checked_ids,
                               day=day, token=token, message=message)

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
        # All-time history; dates are edited on individual links, never used as a filter.
        session.setdefault("visit_guest_admin_token", token_urlsafe(24))
        token = session["visit_guest_admin_token"]
        message = ""
        with closing(connection()) as conn:
            member = member_repository.get_member(conn, member_id)
            if member is None:
                abort(404)
            link_id = request.form.get("link_id") if request.method == "POST" else request.args.get("edit")
            editing = None
            if link_id:
                editing = conn.execute("SELECT * FROM visit_guest_links WHERE id = ? AND (host_user_id = ? OR guest_user_id = ?)",
                                       (link_id, member_id, member_id)).fetchone()
                if editing is None:
                    abort(404)
            form_date = editing["visit_date"] if editing else today().isoformat()
            host_id_text = str(editing["host_user_id"]) if editing else (str(member_id) if member.membership in HOST_MEMBERSHIPS else "")
            guest_id_text = str(editing["guest_user_id"]) if editing else (str(member_id) if member.membership not in HOST_MEMBERSHIPS else "")
            if request.method == "POST":
                if not compare_digest(request.form.get("token", ""), token):
                    abort(400)
                form_date = request.form.get("visit_date", form_date)
                host_id_text = request.form.get("host_id", host_id_text)
                guest_id_text = request.form.get("guest_id", guest_id_text)
                try:
                    conn.execute("BEGIN IMMEDIATE")
                    # Re-read under the write lock before modifying a specific link.
                    if editing is not None:
                        editing = conn.execute("SELECT * FROM visit_guest_links WHERE id = ? AND (host_user_id = ? OR guest_user_id = ?)",
                                               (link_id, member_id, member_id)).fetchone()
                        if editing is None:
                            abort(404)
                    if request.form.get("action") == "remove":
                        if editing is None:
                            abort(400)
                        repository.delete_link(conn, editing)
                    elif request.form.get("action") == "save":
                        try:
                            day = date.fromisoformat(form_date)
                        except ValueError:
                            raise ValueError("Enter a valid visit date.") from None
                        try:
                            host_id = int(host_id_text)
                            guest_id = int(guest_id_text)
                        except ValueError:
                            raise ValueError("Choose a member host and a guest.") from None
                        host = member_repository.get_member(conn, host_id)
                        guest = member_repository.get_member(conn, guest_id)
                        if host is None or guest is None or host_id == guest_id:
                            raise ValueError("Choose a member host and a different guest.")
                        if host.membership not in HOST_MEMBERSHIPS and (editing is None or editing["host_user_id"] != host_id):
                            raise ValueError("Choose a Full or Associate member as host.")
                        if editing is None:
                            if member_id not in (host_id, guest_id):
                                raise ValueError("The new link must include this user as host or guest.")
                            existing = repository.guest_link(conn, guest_id, day)
                            if existing is not None:
                                raise ValueError("This guest already has a link on the selected date. Edit that link instead.")
                            repository.save_link(conn, host_id, guest_id, day)
                        else:
                            repository.update_link(conn, editing, host_id, guest_id, day)
                    else:
                        abort(400)
                    conn.commit()
                    checkin_events.notify_checkins_changed()
                    return redirect(url_for("visit_guests.admin", member_id=member_id))
                except ValueError as exc:
                    conn.rollback()
                    message = str(exc)
            links = [r for r in repository.list_links(conn, date.min, date.max)
                     if member_id in (r["host_user_id"], r["guest_user_id"])]
            users = member_repository.list_members(conn)
            hosts = [user for user in users if user.membership in HOST_MEMBERSHIPS
                     or (editing is not None and user.id == editing["host_user_id"])]
        return render_template("club_admin/visit_guests_admin.html", member=member, users=users, hosts=hosts,
                               links=links, editing=editing, form_date=form_date, host_id=host_id_text,
                               guest_id=guest_id_text, token=token, message=message)

    app.register_blueprint(bp)
