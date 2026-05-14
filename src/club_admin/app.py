'''Flask app for club user management.'''

import io
import os
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from flask import Flask, abort, redirect, render_template, request, url_for

import config as cfg
from club_admin import audit_repository
from club_admin import checkin_repository
from club_admin import csv_import
from club_admin import database
from club_admin import member_repository
from club_admin.models import CheckIn, Member


EDITABLE_MEMBER_FIELDS = (
    "last_name",
    "first_name",
    "card_number",
    "membership",
    "address",
    "address2",
    "city",
    "state",
    "zip",
    "phone",
    "email",
    "work_phone",
    "cell_phone",
)


def _member_from_form(member_id: int, form_data: Any) -> Member:
    return Member(
        id=member_id,
        last_name=form_data.get("last_name", "").strip(),
        first_name=form_data.get("first_name", "").strip(),
        card_number=form_data.get("card_number", "").strip(),
        membership=form_data.get("membership", "").strip(),
        address=form_data.get("address", "").strip() or None,
        address2=form_data.get("address2", "").strip() or None,
        city=form_data.get("city", "").strip() or None,
        state=form_data.get("state", "").strip() or None,
        zip=form_data.get("zip", "").strip() or None,
        phone=form_data.get("phone", "").strip() or None,
        email=form_data.get("email", "").strip() or None,
        work_phone=form_data.get("work_phone", "").strip() or None,
        cell_phone=form_data.get("cell_phone", "").strip() or None,
    )


def create_app(db_path: Path | None = None) -> Flask:
    '''Create the club user management app.'''
    database.init_db(db_path)
    flask_app = Flask(
        __name__,
        template_folder="templates",
        static_folder="static",
    )
    flask_app.config["CLUB_ADMIN_DB_PATH"] = str(db_path or database.get_db_path())

    @flask_app.context_processor
    def inject_app_title() -> dict[str, str]:
        organization_name = str(cfg.USER_MANAGEMENT_ORGANIZATION_NAME).strip()
        return {
            "organization_name": organization_name,
            "app_title": f"{organization_name} User Management",
        }

    @contextmanager
    def open_connection() -> Iterator[sqlite3.Connection]:
        connection = database.connect(Path(flask_app.config["CLUB_ADMIN_DB_PATH"]))
        try:
            yield connection
        finally:
            connection.close()

    @flask_app.route("/")
    def index():
        return redirect(url_for("members"))

    @flask_app.route("/members")
    def members():
        with open_connection() as connection:
            roster = member_repository.list_members(connection)
        return render_template("club_admin/members.html", members=roster)

    @flask_app.route("/members/<int:member_id>")
    def member_detail(member_id: int):
        with open_connection() as connection:
            member = member_repository.get_member(connection, member_id)
            if member is None:
                abort(404)
            checkins = checkin_repository.list_checkins_for_user(connection, member_id)
            audit_entries = audit_repository.list_audit_log_for_entity(
                connection,
                entity_type="user",
                entity_id=member_id,
            )

        return render_template(
            "club_admin/member_detail.html",
            member=member,
            checkins=checkins,
            audit_entries=audit_entries,
        )

    @flask_app.route("/members/<int:member_id>/edit", methods=["GET", "POST"])
    def edit_member(member_id: int):
        with open_connection() as connection:
            member = member_repository.get_member(connection, member_id)
            if member is None:
                abort(404)

            if request.method == "POST":
                updated_member = _member_from_form(member_id, request.form)

                if not updated_member.last_name or not updated_member.first_name:
                    abort(400, "First and last name are required.")
                if not updated_member.card_number or not updated_member.membership:
                    abort(400, "Card number and membership are required.")

                member_repository.update_member(connection, updated_member)
                for field_name in EDITABLE_MEMBER_FIELDS:
                    old_value = getattr(member, field_name)
                    new_value = getattr(updated_member, field_name)
                    if old_value != new_value:
                        audit_repository.record_field_change(
                            connection,
                            entity_type="user",
                            entity_id=member_id,
                            action="edit",
                            field_name=field_name,
                            old_value=old_value,
                            new_value=new_value,
                        )
                connection.commit()
                return redirect(url_for("member_detail", member_id=member_id))

        return render_template("club_admin/member_edit.html", member=member)

    @flask_app.route("/checkins/report")
    def checkins_report():
        today = date.today()
        default_start_date = today
        start_date_raw = request.args.get("start_date", default_start_date.isoformat())
        end_date_raw = request.args.get("end_date", today.isoformat())
        try:
            start_date = date.fromisoformat(start_date_raw)
            end_date = date.fromisoformat(end_date_raw)
        except ValueError:
            abort(400, "Date range must use YYYY-MM-DD dates.")

        if start_date > end_date:
            abort(400, "Start date must be on or before end date.")

        with open_connection() as connection:
            summaries = checkin_repository.summarize_checkins_by_user(
                connection,
                start_date,
                end_date,
            )

        return render_template(
            "club_admin/checkins_report.html",
            summaries=summaries,
            start_date=start_date,
            end_date=end_date,
            total_checkins=sum(summary.checkin_count for summary in summaries),
        )

    @flask_app.route("/checkins/daily")
    def daily_checkins_report():
        today = date.today()
        start_date_raw = request.args.get("start_date", today.isoformat())
        end_date_raw = request.args.get("end_date", today.isoformat())
        try:
            start_date = date.fromisoformat(start_date_raw)
            end_date = date.fromisoformat(end_date_raw)
        except ValueError:
            abort(400, "Date range must use YYYY-MM-DD dates.")

        if start_date > end_date:
            abort(400, "Start date must be on or before end date.")

        with open_connection() as connection:
            checkins = checkin_repository.list_checkins_for_date_range(
                connection,
                start_date,
                end_date,
            )

        return render_template(
            "club_admin/daily_checkins_report.html",
            checkins=checkins,
            start_date=start_date,
            end_date=end_date,
        )

    @flask_app.route("/self-checkin", methods=["GET", "POST"])
    def self_checkin():
        message = ""
        if request.method == "POST":
            phone = request.form.get("phone", "").strip()
            initials = request.form.get("initials", "").strip()
            with open_connection() as connection:
                member = member_repository.find_member_by_phone_and_initials(
                    connection,
                    phone,
                    initials,
                )
                if member is not None:
                    checkin_repository.upsert_checkin(
                        connection,
                        CheckIn(
                            user_id=member.id,
                            member_id=str(member.id),
                            last_name=member.last_name,
                            first_name=member.first_name,
                            card_number=member.card_number,
                            check_in_at=datetime.now().replace(microsecond=0),
                            membership=member.membership,
                        ),
                    )
                    connection.commit()
            message = (
                "Check-in recorded."
                if member is not None
                else "No matching user was found. Please check your phone number and initials."
            )

        return render_template(
            "club_admin/self_checkin.html",
            message=message,
        )

    @flask_app.post("/members/import")
    def import_members():
        csv_file = request.files.get("members_csv")
        if csv_file is None or not csv_file.filename:
            return "CSV file is required.", 400

        stream = io.StringIO(csv_file.stream.read().decode("utf-8-sig"))
        members_to_import = csv_import.read_members_csv(stream)
        with open_connection() as connection:
            for member in members_to_import:
                member_repository.upsert_member(connection, member)
            connection.commit()
        return redirect(url_for("members"))

    @flask_app.post("/checkins/import")
    def import_checkins():
        csv_file = request.files.get("checkins_csv")
        if csv_file is None or not csv_file.filename:
            return "CSV file is required.", 400

        stream = io.StringIO(csv_file.stream.read().decode("utf-8-sig"))
        checkins_to_import = csv_import.read_checkins_csv(stream)
        with open_connection() as connection:
            for checkin in checkins_to_import:
                checkin_repository.upsert_checkin(connection, checkin)
            connection.commit()
        return redirect(url_for("members"))

    return flask_app


if __name__ == "__main__":
    app = create_app()
    flask_debug = os.getenv("FLASK_DEBUG", "").strip().lower() in {"1", "true", "yes", "on"}
    app.run(debug=flask_debug, use_reloader=flask_debug, host="127.0.0.1", port=5052)
