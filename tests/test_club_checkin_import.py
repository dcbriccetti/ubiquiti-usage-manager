import io
import sqlite3
import sys
import tempfile
from contextlib import closing
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from werkzeug.security import generate_password_hash


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from club_admin import checkin_repository
from club_admin import csv_import
from club_admin import database
from club_admin import member_repository
from club_admin.app import create_app
from club_admin.models import Member
import config as cfg


ADMIN_PASSWORD = "test-admin-password"


def create_admin_app(db_path: Path):
    with patch.object(
        cfg,
        "USER_MANAGEMENT_ADMIN_PASSWORD_HASH",
        generate_password_hash(ADMIN_PASSWORD),
    ):
        return create_app(db_path)


def admin_client(flask_app):
    client = flask_app.test_client()
    response = client.post("/admin/login", data={"password": ADMIN_PASSWORD})
    assert response.status_code == 302
    return client


CHECKINS_CSV = (
    "Member ID,Last Name,First Name,Card #,Check-in Date,Check-in Time,Check-out Time,Total Check-ins,Duration,Membership,Result,Date of Birth\n"
    "880,Doe,John,'1861',5/3/2026,3:59:20 PM,N/A,1,N/A,Visitor,Check-in OK\n"
    "35,Doe,Jane,'1024',5/3/2026,2:33:15 PM,N/A,1,N/A,Full Member,Check-in OK\n"
)

CHECKINS_RANGE_CSV = (
    "Member ID,Last Name,First Name,Card #,Check-in Date,Check-in Time,Check-out Time,Total Check-ins,Duration,Membership,Result,Date of Birth\n"
    "880,Doe,John,'1861',5/1/2026,9:00:00 AM,N/A,1,N/A,Visitor,Check-in OK\n"
    "880,Doe,John,'1861',5/3/2026,3:59:20 PM,N/A,1,N/A,Visitor,Check-in OK\n"
    "35,Doe,Jane,'1024',5/4/2026,2:33:15 PM,N/A,1,N/A,Full Member,Check-in OK\n"
)


class ClubCheckInImportTests(unittest.TestCase):
    def test_reads_checkins_csv_with_export_headers(self) -> None:
        checkins = csv_import.read_checkins_csv(io.StringIO(CHECKINS_CSV))

        self.assertEqual(len(checkins), 2)
        self.assertEqual(checkins[0].member_id, "880")
        self.assertEqual(checkins[0].card_number, "1861")
        self.assertEqual(checkins[0].check_in_at, datetime(2026, 5, 3, 15, 59, 20))
        self.assertIsNone(checkins[0].check_out_at)
        self.assertEqual(checkins[0].total_checkins, 1)
        self.assertIsNone(checkins[0].duration)

    def test_reads_checkins_csv_after_report_preamble(self) -> None:
        source = io.StringIO(
            "Check-in Detail Report\n"
            "Generated,5/4/2026\n"
            "\n"
            + CHECKINS_CSV
        )

        checkins = csv_import.read_checkins_csv(source)

        self.assertEqual(len(checkins), 2)
        self.assertEqual(checkins[1].first_name, "Jane")
        self.assertEqual(checkins[1].card_number, "1024")

    def test_reads_checkins_csv_with_merged_first_name_card_header(self) -> None:
        source = io.StringIO(
            "Member ID,Last Name,First NameCard #,Check-in Date,Check-in Time,Check-out Time,Total Check-ins,Duration,Membership,Result,Date of Birth\n"
            "880,Doe,John,'1861',5/3/2026,3:59:20 PM,N/A,1,N/A,Visitor,Check-in OK\n"
        )

        checkins = csv_import.read_checkins_csv(source)

        self.assertEqual(len(checkins), 1)
        self.assertEqual(checkins[0].first_name, "John")
        self.assertEqual(checkins[0].card_number, "1861")

    def test_reads_checkins_csv_strips_trailing_parenthesized_nickname(self) -> None:
        source = io.StringIO(
            "Member ID,Last Name,First Name,Card #,Check-in Date,Check-in Time,Check-out Time,Total Check-ins,Duration,Membership,Result,Date of Birth\n"
            "880,Doe,John (Johnny),'1861',5/3/2026,3:59:20 PM,N/A,1,N/A,Visitor,Check-in OK\n"
            "35,Roe,(none),'1024',5/3/2026,2:33:15 PM,N/A,1,N/A,Visitor,Check-in OK\n"
        )

        checkins = csv_import.read_checkins_csv(source)

        self.assertEqual(checkins[0].first_name, "John")
        self.assertEqual(checkins[1].first_name, "(none)")

    def test_checkins_table_is_created(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            database.init_db(db_path)
            with closing(database.connect(db_path)) as connection:
                table_names = {
                    row["name"]
                    for row in connection.execute(
                        "SELECT name FROM sqlite_master WHERE type = 'table'"
                    ).fetchall()
                }

        self.assertIn("checkins", table_names)

    def test_upsert_checkin_links_matching_user_by_card(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            database.init_db(db_path)
            checkin = csv_import.read_checkins_csv(io.StringIO(CHECKINS_CSV))[0]
            with closing(database.connect(db_path)) as connection:
                member_repository.upsert_member(
                    connection,
                    Member(
                        last_name="Doe",
                        first_name="John",
                        card_number="1861",
                        membership="Visitor",
                    ),
                )
                checkin_repository.upsert_checkin(connection, checkin)
                connection.commit()

                checkins = checkin_repository.list_checkins(connection)

        self.assertEqual(len(checkins), 1)
        self.assertIsNotNone(checkins[0].user_id)
        self.assertEqual(checkins[0].card_number, "1861")

    def test_upsert_checkin_creates_user_when_roster_is_not_imported(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            database.init_db(db_path)
            checkin = csv_import.read_checkins_csv(io.StringIO(CHECKINS_CSV))[0]
            with closing(database.connect(db_path)) as connection:
                checkin_repository.upsert_checkin(connection, checkin)
                connection.commit()

                users = member_repository.list_members(connection)
                checkins = checkin_repository.list_checkins(connection)

        self.assertEqual(len(users), 1)
        self.assertEqual(users[0].card_number, "1861")
        self.assertEqual(checkins[0].user_id, users[0].id)

    def test_checkins_reject_orphaned_user_reference(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            database.init_db(db_path)
            with closing(database.connect(db_path)) as connection:
                with self.assertRaises(sqlite3.IntegrityError):
                    connection.execute(
                        """
                        INSERT INTO checkins (
                            user_id,
                            member_id,
                            last_name,
                            first_name,
                            card_number,
                            check_in_at,
                            membership
                        )
                        VALUES (
                            999,
                            '880',
                            'Doe',
                            'John',
                            '1861',
                            '2026-05-03T15:59:20',
                            'Visitor'
                        )
                        """
                    )

    def test_app_connection_rejects_dropping_users_with_checkins(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            database.init_db(db_path)
            checkin = csv_import.read_checkins_csv(io.StringIO(CHECKINS_CSV))[0]
            with closing(database.connect(db_path)) as connection:
                checkin_repository.upsert_checkin(connection, checkin)
                connection.commit()

                with self.assertRaises(sqlite3.IntegrityError):
                    connection.execute("DROP TABLE users")
                connection.rollback()

    def test_init_db_rejects_database_missing_users_table(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            database.init_db(db_path)
            with closing(sqlite3.connect(db_path)) as raw_connection:
                raw_connection.execute("PRAGMA foreign_keys = OFF")
                raw_connection.execute("DROP TABLE users")
                raw_connection.commit()

            with self.assertRaises(RuntimeError):
                database.init_db(db_path)

    def test_checkins_table_does_not_store_result_or_date_of_birth(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            database.init_db(db_path)
            with closing(database.connect(db_path)) as connection:
                columns = {
                    row["name"]
                    for row in connection.execute("PRAGMA table_info(checkins)").fetchall()
                }

        self.assertNotIn("result", columns)
        self.assertNotIn("date_of_birth", columns)

    def test_summarizes_checkins_by_user_for_date_range(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            database.init_db(db_path)
            checkins_to_import = csv_import.read_checkins_csv(io.StringIO(CHECKINS_RANGE_CSV))
            with closing(database.connect(db_path)) as connection:
                for checkin in checkins_to_import:
                    checkin_repository.upsert_checkin(connection, checkin)
                connection.commit()

                summaries = checkin_repository.summarize_checkins_by_user(
                    connection,
                    datetime(2026, 5, 1).date(),
                    datetime(2026, 5, 3).date(),
                )

        self.assertEqual(len(summaries), 1)
        self.assertEqual(summaries[0].first_name, "John")
        self.assertEqual(summaries[0].checkin_count, 2)
        self.assertEqual(summaries[0].last_check_in_at, datetime(2026, 5, 3, 15, 59, 20))
        self.assertEqual(
            summaries[0].check_in_dates,
            (
                datetime(2026, 5, 3, 15, 59, 20),
                datetime(2026, 5, 1, 9, 0, 0),
            ),
        )

    def test_club_app_renders_checkin_report_for_date_range(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            flask_app = create_admin_app(db_path)
            client = admin_client(flask_app)
            checkins_to_import = csv_import.read_checkins_csv(io.StringIO(CHECKINS_RANGE_CSV))
            with closing(database.connect(db_path)) as connection:
                for checkin in checkins_to_import:
                    checkin_repository.upsert_checkin(connection, checkin)
                connection.commit()

            response = client.get(
                "/checkins/report?start_date=2026-05-01&end_date=2026-05-03"
            )

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Check-ins", body)
        self.assertIn('class="report-tabs"', body)
        self.assertIn(">By User</a>", body)
        self.assertIn(">Daily</a>", body)
        self.assertIn("John", body)
        self.assertIn(">2<", body)
        self.assertIn("2026-05-01 09:00:00", body)
        self.assertIn("2026-05-03 15:59:20", body)
        self.assertNotIn("Jane", body)

    def test_club_app_renders_daily_view_in_combined_checkin_report(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            flask_app = create_admin_app(db_path)
            client = admin_client(flask_app)
            checkins_to_import = csv_import.read_checkins_csv(io.StringIO(CHECKINS_RANGE_CSV))
            with closing(database.connect(db_path)) as connection:
                for checkin in checkins_to_import:
                    checkin_repository.upsert_checkin(connection, checkin)
                connection.commit()

            response = client.get(
                "/checkins/report?view=daily&start_date=2026-05-04&end_date=2026-05-04"
            )

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn('class="active" aria-current="page"', body)
        self.assertIn("Check-in</th>", body)
        self.assertIn("Jane", body)
        self.assertNotIn("John", body)

    def test_member_detail_lists_that_users_checkins(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            flask_app = create_admin_app(db_path)
            client = admin_client(flask_app)
            checkins_to_import = csv_import.read_checkins_csv(io.StringIO(CHECKINS_RANGE_CSV))
            with closing(database.connect(db_path)) as connection:
                for checkin in checkins_to_import:
                    checkin_repository.upsert_checkin(connection, checkin)
                connection.commit()
                john = [
                    member
                    for member in member_repository.list_members(connection)
                    if member.first_name == "John"
                ][0]

            response = client.get(f"/members/{john.id}")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("2026-05-03 15:59:20", body)
        self.assertIn("2026-05-01 09:00:00", body)
        self.assertNotIn("Jane", body)
        self.assertNotIn("Check-out", body)
        self.assertNotIn("Total", body)
        self.assertNotIn("Duration", body)

    def test_main_users_page_shows_checkin_summary_but_not_recent_checkins_section(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            flask_app = create_admin_app(db_path)
            client = admin_client(flask_app)
            checkins_to_import = csv_import.read_checkins_csv(io.StringIO(CHECKINS_CSV))
            with closing(database.connect(db_path)) as connection:
                for checkin in checkins_to_import:
                    checkin_repository.upsert_checkin(connection, checkin)
                connection.commit()

            response = client.get("/members")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertNotIn("Recent Check-ins", body)
        self.assertIn("Last Check-in", body)
        self.assertIn("2026-05-03 15:59:20", body)

    def test_self_checkin_requires_phone_and_initials_to_match(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            flask_app = create_app(db_path)
            with closing(database.connect(db_path)) as connection:
                member_repository.upsert_member(
                    connection,
                    Member(
                        last_name="Doe",
                        first_name="John",
                        card_number="1861",
                        membership="Visitor",
                        cell_phone="(510) 510-5100",
                    ),
                )
                connection.commit()

            response = flask_app.test_client().post(
                "/self-checkin",
                data={"phone": "5105105100", "initials": "JD"},
            )

            with closing(database.connect(db_path)) as connection:
                checkins = checkin_repository.list_checkins(connection)

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Check-in recorded.", body)
        self.assertNotIn("John", body)
        self.assertEqual(len(checkins), 1)

    def test_self_checkin_reports_no_match_for_wrong_initials(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            flask_app = create_app(db_path)
            with closing(database.connect(db_path)) as connection:
                member_repository.upsert_member(
                    connection,
                    Member(
                        last_name="Doe",
                        first_name="John",
                        card_number="1861",
                        membership="Visitor",
                        cell_phone="(510) 510-5100",
                    ),
                )
                connection.commit()

            response = flask_app.test_client().post(
                "/self-checkin",
                data={"phone": "5105105100", "initials": "XX"},
            )

            with closing(database.connect(db_path)) as connection:
                checkins = checkin_repository.list_checkins(connection)

        self.assertEqual(response.status_code, 200)
        self.assertIn("No matching user was found", response.get_data(as_text=True))
        self.assertEqual(len(checkins), 0)

    def test_self_checkin_accepts_first_name_when_initials_are_ambiguous(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            flask_app = create_app(db_path)
            with closing(database.connect(db_path)) as connection:
                member_repository.upsert_member(
                    connection,
                    Member(
                        last_name="Doe",
                        first_name="John",
                        card_number="1861",
                        membership="Visitor",
                        cell_phone="(510) 510-5100",
                    ),
                )
                member_repository.upsert_member(
                    connection,
                    Member(
                        last_name="Dane",
                        first_name="Jane",
                        card_number="1024",
                        membership="Full Member",
                        cell_phone="(510) 510-5100",
                    ),
                )
                connection.commit()

            response = flask_app.test_client().post(
                "/self-checkin",
                data={"phone": "5105105100", "initials": "Jane"},
            )

            with closing(database.connect(db_path)) as connection:
                checkins = checkin_repository.list_checkins(connection)

        self.assertEqual(response.status_code, 200)
        self.assertIn("Check-in recorded.", response.get_data(as_text=True))
        self.assertEqual(len(checkins), 1)
        self.assertEqual(checkins[0].first_name, "Jane")

    def test_self_checkin_accepts_nickname_when_initials_are_ambiguous(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            flask_app = create_app(db_path)
            with closing(database.connect(db_path)) as connection:
                member_repository.upsert_member(
                    connection,
                    Member(
                        last_name="Doe",
                        first_name="John",
                        nickname="Johnny",
                        card_number="1861",
                        membership="Visitor",
                        cell_phone="(510) 510-5100",
                    ),
                )
                member_repository.upsert_member(
                    connection,
                    Member(
                        last_name="Dane",
                        first_name="Jane",
                        card_number="1024",
                        membership="Full Member",
                        cell_phone="(510) 510-5100",
                    ),
                )
                connection.commit()

            response = flask_app.test_client().post(
                "/self-checkin",
                data={"phone": "5105105100", "initials": "Johnny"},
            )

            with closing(database.connect(db_path)) as connection:
                checkins = checkin_repository.list_checkins(connection)

        self.assertEqual(response.status_code, 200)
        self.assertIn("Check-in recorded.", response.get_data(as_text=True))
        self.assertEqual(len(checkins), 1)
        self.assertEqual(checkins[0].first_name, "John")

    def test_self_checkin_rejects_ambiguous_household_initials(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            flask_app = create_app(db_path)
            with closing(database.connect(db_path)) as connection:
                member_repository.upsert_member(
                    connection,
                    Member(
                        last_name="Doe",
                        first_name="John",
                        card_number="1861",
                        membership="Visitor",
                        cell_phone="(510) 510-5100",
                    ),
                )
                member_repository.upsert_member(
                    connection,
                    Member(
                        last_name="Dane",
                        first_name="Jane",
                        card_number="1024",
                        membership="Full Member",
                        cell_phone="(510) 510-5100",
                    ),
                )
                connection.commit()

            response = flask_app.test_client().post(
                "/self-checkin",
                data={"phone": "5105105100", "initials": "JD"},
            )

            with closing(database.connect(db_path)) as connection:
                checkins = checkin_repository.list_checkins(connection)

        self.assertEqual(response.status_code, 200)
        self.assertIn("No matching user was found", response.get_data(as_text=True))
        self.assertEqual(len(checkins), 0)

    def test_self_checkin_never_shows_current_user_information(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            flask_app = create_app(db_path)
            with closing(database.connect(db_path)) as connection:
                member_repository.upsert_member(
                    connection,
                    Member(
                        last_name="Doe",
                        first_name="John",
                        card_number="1861",
                        membership="Visitor",
                        email="john@example.test",
                        cell_phone="(510) 510-5100",
                    ),
                )
                connection.commit()

            response = flask_app.test_client().post(
                "/self-checkin",
                data={"phone": "5105105100", "initials": "JD", "review_info": "1"},
            )

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Check-in recorded.", body)
        self.assertNotIn("John Doe", body)
        self.assertNotIn("john@example.test", body)
        self.assertNotIn("Review my current information", body)

    def test_club_app_imports_checkins_into_configured_database(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            flask_app = create_admin_app(db_path)
            client = admin_client(flask_app)

            response = client.post(
                "/checkins/import",
                data={"checkins_csv": (io.BytesIO(CHECKINS_CSV.encode("utf-8")), "checkins.csv")},
                content_type="multipart/form-data",
            )

            with closing(database.connect(db_path)) as connection:
                users = member_repository.list_members(connection)
                checkins = checkin_repository.list_checkins(connection)

        self.assertEqual(response.status_code, 302)
        self.assertEqual(len(users), 2)
        self.assertEqual(len(checkins), 2)
