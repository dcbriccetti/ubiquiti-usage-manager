import io
import sys
import tempfile
from contextlib import closing
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from club_admin import csv_import
from club_admin import audit_repository
from club_admin import database
from club_admin import member_repository
from club_admin.app import create_app
from club_admin.models import Member


class ClubMemberImportTests(unittest.TestCase):
    def test_reads_members_csv_with_roster_headers(self) -> None:
        source = io.StringIO(
            "Last Name,First Name,Card #,Membership,Expiration,Address,Address2,City,State,Zip,Phone,Email,Work Phone,Cell Phone\n"
            "Doe,John,123,Visitor,10/31/2025,123 Main St,,Everytown,CA,94000,(123) 123-1234,abc@abc.com,,(510) 510-5100\n"
        )

        members = csv_import.read_members_csv(source)

        self.assertEqual(len(members), 1)
        self.assertEqual(members[0].card_number, "123")
        self.assertEqual(members[0].cell_phone, "(510) 510-5100")

    def test_reads_members_csv_with_leading_blank_line(self) -> None:
        source = io.StringIO(
            "\n"
            "Last Name,First Name,Card #,Membership,Expiration\n"
            "Doe,John,123,Visitor,10/31/2025\n"
        )

        members = csv_import.read_members_csv(source)

        self.assertEqual(len(members), 1)
        self.assertEqual(members[0].last_name, "Doe")
        self.assertEqual(members[0].membership, "Visitor")

    def test_reads_members_csv_after_export_preamble(self) -> None:
        source = io.StringIO(
            "sep=,\n"
            "Club User Export\n"
            "\n"
            "Last Name,First Name,Card #,Membership,Expiration\n"
            "Doe,John,123,Visitor,10/31/2025\n"
        )

        members = csv_import.read_members_csv(source)

        self.assertEqual(len(members), 1)
        self.assertEqual(members[0].first_name, "John")
        self.assertEqual(members[0].card_number, "123")

    def test_reads_members_csv_with_merged_first_name_card_header(self) -> None:
        source = io.StringIO(
            "Last Name,First NameCard #,Membership,Expiration\n"
            "Doe,John,123,Visitor,10/31/2025\n"
        )

        members = csv_import.read_members_csv(source)

        self.assertEqual(len(members), 1)
        self.assertEqual(members[0].first_name, "John")
        self.assertEqual(members[0].card_number, "123")

    def test_upsert_member_updates_existing_card_number(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            database.init_db(db_path)
            with closing(database.connect(db_path)) as connection:
                member_repository.upsert_member(
                    connection,
                    Member(
                        last_name="Doe",
                        first_name="John",
                        card_number="123",
                        membership="Visitor",
                    ),
                )
                member_repository.upsert_member(
                    connection,
                    Member(
                        last_name="Doe",
                        first_name="John",
                        card_number="123",
                        membership="Member",
                    ),
                )
                connection.commit()

                members = member_repository.list_members(connection)

        self.assertEqual(len(members), 1)
        self.assertEqual(members[0].membership, "Member")

    def test_database_uses_users_table_name(self) -> None:
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

        self.assertIn("users", table_names)
        self.assertNotIn("members", table_names)

    def test_users_table_does_not_store_expiration_date(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            database.init_db(db_path)
            with closing(database.connect(db_path)) as connection:
                columns = {
                    row["name"]
                    for row in connection.execute("PRAGMA table_info(users)").fetchall()
                }

        self.assertNotIn("expiration_date", columns)

    def test_club_app_imports_csv_into_configured_database(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            flask_app = create_app(db_path)
            csv_bytes = (
                b"Last Name,First Name,Card #,Membership,Expiration\n"
                b"Doe,John,123,Visitor,10/31/2025\n"
            )

            response = flask_app.test_client().post(
                "/members/import",
                data={"members_csv": (io.BytesIO(csv_bytes), "members.csv")},
                content_type="multipart/form-data",
            )

            with closing(database.connect(db_path)) as connection:
                members = member_repository.list_members(connection)

        self.assertEqual(response.status_code, 302)
        self.assertEqual(len(members), 1)
        self.assertEqual(members[0].first_name, "John")

    def test_members_page_links_names_to_detail_page(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            flask_app = create_app(db_path)
            with closing(database.connect(db_path)) as connection:
                member_repository.upsert_member(
                    connection,
                    Member(
                        last_name="Doe",
                        first_name="John",
                        card_number="123",
                        membership="Visitor",
                        email="john@example.test",
                    ),
                )
                connection.commit()
                member = member_repository.list_members(connection)[0]

            response = flask_app.test_client().get("/members")

        self.assertEqual(response.status_code, 200)
        self.assertIn(f'/members/{member.id}', response.get_data(as_text=True))

    def test_member_detail_page_shows_full_user_record(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            flask_app = create_app(db_path)
            with closing(database.connect(db_path)) as connection:
                member_repository.upsert_member(
                    connection,
                    Member(
                        last_name="Doe",
                        first_name="John",
                        card_number="123",
                        membership="Visitor",
                        address="123 Main St",
                        city="Everytown",
                        state="CA",
                        zip="94000",
                        phone="(123) 123-1234",
                        email="john@example.test",
                        cell_phone="(510) 510-5100",
                    ),
                )
                connection.commit()
                member = member_repository.list_members(connection)[0]

            response = flask_app.test_client().get(f"/members/{member.id}")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("john@example.test", body)
        self.assertIn("123 Main St", body)
        self.assertIn("(510) 510-5100", body)

    def test_member_detail_returns_404_for_missing_user(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            flask_app = create_app(db_path)

            response = flask_app.test_client().get("/members/999")

        self.assertEqual(response.status_code, 404)

    def test_edit_member_updates_user_and_records_audit_log(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            flask_app = create_app(db_path)
            with closing(database.connect(db_path)) as connection:
                member_repository.upsert_member(
                    connection,
                    Member(
                        last_name="Doe",
                        first_name="John",
                        card_number="123",
                        membership="Visitor",
                        email="old@example.test",
                    ),
                )
                connection.commit()
                member = member_repository.list_members(connection)[0]

            response = flask_app.test_client().post(
                f"/members/{member.id}/edit",
                data={
                    "last_name": "Doe",
                    "first_name": "John",
                    "card_number": "123",
                    "membership": "Full Member",
                    "address": "",
                    "address2": "",
                    "city": "",
                    "state": "",
                    "zip": "",
                    "phone": "",
                    "email": "new@example.test",
                    "work_phone": "",
                    "cell_phone": "",
                },
            )

            with closing(database.connect(db_path)) as connection:
                updated_member = member_repository.get_member(connection, member.id)
                audit_entries = audit_repository.list_audit_log_for_entity(
                    connection,
                    entity_type="user",
                    entity_id=member.id,
                )

        self.assertEqual(response.status_code, 302)
        self.assertIsNotNone(updated_member)
        assert updated_member is not None
        self.assertEqual(updated_member.membership, "Full Member")
        self.assertEqual(updated_member.email, "new@example.test")
        changed_fields = {entry.field_name for entry in audit_entries}
        self.assertIn("membership", changed_fields)
        self.assertIn("email", changed_fields)

    def test_member_detail_shows_audit_log_entries(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            flask_app = create_app(db_path)
            with closing(database.connect(db_path)) as connection:
                member_repository.upsert_member(
                    connection,
                    Member(
                        last_name="Doe",
                        first_name="John",
                        card_number="123",
                        membership="Visitor",
                    ),
                )
                connection.commit()
                member = member_repository.list_members(connection)[0]
                audit_repository.record_field_change(
                    connection,
                    entity_type="user",
                    entity_id=member.id,
                    action="edit",
                    field_name="membership",
                    old_value="Visitor",
                    new_value="Full Member",
                )
                connection.commit()

            response = flask_app.test_client().get(f"/members/{member.id}")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Change Log", body)
        self.assertIn("Full Member", body)
