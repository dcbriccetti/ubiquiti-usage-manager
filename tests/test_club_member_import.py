import io
import sys
import tempfile
from contextlib import closing
from datetime import datetime
import unittest
from unittest.mock import patch
from pathlib import Path

from PIL import Image, ImageDraw, ImageStat
from werkzeug.security import generate_password_hash


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from club_admin import csv_import
from club_admin import audit_repository
from club_admin import checkin_repository
from club_admin import database
from club_admin import guest_registration_repository
from club_admin import member_repository
from club_admin.repair_driver_license_scans import _prepare_stored_driver_license_image
from club_admin.app import create_app
from club_admin.models import CheckIn, GuestRegistration, Member
import config as cfg


ADMIN_PASSWORD = "test-admin-password"


def create_admin_app(
    db_path: Path,
    documents_dir: str = "",
    zip_coordinates: dict[str, tuple[float, float]] | None = None,
    guest_form_definition_path: str = "",
):
    with patch.object(
        cfg,
        "USER_MANAGEMENT_ADMIN_PASSWORD_HASH",
        generate_password_hash(ADMIN_PASSWORD),
    ), patch.object(cfg, "USER_MANAGEMENT_DOCUMENTS_DIR", documents_dir), patch.object(
        cfg,
        "USER_MANAGEMENT_ZIP_COORDINATES",
        zip_coordinates or {},
    ), patch.object(
        cfg,
        "USER_MANAGEMENT_GUEST_FORM_DEFINITION_PATH",
        guest_form_definition_path,
    ):
        return create_app(db_path)


def admin_client(flask_app):
    client = flask_app.test_client()
    response = client.post("/admin/login", data={"password": ADMIN_PASSWORD})
    assert response.status_code == 302
    return client


class ClubMemberImportTests(unittest.TestCase):
    def test_reads_members_csv_with_roster_headers(self) -> None:
        source = io.StringIO(
            "Last Name,First Name,Card #,Membership,Member Since,Date of Birth,Expiration,Address,Address2,City,State,Zip,Phone,Email,Work Phone,Cell Phone\n"
            "Doe,John,123,Visitor,5/1/2020,7/4/1980,10/31/2025,123 Main St,,Everytown,CA,94000,(123) 123-1234,abc@abc.com,,(510) 510-5100\n"
        )

        members = csv_import.read_members_csv(source)

        self.assertEqual(len(members), 1)
        self.assertEqual(members[0].card_number, "123")
        self.assertEqual(members[0].member_since.isoformat(), "2020-05-01")
        self.assertEqual(members[0].date_of_birth.isoformat(), "1980-07-04")
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

    def test_reads_members_csv_extracts_trailing_parenthesized_nickname(self) -> None:
        source = io.StringIO(
            "Last Name,First Name,Card #,Membership\n"
            "Doe,John (Johnny),123,Visitor\n"
            "Public,Jane (JP) Smith,456,Full Member\n"
            "Roe,(none),789,Visitor\n"
        )

        members = csv_import.read_members_csv(source)

        self.assertEqual(members[0].first_name, "John")
        self.assertEqual(members[0].nickname, "Johnny")
        self.assertEqual(members[1].first_name, "Jane (JP) Smith")
        self.assertIsNone(members[1].nickname)
        self.assertEqual(members[2].first_name, "(none)")
        self.assertIsNone(members[2].nickname)

    def test_reads_members_csv_treats_placeholder_address_as_blank(self) -> None:
        source = io.StringIO(
            "Last Name,First Name,Card #,Membership,Address,City,State,Zip\n"
            'Doe,John,123,Visitor,"Address, City CA",,,\n'
            'Public,Jane,456,Full Member,"Address, City CA 12345",,,\n'
            "Roe,Richard,789,Visitor,Address,City,CA,12345\n"
            "Smith,Sue,321,Visitor,Address,Everytown,CA,94000\n"
            "Stone,Sam,654,Visitor,123 Main St,City,CA,94000\n"
        )

        members = csv_import.read_members_csv(source)

        self.assertEqual(len(members), 5)
        self.assertIsNone(members[0].address)
        self.assertIsNone(members[1].address)
        self.assertIsNone(members[2].address)
        self.assertIsNone(members[2].city)
        self.assertIsNone(members[2].state)
        self.assertIsNone(members[2].zip)
        self.assertIsNone(members[3].address)
        self.assertEqual(members[3].city, "Everytown")
        self.assertEqual(members[4].address, "123 Main St")
        self.assertIsNone(members[4].city)

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
                        nickname="Johnny",
                        card_number="123",
                        membership="Visitor",
                    ),
                )
                member_repository.upsert_member(
                    connection,
                    Member(
                        last_name="Doe",
                        first_name="John",
                        nickname="Johnny",
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
        self.assertIn("nickname", columns)
        self.assertIn("member_since", columns)
        self.assertIn("date_of_birth", columns)

    def test_database_adds_user_date_columns_to_existing_database(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            with closing(database.connect(db_path)) as connection:
                connection.execute(
                    """
                    CREATE TABLE users (
                        id INTEGER PRIMARY KEY,
                        last_name TEXT NOT NULL,
                        first_name TEXT NOT NULL,
                        nickname TEXT,
                        card_number TEXT NOT NULL UNIQUE,
                        membership TEXT NOT NULL,
                        address TEXT,
                        address2 TEXT,
                        city TEXT,
                        state TEXT,
                        zip TEXT,
                        phone TEXT,
                        email TEXT,
                        work_phone TEXT,
                        cell_phone TEXT,
                        imported_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
                connection.commit()

            database.init_db(db_path)
            with closing(database.connect(db_path)) as connection:
                columns = {
                    row["name"]
                    for row in connection.execute("PRAGMA table_info(users)").fetchall()
                }

        self.assertIn("member_since", columns)
        self.assertIn("date_of_birth", columns)

    def test_admin_pages_redirect_to_login_when_not_authenticated(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            flask_app = create_admin_app(db_path)

            response = flask_app.test_client().get("/members")

        self.assertEqual(response.status_code, 302)
        self.assertIn("/admin/login", response.headers["Location"])

    def test_admin_login_rejects_wrong_password(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            flask_app = create_admin_app(db_path)
            client = flask_app.test_client()

            response = client.post("/admin/login", data={"password": "wrong"})
            members_response = client.get("/members")

        self.assertEqual(response.status_code, 200)
        self.assertIn("Password was not accepted.", response.get_data(as_text=True))
        self.assertEqual(members_response.status_code, 302)

    def test_admin_pages_fail_closed_when_password_is_not_configured(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            with patch.object(cfg, "USER_MANAGEMENT_ADMIN_PASSWORD_HASH", ""):
                flask_app = create_app(db_path)
            client = flask_app.test_client()

            response = client.get("/admin/login")
            members_response = client.get("/members")

        self.assertEqual(response.status_code, 503)
        self.assertIn("Admin access is not configured.", response.get_data(as_text=True))
        self.assertEqual(members_response.status_code, 302)

    def test_self_checkin_page_stays_public_without_admin_login(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            flask_app = create_admin_app(db_path)

            response = flask_app.test_client().get("/self-checkin")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Self Check-in", body)
        self.assertIn('href="/guest-registration"', body)
        self.assertIn('name="phone" autocomplete="tel" required autofocus', body)
        self.assertNotIn('name="barcode_token"\n                  autocomplete="off"\n                  autofocus', body)

    def test_guest_registration_thanks_returns_to_self_checkin_after_delay(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            flask_app = create_admin_app(db_path)

            response = flask_app.test_client().get("/guest-registration/thanks")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["Refresh"], "60; url=/self-checkin")
        body = response.get_data(as_text=True)
        self.assertIn("Registration Submitted", body)
        self.assertIn('<meta http-equiv="refresh" content="60;url=/self-checkin">', body)
        self.assertIn('href="/self-checkin"', body)
        self.assertIn("Back to Check-in", body)
        self.assertIn("autoReturnDelay = 60000", body)

    def test_import_forms_live_on_dedicated_admin_page(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            flask_app = create_admin_app(db_path)
            client = admin_client(flask_app)

            members_response = client.get("/members")
            imports_response = client.get("/imports")

        self.assertEqual(members_response.status_code, 200)
        members_body = members_response.get_data(as_text=True)
        self.assertIn("Imports", members_body)
        self.assertNotIn('name="members_csv"', members_body)
        self.assertNotIn('name="checkins_csv"', members_body)
        self.assertEqual(imports_response.status_code, 200)
        imports_body = imports_response.get_data(as_text=True)
        self.assertIn('name="members_csv"', imports_body)
        self.assertIn('name="remove_existing_users" value="1"', imports_body)
        self.assertIn("First remove all existing checkins and users", imports_body)
        self.assertIn('name="checkins_csv"', imports_body)

    def test_admin_can_check_in_selected_members_from_users_page(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            flask_app = create_admin_app(db_path)
            client = admin_client(flask_app)
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
                        last_name="Public",
                        first_name="Jane",
                        card_number="456",
                        membership="Full Member",
                    ),
                )
                connection.commit()
                members_by_first_name = {
                    member.first_name: member
                    for member in member_repository.list_members(connection)
                }
                john = members_by_first_name["John"]
                jane = members_by_first_name["Jane"]

            members_response = client.get("/members")
            response = client.post(
                "/members/check-ins",
                data={"member_ids": [str(john.id), str(jane.id)]},
                follow_redirects=True,
            )

            with closing(database.connect(db_path)) as connection:
                john_checkins = checkin_repository.list_checkins_for_user(connection, john.id)
                jane_checkins = checkin_repository.list_checkins_for_user(connection, jane.id)
                john_audit_entries = audit_repository.list_audit_log_for_entity(
                    connection,
                    entity_type="user",
                    entity_id=john.id,
                )
                jane_audit_entries = audit_repository.list_audit_log_for_entity(
                    connection,
                    entity_type="user",
                    entity_id=jane.id,
                )

        self.assertEqual(members_response.status_code, 200)
        members_body = members_response.get_data(as_text=True)
        self.assertIn('action="/members/check-ins"', members_body)
        self.assertIn('data-checkin-submit disabled', members_body)
        self.assertIn(f'name="member_ids" value="{john.id}"', members_body)
        self.assertIn("Check In Selected", members_body)
        self.assertNotIn("Select shown", members_body)
        self.assertEqual(response.status_code, 200)
        self.assertIn("Checked in 2 users.", response.get_data(as_text=True))
        self.assertEqual(len(john_checkins), 1)
        self.assertEqual(john_checkins[0].user_id, john.id)
        self.assertEqual(john_checkins[0].card_number, "123")
        self.assertEqual(john_checkins[0].membership, "Visitor")
        self.assertEqual(len(jane_checkins), 1)
        self.assertEqual(jane_checkins[0].user_id, jane.id)
        self.assertEqual(jane_checkins[0].card_number, "456")
        self.assertEqual(jane_checkins[0].membership, "Full Member")
        for audit_entries in (john_audit_entries, jane_audit_entries):
            checkin_audit_entries = [
                entry for entry in audit_entries if entry.field_name == "check-in added"
            ]
            self.assertEqual(len(checkin_audit_entries), 1)
            self.assertIsNone(checkin_audit_entries[0].old_value)
            self.assertIsNotNone(checkin_audit_entries[0].new_value)

    def test_guest_registration_creates_visitor_user_without_admin_login(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            flask_app = create_admin_app(db_path)
            with closing(database.connect(db_path)) as connection:
                member_repository.upsert_member(
                    connection,
                    Member(
                        last_name="Existing",
                        first_name="User",
                        card_number="999",
                        membership="Visitor",
                    ),
                )
                member_repository.upsert_member(
                    connection,
                    Member(
                        last_name="Old",
                        first_name="Guest",
                        card_number="GUEST-OLD",
                        membership="Visitor",
                    ),
                )
                connection.commit()

            response = flask_app.test_client().post(
                "/guest-registration",
                data={
                    "visit_date": "2026-05-14",
                    "last_name": "Doe",
                    "first_name": "John",
                    "date_of_birth": "1990-06-15",
                    "middle_name": "Q",
                    "nickname": "Johnny",
                    "address": "123 Main St",
                    "city": "Everytown",
                    "state": "CA",
                    "zip": "94000",
                    "cell_phone": "510-510-5100",
                    "other_phone": "123-123-1234",
                    "other_phone_type": "home",
                    "email": "john@example.test",
                    "marital_status": "single",
                    "guest_of_member": "1",
                    "member_name": "Member Name",
                    "heard_about": "Friend",
                    "newsletter_opt_out": "1",
                },
            )

            with closing(database.connect(db_path)) as connection:
                users = member_repository.list_members(connection)
                records = guest_registration_repository.list_guest_registration_records(
                    connection
                )

        self.assertEqual(response.status_code, 302)
        self.assertIn("/guest-registration/thanks", response.headers["Location"])
        new_user = next(user for user in users if user.last_name == "Doe")
        self.assertEqual(len(users), 3)
        self.assertEqual(new_user.membership, "Visitor")
        self.assertEqual(new_user.card_number, "1000")
        self.assertEqual(new_user.nickname, "Johnny")
        self.assertEqual(new_user.date_of_birth.isoformat(), "1990-06-15")
        self.assertEqual(new_user.phone, "123-123-1234")
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].registration.middle_name, "Q")
        self.assertTrue(records[0].registration.guest_of_member)
        self.assertTrue(records[0].registration.newsletter_opt_out)

    def test_guest_registration_marks_required_fields_without_required_badges(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            flask_app = create_admin_app(db_path)

            response = flask_app.test_client().get("/guest-registration")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Items marked", body)
        self.assertNotIn("<strong>Required</strong>", body)
        self.assertIn('id="contact-section-title" class="required-section-title"', body)
        self.assertIn('Contact <span class="visually-hidden">required</span>', body)
        self.assertIn("A phone number or email is required.", body)
        self.assertIn('class="detail-wide required-field"', body)
        self.assertNotIn('name="cell_phone" value="" autocomplete="tel" inputmode="tel" required', body)
        self.assertNotIn('name="other_phone" value="" inputmode="tel" required', body)
        self.assertNotIn('name="email" value="" autocomplete="email" required', body)
        self.assertIn('name="date_of_birth" value="" autocomplete="bday" required', body)
        self.assertIn('name="address" value="" autocomplete="street-address" required', body)
        self.assertIn('name="city" value="" autocomplete="address-level2" required', body)
        self.assertIn('name="state" value="" autocomplete="address-level1" maxlength="2" required', body)
        self.assertIn('name="zip" value="" autocomplete="postal-code" inputmode="numeric" required', body)
        self.assertIn('select name="marital_status" required', body)

    def test_guest_registration_requires_name_and_contact(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            flask_app = create_admin_app(db_path)

            response = flask_app.test_client().post(
                "/guest-registration",
                data={
                    "last_name": "Doe",
                    "first_name": "John",
                },
            )

        self.assertEqual(response.status_code, 400)
        body = response.get_data(as_text=True)
        self.assertIn("Phone or email is required.", body)
        self.assertIn("Guest Registration", body)
        self.assertIn('value="Doe"', body)
        self.assertIn('value="John"', body)

    def test_guest_registration_missing_name_rerenders_form(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            flask_app = create_admin_app(db_path)

            response = flask_app.test_client().post(
                "/guest-registration",
                data={
                    "first_name": "John",
                    "email": "john@example.test",
                },
            )

        self.assertEqual(response.status_code, 400)
        body = response.get_data(as_text=True)
        self.assertIn("First and last name are required.", body)
        self.assertIn('value="John"', body)
        self.assertIn('value="john@example.test"', body)

    def test_guest_registration_requires_address_fields(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            flask_app = create_admin_app(db_path)

            response = flask_app.test_client().post(
                "/guest-registration",
                data={
                    "last_name": "Doe",
                    "first_name": "John",
                    "email": "john@example.test",
                    "date_of_birth": "1990-06-15",
                    "marital_status": "single",
                },
            )

        self.assertEqual(response.status_code, 400)
        body = response.get_data(as_text=True)
        self.assertIn("Street address, city, state, and zip code are required.", body)
        self.assertIn('value="john@example.test"', body)

    def test_guest_registration_requires_marital_status(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            flask_app = create_admin_app(db_path)

            response = flask_app.test_client().post(
                "/guest-registration",
                data={
                    "last_name": "Doe",
                    "first_name": "John",
                    "email": "john@example.test",
                    "date_of_birth": "1990-06-15",
                    "address": "123 Main St",
                    "city": "Everytown",
                    "state": "CA",
                    "zip": "94000",
                },
            )

        self.assertEqual(response.status_code, 400)
        body = response.get_data(as_text=True)
        self.assertIn("Marital status is required.", body)
        self.assertIn('value="123 Main St"', body)

    def test_guest_registration_bad_visit_date_rerenders_form(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            flask_app = create_admin_app(db_path)

            response = flask_app.test_client().post(
                "/guest-registration",
                data={
                    "visit_date": "May 14",
                    "last_name": "Doe",
                    "first_name": "John",
                    "email": "john@example.test",
                },
            )

        self.assertEqual(response.status_code, 400)
        body = response.get_data(as_text=True)
        self.assertIn("Visit date must use YYYY-MM-DD.", body)
        self.assertIn('value="May 14"', body)
        self.assertIn('value="Doe"', body)

    def test_guest_registration_bad_date_of_birth_rerenders_form(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            flask_app = create_admin_app(db_path)

            response = flask_app.test_client().post(
                "/guest-registration",
                data={
                    "last_name": "Doe",
                    "first_name": "John",
                    "date_of_birth": "June 15",
                    "email": "john@example.test",
                },
            )

        self.assertEqual(response.status_code, 400)
        body = response.get_data(as_text=True)
        self.assertIn("Date of birth must use YYYY-MM-DD.", body)
        self.assertIn('value="June 15"', body)
        self.assertIn('value="Doe"', body)

    def test_club_app_imports_csv_into_configured_database(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            flask_app = create_admin_app(db_path)
            client = admin_client(flask_app)
            csv_bytes = (
                b"Last Name,First Name,Card #,Membership,Expiration\n"
                b"Doe,John,123,Visitor,10/31/2025\n"
            )

            response = client.post(
                "/members/import",
                data={"members_csv": (io.BytesIO(csv_bytes), "members.csv")},
                content_type="multipart/form-data",
            )

            with closing(database.connect(db_path)) as connection:
                members = member_repository.list_members(connection)

        self.assertEqual(response.status_code, 302)
        self.assertEqual(len(members), 1)
        self.assertEqual(members[0].first_name, "John")

    def test_member_import_can_first_remove_existing_users_and_checkins(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            flask_app = create_admin_app(db_path)
            client = admin_client(flask_app)
            with closing(database.connect(db_path)) as connection:
                existing_user_id = member_repository.insert_member(
                    connection,
                    Member(
                        last_name="Old",
                        first_name="User",
                        card_number="OLD-1",
                        membership="Visitor",
                    ),
                )
                guest_user_id = member_repository.insert_member(
                    connection,
                    Member(
                        last_name="Guest",
                        first_name="Person",
                        card_number="GUEST-1",
                        membership="Visitor",
                    ),
                )
                guest_registration_repository.insert_guest_registration(
                    connection,
                    GuestRegistration(
                        user_id=guest_user_id,
                        visit_date=datetime(2026, 5, 14).date(),
                    ),
                )
                checkin_repository.upsert_checkin(
                    connection,
                    CheckIn(
                        member_id="OLD-1",
                        last_name="Old",
                        first_name="User",
                        card_number="OLD-1",
                        check_in_at=datetime(2026, 5, 1, 9, 0, 0),
                        membership="Visitor",
                    ),
                )
                audit_repository.record_field_change(
                    connection,
                    entity_type="user",
                    entity_id=existing_user_id,
                    action="edit",
                    field_name="membership",
                    old_value="Visitor",
                    new_value="Full Member",
                )
                connection.commit()
            csv_bytes = (
                b"Last Name,First Name,Card #,Membership,Expiration\n"
                b"New,User,NEW-1,Full Member,10/31/2026\n"
            )

            response = client.post(
                "/members/import",
                data={
                    "members_csv": (io.BytesIO(csv_bytes), "members.csv"),
                    "remove_existing_users": "1",
                },
                content_type="multipart/form-data",
            )

            with closing(database.connect(db_path)) as connection:
                members = member_repository.list_members(connection)
                checkin_count = connection.execute(
                    "SELECT COUNT(*) FROM checkins"
                ).fetchone()[0]
                guest_registration_count = connection.execute(
                    "SELECT COUNT(*) FROM guest_registrations"
                ).fetchone()[0]
                audit_count = connection.execute(
                    "SELECT COUNT(*) FROM audit_log WHERE entity_type = 'user'"
                ).fetchone()[0]

        self.assertEqual(response.status_code, 302)
        self.assertEqual([member.card_number for member in members], ["NEW-1"])
        self.assertEqual(checkin_count, 0)
        self.assertEqual(guest_registration_count, 0)
        self.assertEqual(audit_count, 0)

    def test_members_page_links_names_to_detail_page(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            flask_app = create_admin_app(db_path)
            client = admin_client(flask_app)
            with closing(database.connect(db_path)) as connection:
                member_repository.upsert_member(
                    connection,
                    Member(
                        last_name="Doe",
                        first_name="John",
                        nickname="Johnny",
                        card_number="123",
                        membership="Visitor",
                        email="john@example.test",
                    ),
                )
                connection.commit()
                member = member_repository.list_members(connection)[0]

            response = client.get("/members")

        self.assertEqual(response.status_code, 200)
        self.assertIn(f'/members/{member.id}', response.get_data(as_text=True))

    def test_members_page_shows_address_and_all_time_checkin_stats(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            db_path = temp_path / "club-users.db"
            documents_dir = temp_path / "documents"
            card_123_dir = documents_dir / "123"
            card_123_dir.mkdir(parents=True)
            (card_123_dir / "Guest Form scan.JPG").write_bytes(b"\xff\xd8\xff\xe0test-jpeg")
            (card_123_dir / "waiver.pdf").write_text("synthetic waiver")
            (card_123_dir / ".DS_Store").write_text("synthetic finder metadata")
            flask_app = create_admin_app(db_path, str(documents_dir))
            client = admin_client(flask_app)
            with closing(database.connect(db_path)) as connection:
                member_repository.upsert_member(
                    connection,
                    Member(
                        last_name="Doe",
                        first_name="John",
                        nickname="Johnny",
                        card_number="123",
                        membership="Visitor",
                        member_since=datetime(2020, 5, 1).date(),
                        date_of_birth=datetime(1980, 7, 4).date(),
                        address="123 Main St",
                        address2="Unit 4",
                        city="Everytown",
                        state="CA",
                        zip="94000",
                    ),
                )
                member_repository.upsert_member(
                    connection,
                    Member(
                        last_name="Public",
                        first_name="Jane",
                        card_number="456",
                        membership="Full Member",
                    ),
                )
                for check_in_at in (
                    datetime(2026, 5, 1, 9, 0, 0),
                    datetime(2026, 5, 3, 15, 59, 20),
                ):
                    checkin_repository.upsert_checkin(
                        connection,
                        CheckIn(
                            member_id="1",
                            last_name="Doe",
                            first_name="John",
                            card_number="123",
                            check_in_at=check_in_at,
                            membership="Visitor",
                        ),
                    )
                connection.commit()

            response = client.get("/members")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn('class="page-header users-header"', body)
        self.assertIn('class="nav-links users-nav"', body)
        self.assertIn('href="/imports"', body)
        self.assertIn('src="/static/club-admin-table-sort.js"', body)
        self.assertIn('class="users-list-controls"', body)
        self.assertIn('data-table-search', body)
        self.assertIn('data-table-search-column', body)
        self.assertIn('<option value="1">First Name</option>', body)
        self.assertIn('placeholder="Search users"', body)
        self.assertNotIn('placeholder="Dave"', body)
        self.assertIn('data-table-search-count', body)
        self.assertIn('class="users-table" data-sortable-table', body)
        self.assertIn('data-sort-column="0" data-sort-type="text"', body)
        self.assertIn('data-sort-column="5" data-sort-type="date"', body)
        self.assertIn('data-sort-column="6" data-sort-type="date"', body)
        self.assertIn('data-sort-column="10" data-sort-type="number"', body)
        self.assertIn('data-sort-column="12" data-sort-type="date"', body)
        self.assertIn("No users match this search.", body)
        self.assertNotIn('class="file-field"', body)
        self.assertNotIn("Users CSV", body)
        self.assertNotIn("Check-ins CSV", body)
        self.assertIn("Nickname", body)
        self.assertIn("Johnny", body)
        self.assertIn("Member Since", body)
        self.assertIn("Date of Birth", body)
        self.assertIn(">2020-05-01<", body)
        self.assertIn(">1980-07-04<", body)
        self.assertIn("123 Main St", body)
        self.assertIn("Unit 4", body)
        self.assertIn("Everytown CA 94000", body)
        self.assertIn("2026-05-03 15:59:20", body)
        self.assertIn("Docs", body)
        self.assertIn(">2<", body)
        self.assertIn('<td data-sort-value="123">', body)
        self.assertIn('<td class="numeric" data-sort-value="0"></td>', body)
        self.assertIn('data-sort-value="2026-05-03T15:59:20"', body)
        self.assertNotIn(">0<", body)

    def test_members_map_summarizes_users_by_zip_without_addresses(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            flask_app = create_admin_app(
                db_path,
                zip_coordinates={
                    "94000": (37.10, -122.10),
                    "94001": (37.20, -122.20),
                },
            )
            client = admin_client(flask_app)
            with closing(database.connect(db_path)) as connection:
                for member in (
                    Member(
                        last_name="Doe",
                        first_name="John",
                        nickname="Old Nickname",
                        card_number="123",
                        membership="Visitor",
                        address="123 Main St",
                        city="Everytown",
                        state="CA",
                        zip="94000",
                    ),
                    Member(
                        last_name="Public",
                        first_name="Jane",
                        card_number="456",
                        membership="Full Member",
                        address="456 Oak Ave",
                        city="Everytown",
                        state="CA",
                        zip="94000-1234",
                    ),
                    Member(
                        last_name="Example",
                        first_name="Sam",
                        card_number="789",
                        membership="Visitor",
                        address="789 Pine Rd",
                        city="Elsewhere",
                        state="CA",
                        zip="94001",
                    ),
                    Member(
                        last_name="Missing",
                        first_name="Morgan",
                        card_number="321",
                        membership="Visitor",
                        address="321 Cedar St",
                        city="Faraway",
                        state="CA",
                        zip="99999",
                    ),
                ):
                    member_repository.upsert_member(connection, member)
                connection.commit()

            response = client.get("/members/map")
            body = response.get_data(as_text=True)

        self.assertEqual(response.status_code, 200)
        self.assertIn("User ZIP Map", body)
        self.assertIn("94000", body)
        self.assertIn("94001", body)
        self.assertIn("99999", body)
        self.assertNotIn("ZIPs Needing Coordinates", body)
        self.assertNotIn("Unmapped ZIPs", body)
        self.assertNotIn("Mapped ZIPs", body)
        self.assertNotIn("<th>Latitude</th>", body)
        self.assertNotIn("<th>Longitude</th>", body)
        self.assertIn("https://unpkg.com/leaflet@1.9.4/dist/leaflet.css", body)
        self.assertIn("https://tile.openstreetmap.org/{z}/{x}/{y}.png", body)
        self.assertIn("api.zippopotam.us/us/", body)
        self.assertIn("/members/map/zip-coordinates", body)
        self.assertIn('"zip_code": "94000"', body)
        self.assertIn('"count": 2', body)
        self.assertNotIn("123 Main St", body)
        self.assertNotIn("456 Oak Ave", body)
        self.assertNotIn("789 Pine Rd", body)

    def test_members_map_renders_map_container_for_unconfigured_zip_coordinates(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            flask_app = create_admin_app(db_path)
            client = admin_client(flask_app)
            with closing(database.connect(db_path)) as connection:
                member_repository.upsert_member(
                    connection,
                    Member(
                        last_name="Doe",
                        first_name="John",
                        nickname="Johnny",
                        card_number="123",
                        membership="Visitor",
                        address="123 Main St",
                        city="Everytown",
                        state="CA",
                        zip="94000",
                    ),
                )
                connection.commit()

            response = client.get("/members/map")
            body = response.get_data(as_text=True)

        self.assertEqual(response.status_code, 200)
        self.assertIn('id="user-zip-map"', body)
        self.assertIn('"lookupZips": [{"count": 1, "zip_code": "94000"}]', body)
        self.assertIn('"points": []', body)
        self.assertIn("api.zippopotam.us/us/", body)
        self.assertNotIn("No ZIP coordinates are configured", body)
        self.assertNotIn("123 Main St", body)

    def test_members_map_imports_zip_coordinate_csv_for_local_cache(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            flask_app = create_admin_app(db_path)
            client = admin_client(flask_app)
            with closing(database.connect(db_path)) as connection:
                member_repository.upsert_member(
                    connection,
                    Member(
                        last_name="Doe",
                        first_name="John",
                        nickname="Johnny",
                        card_number="123",
                        membership="Visitor",
                        address="123 Main St",
                        city="Everytown",
                        state="CA",
                        zip="94000",
                    ),
                )
                connection.commit()

            import_response = client.post(
                "/members/map/zip-coordinates/import",
                data={
                    "zip_coordinates_csv": (
                        io.BytesIO(b"zip,latitude,longitude\n94000,37.1,-122.1\n"),
                        "zip_coordinates.csv",
                    )
                },
                content_type="multipart/form-data",
            )
            map_response = client.get("/members/map")
            body = map_response.get_data(as_text=True)

        self.assertEqual(import_response.status_code, 302)
        self.assertIn("imported=1", import_response.headers["Location"])
        self.assertEqual(map_response.status_code, 200)
        self.assertIn('"points": [{"count": 1, "latitude": 37.1, "longitude": -122.1, "zip_code": "94000"}]', body)
        self.assertIn('"lookupZips": []', body)
        self.assertNotIn("Mapped ZIPs", body)
        self.assertNotIn("<th>Latitude</th>", body)
        self.assertNotIn("<th>Longitude</th>", body)

    def test_members_map_saves_auto_located_zip_coordinates(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            flask_app = create_admin_app(db_path)
            client = admin_client(flask_app)
            with closing(database.connect(db_path)) as connection:
                member_repository.upsert_member(
                    connection,
                    Member(
                        last_name="Doe",
                        first_name="John",
                        nickname="Johnny",
                        card_number="123",
                        membership="Visitor",
                        address="123 Main St",
                        city="Everytown",
                        state="CA",
                        zip="94000",
                    ),
                )
                connection.commit()

            save_response = client.post(
                "/members/map/zip-coordinates",
                json={
                    "coordinates": [
                        {
                            "zip_code": "94000",
                            "latitude": 37.1,
                            "longitude": -122.1,
                        }
                    ]
                },
            )
            map_response = client.get("/members/map")
            body = map_response.get_data(as_text=True)

        self.assertEqual(save_response.status_code, 200)
        self.assertEqual(save_response.get_json(), {"saved": 1})
        self.assertEqual(map_response.status_code, 200)
        self.assertIn('"points": [{"count": 1, "latitude": 37.1, "longitude": -122.1, "zip_code": "94000"}]', body)
        self.assertIn('"lookupZips": []', body)
        self.assertNotIn("Mapped ZIPs", body)
        self.assertNotIn("<th>Latitude</th>", body)
        self.assertNotIn("<th>Longitude</th>", body)

    def test_members_map_requires_admin_login(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            flask_app = create_admin_app(db_path)

            response = flask_app.test_client().get("/members/map")

        self.assertEqual(response.status_code, 302)
        self.assertIn("/admin/login", response.headers["Location"])

    def test_member_detail_page_shows_full_user_record(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            flask_app = create_admin_app(db_path)
            client = admin_client(flask_app)
            with closing(database.connect(db_path)) as connection:
                member_repository.upsert_member(
                    connection,
                    Member(
                        last_name="Doe",
                        first_name="John",
                        nickname="Johnny",
                        card_number="123",
                        membership="Visitor",
                        member_since=datetime(2020, 5, 1).date(),
                        date_of_birth=datetime(1980, 7, 4).date(),
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

            response = client.get(f"/members/{member.id}")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("john@example.test", body)
        self.assertIn("Nickname", body)
        self.assertIn("Johnny", body)
        self.assertIn("Member Since", body)
        self.assertIn("2020-05-01", body)
        self.assertIn("Date of Birth", body)
        self.assertIn("1980-07-04", body)
        self.assertIn("123 Main St", body)
        self.assertIn("(510) 510-5100", body)

    def test_member_detail_shows_guest_form_image_when_configured_file_exists(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            db_path = temp_path / "club-users.db"
            documents_dir = temp_path / "documents"
            guest_form_dir = documents_dir / "123"
            guest_form_dir.mkdir(parents=True)
            guest_form_path = guest_form_dir / "guest  form_doe  john.JPG"
            guest_form_path.write_bytes(b"\xff\xd8\xff\xe0test-jpeg")
            other_document_path = guest_form_dir / "waiver.pdf"
            other_document_path.write_bytes(b"synthetic waiver")
            flask_app = create_admin_app(db_path, str(documents_dir))
            client = admin_client(flask_app)
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

            response = client.get(f"/members/{member.id}")
            image_response = client.get(f"/members/{member.id}/guest-form.jpg")
            image_bytes = image_response.get_data()
            image_response.close()
            document_response = client.get(
                f"/members/{member.id}/document",
                query_string={"name": "waiver.pdf"},
            )
            document_bytes = document_response.get_data()
            document_response.close()

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn(f'/members/{member.id}/guest-form.jpg', body)
        self.assertIn("Other Documents", body)
        self.assertIn("waiver.pdf", body)
        self.assertIn(f'/members/{member.id}/document?name=waiver.pdf', body)
        self.assertEqual(image_response.status_code, 200)
        self.assertEqual(image_response.mimetype, "image/jpeg")
        self.assertEqual(image_bytes, b"\xff\xd8\xff\xe0test-jpeg")
        self.assertEqual(document_response.status_code, 200)
        self.assertEqual(document_bytes, b"synthetic waiver")

    def test_member_detail_shows_driver_license_when_guest_form_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            db_path = temp_path / "club-users.db"
            documents_dir = temp_path / "documents"
            member_document_dir = documents_dir / "123"
            member_document_dir.mkdir(parents=True)
            driver_license_path = member_document_dir / "Driver License.jpg"
            driver_license_path.write_bytes(b"\xff\xd8\xff\xe0synthetic-license")
            flask_app = create_admin_app(db_path, str(documents_dir))
            client = admin_client(flask_app)
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

            response = client.get(f"/members/{member.id}")
            document_response = client.get(
                f"/members/{member.id}/document",
                query_string={"name": "Driver License.jpg"},
            )
            document_bytes = document_response.get_data()
            document_response.close()

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("<h2>Driver License</h2>", body)
        self.assertIn(f'/members/{member.id}/document?name=Driver+License.jpg', body)
        self.assertNotIn(f'/members/{member.id}/guest-form.jpg', body)
        self.assertIn("No other documents found for this user.", body)
        self.assertEqual(document_response.status_code, 200)
        self.assertEqual(document_response.mimetype, "image/jpeg")
        self.assertEqual(document_bytes, b"\xff\xd8\xff\xe0synthetic-license")

    def test_member_detail_prefers_guest_form_over_driver_license_preview(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            db_path = temp_path / "club-users.db"
            documents_dir = temp_path / "documents"
            member_document_dir = documents_dir / "123"
            member_document_dir.mkdir(parents=True)
            (member_document_dir / "Guest Form.jpg").write_bytes(b"\xff\xd8\xff\xe0guest")
            (member_document_dir / "Driver License.jpg").write_bytes(
                b"\xff\xd8\xff\xe0synthetic-license"
            )
            flask_app = create_admin_app(db_path, str(documents_dir))
            client = admin_client(flask_app)
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

            response = client.get(f"/members/{member.id}")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("<h2>Guest Form</h2>", body)
        self.assertIn(f'/members/{member.id}/guest-form.jpg', body)
        self.assertIn("Driver License.jpg", body)
        self.assertIn(f'/members/{member.id}/document?name=Driver+License.jpg', body)

    def test_member_detail_can_attach_document_to_user_folder(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            db_path = temp_path / "club-users.db"
            documents_dir = temp_path / "documents"
            flask_app = create_admin_app(db_path, str(documents_dir))
            client = admin_client(flask_app)
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

            response = client.post(
                f"/members/{member.id}/documents",
                data={
                    "member_document": (
                        io.BytesIO(b"synthetic insurance card"),
                        "Insurance Card.pdf",
                    ),
                },
                content_type="multipart/form-data",
                follow_redirects=True,
            )
            duplicate_response = client.post(
                f"/members/{member.id}/documents",
                data={
                    "member_document": (
                        io.BytesIO(b"replacement insurance card"),
                        "Insurance Card.pdf",
                    ),
                },
                content_type="multipart/form-data",
                follow_redirects=True,
            )

            saved_document_path = documents_dir / "123" / "Insurance Card.pdf"
            duplicate_document_path = documents_dir / "123" / "Insurance Card 2.pdf"
            saved_document_bytes = saved_document_path.read_bytes()
            duplicate_document_bytes = duplicate_document_path.read_bytes()

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Attach Document", body)
        self.assertIn("Insurance Card.pdf", body)
        self.assertIn(
            f'/members/{member.id}/document?name=Insurance+Card.pdf',
            body,
        )
        self.assertEqual(saved_document_bytes, b"synthetic insurance card")
        self.assertEqual(duplicate_response.status_code, 200)
        self.assertEqual(
            duplicate_document_bytes,
            b"replacement insurance card",
        )

    def test_member_detail_guest_form_lookup_uses_card_folder_and_guest_form_prefix(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            db_path = temp_path / "club-users.db"
            documents_dir = temp_path / "documents"
            guest_form_dir = documents_dir / "1275"
            guest_form_dir.mkdir(parents=True)
            guest_form_path = guest_form_dir / "Guest Form_some older scanned name.jpeg"
            guest_form_path.write_bytes(b"\xff\xd8\xff\xe0test-jpeg")
            flask_app = create_admin_app(db_path, str(documents_dir))
            client = admin_client(flask_app)
            with closing(database.connect(db_path)) as connection:
                member_repository.upsert_member(
                    connection,
                    Member(
                        last_name="Example",
                        first_name="David",
                        card_number="1275",
                        membership="Visitor",
                    ),
                )
                connection.commit()
                member = member_repository.list_members(connection)[0]

            response = client.get(f"/members/{member.id}")
            image_response = client.get(f"/members/{member.id}/guest-form.jpg")
            image_bytes = image_response.get_data()
            image_response.close()

        self.assertEqual(response.status_code, 200)
        self.assertIn(f'/members/{member.id}/guest-form.jpg', response.get_data(as_text=True))
        self.assertEqual(image_response.status_code, 200)
        self.assertEqual(image_bytes, b"\xff\xd8\xff\xe0test-jpeg")

    def test_documents_report_requires_admin_login(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            flask_app = create_admin_app(db_path)
            client = flask_app.test_client()

            response = client.get("/documents/report")

        self.assertEqual(response.status_code, 302)
        self.assertIn("/admin/login", response.headers["Location"])

    def test_documents_report_handles_unconfigured_directory(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            flask_app = create_admin_app(db_path)
            client = admin_client(flask_app)

            response = client.get("/documents/report")

        self.assertEqual(response.status_code, 200)
        self.assertIn(
            "Document scanning is not configured.",
            response.get_data(as_text=True),
        )

    def test_documents_report_describes_guest_form_coverage_and_extra_entries(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            db_path = temp_path / "club-users.db"
            documents_dir = temp_path / "documents"
            documents_dir.mkdir()
            card_123_dir = documents_dir / "123"
            card_123_dir.mkdir()
            (card_123_dir / "Guest Form scan.JPG").write_bytes(b"\xff\xd8\xff\xe0test-jpeg")
            (card_123_dir / "waiver.pdf").write_text("synthetic waiver")
            (card_123_dir / ".DS_Store").write_text("synthetic finder metadata")
            card_456_dir = documents_dir / "456"
            card_456_dir.mkdir()
            (card_456_dir / "notes.txt").write_text("synthetic note")
            (card_456_dir / "other-photo.png").write_bytes(b"synthetic-png")
            card_999_dir = documents_dir / "999"
            card_999_dir.mkdir()
            (card_999_dir / "Guest Form_unknown.jpg").write_bytes(b"\xff\xd8\xff\xe0test-jpeg")
            (documents_dir / "top-level.txt").write_text("synthetic top-level note")
            (documents_dir / "Guest Form extra.jpg").write_bytes(b"\xff\xd8\xff\xe0test-jpeg")
            (documents_dir / ".DS_Store").write_text("synthetic finder metadata")
            flask_app = create_admin_app(db_path, str(documents_dir))
            client = admin_client(flask_app)
            with closing(database.connect(db_path)) as connection:
                for member in (
                    Member(
                        last_name="Doe",
                        first_name="John",
                        card_number="123",
                        membership="Visitor",
                    ),
                    Member(
                        last_name="Public",
                        first_name="Jane",
                        card_number="456",
                        membership="Full Member",
                    ),
                    Member(
                        last_name="Example",
                        first_name="Sam",
                        card_number="789",
                        membership="Visitor",
                    ),
                ):
                    member_repository.upsert_member(connection, member)
                connection.commit()

            response = client.get("/documents/report")
            body = response.get_data(as_text=True)
            matched_image_response = client.get(
                "/documents/image",
                query_string={"folder": "456", "name": "other-photo.png"},
            )
            matched_image_bytes = matched_image_response.get_data()
            matched_image_response.close()
            unmatched_image_response = client.get(
                "/documents/image",
                query_string={"folder": "999", "name": "Guest Form_unknown.jpg"},
            )
            unmatched_image_bytes = unmatched_image_response.get_data()
            unmatched_image_response.close()
            non_image_response = client.get(
                "/documents/image",
                query_string={"folder": "456", "name": "notes.txt"},
            )

        self.assertEqual(response.status_code, 200)
        self.assertIn("Documents Report", body)
        self.assertIn("Filename Patterns", body)
        self.assertIn("Extension Counts", body)
        self.assertIn("With Guest Form", body)
        self.assertIn("Without Guest Form", body)
        self.assertIn("Public", body)
        self.assertIn("Jane", body)
        self.assertIn("Example", body)
        self.assertIn("Sam", body)
        self.assertIn("waiver.pdf", body)
        self.assertIn("notes.txt", body)
        self.assertIn("other-photo.png", body)
        self.assertIn("999", body)
        self.assertIn("Guest Form_unknown.jpg", body)
        self.assertIn("/documents/image", body)
        self.assertIn("top-level.txt", body)
        self.assertIn("Guest Form &lt;text&gt;.jpg", body)
        self.assertIn("Guest Form_&lt;text&gt;.jpg", body)
        self.assertIn(".jpg", body)
        self.assertIn(".png", body)
        self.assertNotIn(".DS_Store", body)
        self.assertNotIn(str(documents_dir), body)
        self.assertEqual(matched_image_response.status_code, 200)
        self.assertEqual(matched_image_bytes, b"synthetic-png")
        self.assertEqual(unmatched_image_response.status_code, 200)
        self.assertEqual(unmatched_image_bytes, b"\xff\xd8\xff\xe0test-jpeg")
        self.assertEqual(non_image_response.status_code, 404)

    def test_member_detail_returns_404_for_missing_user(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            flask_app = create_admin_app(db_path)
            client = admin_client(flask_app)

            response = client.get("/members/999")

        self.assertEqual(response.status_code, 404)

    def test_edit_member_updates_user_and_records_audit_log(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            flask_app = create_admin_app(db_path)
            client = admin_client(flask_app)
            with closing(database.connect(db_path)) as connection:
                member_repository.upsert_member(
                    connection,
                    Member(
                        last_name="Doe",
                        first_name="John",
                        card_number="123",
                        membership="Visitor",
                        nickname="Old Nickname",
                        email="old@example.test",
                    ),
                )
                connection.commit()
                member = member_repository.list_members(connection)[0]

            response = client.post(
                f"/members/{member.id}/edit",
                data={
                    "last_name": "Doe",
                    "first_name": "John",
                    "nickname": "Johnny",
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
        self.assertEqual(updated_member.nickname, "Johnny")
        self.assertEqual(updated_member.email, "new@example.test")
        changed_fields = {entry.field_name for entry in audit_entries}
        self.assertIn("membership", changed_fields)
        self.assertIn("nickname", changed_fields)
        self.assertIn("email", changed_fields)

    def test_edit_member_renders_membership_dropdown(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            flask_app = create_admin_app(db_path)
            client = admin_client(flask_app)
            with closing(database.connect(db_path)) as connection:
                member_repository.upsert_member(
                    connection,
                    Member(
                        last_name="Doe",
                        first_name="John",
                        card_number="123",
                        membership="Associate Member",
                    ),
                )
                connection.commit()
                member = member_repository.list_members(connection)[0]

            response = client.get(f"/members/{member.id}/edit")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn('<select name="membership" required>', body)
        self.assertIn('<option value="AANR Member"', body)
        self.assertIn('<option value="Associate Member" selected', body)
        self.assertIn('<option value="Full Member"', body)
        self.assertIn('<option value="Visitor"', body)
        self.assertNotIn('name="membership" value=', body)

    def test_edit_member_rejects_unknown_membership(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            flask_app = create_admin_app(db_path)
            client = admin_client(flask_app)
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

            response = client.post(
                f"/members/{member.id}/edit",
                data={
                    "last_name": "Doe",
                    "first_name": "John",
                    "nickname": "",
                    "card_number": "123",
                    "membership": "Invented Member",
                    "address": "",
                    "address2": "",
                    "city": "",
                    "state": "",
                    "zip": "",
                    "phone": "",
                    "email": "",
                    "work_phone": "",
                    "cell_phone": "",
                },
            )

            with closing(database.connect(db_path)) as connection:
                unchanged_member = member_repository.get_member(connection, member.id)

        self.assertEqual(response.status_code, 400)
        self.assertIn("Choose a valid membership.", response.get_data(as_text=True))
        self.assertIsNotNone(unchanged_member)
        assert unchanged_member is not None
        self.assertEqual(unchanged_member.membership, "Visitor")

    def test_edit_member_renders_checkin_editor(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            flask_app = create_admin_app(db_path)
            client = admin_client(flask_app)
            with closing(database.connect(db_path)) as connection:
                checkin_repository.upsert_checkin(
                    connection,
                    CheckIn(
                        member_id="880",
                        last_name="Doe",
                        first_name="John",
                        card_number="123",
                        check_in_at=datetime(2026, 5, 3, 15, 59, 20),
                        membership="Visitor",
                    ),
                )
                connection.commit()
                member = member_repository.list_members(connection)[0]
                checkin = checkin_repository.list_checkins_for_user(connection, member.id)[0]

            response = client.get(f"/members/{member.id}/edit")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Check-ins", body)
        self.assertIn(f'name="checkin_{checkin.id}_check_in_at"', body)
        self.assertIn('value="2026-05-03T15:59:20"', body)
        self.assertIn(f'name="delete_checkin_{checkin.id}"', body)
        self.assertIn('name="new_checkin_at"', body)
        self.assertIn('data-member-edit-form', body)
        self.assertIn('data-checkin-dirty-field', body)
        self.assertIn("After changing check-in times, click Save Changes", body)
        self.assertIn("beforeunload", body)
        self.assertIn('event.key !== "Enter"', body)
        self.assertIn("event.preventDefault();", body)
        self.assertNotIn("addEventListener(&#34;input&#34;", body)
        self.assertNotIn("data-checkin-dirty-notice", body)
        self.assertNotIn("has-unsaved-checkins", body)

    def test_edit_member_can_modify_add_and_delete_checkins(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            flask_app = create_admin_app(db_path)
            client = admin_client(flask_app)
            with closing(database.connect(db_path)) as connection:
                for check_in_at in (
                    datetime(2026, 5, 1, 9, 0, 0),
                    datetime(2026, 5, 3, 15, 59, 20),
                ):
                    checkin_repository.upsert_checkin(
                        connection,
                        CheckIn(
                            member_id="880",
                            last_name="Doe",
                            first_name="John",
                            card_number="123",
                            check_in_at=check_in_at,
                            membership="Visitor",
                        ),
                    )
                connection.commit()
                member = member_repository.list_members(connection)[0]
                checkins = checkin_repository.list_checkins_for_user(connection, member.id)
                deleted_checkin = checkins[0]
                edited_checkin = checkins[1]

            response = client.post(
                f"/members/{member.id}/edit",
                data={
                    "last_name": "Doe",
                    "first_name": "John",
                    "nickname": "",
                    "card_number": "123",
                    "membership": "Full Member",
                    "address": "",
                    "address2": "",
                    "city": "",
                    "state": "",
                    "zip": "",
                    "phone": "",
                    "email": "",
                    "work_phone": "",
                    "cell_phone": "",
                    f"checkin_{deleted_checkin.id}_check_in_at": "2026-05-03T15:59:20",
                    f"delete_checkin_{deleted_checkin.id}": "1",
                    f"checkin_{edited_checkin.id}_check_in_at": "2026-05-02T10:30:00",
                    "new_checkin_at": "2026-05-04T18:45:00",
                },
            )

            with closing(database.connect(db_path)) as connection:
                saved_member = member_repository.get_member(connection, member.id)
                saved_checkins = checkin_repository.list_checkins_for_user(connection, member.id)
                audit_entries = audit_repository.list_audit_log_for_entity(
                    connection,
                    entity_type="user",
                    entity_id=member.id,
                )

            detail_response = client.get(f"/members/{member.id}")

        self.assertEqual(response.status_code, 302)
        self.assertIsNotNone(saved_member)
        assert saved_member is not None
        self.assertEqual(saved_member.membership, "Full Member")
        self.assertEqual(
            {checkin.check_in_at for checkin in saved_checkins},
            {
                datetime(2026, 5, 2, 10, 30, 0),
                datetime(2026, 5, 4, 18, 45, 0),
            },
        )
        self.assertEqual({checkin.membership for checkin in saved_checkins}, {"Full Member"})
        self.assertEqual({checkin.member_id for checkin in saved_checkins}, {"880"})
        checkin_audit = {
            entry.field_name: (entry.old_value, entry.new_value)
            for entry in audit_entries
            if entry.field_name.startswith("check-in ")
        }
        self.assertEqual(
            checkin_audit,
            {
                "check-in deleted": ("2026-05-03 15:59:20", None),
                "check-in edited": ("2026-05-01 09:00:00", "2026-05-02 10:30:00"),
                "check-in added": (None, "2026-05-04 18:45:00"),
            },
        )
        self.assertEqual(detail_response.status_code, 200)
        detail_body = detail_response.get_data(as_text=True)
        self.assertNotIn("check-in deleted", detail_body)
        self.assertNotIn("check-in edited", detail_body)
        self.assertNotIn("check-in added", detail_body)
        self.assertIn("2026-05-02 10:30:00", detail_body)
        self.assertIn("2026-05-04 18:45:00", detail_body)

    def test_edit_member_rejects_bad_checkin_datetime(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            flask_app = create_admin_app(db_path)
            client = admin_client(flask_app)
            with closing(database.connect(db_path)) as connection:
                checkin_repository.upsert_checkin(
                    connection,
                    CheckIn(
                        member_id="880",
                        last_name="Doe",
                        first_name="John",
                        card_number="123",
                        check_in_at=datetime(2026, 5, 1, 9, 0, 0),
                        membership="Visitor",
                    ),
                )
                connection.commit()
                member = member_repository.list_members(connection)[0]
                checkin = checkin_repository.list_checkins_for_user(connection, member.id)[0]

            response = client.post(
                f"/members/{member.id}/edit",
                data={
                    "last_name": "Doe",
                    "first_name": "John",
                    "nickname": "",
                    "card_number": "123",
                    "membership": "Visitor",
                    "address": "",
                    "address2": "",
                    "city": "",
                    "state": "",
                    "zip": "",
                    "phone": "",
                    "email": "",
                    "work_phone": "",
                    "cell_phone": "",
                    f"checkin_{checkin.id}_check_in_at": "not a date",
                    "new_checkin_at": "",
                },
            )

        self.assertEqual(response.status_code, 400)
        self.assertIn("Enter a valid check-in date and time.", response.get_data(as_text=True))

    def test_member_detail_shows_audit_log_entries(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            flask_app = create_admin_app(db_path)
            client = admin_client(flask_app)
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

            response = client.get(f"/members/{member.id}")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Change Log", body)
        self.assertIn("Full Member", body)

    def test_member_detail_hides_checkin_audit_log_entries(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            flask_app = create_admin_app(db_path)
            client = admin_client(flask_app)
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
                    field_name="check-in edited",
                    old_value=datetime(2026, 5, 1, 9, 0, 0),
                    new_value=datetime(2026, 5, 2, 10, 30, 0),
                )
                connection.commit()

            response = client.get(f"/members/{member.id}")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Change Log", body)
        self.assertIn("No changes recorded yet.", body)
        self.assertNotIn("check-in edited", body)

    def test_admin_guest_registration_queue_and_filled_form_use_definition_file(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            db_path = temp_path / "club-users.db"
            documents_dir = temp_path / "documents"
            documents_dir.mkdir()
            definition_path = temp_path / "guest-form.toml"
            definition_path.write_text(
                "\n".join(
                    [
                        'title = "Visitor Intake"',
                        'subtitle = "Please review before signing"',
                        'version = "FORM-TEST"',
                        "",
                        "[labels]",
                        'name = "Legal Name"',
                        'heard_about = "Referral Source"',
                        "",
                        "[agreement]",
                        'title = "House Agreement"',
                        'paragraphs = ["Custom agreement paragraph."]',
                    ]
                ),
                encoding="utf-8",
            )
            flask_app = create_admin_app(
                db_path,
                documents_dir=str(documents_dir),
                guest_form_definition_path=str(definition_path),
            )
            visitor_client = flask_app.test_client()
            visitor_client.post(
                "/guest-registration",
                data={
                    "visit_date": "2026-05-14",
                    "last_name": "Doe",
                    "first_name": "John",
                    "date_of_birth": "1990-06-15",
                    "middle_name": "Q",
                    "nickname": "Johnny",
                    "cell_phone": "510-510-5100",
                    "email": "john@example.test",
                    "address": "123 Main St",
                    "city": "Everytown",
                    "state": "CA",
                    "zip": "94000",
                    "marital_status": "single",
                    "heard_about": "Friend",
                },
            )
            client = admin_client(flask_app)

            queue_response = client.get("/guest-registrations")
            queue_body = queue_response.get_data(as_text=True)
            with closing(database.connect(db_path)) as connection:
                record = guest_registration_repository.list_guest_registration_records(
                    connection
                )[0]
            upload_image = Image.new("RGB", (1600, 900), "white")
            upload_buffer = io.BytesIO()
            upload_image.save(upload_buffer, format="PNG")
            upload_buffer.seek(0)
            upload_response = client.post(
                f"/guest-registrations/{record.registration.id}/driver-license",
                data={
                    "driver_license": (upload_buffer, "scan.png"),
                },
                content_type="multipart/form-data",
            )
            form_response = client.get(
                f"/guest-registrations/{record.registration.id}/form"
            )
            saved_license_path = (
                documents_dir / record.member.card_number / "Driver License.jpg"
            )
            with Image.open(saved_license_path) as saved_license_image:
                saved_license_size = saved_license_image.size

        self.assertEqual(upload_response.status_code, 302)
        self.assertEqual(saved_license_size, (2026, 1152))
        self.assertEqual(queue_response.status_code, 200)
        self.assertIn("Guest Registrations", queue_body)
        self.assertIn("Print Form", queue_body)
        self.assertIn("Attach ID", queue_body)
        self.assertNotIn(str(definition_path), queue_body)
        self.assertEqual(form_response.status_code, 200)
        form_body = form_response.get_data(as_text=True)
        self.assertIn("Visitor Intake", form_body)
        self.assertIn("Please review before signing", form_body)
        self.assertIn("Legal Name", form_body)
        self.assertIn("Referral Source", form_body)
        self.assertIn("House Agreement", form_body)
        self.assertIn("Custom agreement paragraph.", form_body)
        self.assertIn("FORM-TEST", form_body)
        self.assertIn('class="driver-license-slot"', form_body)
        self.assertIn(
            f'/members/{record.member.id}/document?name=Driver+License.jpg',
            form_body,
        )
        self.assertIn("Replace ID Image", form_body)
        self.assertIn("John", form_body)
        self.assertIn("Johnny", form_body)
        self.assertIn("1990-06-15", form_body)
        self.assertIn("Friend", form_body)
        self.assertNotIn(str(definition_path), form_body)
        self.assertNotIn(str(documents_dir), form_body)

    def test_admin_guest_registration_queue_exposes_live_refresh_payload(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            flask_app = create_admin_app(db_path)
            visitor_client = flask_app.test_client()
            visitor_client.post(
                "/guest-registration",
                data={
                    "last_name": "Doe",
                    "first_name": "John",
                    "date_of_birth": "1990-06-15",
                    "cell_phone": "510-510-5100",
                    "address": "123 Main St",
                    "city": "Everytown",
                    "state": "CA",
                    "zip": "94000",
                    "marital_status": "single",
                },
            )
            client = admin_client(flask_app)

            queue_response = client.get("/guest-registrations")
            recent_response = client.get("/guest-registrations/recent")

        self.assertEqual(queue_response.status_code, 200)
        queue_body = queue_response.get_data(as_text=True)
        self.assertIn("data-guest-registrations", queue_body)
        self.assertIn("data-recent-url", queue_body)
        self.assertIn("data-enable-guest-alerts", queue_body)
        self.assertIn("Watching for new submissions.", queue_body)
        self.assertEqual(recent_response.status_code, 200)
        payload = recent_response.get_json()
        assert payload is not None
        self.assertEqual(payload["count"], 1)
        self.assertEqual(payload["latest_guest_name"], "John Doe")
        self.assertEqual(len(payload["registration_ids"]), 1)
        self.assertIn("John", payload["rows_html"])
        self.assertIn("Print Form", payload["rows_html"])
        self.assertIn("Attach ID", payload["rows_html"])

    def test_driver_license_upload_preserves_off_center_image_content(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            db_path = temp_path / "club-users.db"
            documents_dir = temp_path / "documents"
            documents_dir.mkdir()
            flask_app = create_admin_app(db_path, documents_dir=str(documents_dir))
            visitor_client = flask_app.test_client()
            visitor_client.post(
                "/guest-registration",
                data={
                    "last_name": "Doe",
                    "first_name": "John",
                    "date_of_birth": "1990-06-15",
                    "cell_phone": "510-510-5100",
                    "address": "123 Main St",
                    "city": "Everytown",
                    "state": "CA",
                    "zip": "94000",
                    "marital_status": "single",
                },
            )
            with closing(database.connect(db_path)) as connection:
                record = guest_registration_repository.list_guest_registration_records(
                    connection
                )[0]

            upload_image = Image.new("RGB", (2000, 1200), "white")
            draw = ImageDraw.Draw(upload_image)
            draw.line((520, 0, 520, 1199), fill=(225, 225, 225), width=2)
            draw.rectangle((40, 40, 700, 420), fill=(30, 90, 180))
            draw.rectangle((90, 110, 650, 180), fill=(245, 245, 245))
            draw.rectangle((90, 240, 460, 300), fill=(245, 245, 245))
            upload_buffer = io.BytesIO()
            upload_image.save(upload_buffer, format="PNG")
            upload_buffer.seek(0)

            response = admin_client(flask_app).post(
                f"/guest-registrations/{record.registration.id}/driver-license",
                data={"driver_license": (upload_buffer, "scan.png")},
                content_type="multipart/form-data",
            )

            saved_license_path = (
                documents_dir / record.member.card_number / "Driver License.jpg"
            )
            with Image.open(saved_license_path) as saved_license_image:
                saved_size = saved_license_image.size
                grayscale = saved_license_image.convert("L")
                content_mask = grayscale.point(lambda value: 255 if value < 245 else 0)
                content_bbox = content_mask.getbbox()
                stats = ImageStat.Stat(grayscale)

        self.assertEqual(response.status_code, 302)
        self.assertEqual(saved_size, (2026, 1152))
        self.assertIsNotNone(content_bbox)
        assert content_bbox is not None
        self.assertGreater(content_bbox[2] - content_bbox[0], 1450)
        self.assertGreater(content_bbox[3] - content_bbox[1], 900)
        self.assertLess(stats.mean[0], 245)

    def test_driver_license_upload_ignores_scanner_edge_shadows(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            db_path = temp_path / "club-users.db"
            documents_dir = temp_path / "documents"
            documents_dir.mkdir()
            flask_app = create_admin_app(db_path, documents_dir=str(documents_dir))
            visitor_client = flask_app.test_client()
            visitor_client.post(
                "/guest-registration",
                data={
                    "last_name": "Doe",
                    "first_name": "John",
                    "date_of_birth": "1990-06-15",
                    "cell_phone": "510-510-5100",
                    "address": "123 Main St",
                    "city": "Everytown",
                    "state": "CA",
                    "zip": "94000",
                    "marital_status": "single",
                },
            )
            with closing(database.connect(db_path)) as connection:
                record = guest_registration_repository.list_guest_registration_records(
                    connection
                )[0]

            upload_image = Image.new("RGB", (2600, 1600), "white")
            draw = ImageDraw.Draw(upload_image)
            draw.rectangle((0, 0, 2599, 34), fill=(205, 205, 205))
            draw.rectangle((0, 0, 34, 1599), fill=(205, 205, 205))
            draw.rectangle((2535, 0, 2599, 1599), fill=(214, 214, 214))
            draw.rectangle((0, 1538, 2599, 1599), fill=(214, 214, 214))
            draw.rectangle((34, 34, 880, 570), fill=(252, 252, 252), outline=(70, 70, 70), width=3)
            draw.rectangle((80, 100, 330, 390), fill=(40, 90, 160))
            draw.rectangle((380, 120, 800, 170), fill=(30, 30, 30))
            draw.rectangle((380, 225, 760, 265), fill=(70, 70, 70))
            draw.rectangle((380, 320, 810, 360), fill=(70, 70, 70))
            for y in range(850, 1420, 60):
                for x in range(420, 2100, 90):
                    draw.rectangle((x, y, x + 8, y + 8), fill=(212, 212, 212))
            upload_buffer = io.BytesIO()
            upload_image.save(upload_buffer, format="PNG")
            upload_buffer.seek(0)

            response = admin_client(flask_app).post(
                f"/guest-registrations/{record.registration.id}/driver-license",
                data={"driver_license": (upload_buffer, "scan.png")},
                content_type="multipart/form-data",
            )

            saved_license_path = (
                documents_dir / record.member.card_number / "Driver License.jpg"
            )
            with Image.open(saved_license_path) as saved_license_image:
                saved_size = saved_license_image.size
                grayscale = saved_license_image.convert("L")
                content_mask = grayscale.point(lambda value: 255 if value < 245 else 0)
                content_bbox = content_mask.getbbox()
                rgb_image = saved_license_image.convert("RGB")
                blue_mask = Image.new("L", rgb_image.size)
                rgb_pixels = (
                    rgb_image.get_flattened_data()
                    if hasattr(rgb_image, "get_flattened_data")
                    else rgb_image.getdata()
                )
                blue_mask.putdata([
                    255 if blue > red + 40 and blue > green + 20 else 0
                    for red, green, blue in rgb_pixels
                ])
                blue_bbox = blue_mask.getbbox()

        self.assertEqual(response.status_code, 302)
        self.assertEqual(saved_size, (2026, 1152))
        self.assertIsNotNone(content_bbox)
        assert content_bbox is not None
        self.assertGreater(content_bbox[2] - content_bbox[0], 1600)
        self.assertGreater(content_bbox[3] - content_bbox[1], 950)
        self.assertIsNotNone(blue_bbox)
        assert blue_bbox is not None
        self.assertGreater(blue_bbox[2] - blue_bbox[0], 450)
        self.assertGreater(blue_bbox[3] - blue_bbox[1], 500)

    def test_driver_license_upload_crops_dark_landscape_scanner_background(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            db_path = temp_path / "club-users.db"
            documents_dir = temp_path / "documents"
            documents_dir.mkdir()
            flask_app = create_admin_app(db_path, documents_dir=str(documents_dir))
            visitor_client = flask_app.test_client()
            visitor_client.post(
                "/guest-registration",
                data={
                    "last_name": "Doe",
                    "first_name": "John",
                    "date_of_birth": "1990-06-15",
                    "cell_phone": "510-510-5100",
                    "address": "123 Main St",
                    "city": "Everytown",
                    "state": "CA",
                    "zip": "94000",
                    "marital_status": "single",
                },
            )
            with closing(database.connect(db_path)) as connection:
                record = guest_registration_repository.list_guest_registration_records(
                    connection
                )[0]

            upload_image = Image.new("RGB", (3000, 1800), (5, 5, 5))
            draw = ImageDraw.Draw(upload_image)
            draw.rectangle((85, 70, 1090, 705), fill=(250, 250, 244), outline=(70, 70, 70), width=4)
            draw.rectangle((135, 150, 430, 500), fill=(40, 90, 160))
            draw.rectangle((500, 165, 1000, 215), fill=(30, 30, 30))
            draw.rectangle((500, 300, 960, 345), fill=(70, 70, 70))
            draw.rectangle((500, 430, 1040, 475), fill=(70, 70, 70))
            for y in range(1200, 1680, 65):
                for x in range(1500, 2800, 100):
                    draw.rectangle((x, y, x + 7, y + 7), fill=(36, 36, 36))
            upload_buffer = io.BytesIO()
            upload_image.save(upload_buffer, format="PNG")
            upload_buffer.seek(0)

            response = admin_client(flask_app).post(
                f"/guest-registrations/{record.registration.id}/driver-license",
                data={"driver_license": (upload_buffer, "scan.png")},
                content_type="multipart/form-data",
            )

            saved_license_path = (
                documents_dir / record.member.card_number / "Driver License.jpg"
            )
            with Image.open(saved_license_path) as saved_license_image:
                saved_size = saved_license_image.size
                rgb_image = saved_license_image.convert("RGB")
                blue_mask = Image.new("L", rgb_image.size)
                rgb_pixels = (
                    rgb_image.get_flattened_data()
                    if hasattr(rgb_image, "get_flattened_data")
                    else rgb_image.getdata()
                )
                blue_mask.putdata([
                    255 if blue > red + 40 and blue > green + 20 else 0
                    for red, green, blue in rgb_pixels
                ])
                blue_bbox = blue_mask.getbbox()

        self.assertEqual(response.status_code, 302)
        self.assertEqual(saved_size, (2026, 1152))
        self.assertIsNotNone(blue_bbox)
        assert blue_bbox is not None
        self.assertGreater(blue_bbox[2] - blue_bbox[0], 400)
        self.assertGreater(blue_bbox[3] - blue_bbox[1], 500)

    def test_driver_license_repair_recrops_existing_white_canvas_file(self) -> None:
        stored_scan = Image.new("RGB", (2026, 1152), "white")
        draw = ImageDraw.Draw(stored_scan)
        draw.rectangle((0, 0, 24, 1151), fill=(224, 224, 224))
        draw.rectangle((1998, 0, 2025, 1151), fill=(224, 224, 224))
        draw.rectangle((775, 20, 1240, 315), fill=(252, 252, 252), outline=(70, 70, 70), width=2)
        draw.rectangle((810, 68, 945, 225), fill=(40, 90, 160))
        draw.rectangle((980, 80, 1200, 105), fill=(30, 30, 30))
        draw.rectangle((980, 145, 1180, 170), fill=(70, 70, 70))
        draw.rectangle((980, 210, 1205, 235), fill=(70, 70, 70))
        for y in range(520, 1000, 70):
            for x in range(600, 1500, 95):
                draw.rectangle((x, y, x + 5, y + 5), fill=(218, 218, 218))

        repaired = _prepare_stored_driver_license_image(stored_scan)
        rgb_image = repaired.convert("RGB")
        blue_mask = Image.new("L", rgb_image.size)
        rgb_pixels = (
            rgb_image.get_flattened_data()
            if hasattr(rgb_image, "get_flattened_data")
            else rgb_image.getdata()
        )
        blue_mask.putdata([
            255 if blue > red + 40 and blue > green + 20 else 0
            for red, green, blue in rgb_pixels
        ])
        blue_bbox = blue_mask.getbbox()

        self.assertEqual(repaired.size, (2026, 1152))
        self.assertIsNotNone(blue_bbox)
        assert blue_bbox is not None
        self.assertGreater(blue_bbox[2] - blue_bbox[0], 450)
        self.assertGreater(blue_bbox[3] - blue_bbox[1], 500)

    def test_driver_license_repair_recrops_existing_dark_canvas_file(self) -> None:
        stored_scan = Image.new("RGB", (2026, 1152), (5, 5, 5))
        draw = ImageDraw.Draw(stored_scan)
        draw.rectangle((65, 45, 690, 440), fill=(250, 250, 244), outline=(70, 70, 70), width=2)
        draw.rectangle((95, 95, 275, 310), fill=(40, 90, 160))
        draw.rectangle((320, 105, 640, 135), fill=(30, 30, 30))
        draw.rectangle((320, 205, 610, 235), fill=(70, 70, 70))
        draw.rectangle((320, 305, 655, 335), fill=(70, 70, 70))
        for y in range(700, 1040, 60):
            for x in range(1050, 1840, 95):
                draw.rectangle((x, y, x + 5, y + 5), fill=(36, 36, 36))

        repaired = _prepare_stored_driver_license_image(stored_scan)
        rgb_image = repaired.convert("RGB")
        blue_mask = Image.new("L", rgb_image.size)
        rgb_pixels = (
            rgb_image.get_flattened_data()
            if hasattr(rgb_image, "get_flattened_data")
            else rgb_image.getdata()
        )
        blue_mask.putdata([
            255 if blue > red + 40 and blue > green + 20 else 0
            for red, green, blue in rgb_pixels
        ])
        blue_bbox = blue_mask.getbbox()

        self.assertEqual(repaired.size, (2026, 1152))
        self.assertIsNotNone(blue_bbox)
        assert blue_bbox is not None
        self.assertGreater(blue_bbox[2] - blue_bbox[0], 450)
        self.assertGreater(blue_bbox[3] - blue_bbox[1], 500)
