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
from club_admin.app import create_app
from club_admin.models import CheckIn, Member
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
        self.assertIn("Self Check-in", response.get_data(as_text=True))

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
        self.assertIn('name="checkins_csv"', imports_body)

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
        self.assertEqual(new_user.phone, "123-123-1234")
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].registration.middle_name, "Q")
        self.assertTrue(records[0].registration.guest_of_member)
        self.assertTrue(records[0].registration.newsletter_opt_out)

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
        self.assertNotIn('class="file-field"', body)
        self.assertNotIn("Users CSV", body)
        self.assertNotIn("Check-ins CSV", body)
        self.assertIn("Nickname", body)
        self.assertIn("Johnny", body)
        self.assertIn("123 Main St", body)
        self.assertIn("Unit 4", body)
        self.assertIn("Everytown CA 94000", body)
        self.assertIn("2026-05-03 15:59:20", body)
        self.assertIn("Docs", body)
        self.assertIn(">2<", body)
        self.assertIn('<td class="numeric"></td>', body)
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
                    "middle_name": "Q",
                    "nickname": "Johnny",
                    "cell_phone": "510-510-5100",
                    "email": "john@example.test",
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
        self.assertIn("Friend", form_body)
        self.assertNotIn(str(definition_path), form_body)
        self.assertNotIn(str(documents_dir), form_body)

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
                    "cell_phone": "510-510-5100",
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
        self.assertGreater(content_bbox[2] - content_bbox[0], 1600)
        self.assertGreater(content_bbox[3] - content_bbox[1], 900)
        self.assertLess(stats.mean[0], 245)
