import io
import os
import sqlite3
import sys
import tempfile
from contextlib import closing
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

from werkzeug.security import generate_password_hash


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from club_admin import checkin_repository
from club_admin import database
from club_admin import member_repository
from club_admin.app import (
    _barcode_secret_for_connection,
    _barcode_token_for_card_number,
    create_app,
)
from club_admin.models import CheckIn, Member
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


def checkin_fixture(
    *,
    member_id: str = "880",
    last_name: str = "Doe",
    first_name: str = "John",
    card_number: str = "1861",
    check_in_at: datetime = datetime(2026, 5, 3, 15, 59, 20),
    membership: str = "Visitor",
) -> CheckIn:
    return CheckIn(
        member_id=member_id,
        last_name=last_name,
        first_name=first_name,
        card_number=card_number,
        check_in_at=check_in_at,
        total_checkins=1,
        membership=membership,
    )


def checkin_range_fixtures() -> list[CheckIn]:
    return [
        checkin_fixture(check_in_at=datetime(2026, 5, 1, 9, 0, 0)),
        checkin_fixture(check_in_at=datetime(2026, 5, 3, 15, 59, 20)),
        checkin_fixture(
            member_id="35",
            first_name="Jane",
            card_number="1024",
            check_in_at=datetime(2026, 5, 4, 14, 33, 15),
            membership="Full Member",
        ),
    ]


class ClubCheckInImportTests(unittest.TestCase):
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
            checkin = checkin_fixture()
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

    def test_upsert_checkin_does_not_replace_existing_user_membership(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            database.init_db(db_path)
            checkin = checkin_fixture()
            with closing(database.connect(db_path)) as connection:
                member_repository.upsert_member(
                    connection,
                    Member(
                        last_name="Doe",
                        first_name="John",
                        card_number="1861",
                        membership="Full Member",
                    ),
                )
                checkin_repository.upsert_checkin(connection, checkin)
                connection.commit()

                users = member_repository.list_members(connection)
                checkins = checkin_repository.list_checkins(connection)

        self.assertEqual(users[0].membership, "Full Member")
        self.assertEqual(checkins[0].membership, "Visitor")

    def test_upsert_checkin_creates_user_when_roster_is_not_imported(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            database.init_db(db_path)
            checkin = checkin_fixture()
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
            checkin = checkin_fixture()
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
            checkins_to_import = checkin_range_fixtures()
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

    def test_lists_checkin_report_rows_by_name_before_checkin_time(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            database.init_db(db_path)
            checkins_to_import = [
                checkin_fixture(
                    member_id="91",
                    last_name="Zebra",
                    first_name="Zoe",
                    card_number="2091",
                    check_in_at=datetime(2026, 5, 3, 15, 59, 20),
                ),
                checkin_fixture(
                    member_id="42",
                    last_name="Adams",
                    first_name="Ada",
                    card_number="1042",
                    check_in_at=datetime(2026, 5, 3, 15, 40, 0),
                ),
                checkin_fixture(
                    member_id="91",
                    last_name="Zebra",
                    first_name="Zoe",
                    card_number="2091",
                    check_in_at=datetime(2026, 5, 3, 15, 30, 0),
                ),
            ]
            with closing(database.connect(db_path)) as connection:
                for checkin in checkins_to_import:
                    checkin_repository.upsert_checkin(connection, checkin)
                connection.commit()

                checkins = checkin_repository.list_checkins_for_date_range(
                    connection,
                    datetime(2026, 5, 3).date(),
                    datetime(2026, 5, 3).date(),
                )

        self.assertEqual(
            [(checkin.last_name, checkin.check_in_at) for checkin in checkins],
            [
                ("Adams", datetime(2026, 5, 3, 15, 40, 0)),
                ("Zebra", datetime(2026, 5, 3, 15, 59, 20)),
                ("Zebra", datetime(2026, 5, 3, 15, 30, 0)),
            ],
        )

    def test_checkin_report_uses_user_nickname_when_available(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            database.init_db(db_path)
            checkin = checkin_fixture()
            with closing(database.connect(db_path)) as connection:
                member_repository.upsert_member(
                    connection,
                    Member(
                        last_name="Doe",
                        first_name="John",
                        nickname="Johnny",
                        card_number="1861",
                        membership="Visitor",
                    ),
                )
                checkin_repository.upsert_checkin(connection, checkin)
                connection.commit()

                checkins = checkin_repository.list_checkins_for_date_range(
                    connection,
                    datetime(2026, 5, 3).date(),
                    datetime(2026, 5, 3).date(),
                )

        self.assertEqual(len(checkins), 1)
        self.assertEqual(checkins[0].first_name, "Johnny")

    def test_club_app_renders_sortable_checkin_report_for_date_range(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            flask_app = create_admin_app(db_path)
            client = admin_client(flask_app)
            checkins_to_import = checkin_range_fixtures()
            with closing(database.connect(db_path)) as connection:
                for checkin in checkins_to_import:
                    checkin_repository.upsert_checkin(connection, checkin)
                john = member_repository.get_member_by_card_number(connection, "1861")
                connection.commit()

            response = client.get(
                "/checkins/report?start_date=2026-05-01&end_date=2026-05-03"
            )

        self.assertEqual(response.status_code, 200)
        self.assertIsNotNone(john)
        body = response.get_data(as_text=True)
        self.assertIn("Check-ins", body)
        self.assertIn('class="page checkins-report-page"', body)
        self.assertIn('class="print-report-range">2026-05-01 to 2026-05-03', body)
        self.assertIn('class="report-date-fields"', body)
        self.assertIn('class="report-presets"', body)
        self.assertIn(">Today</a>", body)
        self.assertIn(">Yesterday</a>", body)
        self.assertIn(">This Week</a>", body)
        self.assertIn(">Last Week</a>", body)
        self.assertIn(">This Month</a>", body)
        self.assertIn(">Last Month</a>", body)
        self.assertIn('class="checkins-time-chart"', body)
        self.assertIn("Check-ins by Day", body)
        self.assertIn("Week of Apr 27", body)
        self.assertIn('class="checkins-chart-group-total">2 check-ins', body)
        self.assertIn("May 1", body)
        self.assertNotIn("May 2", body)
        self.assertIn("May 3", body)
        self.assertIn('class="checkins-chart-total">1</span>', body)
        self.assertNotIn('class="report-tabs"', body)
        self.assertNotIn('name="view"', body)
        self.assertIn('src="/static/club-admin-table-sort.js"', body)
        self.assertIn('class="checkins-table" data-sortable-table', body)
        self.assertIn('tr data-sortable-row', body)
        self.assertIn("Check-in #", body)
        self.assertIn("First/Nickname", body)
        self.assertNotIn(">Card #<", body)
        self.assertIn('role="button" tabindex="0" data-sort-column="0"', body)
        self.assertIn('data-sort-column="0" data-sort-type="date"', body)
        self.assertIn('data-sort-column="1" data-sort-type="number"', body)
        self.assertIn('data-sort-column="4" data-sort-type="text"', body)
        self.assertNotIn('button type="button" class="sortable-heading"', body)
        self.assertIn(f'href="/members/{john.id}"', body)
        self.assertIn("John", body)
        self.assertIn("2026-05-01 09:00:00", body)
        self.assertIn("2026-05-03 15:59:20", body)
        self.assertIn('<td class="numeric" data-sort-value="1">1</td>', body)
        self.assertIn('<td class="numeric" data-sort-value="2">2</td>', body)
        self.assertNotIn("Jane", body)

    def test_checkin_report_marks_active_date_preset(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            flask_app = create_admin_app(db_path)
            client = admin_client(flask_app)

            response = client.get("/checkins/report")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertEqual(body.count('aria-current="page"'), 1)
        self.assertIn('class="active" aria-current="page"', body)
        self.assertIn(">Today</a>", body)

    def test_checkin_report_shows_membership_breakdown_by_distinct_user(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            flask_app = create_admin_app(db_path)
            client = admin_client(flask_app)
            with closing(database.connect(db_path)) as connection:
                for card_number, first_name, last_name, membership in (
                    ("100", "Fran", "Full", "Full Member"),
                    ("200", "Annie", "Associate", "Associate Member"),
                    ("300", "Alex", "AANR", "AANR Member"),
                    ("400", "Vera", "Visitor", "Visitor"),
                ):
                    member_repository.upsert_member(
                        connection,
                        Member(
                            first_name=first_name,
                            last_name=last_name,
                            card_number=card_number,
                            membership=membership,
                        ),
                    )
                for card_number, first_name, last_name, membership, check_in_at in (
                    ("100", "Fran", "Full", "Full Member", datetime(2026, 5, 1, 9, 0, 0)),
                    ("200", "Annie", "Associate", "Associate Member", datetime(2026, 5, 1, 9, 5, 0)),
                    ("300", "Alex", "AANR", "AANR Member", datetime(2026, 5, 1, 9, 10, 0)),
                    ("400", "Vera", "Visitor", "Visitor", datetime(2026, 5, 1, 9, 15, 0)),
                    ("400", "Vera", "Visitor", "Visitor", datetime(2026, 5, 1, 10, 15, 0)),
                ):
                    checkin_repository.upsert_checkin(
                        connection,
                        CheckIn(
                            member_id=card_number,
                            first_name=first_name,
                            last_name=last_name,
                            card_number=card_number,
                            membership=membership,
                            check_in_at=check_in_at,
                        ),
                    )
                connection.commit()

            response = client.get(
                "/checkins/report?start_date=2026-05-01&end_date=2026-05-01"
            )

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn('class="checkins-membership-breakdown"', body)
        self.assertIn("Check-ins by Hour", body)
        self.assertIn("Full Member: 1", body)
        self.assertIn("Assoc.: 1", body)
        self.assertIn("AANR: 1", body)
        self.assertIn("Visitor: 1", body)
        self.assertIn('class="checkins-chart-total">4</span>', body)
        self.assertIn('class="checkins-chart-total">1</span>', body)
        self.assertNotIn('class="checkins-chart-counts"', body)
        self.assertNotIn("8 AM", body)
        self.assertIn("9 AM", body)
        self.assertIn("10 AM", body)
        self.assertNotIn("11 AM", body)
        self.assertIn('class="checkins-chart-segment membership-visitor"', body)

    def test_checkin_report_charts_checkins_by_lifetime_visit_number(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            flask_app = create_admin_app(db_path)
            client = admin_client(flask_app)
            with closing(database.connect(db_path)) as connection:
                checkins = [
                    checkin_fixture(
                        member_id="100",
                        card_number="100",
                        first_name="First",
                        last_name="Visitor",
                        check_in_at=datetime(2026, 5, 10, 9, 0, 0),
                    ),
                    checkin_fixture(
                        member_id="200",
                        card_number="200",
                        first_name="Second",
                        last_name="Visitor",
                        check_in_at=datetime(2026, 5, 1, 9, 0, 0),
                    ),
                    checkin_fixture(
                        member_id="200",
                        card_number="200",
                        first_name="Second",
                        last_name="Visitor",
                        check_in_at=datetime(2026, 5, 10, 9, 5, 0),
                    ),
                    checkin_fixture(
                        member_id="300",
                        card_number="300",
                        first_name="Another",
                        last_name="Second",
                        check_in_at=datetime(2026, 5, 2, 9, 10, 0),
                    ),
                    checkin_fixture(
                        member_id="300",
                        card_number="300",
                        first_name="Another",
                        last_name="Second",
                        check_in_at=datetime(2026, 5, 10, 9, 10, 0),
                    ),
                ]
                for day in range(1, 10):
                    checkins.append(
                        checkin_fixture(
                            member_id="900",
                            card_number="900",
                            first_name="Tenth",
                            last_name="Visitor",
                            check_in_at=datetime(2026, 4, day, 9, 0, 0),
                        )
                    )
                checkins.append(
                    checkin_fixture(
                        member_id="900",
                        card_number="900",
                        first_name="Tenth",
                        last_name="Visitor",
                        check_in_at=datetime(2026, 5, 10, 9, 15, 0),
                    )
                )
                for day in range(1, 11):
                    checkins.append(
                        checkin_fixture(
                            member_id="901",
                            card_number="901",
                            first_name="Eleventh",
                            last_name="Visitor",
                            check_in_at=datetime(2026, 4, day, 10, 0, 0),
                        )
                    )
                checkins.append(
                    checkin_fixture(
                        member_id="901",
                        card_number="901",
                        first_name="Eleventh",
                        last_name="Visitor",
                        check_in_at=datetime(2026, 5, 10, 9, 20, 0),
                    )
                )
                for checkin in checkins:
                    checkin_repository.upsert_checkin(connection, checkin)
                connection.commit()

            response = client.get(
                "/checkins/report?start_date=2026-05-10&end_date=2026-05-10"
            )

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Check-ins by Check-in Number", body)
        self.assertIn('class="checkins-time-chart checkins-visit-number-chart"', body)
        self.assertIn('aria-label="1: 1 check-in"', body)
        self.assertIn('aria-label="2: 2 check-ins"', body)
        self.assertNotIn('aria-label="9: 0 check-ins"', body)
        self.assertIn('aria-label="10+: 2 check-ins"', body)
        self.assertIn('aria-label="Check-in group legend"', body)
        self.assertIn('class="checkins-chart-key membership-full"', body)
        self.assertIn('class="checkins-chart-key membership-visitor"', body)
        self.assertIn('class="checkins-chart-segment membership-visitor"', body)

    def test_checkin_report_stacks_visit_number_chart_by_membership_type(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            flask_app = create_admin_app(db_path)
            client = admin_client(flask_app)
            with closing(database.connect(db_path)) as connection:
                for checkin in (
                    checkin_fixture(
                        member_id="100",
                        card_number="100",
                        first_name="Fran",
                        last_name="Full",
                        membership="Full Member",
                        check_in_at=datetime(2026, 5, 10, 9, 0, 0),
                    ),
                    checkin_fixture(
                        member_id="150",
                        card_number="150",
                        first_name="Annie",
                        last_name="Associate",
                        membership="Associate Member",
                        check_in_at=datetime(2026, 5, 10, 9, 3, 0),
                    ),
                    checkin_fixture(
                        member_id="200",
                        card_number="200",
                        first_name="Alex",
                        last_name="AANR",
                        membership="AANR Member",
                        check_in_at=datetime(2026, 5, 10, 9, 5, 0),
                    ),
                    checkin_fixture(
                        member_id="300",
                        card_number="300",
                        first_name="Vera",
                        last_name="Visitor",
                        membership="Visitor",
                        check_in_at=datetime(2026, 5, 10, 9, 10, 0),
                    ),
                ):
                    checkin_repository.upsert_checkin(connection, checkin)
                connection.commit()

            response = client.get(
                "/checkins/report?start_date=2026-05-10&end_date=2026-05-10"
            )

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Full Member: 1", body)
        self.assertIn("Assoc.: 1", body)
        self.assertIn("AANR: 1", body)
        self.assertIn("Visitor: 1", body)
        self.assertIn(">Full</a>", body)
        self.assertIn(">Associate</a>", body)
        self.assertIn(">AANR</a>", body)
        self.assertIn(">Visitor</a>", body)
        self.assertNotIn('class="chart-toggle-form"', body)
        self.assertNotIn('name="visit_number_membership"', body)
        self.assertNotIn("visit_number_membership=", body)
        self.assertIn('aria-label="1: 4 check-ins"', body)
        self.assertIn('title="Full Member: 1"', body)
        self.assertIn('title="Assoc.: 1"', body)
        self.assertIn('title="AANR: 1"', body)
        self.assertIn('title="Visitor: 1"', body)
        self.assertIn('class="checkins-chart-segment membership-full"', body)
        self.assertIn('class="checkins-chart-segment membership-assoc"', body)
        self.assertIn('class="checkins-chart-segment membership-aanr"', body)
        self.assertIn('class="checkins-chart-segment membership-visitor"', body)

    def test_club_app_renders_singular_checkin_report_count(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            flask_app = create_admin_app(db_path)
            client = admin_client(flask_app)
            checkin = checkin_fixture()
            with closing(database.connect(db_path)) as connection:
                checkin_repository.upsert_checkin(connection, checkin)
                connection.commit()

            response = client.get(
                "/checkins/report?start_date=2026-05-03&end_date=2026-05-03"
            )

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("1 check-in", body)
        self.assertNotIn("1 check-ins", body)

    def test_member_detail_lists_that_users_checkins(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            flask_app = create_admin_app(db_path)
            client = admin_client(flask_app)
            checkins_to_import = checkin_range_fixtures()
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
            checkins_to_import = [
                checkin_fixture(),
                checkin_fixture(
                    member_id="35",
                    first_name="Jane",
                    card_number="1024",
                    check_in_at=datetime(2026, 5, 3, 14, 33, 15),
                    membership="Full Member",
                ),
            ]
            with closing(database.connect(db_path)) as connection:
                for checkin in checkins_to_import:
                    checkin_repository.upsert_checkin(connection, checkin)
                connection.commit()

            response = client.get("/members")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertNotIn("Recent Check-ins", body)
        self.assertIn("First Visit", body)
        self.assertIn("Last Visit", body)
        self.assertIn("2026-05-03", body)
        self.assertNotIn("2026-05-03 15:59:20", body)

    def test_self_checkin_requires_phone_and_initials_to_match(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            with patch.dict(
                os.environ,
                {
                    "USER_MANAGEMENT_BARCODE_SECRET": "",
                    "USER_MANAGEMENT_SESSION_SECRET": "",
                },
            ), patch.object(cfg, "USER_MANAGEMENT_BARCODE_SECRET", ""), patch.object(
                cfg,
                "USER_MANAGEMENT_SESSION_SECRET",
                "",
            ):
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
                data={"phone": "+1 (510) 510-5100", "initials": "JD"},
            )

            with closing(database.connect(db_path)) as connection:
                checkins = checkin_repository.list_checkins(connection)
                barcode_secret_row = connection.execute(
                    "SELECT value FROM app_settings WHERE key = ?",
                    ("self_checkin_barcode_secret",),
                ).fetchone()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["Refresh"], "60; url=/self-checkin")
        body = response.get_data(as_text=True)
        self.assertIn('<meta http-equiv="refresh" content="60;url=/self-checkin">', body)
        self.assertIn('class="self-checkin-status is-success"', body)
        self.assertIn("You are checked in", body)
        self.assertIn("Check-in recorded.", body)
        self.assertIn("Continue</button>", body)
        self.assertIn('data-auto-reset-delay="60000"', body)
        self.assertNotIn("Phone Check-in", body)
        self.assertNotIn("John", body)
        self.assertNotIn("1861", body)
        self.assertNotIn("UM1:", body)
        self.assertIn("checkin-barcode", body)
        self.assertIsNotNone(barcode_secret_row)
        self.assertEqual(len(checkins), 1)

    def test_self_checkin_accepts_signed_barcode(self) -> None:
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

            with closing(database.connect(db_path)) as connection:
                barcode_secret = _barcode_secret_for_connection(
                    connection,
                    flask_app.config["USER_MANAGEMENT_BARCODE_SECRET"],
                )
                connection.commit()
            token = _barcode_token_for_card_number("1861", barcode_secret)
            self.assertNotIn("1861", token)
            self.assertNotIn("MTg2MQ", token)
            response = flask_app.test_client().post(
                "/self-checkin",
                data={"barcode_token": token},
            )

            with closing(database.connect(db_path)) as connection:
                checkins = checkin_repository.list_checkins(connection)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["Refresh"], "60; url=/self-checkin")
        body = response.get_data(as_text=True)
        self.assertIn('<meta http-equiv="refresh" content="60;url=/self-checkin">', body)
        self.assertIn('class="self-checkin-status is-success"', body)
        self.assertIn("You are checked in", body)
        self.assertIn("Check-in recorded.", body)
        self.assertIn("Continue</button>", body)
        self.assertIn('data-auto-reset-delay="60000"', body)
        self.assertNotIn("Phone Check-in", body)
        self.assertNotIn("John", body)
        self.assertNotIn("1861", body)
        self.assertNotIn("UM1:", body)
        self.assertEqual(len(checkins), 1)

    def test_self_checkin_ignores_repeat_checkin_within_one_hour(self) -> None:
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

            client = flask_app.test_client()
            first_response = client.post(
                "/self-checkin",
                data={"phone": "+1 (510) 510-5100", "initials": "JD"},
            )
            second_response = client.post(
                "/self-checkin",
                data={"phone": "+1 (510) 510-5100", "initials": "JD"},
            )

            with closing(database.connect(db_path)) as connection:
                checkins = checkin_repository.list_checkins(connection)

        self.assertEqual(first_response.status_code, 200)
        self.assertEqual(second_response.status_code, 200)
        self.assertEqual(len(checkins), 1)
        body = second_response.get_data(as_text=True)
        self.assertIn('class="self-checkin-status is-success"', body)
        self.assertIn("Already checked in within the past hour.", body)

    def test_self_checkin_records_again_after_one_hour(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            flask_app = create_app(db_path)
            old_checkin_at = datetime.now().replace(microsecond=0) - timedelta(
                hours=1,
                minutes=1,
            )
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
                member = member_repository.list_members(connection)[0]
                checkin_repository.upsert_checkin(
                    connection,
                    CheckIn(
                        user_id=member.id,
                        member_id=str(member.id),
                        last_name=member.last_name,
                        first_name=member.first_name,
                        card_number=member.card_number,
                        check_in_at=old_checkin_at,
                        membership=member.membership,
                    ),
                )
                connection.commit()

            response = flask_app.test_client().post(
                "/self-checkin",
                data={"phone": "+1 (510) 510-5100", "initials": "JD"},
            )

            with closing(database.connect(db_path)) as connection:
                checkins = checkin_repository.list_checkins(connection)

        self.assertEqual(response.status_code, 200)
        self.assertIn("Check-in recorded.", response.get_data(as_text=True))
        self.assertEqual(len(checkins), 2)

    def test_self_checkin_barcode_survives_app_restart(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            with patch.dict(
                os.environ,
                {
                    "USER_MANAGEMENT_BARCODE_SECRET": "",
                    "USER_MANAGEMENT_SESSION_SECRET": "",
                },
            ), patch.object(cfg, "USER_MANAGEMENT_BARCODE_SECRET", ""), patch.object(
                cfg,
                "USER_MANAGEMENT_SESSION_SECRET",
                "",
            ):
                first_app = create_app(db_path)
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
                barcode_secret = _barcode_secret_for_connection(
                    connection,
                    first_app.config["USER_MANAGEMENT_BARCODE_SECRET"],
                )
                connection.commit()

            token = _barcode_token_for_card_number("1861", barcode_secret)
            with patch.dict(
                os.environ,
                {
                    "USER_MANAGEMENT_BARCODE_SECRET": "",
                    "USER_MANAGEMENT_SESSION_SECRET": "",
                },
            ), patch.object(cfg, "USER_MANAGEMENT_BARCODE_SECRET", ""), patch.object(
                cfg,
                "USER_MANAGEMENT_SESSION_SECRET",
                "",
            ):
                second_app = create_app(db_path)
            second_app.secret_key = "different-session-secret"
            response = second_app.test_client().post(
                "/self-checkin",
                data={"barcode_token": token},
            )

            with closing(database.connect(db_path)) as connection:
                checkins = checkin_repository.list_checkins(connection)

        self.assertEqual(response.status_code, 200)
        self.assertIn("Check-in recorded.", response.get_data(as_text=True))
        self.assertEqual(len(checkins), 1)

    def test_self_checkin_rejects_plain_card_number_barcode(self) -> None:
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
                data={"barcode_token": "1861"},
            )

            with closing(database.connect(db_path)) as connection:
                checkins = checkin_repository.list_checkins(connection)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["Refresh"], "60; url=/self-checkin")
        body = response.get_data(as_text=True)
        self.assertIn('<meta http-equiv="refresh" content="60;url=/self-checkin">', body)
        self.assertIn('class="self-checkin-status is-error"', body)
        self.assertIn("Check-in not found", body)
        self.assertIn("No matching user was found", body)
        self.assertIn("Try Again</button>", body)
        self.assertNotIn("Phone Check-in", body)
        self.assertEqual(len(checkins), 0)

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
        self.assertNotIn("1861", body)
        self.assertNotIn("UM1:", body)
        self.assertNotIn("Review my current information", body)

    def test_checkins_import_route_is_removed(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            flask_app = create_admin_app(db_path)
            client = admin_client(flask_app)

            response = client.post(
                "/checkins/import",
                data={"checkins_csv": (io.BytesIO(b"not used"), "checkins.csv")},
                content_type="multipart/form-data",
            )

            with closing(database.connect(db_path)) as connection:
                users = member_repository.list_members(connection)
                checkins = checkin_repository.list_checkins(connection)

        self.assertEqual(response.status_code, 404)
        self.assertEqual(len(users), 0)
        self.assertEqual(len(checkins), 0)
