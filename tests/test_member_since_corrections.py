import io
import sys
import tempfile
import unittest
from contextlib import closing, redirect_stdout
from datetime import date
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from club_admin import audit_repository
from club_admin import database
from club_admin import member_repository
from club_admin.member_since_corrections import (
    apply_corrections,
    main,
    name_key_from_correction,
    plan_corrections,
    read_corrections,
)
from club_admin.models import Member


class MemberSinceCorrectionTests(unittest.TestCase):
    def test_parses_supported_name_formats(self) -> None:
        self.assertEqual(name_key_from_correction("Amato, Allison"), ("allison", "amato"))
        self.assertEqual(name_key_from_correction("Anthony Ray Flores"), ("anthony", "flores"))

    def test_reads_correction_csv(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "corrections.csv"
            path.write_text(
                "Customer full name,Date Joined\n"
                '"Amato, Allison",7/9/22\n'
                "Anthony Ray Flores,8/10/25\n",
                encoding="utf-8",
            )

            corrections = read_corrections(path)

        self.assertEqual(len(corrections), 2)
        self.assertEqual(corrections[0].name_key, ("allison", "amato"))
        self.assertEqual(corrections[0].member_since, date(2022, 7, 9))
        self.assertEqual(corrections[1].name_key, ("anthony", "flores"))
        self.assertEqual(corrections[1].member_since, date(2025, 8, 10))

    def test_plans_only_confident_suspect_date_matches(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            database.init_db(db_path)
            with closing(database.connect(db_path)) as connection:
                for member in (
                    Member(
                        last_name="Amato",
                        first_name="Allison",
                        card_number="100",
                        membership="Full Member",
                        member_since=date(2025, 4, 5),
                    ),
                    Member(
                        last_name="Flores",
                        first_name="Anthony",
                        card_number="101",
                        membership="Full Member",
                        member_since=date(2020, 1, 1),
                    ),
                    Member(
                        last_name="Antonick",
                        first_name="Eric",
                        card_number="102",
                        membership="Full Member",
                        member_since=date(2026, 4, 6),
                    ),
                    Member(
                        last_name="Antonick",
                        first_name="Eric",
                        card_number="103",
                        membership="Associate Member",
                        member_since=date(2026, 4, 6),
                    ),
                ):
                    member_repository.upsert_member(connection, member)
                connection.commit()

                corrections_path = Path(temp_dir) / "corrections.csv"
                corrections_path.write_text(
                    "Customer full name,Date Joined\n"
                    '"Amato, Allison",7/9/22\n'
                    "Anthony Ray Flores,8/10/25\n"
                    '"Antonick, Eric",6/4/22\n'
                    '"Missing, Person",1/2/21\n',
                    encoding="utf-8",
                )
                results = plan_corrections(
                    member_repository.list_members(connection),
                    read_corrections(corrections_path),
                )

        statuses = {result.row.raw_name: result.status for result in results}
        messages = {result.row.raw_name: result.message for result in results}
        self.assertEqual(statuses["Amato, Allison"], "ready")
        self.assertEqual(statuses["Anthony Ray Flores"], "skipped")
        self.assertIn("not suspect", messages["Anthony Ray Flores"])
        self.assertEqual(statuses["Antonick, Eric"], "skipped")
        self.assertIn("ambiguous", messages["Antonick, Eric"])
        self.assertEqual(statuses["Missing, Person"], "skipped")
        self.assertEqual(messages["Missing, Person"], "no user match")

    def test_plans_incomplete_rows_as_skipped(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            corrections_path = Path(temp_dir) / "corrections.csv"
            corrections_path.write_text(
                "Customer full name,Date Joined\n"
                '" Sunday, January 18, 2026 05:09 PM GMTZ",\n',
                encoding="utf-8",
            )

            results = plan_corrections([], read_corrections(corrections_path))

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].status, "skipped")
        self.assertEqual(results[0].message, "missing date joined")

    def test_apply_updates_ready_rows_and_records_audit_log(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            database.init_db(db_path)
            with closing(database.connect(db_path)) as connection:
                member_repository.upsert_member(
                    connection,
                    Member(
                        last_name="Amato",
                        first_name="Allison",
                        card_number="100",
                        membership="Full Member",
                        member_since=date(2025, 4, 5),
                    ),
                )
                connection.commit()

                corrections_path = Path(temp_dir) / "corrections.csv"
                corrections_path.write_text(
                    "Customer full name,Date Joined\n"
                    '"Amato, Allison",7/9/22\n',
                    encoding="utf-8",
                )
                members = member_repository.list_members(connection)
                results = plan_corrections(members, read_corrections(corrections_path))
                applied_count = apply_corrections(connection, results)
                connection.commit()
                updated_member = member_repository.list_members(connection)[0]
                audit_entries = audit_repository.list_audit_log_for_entity(
                    connection,
                    entity_type="user",
                    entity_id=updated_member.id,
                )

        self.assertEqual(applied_count, 1)
        self.assertEqual(updated_member.member_since, date(2022, 7, 9))
        self.assertEqual(len(audit_entries), 1)
        self.assertEqual(audit_entries[0].field_name, "member_since")
        self.assertEqual(audit_entries[0].old_value, "2025-04-05")
        self.assertEqual(audit_entries[0].new_value, "2022-07-09")

    def test_main_defaults_to_dry_run(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            database.init_db(db_path)
            with closing(database.connect(db_path)) as connection:
                member_repository.upsert_member(
                    connection,
                    Member(
                        last_name="Amato",
                        first_name="Allison",
                        card_number="100",
                        membership="Full Member",
                        member_since=date(2025, 4, 5),
                    ),
                )
                connection.commit()

            corrections_path = Path(temp_dir) / "corrections.csv"
            corrections_path.write_text(
                "Customer full name,Date Joined\n"
                '"Amato, Allison",7/9/22\n',
                encoding="utf-8",
            )
            output = io.StringIO()
            with redirect_stdout(output):
                exit_code = main([str(corrections_path), "--db", str(db_path)])
            with closing(database.connect(db_path)) as connection:
                member = member_repository.list_members(connection)[0]

        self.assertEqual(exit_code, 0)
        self.assertEqual(member.member_since, date(2025, 4, 5))
        self.assertIn("READY", output.getvalue())
        self.assertIn("Dry run only", output.getvalue())


if __name__ == "__main__":
    unittest.main()
