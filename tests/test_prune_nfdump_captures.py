import importlib.util
import os
import sqlite3
import sys
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path


SCRIPT_PATH = (
    Path(__file__).resolve().parents[1]
    / "deploy"
    / "scripts"
    / "prune-nfdump-captures.py"
)
spec = importlib.util.spec_from_file_location("prune_nfdump_captures", SCRIPT_PATH)
assert spec is not None
prune_nfdump_captures = importlib.util.module_from_spec(spec)
assert spec.loader is not None
sys.modules["prune_nfdump_captures"] = prune_nfdump_captures
spec.loader.exec_module(prune_nfdump_captures)


class PruneNfdumpCapturesTests(unittest.TestCase):
    def create_db(self, path: Path, imported_names: list[str]) -> None:
        connection = sqlite3.connect(path)
        try:
            connection.execute("CREATE TABLE flow_imports (source_file TEXT NOT NULL)")
            connection.executemany(
                "INSERT INTO flow_imports (source_file) VALUES (?)",
                [(name,) for name in imported_names],
            )
            connection.commit()
        finally:
            connection.close()

    def touch_with_age(self, path: Path, age_days: int, content: bytes = b"x") -> None:
        path.write_bytes(content)
        timestamp = (datetime.now() - timedelta(days=age_days)).timestamp()
        os.utime(path, (timestamp, timestamp))

    def test_dry_run_reports_reclaimable_space_without_deleting(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            capture_dir = root / "nfdump"
            capture_dir.mkdir()
            db_path = root / "meter.db"
            self.create_db(db_path, ["nfcapd.202609010101"])
            capture = capture_dir / "nfcapd.202609010101"
            self.touch_with_age(capture, 5, b"12345")

            result = prune_nfdump_captures.prune_captures(
                capture_dir=capture_dir,
                db_path=db_path,
                retention_days=3,
                apply=False,
            )

            self.assertTrue(capture.exists())
            self.assertEqual(result.candidate_files, 1)
            self.assertEqual(result.deleted_files, 0)
            self.assertEqual(result.deleted_bytes, 5)

    def test_apply_deletes_only_old_imported_completed_captures(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            capture_dir = root / "nfdump"
            capture_dir.mkdir()
            db_path = root / "meter.db"
            self.create_db(
                db_path,
                [
                    "nfcapd.202609010101",
                    "nfcapd.202609020202",
                    "nfcapd.202609030303",
                ],
            )
            old_imported = capture_dir / "nfcapd.202609010101"
            old_unimported = capture_dir / "nfcapd.202609040404"
            recent_imported = capture_dir / "nfcapd.202609020202"
            current = capture_dir / "nfcapd.current.1234"
            non_capture = capture_dir / "notes.txt"
            malformed = capture_dir / "nfcapd.20260901"
            for path, age in (
                (old_imported, 5),
                (old_unimported, 5),
                (recent_imported, 1),
                (current, 5),
                (non_capture, 5),
                (malformed, 5),
            ):
                self.touch_with_age(path, age)

            result = prune_nfdump_captures.prune_captures(
                capture_dir=capture_dir,
                db_path=db_path,
                retention_days=3,
                apply=True,
            )

            self.assertFalse(old_imported.exists())
            self.assertTrue(old_unimported.exists())
            self.assertTrue(recent_imported.exists())
            self.assertTrue(current.exists())
            self.assertTrue(non_capture.exists())
            self.assertTrue(malformed.exists())
            self.assertEqual(result.candidate_files, 1)
            self.assertEqual(result.deleted_files, 1)
            self.assertEqual(result.skipped_unimported_files, 1)
            self.assertEqual(result.skipped_recent_files, 1)
            self.assertEqual(result.skipped_non_capture_files, 3)


if __name__ == "__main__":
    unittest.main()
