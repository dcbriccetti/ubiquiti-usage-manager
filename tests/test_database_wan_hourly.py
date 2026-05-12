import sys
import unittest
from datetime import datetime
from importlib import import_module
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))


if "database" in sys.modules and not hasattr(sys.modules["database"], "SessionLocal"):
    del sys.modules["database"]
db = import_module("database")


class GlobalWanHourlyUsageTests(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = create_engine("sqlite:///:memory:")
        self.original_session_local = db.SessionLocal
        db.SessionLocal = sessionmaker(bind=self.engine)
        db.Base.metadata.create_all(self.engine)

    def tearDown(self) -> None:
        db.SessionLocal = self.original_session_local
        db.Base.metadata.drop_all(self.engine)
        self.engine.dispose()

    def add_flow(self, source_file: str, started_at: datetime, bytes_used: int) -> None:
        with db.SessionLocal() as session:
            session.add(
                db.WanFlowUsage(
                    source_file=source_file,
                    started_at=started_at,
                    ended_at=started_at,
                    duration_seconds=60.0,
                    proto="TCP",
                    src_ip="192.168.1.20",
                    src_port=12345,
                    dst_ip="8.8.8.8",
                    dst_port=443,
                    packets=10,
                    bytes=bytes_used,
                    direction="download",
                    client_ip="192.168.1.20",
                )
            )
            session.commit()

    def test_global_wan_hourly_usage_aggregates_and_fills_empty_hours(self) -> None:
        self.add_flow("capture-1", datetime(2026, 5, 1, 1, 5), 1_000_000)
        self.add_flow("capture-2", datetime(2026, 5, 1, 1, 45), 2_000_000)
        self.add_flow("capture-3", datetime(2026, 5, 1, 3, 0), 4_000_000)
        self.add_flow("capture-4", datetime(2026, 5, 2, 0, 0), 8_000_000)

        series = db.get_global_wan_hourly_usage_current_month(
            period_start=datetime(2026, 5, 1, 0, 0),
            period_end=datetime(2026, 5, 1, 3, 59),
        )

        self.assertEqual(
            [row.bucket_start for row in series],
            [
                datetime(2026, 5, 1, 0, 0),
                datetime(2026, 5, 1, 1, 0),
                datetime(2026, 5, 1, 2, 0),
                datetime(2026, 5, 1, 3, 0),
            ],
        )
        self.assertEqual([row.total_mb for row in series], [0.0, 3.0, 0.0, 4.0])


if __name__ == "__main__":
    unittest.main()
