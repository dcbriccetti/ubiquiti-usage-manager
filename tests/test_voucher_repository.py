import sys
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

if "database" in sys.modules and not hasattr(sys.modules["database"], "UsageRecord"):
    del sys.modules["database"]

import database as db
import voucher_repository


class ActiveVoucherSummaryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        db_path = Path(self.temp_dir.name) / "meter-test.db"
        self.engine = create_engine(f"sqlite:///{db_path}")
        self.original_session_local = db.SessionLocal
        db.SessionLocal = sessionmaker(bind=self.engine)
        db.Base.metadata.create_all(self.engine)

    def tearDown(self) -> None:
        db.SessionLocal = self.original_session_local
        db.Base.metadata.drop_all(self.engine)
        self.engine.dispose()
        self.temp_dir.cleanup()

    def test_active_summaries_batch_legacy_and_wan_usage(self) -> None:
        generated_at = datetime(2026, 5, 1, 10, 0)
        with db.SessionLocal() as session:
            session.add_all(
                [
                    db.PlusVoucher(
                        batch_id="legacy",
                        user_id=101,
                        password="legacy1",
                        allocation_gb=10,
                        generated_at=generated_at,
                    ),
                    db.PlusVoucher(
                        batch_id="wan",
                        user_id=102,
                        password="wan102",
                        allocation_gb=20,
                        generated_at=generated_at + timedelta(hours=3),
                    ),
                ]
            )
            session.add_all(
                [
                    db.UsageRecord(
                        timestamp=generated_at + timedelta(hours=1),
                        mac="aa:bb:cc:dd:ee:01",
                        user_id="101",
                        name="Legacy voucher",
                        vlan="Plus",
                        mb_used=300.0,
                        profile="default",
                        ap_name="AP",
                        signal=-50,
                    ),
                    db.UsageRecord(
                        timestamp=generated_at + timedelta(hours=2),
                        mac="aa:bb:cc:dd:ee:01",
                        user_id="101",
                        name="Legacy voucher",
                        vlan="Plus",
                        mb_used=200.0,
                        profile="default",
                        ap_name="AP",
                        signal=-50,
                    ),
                    db.ClientIpIdentity(
                        observed_at=generated_at + timedelta(minutes=30),
                        ip_address="192.168.1.20",
                        mac="aa:bb:cc:dd:ee:02",
                        name="WAN voucher",
                        user_id="102",
                        vlan="Plus",
                    ),
                    db.WanFlowUsage(
                        source_file="nfcapd.202605011200",
                        started_at=generated_at + timedelta(hours=2),
                        ended_at=generated_at + timedelta(hours=2, minutes=1),
                        duration_seconds=60.0,
                        proto="TCP",
                        src_ip="192.168.1.20",
                        src_port=12345,
                        dst_ip="8.8.8.8",
                        dst_port=443,
                        packets=10,
                        bytes=1_000_000,
                        direction="upload",
                        client_ip="192.168.1.20",
                    ),
                    db.WanFlowUsage(
                        source_file="nfcapd.202605011400",
                        started_at=generated_at + timedelta(hours=4),
                        ended_at=generated_at + timedelta(hours=4, minutes=1),
                        duration_seconds=60.0,
                        proto="TCP",
                        src_ip="8.8.8.8",
                        src_port=443,
                        dst_ip="192.168.1.20",
                        dst_port=12345,
                        packets=10,
                        bytes=2_000_000,
                        direction="download",
                        client_ip="192.168.1.20",
                    ),
                ]
            )
            session.commit()

        with patch.object(voucher_repository, "get_plus_voucher_usage_summary", side_effect=AssertionError):
            summaries = voucher_repository.get_active_plus_voucher_summaries()

        summaries_by_user_id = {summary.voucher.user_id: summary for summary in summaries}
        self.assertEqual(set(summaries_by_user_id), {101, 102})
        self.assertEqual(summaries_by_user_id[101].activated_at, generated_at + timedelta(hours=1))
        self.assertEqual(summaries_by_user_id[101].used_mb, 500.0)
        self.assertEqual(summaries_by_user_id[102].activated_at, generated_at + timedelta(hours=4))
        self.assertEqual(summaries_by_user_id[102].used_mb, 2.0)


if __name__ == "__main__":
    unittest.main()
