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

    def test_active_summaries_use_wan_usage_without_sampled_fallback(self) -> None:
        generated_at = datetime(2026, 5, 1, 10, 0)
        with db.SessionLocal() as session:
            session.add_all(
                [
                    db.PlusVoucher(
                        batch_id="sampled",
                        user_id=101,
                        password="sample1",
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
                        name="Sampled-only voucher",
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
                        name="Sampled-only voucher",
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
        self.assertIsNone(summaries_by_user_id[101].activated_at)
        self.assertEqual(summaries_by_user_id[101].used_mb, 0.0)
        self.assertEqual(summaries_by_user_id[102].activated_at, generated_at + timedelta(hours=4))
        self.assertEqual(summaries_by_user_id[102].used_mb, 2.0)

    def test_active_summary_sums_multiple_devices_for_one_voucher(self) -> None:
        generated_at = datetime(2026, 5, 1, 10, 0)
        with db.SessionLocal() as session:
            session.add(
                db.PlusVoucher(
                    batch_id="multi-device",
                    user_id=301,
                    password="multi301",
                    allocation_gb=10,
                    generated_at=generated_at,
                )
            )
            session.add_all(
                [
                    db.ClientIpIdentity(
                        observed_at=generated_at + timedelta(minutes=1),
                        ip_address="192.168.6.10",
                        mac="aa:bb:cc:dd:ee:10",
                        name="Voucher laptop",
                        user_id="301",
                        vlan="Plus",
                    ),
                    db.ClientIpIdentity(
                        observed_at=generated_at + timedelta(minutes=2),
                        ip_address="192.168.6.11",
                        mac="aa:bb:cc:dd:ee:11",
                        name="Voucher phone",
                        user_id="301",
                        vlan="Plus",
                    ),
                    db.ClientIpIdentity(
                        observed_at=generated_at + timedelta(minutes=8),
                        ip_address="192.168.6.11",
                        mac="aa:bb:cc:dd:ee:99",
                        name="Basic replacement",
                        user_id="",
                        vlan="Basic",
                    ),
                ]
            )
            session.add_all(
                [
                    db.WanFlowUsage(
                        source_file="nfcapd.202605011005",
                        started_at=generated_at + timedelta(minutes=5),
                        ended_at=generated_at + timedelta(minutes=5, seconds=30),
                        duration_seconds=30.0,
                        proto="TCP",
                        src_ip="192.168.6.10",
                        src_port=12345,
                        dst_ip="8.8.8.8",
                        dst_port=443,
                        packets=10,
                        bytes=1_000_000_000,
                        direction="download",
                        client_ip="192.168.6.10",
                    ),
                    db.WanFlowUsage(
                        source_file="nfcapd.202605011006",
                        started_at=generated_at + timedelta(minutes=6),
                        ended_at=generated_at + timedelta(minutes=6, seconds=30),
                        duration_seconds=30.0,
                        proto="TCP",
                        src_ip="192.168.6.11",
                        src_port=23456,
                        dst_ip="8.8.4.4",
                        dst_port=443,
                        packets=10,
                        bytes=2_000_000_000,
                        direction="download",
                        client_ip="192.168.6.11",
                    ),
                    db.WanFlowUsage(
                        source_file="nfcapd.202605011009",
                        started_at=generated_at + timedelta(minutes=9),
                        ended_at=generated_at + timedelta(minutes=9, seconds=30),
                        duration_seconds=30.0,
                        proto="TCP",
                        src_ip="192.168.6.11",
                        src_port=34567,
                        dst_ip="1.1.1.1",
                        dst_port=443,
                        packets=10,
                        bytes=5_000_000_000,
                        direction="download",
                        client_ip="192.168.6.11",
                    ),
                ]
            )
            session.commit()

        summaries = voucher_repository.get_active_plus_voucher_summaries()

        self.assertEqual(len(summaries), 1)
        self.assertEqual(summaries[0].voucher.user_id, 301)
        self.assertEqual(summaries[0].activated_at, generated_at + timedelta(minutes=5))
        self.assertEqual(summaries[0].used_mb, 3000.0)

    def test_mark_plus_voucher_consumed_sets_consumed_at_once(self) -> None:
        generated_at = datetime(2026, 5, 1, 10, 0)
        consumed_at = datetime(2026, 5, 8, 9, 30)
        with db.SessionLocal() as session:
            voucher = db.PlusVoucher(
                batch_id="batch",
                user_id=321,
                password="sample1",
                allocation_gb=10,
                generated_at=generated_at,
            )
            session.add(voucher)
            session.commit()
            voucher_id = voucher.id

        consumed_voucher = voucher_repository.mark_plus_voucher_consumed(voucher_id, consumed_at)

        self.assertIsNotNone(consumed_voucher)
        assert consumed_voucher is not None
        self.assertEqual(consumed_voucher.user_id, 321)
        self.assertEqual(consumed_voucher.consumed_at, consumed_at)

        second_consumed_at = datetime(2026, 5, 9, 9, 30)
        unchanged_voucher = voucher_repository.mark_plus_voucher_consumed(voucher_id, second_consumed_at)

        self.assertIsNotNone(unchanged_voucher)
        assert unchanged_voucher is not None
        self.assertEqual(unchanged_voucher.consumed_at, consumed_at)

    def test_consumption_trend_groups_active_voucher_wan_usage_by_day(self) -> None:
        generated_at = datetime(2026, 5, 1, 10, 0)
        period_end = datetime(2026, 5, 7, 12, 0)
        with db.SessionLocal() as session:
            session.add(
                db.PlusVoucher(
                    batch_id="trend",
                    user_id=201,
                    password="trend201",
                    allocation_gb=20,
                    generated_at=generated_at,
                )
            )
            session.add(
                db.ClientIpIdentity(
                    observed_at=generated_at + timedelta(minutes=10),
                    ip_address="192.168.1.30",
                    mac="aa:bb:cc:dd:ee:03",
                    name="Trend voucher",
                    user_id="201",
                    vlan="Plus",
                )
            )
            session.add_all(
                [
                    db.WanFlowUsage(
                        source_file="nfcapd.202605011200",
                        started_at=datetime(2026, 5, 1, 12, 0),
                        ended_at=datetime(2026, 5, 1, 12, 1),
                        duration_seconds=60.0,
                        proto="TCP",
                        src_ip="192.168.1.30",
                        src_port=12345,
                        dst_ip="8.8.8.8",
                        dst_port=443,
                        packets=10,
                        bytes=1_000_000_000,
                        direction="upload",
                        client_ip="192.168.1.30",
                    ),
                    db.WanFlowUsage(
                        source_file="nfcapd.202605021200",
                        started_at=datetime(2026, 5, 2, 12, 0),
                        ended_at=datetime(2026, 5, 2, 12, 1),
                        duration_seconds=60.0,
                        proto="TCP",
                        src_ip="192.168.1.30",
                        src_port=12345,
                        dst_ip="8.8.4.4",
                        dst_port=443,
                        packets=20,
                        bytes=2_000_000_000,
                        direction="download",
                        client_ip="192.168.1.30",
                    ),
                    db.WanFlowUsage(
                        source_file="nfcapd.202605041200",
                        started_at=datetime(2026, 5, 4, 12, 0),
                        ended_at=datetime(2026, 5, 4, 12, 1),
                        duration_seconds=60.0,
                        proto="TCP",
                        src_ip="192.168.1.30",
                        src_port=12345,
                        dst_ip="1.1.1.1",
                        dst_port=443,
                        packets=40,
                        bytes=4_000_000_000,
                        direction="download",
                        client_ip="192.168.1.30",
                    ),
                ]
            )
            session.commit()

        summaries = voucher_repository.get_active_plus_voucher_summaries()
        trend = voucher_repository.get_plus_voucher_consumption_trend(
            summaries,
            lookback_days=7,
            recent_days=7,
            period_end=period_end,
        )

        self.assertEqual(trend.period_start, datetime(2026, 5, 1).date())
        self.assertEqual(trend.period_end, datetime(2026, 5, 7).date())
        self.assertEqual([row.used_mb for row in trend.daily_usage], [1000.0, 2000.0, 0.0, 4000.0, 0.0, 0.0, 0.0])
        self.assertEqual(trend.total_used_mb, 7000.0)
        self.assertEqual(trend.total_remaining_mb, 13_000.0)
        self.assertEqual(trend.recent_average_daily_mb, 1000.0)
        self.assertEqual(trend.projected_days_remaining, 13.0)
        self.assertEqual(trend.projected_depletion_date, datetime(2026, 5, 20).date())

    def test_consumption_trend_keeps_production_local_wan_flow_day(self) -> None:
        generated_at = datetime(2026, 5, 31, 20, 0)
        flow_started_at = datetime(2026, 6, 1, 0, 30)
        with db.SessionLocal() as session:
            session.add(
                db.PlusVoucher(
                    batch_id="local-time",
                    user_id=3510,
                    password="local3510",
                    allocation_gb=40,
                    generated_at=generated_at,
                )
            )
            session.add(
                db.ClientIpIdentity(
                    observed_at=datetime(2026, 6, 1, 0, 15),
                    ip_address="192.168.4.227",
                    mac="aa:bb:cc:dd:ee:35",
                    name="Local-time voucher",
                    user_id="3510",
                    vlan="Plus",
                )
            )
            session.add(
                db.WanFlowUsage(
                    source_file="nfcapd.202606010030",
                    started_at=flow_started_at,
                    ended_at=flow_started_at + timedelta(minutes=1),
                    duration_seconds=60.0,
                    proto="TCP",
                    src_ip="192.168.4.227",
                    src_port=12345,
                    dst_ip="8.8.8.8",
                    dst_port=443,
                    packets=10,
                    bytes=1_200_000_000,
                    direction="download",
                    client_ip="192.168.4.227",
                )
            )
            session.commit()

        summaries = voucher_repository.get_active_plus_voucher_summaries()
        self.assertEqual(summaries[0].activated_at, flow_started_at)

        trend = voucher_repository.get_plus_voucher_consumption_trend(
            summaries,
            lookback_days=2,
            period_end=datetime(2026, 6, 1, 1, 0),
        )

        self.assertEqual([row.day for row in trend.daily_usage], [datetime(2026, 5, 31).date(), datetime(2026, 6, 1).date()])
        self.assertEqual([row.used_mb for row in trend.daily_usage], [0.0, 1200.0])


if __name__ == "__main__":
    unittest.main()
