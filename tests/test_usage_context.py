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
from speedlimit import SpeedLimit
import usage_context


class FixedDateTime(datetime):
    @classmethod
    def now(cls, tz=None):  # type: ignore[override]
        fixed = cls(2026, 5, 9, 22, 15)
        if tz is not None:
            return fixed.replace(tzinfo=tz)
        return fixed


class FixedDateTimeMay10(datetime):
    @classmethod
    def now(cls, tz=None):  # type: ignore[override]
        fixed = cls(2026, 5, 10, 10, 40)
        if tz is not None:
            return fixed.replace(tzinfo=tz)
        return fixed


class ClientUsageContextTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        db_path = Path(self.temp_dir.name) / "meter-test.db"
        self.engine = create_engine(f"sqlite:///{db_path}")
        self.original_session_local = db.SessionLocal
        db.SessionLocal = sessionmaker(bind=self.engine)
        db.Base.metadata.create_all(self.engine)

    def tearDown(self) -> None:
        usage_context._speed_limits_cache = None
        db.SessionLocal = self.original_session_local
        db.Base.metadata.drop_all(self.engine)
        self.engine.dispose()
        self.temp_dir.cleanup()

    def test_speed_limit_lookup_is_cached_for_detail_page_renders(self) -> None:
        usage_context._speed_limits_cache = None
        with patch.object(
            usage_context.api,
            "get_speed_limits",
            return_value=[
                SpeedLimit(id="slow-id", name="slow", up_kbps=1000, down_kbps=2000),
            ],
        ) as get_speed_limits:
            first_limits = usage_context.get_speed_limits_by_name()
            second_limits = usage_context.get_speed_limits_by_name()

        self.assertEqual(first_limits, second_limits)
        self.assertEqual(set(first_limits), {"slow"})
        self.assertEqual(get_speed_limits.call_count, 1)

    def test_wan_usage_fills_zero_sampled_usage_and_voucher_identity(self) -> None:
        mac = "42:3e:c1:5d:fc:59"
        generated_at = datetime(2026, 5, 6, 13, 32)
        observed_at = datetime(2026, 5, 9, 22, 0)
        with db.SessionLocal() as session:
            session.add(
                db.PlusVoucher(
                    batch_id="voucher",
                    user_id=2119,
                    password="paper123",
                    allocation_gb=200,
                    generated_at=generated_at,
                )
            )
            session.add(
                db.UsageRecord(
                    timestamp=datetime(2026, 5, 9, 22, 1),
                    mac=mac,
                    user_id="",
                    name="iPad",
                    vlan="",
                    mb_used=0.0,
                    profile="default",
                    ap_name="AP",
                    signal=-50,
                )
            )
            session.add(
                db.ClientIpIdentity(
                    observed_at=observed_at,
                    ip_address="192.168.6.143",
                    mac=mac,
                    name="iPad",
                    user_id="2119",
                    vlan="Plus",
                )
            )
            session.add(
                db.FlowImport(
                    source_file="nfcapd.202605092205",
                    imported_at=observed_at + timedelta(minutes=10),
                    record_count=2,
                    skipped_count=0,
                )
            )
            session.add_all(
                [
                    db.WanFlowUsage(
                        source_file="nfcapd.202605092205",
                        started_at=observed_at + timedelta(minutes=5),
                        ended_at=observed_at + timedelta(minutes=6),
                        duration_seconds=60.0,
                        proto="TCP",
                        src_ip="8.8.8.8",
                        src_port=443,
                        dst_ip="192.168.6.143",
                        dst_port=52344,
                        packets=10,
                        bytes=2_000_000,
                        direction="download",
                        client_ip="192.168.6.143",
                    ),
                    db.WanFlowUsage(
                        source_file="nfcapd.202605092205",
                        started_at=observed_at + timedelta(minutes=6),
                        ended_at=observed_at + timedelta(minutes=7),
                        duration_seconds=60.0,
                        proto="TCP",
                        src_ip="192.168.6.143",
                        src_port=52344,
                        dst_ip="8.8.8.8",
                        dst_port=443,
                        packets=10,
                        bytes=1_000_000,
                        direction="upload",
                        client_ip="192.168.6.143",
                    ),
                    db.ClientIpIdentity(
                        observed_at=observed_at,
                        ip_address="192.168.6.200",
                        mac="aa:bb:cc:dd:ee:99",
                        name="Other client",
                        user_id="9999",
                        vlan="Plus",
                    ),
                    db.WanFlowUsage(
                        source_file="nfcapd.202605092207",
                        started_at=observed_at + timedelta(minutes=7),
                        ended_at=observed_at + timedelta(minutes=8),
                        duration_seconds=60.0,
                        proto="TCP",
                        src_ip="8.8.8.8",
                        src_port=443,
                        dst_ip="192.168.6.200",
                        dst_port=52344,
                        packets=10,
                        bytes=5_000_000_000,
                        direction="download",
                        client_ip="192.168.6.200",
                    ),
                ]
            )
            session.commit()

        with (
            patch.object(usage_context, "datetime", FixedDateTime),
            patch.object(db, "datetime", FixedDateTime),
            patch.object(usage_context, "get_speed_limits_by_name", return_value={}),
            patch.object(db, "get_plus_voucher_usage_summary", side_effect=AssertionError),
        ):
            context = usage_context.get_client_usage_context(mac)

        self.assertEqual(context["latest_record"].user_id, "2119")
        self.assertEqual(context["latest_record"].vlan, "Plus")
        self.assertAlmostEqual(context["daily_total_mb"], 3.0)
        self.assertAlmostEqual(context["last_7_days_total_mb"], 3.0)
        self.assertAlmostEqual(context["calendar_month_total_mb"], 3.0)
        self.assertAlmostEqual(context["wan_today_total_mb"], 3.0)
        self.assertIsNotNone(context["voucher_usage"])
        assert context["voucher_usage"] is not None
        self.assertAlmostEqual(context["voucher_usage"]["used_mb"], 3.0)
        self.assertEqual(len(context["wan_import_usage_rows"]), 1)
        recent_import = context["wan_import_usage_rows"][0]
        self.assertEqual(recent_import["source_file"], "nfcapd.202605092205")
        self.assertEqual(recent_import["source_label"], "nfcapd.202605092205")
        self.assertEqual(recent_import["imported_at"], observed_at + timedelta(minutes=10))
        self.assertEqual(recent_import["first_flow_at"], observed_at + timedelta(minutes=5))
        self.assertEqual(recent_import["last_flow_at"], observed_at + timedelta(minutes=6))
        self.assertAlmostEqual(recent_import["total_mb"], 3.0)
        self.assertAlmostEqual(recent_import["download_mb"], 2.0)
        self.assertAlmostEqual(recent_import["upload_mb"], 1.0)
        self.assertEqual(recent_import["flow_count"], 2)

        monthly_scale = next(scale for scale in context["usage_scales"] if scale["key"] == "monthly")
        day_9_point = next(point for point in monthly_scale["points"] if point["bucket_value"] == 9)
        self.assertAlmostEqual(day_9_point["total_mb"], 3.0)
        self.assertEqual(
            [series["label"] for series in monthly_scale["usage_device_series"]],
            ["Down", "Up"],
        )
        day_9_index = [point["bucket_value"] for point in monthly_scale["points"]].index(9)
        down_series = monthly_scale["usage_device_series"][0]["data"]
        up_series = monthly_scale["usage_device_series"][1]["data"]
        self.assertIsInstance(down_series, list)
        self.assertIsInstance(up_series, list)
        self.assertAlmostEqual(down_series[day_9_index], 2.0)
        self.assertAlmostEqual(up_series[day_9_index], 1.0)

    def test_voucher_balance_uses_mac_wan_usage_when_identity_changes(self) -> None:
        mac = "42:3e:c1:5d:fc:59"
        generated_at = datetime(2026, 5, 6, 13, 32)
        with db.SessionLocal() as session:
            session.add(
                db.PlusVoucher(
                    batch_id="voucher",
                    user_id=5921,
                    password="paper123",
                    allocation_gb=40,
                    generated_at=generated_at,
                )
            )
            session.add(
                db.UsageRecord(
                    timestamp=datetime(2026, 5, 10, 10, 1),
                    mac=mac,
                    user_id="",
                    name="iPad",
                    vlan="",
                    mb_used=0.0,
                    profile="default",
                    ap_name="Office AP",
                    signal=-50,
                )
            )
            session.add_all(
                [
                    db.ClientIpIdentity(
                        observed_at=datetime(2026, 5, 9, 22, 0),
                        ip_address="192.168.6.143",
                        mac=mac,
                        name="iPad",
                        user_id="",
                        vlan="Plus",
                    ),
                    db.ClientIpIdentity(
                        observed_at=datetime(2026, 5, 10, 10, 40),
                        ip_address="192.168.6.143",
                        mac=mac,
                        name="iPad",
                        user_id="5921",
                        vlan="Plus",
                    ),
                    db.WanFlowUsage(
                        source_file="nfcapd.202605092215",
                        started_at=datetime(2026, 5, 9, 22, 15),
                        ended_at=datetime(2026, 5, 9, 22, 16),
                        duration_seconds=60.0,
                        proto="TCP",
                        src_ip="8.8.8.8",
                        src_port=443,
                        dst_ip="192.168.6.143",
                        dst_port=52344,
                        packets=10,
                        bytes=1_700_000_000,
                        direction="download",
                        client_ip="192.168.6.143",
                    ),
                    db.WanFlowUsage(
                        source_file="nfcapd.202605101040",
                        started_at=datetime(2026, 5, 10, 10, 40),
                        ended_at=datetime(2026, 5, 10, 10, 41),
                        duration_seconds=60.0,
                        proto="TCP",
                        src_ip="192.168.6.143",
                        src_port=52344,
                        dst_ip="8.8.8.8",
                        dst_port=443,
                        packets=10,
                        bytes=17_000_000,
                        direction="upload",
                        client_ip="192.168.6.143",
                    ),
                ]
            )
            session.commit()

        with (
            patch.object(usage_context, "datetime", FixedDateTimeMay10),
            patch.object(db, "datetime", FixedDateTimeMay10),
            patch.object(usage_context, "get_speed_limits_by_name", return_value={}),
            patch.object(db, "get_plus_voucher_usage_summary", side_effect=AssertionError),
        ):
            context = usage_context.get_client_usage_context(mac)

        self.assertEqual(context["latest_record"].user_id, "5921")
        self.assertAlmostEqual(context["calendar_month_total_mb"], 1717.0)
        self.assertIsNotNone(context["voucher_usage"])
        assert context["voucher_usage"] is not None
        self.assertAlmostEqual(context["voucher_usage"]["used_mb"], 1717.0)
        self.assertEqual(context["voucher_usage"]["activated_at"], datetime(2026, 5, 9, 22, 15))


if __name__ == "__main__":
    unittest.main()
