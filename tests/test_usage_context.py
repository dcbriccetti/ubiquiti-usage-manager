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
        fixed = cls(2026, 5, 10, 10, 41)
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

    def test_time_range_labels_elide_matching_end_date(self) -> None:
        self.assertEqual(
            usage_context.render_time_range_label(
                datetime(2026, 5, 9, 22, 5),
                datetime(2026, 5, 9, 22, 7),
            ),
            "May 9, 22:05–22:07",
        )
        self.assertEqual(
            usage_context.render_time_range_label(
                datetime(2026, 5, 9, 23, 59),
                datetime(2026, 5, 10, 0, 1),
            ),
            "May 9, 23:59–May 10, 00:01",
        )

    def test_wan_chart_buckets_use_flow_end_time(self) -> None:
        flow = db.WanMacFlowUsage(
            source_file="nfcapd.202605100000",
            started_at=datetime(2026, 5, 9, 23, 59),
            ended_at=datetime(2026, 5, 10, 0, 1),
            bytes=2_000_000,
            direction="download",
        )

        daily_totals = usage_context.build_wan_flow_bucket_totals(
            [flow],
            datetime(2026, 5, 10, 0, 0),
            datetime(2026, 5, 10, 23, 59),
            "day",
        )
        hourly_totals = usage_context.build_wan_flow_bucket_totals(
            [flow],
            datetime(2026, 5, 10, 0, 0),
            datetime(2026, 5, 10, 23, 59),
            "hour",
        )
        direction_series = usage_context.build_wan_flow_direction_series(
            [flow],
            datetime(2026, 5, 10, 0, 0),
            datetime(2026, 5, 10, 23, 59),
            "day",
            [9, 10],
        )

        self.assertEqual(daily_totals, {10: 2.0})
        self.assertEqual(hourly_totals, {0: 2.0})
        self.assertEqual(direction_series[0]["data"], [0.0, 2.0])

    def test_recent_wan_rows_enrich_only_displayed_limit(self) -> None:
        mac = "aa:bb:cc:dd:ee:20"
        flows = [
            db.WanMacIdentityFlowUsage(
                source_file=f"nfcapd.20260509{minute:04d}",
                started_at=datetime(2026, 5, 9, 12, minute),
                ended_at=datetime(2026, 5, 9, 12, minute),
                proto="TCP",
                src_ip="8.8.8.8",
                src_port=443,
                dst_ip="192.168.1.20",
                dst_port=50000 + minute,
                packets=1,
                bytes=1_000_000,
                direction="download",
                client_ip="192.168.1.20",
                mac=mac,
                name="Test client",
                user_id="",
                vlan="Plus",
            )
            for minute in range(10)
        ]

        with (
            patch.object(db, "get_access_point_labels_for_windows", return_value={}) as ap_labels,
            patch.object(usage_context, "resolve_host_labels", return_value={}),
        ):
            rows = usage_context.build_wan_import_usage_context(
                mac,
                flows,
                datetime(2026, 5, 9, 12, 0),
                datetime(2026, 5, 9, 12, 59),
                limit=3,
            )

        self.assertEqual([row["source_file"] for row in rows], [
            "nfcapd.202605090009",
            "nfcapd.202605090008",
            "nfcapd.202605090007",
        ])
        ap_labels.assert_called_once()
        self.assertEqual(len(ap_labels.call_args.args[1]), 3)

    def test_recent_wan_rows_default_to_full_month_activity(self) -> None:
        mac = "aa:bb:cc:dd:ee:21"
        flows = [
            db.WanMacIdentityFlowUsage(
                source_file=f"nfcapd.20260509{minute:04d}",
                started_at=datetime(2026, 5, 9, 12, minute),
                ended_at=datetime(2026, 5, 9, 12, minute),
                proto="TCP",
                src_ip="8.8.8.8",
                src_port=443,
                dst_ip="192.168.1.21",
                dst_port=50000 + minute,
                packets=1,
                bytes=1_000_000,
                direction="download",
                client_ip="192.168.1.21",
                mac=mac,
                name="Test client",
                user_id="",
                vlan="Plus",
            )
            for minute in range(45)
        ]

        with (
            patch.object(db, "get_access_point_labels_for_windows", return_value={}) as ap_labels,
            patch.object(usage_context, "resolve_host_labels", return_value={}),
        ):
            rows = usage_context.build_wan_import_usage_context(
                mac,
                flows,
                datetime(2026, 5, 9, 12, 0),
                datetime(2026, 5, 9, 12, 59),
            )

        self.assertEqual(len(rows), 45)
        self.assertEqual(rows[0]["source_file"], "nfcapd.202605090044")
        self.assertEqual(rows[-1]["source_file"], "nfcapd.202605090000")
        ap_labels.assert_called_once()
        self.assertEqual(len(ap_labels.call_args.args[1]), 45)

    def test_recent_wan_rows_aggregate_consecutive_tiny_batches(self) -> None:
        def recent_row(
            minute: int,
            total_mb: float,
            host_label: str,
            access_point_label: str = "Rec Hall Roof",
        ) -> usage_context.WanImportUsageContext:
            flow_time = datetime(2026, 5, 12, 22, minute)
            return {
                "source_file": f"nfcapd.2026051222{minute:02d}",
                "source_label": f"Capture {minute}",
                "imported_label": f"Imported {minute}",
                "flow_window_label": usage_context.render_time_range_label(flow_time, flow_time),
                "access_point_label": access_point_label,
                "access_point_detail": access_point_label,
                "host_label": host_label,
                "host_detail": host_label,
                "host_ip": "",
                "host_extra_count": 0,
                "imported_at": flow_time,
                "first_flow_at": flow_time,
                "last_flow_at": flow_time,
                "download_mb": total_mb,
                "upload_mb": 0.0,
                "total_mb": total_mb,
                "flow_count": 1,
            }

        rows = usage_context.aggregate_tiny_wan_import_rows(
            [
                recent_row(23, 0.01, "compute-1.amazonaws.com"),
                recent_row(22, 0.02, "compute-1.amazonaws.com +1"),
                recent_row(21, 1.0, "video.example.com"),
                recent_row(20, 0.03, "cloudfront.net"),
                recent_row(19, 0.01, "cloudfront.net"),
            ]
        )

        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0]["source_label"], "2 small data batches")
        self.assertEqual(rows[0]["flow_window_label"], "May 12, 22:22–22:23")
        self.assertEqual(rows[0]["host_label"], "compute-1.amazonaws.com")
        self.assertAlmostEqual(rows[0]["total_mb"], 0.03)
        self.assertEqual(rows[1]["host_label"], "video.example.com")
        self.assertEqual(rows[2]["source_label"], "2 small data batches")
        self.assertEqual(rows[2]["flow_window_label"], "May 12, 22:19–22:20")

    def test_client_context_can_skip_deferred_wan_details(self) -> None:
        mac = "42:3e:c1:5d:fc:59"
        with db.SessionLocal() as session:
            session.add(
                db.UsageRecord(
                    timestamp=datetime(2026, 5, 9, 22, 1),
                    mac=mac,
                    user_id="",
                    name="iPad",
                    vlan="Plus",
                    mb_used=0.0,
                    profile="default",
                    ap_name="Office AP",
                    signal=-50,
                )
            )
            session.commit()

        with (
            patch.object(usage_context, "datetime", FixedDateTime),
            patch.object(db, "datetime", FixedDateTime),
            patch.object(usage_context, "get_speed_limits_by_name", return_value={}),
            patch.object(db, "get_wan_identity_flow_rows_for_mac") as flow_rows,
            patch.object(usage_context, "build_wan_import_usage_context") as import_rows,
            patch.object(usage_context, "build_flow_activity_context") as activity_rows,
        ):
            context = usage_context.get_client_usage_context(mac, include_wan_details=False)

        self.assertEqual(context["wan_import_usage_rows"], [])
        self.assertEqual(context["flow_activity_rows"], [])
        flow_rows.assert_not_called()
        import_rows.assert_not_called()
        activity_rows.assert_not_called()

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
                db.UsageRecord(
                    timestamp=datetime(2026, 5, 9, 22, 5),
                    mac=mac,
                    user_id="2119",
                    name="iPad",
                    vlan="Plus",
                    mb_used=0.0,
                    profile="default",
                    ap_name="Office AP",
                    signal=-50,
                )
            )
            session.add(
                db.UsageRecord(
                    timestamp=datetime(2026, 5, 9, 22, 6),
                    mac=mac,
                    user_id="2119",
                    name="iPad",
                    vlan="Plus",
                    mb_used=0.0,
                    profile="default",
                    ap_name="Office AP",
                    signal=-50,
                )
            )
            session.add(
                db.UsageRecord(
                    timestamp=datetime(2026, 5, 9, 22, 7),
                    mac=mac,
                    user_id="2119",
                    name="iPad",
                    vlan="Plus",
                    mb_used=0.0,
                    profile="default",
                    ap_name="Lawn AP",
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
            patch.object(usage_context, "resolve_host_labels", return_value={}),
        ):
            context = usage_context.get_client_usage_context(mac)

        self.assertEqual(context["latest_record"].user_id, "2119")
        self.assertEqual(context["latest_record"].vlan, "Plus")
        self.assertAlmostEqual(context["daily_total_mb"], 3.0)
        self.assertAlmostEqual(context["last_7_days_total_mb"], 3.0)
        self.assertAlmostEqual(context["calendar_month_total_mb"], 3.0)
        self.assertAlmostEqual(context["wan_today_total_mb"], 3.0)
        self.assertEqual(len(context["flow_activity_rows"]), 1)
        self.assertEqual(context["flow_activity_rows"][0]["label"], "Secure web and apps")
        self.assertEqual(context["flow_activity_rows"][0]["detail"], "8.8.8.8")
        self.assertAlmostEqual(context["flow_activity_rows"][0]["total_mb"], 3.0)
        self.assertAlmostEqual(context["flow_activity_rows"][0]["download_mb"], 2.0)
        self.assertAlmostEqual(context["flow_activity_rows"][0]["upload_mb"], 1.0)
        self.assertIsNotNone(context["voucher_usage"])
        assert context["voucher_usage"] is not None
        self.assertAlmostEqual(context["voucher_usage"]["used_mb"], 3.0)
        access_modes = {row["key"]: row for row in context["access_mode_usage_rows"]}
        self.assertNotIn("basic", access_modes)
        self.assertNotIn("plus_paid", access_modes)
        self.assertAlmostEqual(access_modes["plus_voucher"]["month_mb"], 3.0)
        self.assertEqual(len(context["wan_import_usage_rows"]), 1)
        recent_import = context["wan_import_usage_rows"][0]
        self.assertEqual(recent_import["source_file"], "nfcapd.202605092205")
        self.assertEqual(recent_import["source_label"], "Capture May 9, 22:05")
        self.assertEqual(recent_import["imported_label"], "May 9, 22:10")
        self.assertEqual(recent_import["flow_window_label"], "May 9, 22:05–22:07")
        self.assertEqual(recent_import["access_point_label"], "Office +1")
        self.assertEqual(recent_import["access_point_detail"], "Office (2m), Lawn (1m)")
        self.assertEqual(recent_import["host_label"], "8.8.8.8")
        self.assertEqual(recent_import["host_detail"], "8.8.8.8: 3.0 MB")
        self.assertEqual(recent_import["imported_at"], observed_at + timedelta(minutes=10))
        self.assertEqual(recent_import["first_flow_at"], observed_at + timedelta(minutes=5))
        self.assertEqual(recent_import["last_flow_at"], observed_at + timedelta(minutes=7))
        self.assertAlmostEqual(recent_import["total_mb"], 3.0)
        self.assertAlmostEqual(recent_import["download_mb"], 2.0)
        self.assertAlmostEqual(recent_import["upload_mb"], 1.0)
        self.assertEqual(recent_import["flow_count"], 2)

        monthly_scale = next(scale for scale in context["usage_scales"] if scale["key"] == "monthly")
        day_9_point = next(point for point in monthly_scale["points"] if point["bucket_value"] == 9)
        self.assertAlmostEqual(day_9_point["total_mb"], 3.0)
        self.assertEqual(
            monthly_scale["throttle_x_values"],
            [point["bucket_value"] for point in monthly_scale["points"]],
        )
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
        self.assertEqual(monthly_scale["throttle_datasets"], [])
        self.assertFalse(monthly_scale["show_access_point_activity"])
        self.assertNotIn("active minutes", monthly_scale["summary_text"].lower())

    def test_wired_client_hides_access_point_activity_charts(self) -> None:
        mac = "aa:bb:cc:dd:ee:40"
        with db.SessionLocal() as session:
            session.add(
                db.UsageRecord(
                    timestamp=datetime(2026, 5, 10, 10, 1),
                    mac=mac,
                    user_id="wired",
                    name="Wired Client",
                    vlan="Basic",
                    mb_used=0.0,
                    profile="default",
                    ap_name="",
                    signal=None,
                )
            )
            session.commit()

        with (
            patch.object(usage_context, "datetime", FixedDateTimeMay10),
            patch.object(db, "datetime", FixedDateTimeMay10),
            patch.object(usage_context, "get_speed_limits_by_name", return_value={}),
            patch.object(usage_context, "resolve_host_labels", return_value={}),
        ):
            context = usage_context.get_client_usage_context(mac)

        self.assertEqual(context["latest_record"].ap_name, "")
        for usage_scale in context["usage_scales"]:
            self.assertFalse(usage_scale["show_access_point_activity"])
            self.assertNotIn("access point", usage_scale["summary_text"].lower())

    def test_voucher_balance_uses_flow_time_voucher_identity_when_client_modes_change(self) -> None:
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
                    db.ClientIpIdentity(
                        observed_at=datetime(2026, 5, 10, 9, 0),
                        ip_address="192.168.6.143",
                        mac=mac,
                        name="iPad",
                        user_id="basic-user",
                        vlan="Basic",
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
                        source_file="nfcapd.202605100930",
                        started_at=datetime(2026, 5, 10, 9, 30),
                        ended_at=datetime(2026, 5, 10, 9, 31),
                        duration_seconds=60.0,
                        proto="TCP",
                        src_ip="8.8.8.8",
                        src_port=443,
                        dst_ip="192.168.6.143",
                        dst_port=52344,
                        packets=10,
                        bytes=300_000_000,
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
            patch.object(usage_context, "resolve_host_labels", return_value={}),
        ):
            context = usage_context.get_client_usage_context(mac)

        self.assertEqual(context["latest_record"].user_id, "5921")
        self.assertAlmostEqual(context["calendar_month_total_mb"], 2017.0)
        self.assertIsNotNone(context["voucher_usage"])
        assert context["voucher_usage"] is not None
        self.assertAlmostEqual(context["voucher_usage"]["used_mb"], 17.0)
        self.assertEqual(context["voucher_usage"]["activated_at"], datetime(2026, 5, 10, 10, 40))
        access_modes = {row["key"]: row for row in context["access_mode_usage_rows"]}
        self.assertAlmostEqual(access_modes["basic"]["month_mb"], 300.0)
        self.assertAlmostEqual(access_modes["plus_paid"]["month_mb"], 1700.0)
        self.assertAlmostEqual(access_modes["plus_voucher"]["month_mb"], 17.0)
        self.assertAlmostEqual(access_modes["plus_paid"]["month_cost_cents"], 85.0)


if __name__ == "__main__":
    unittest.main()
