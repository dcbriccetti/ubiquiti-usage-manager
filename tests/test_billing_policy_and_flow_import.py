import ipaddress
import sys
import types
import unittest
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from unittest.mock import patch


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))


@dataclass(frozen=True, kw_only=True)
class StubWanFlowUsageRecord:
    source_file: str
    started_at: datetime
    ended_at: datetime
    duration_seconds: float
    proto: str
    src_ip: str
    src_port: int | None
    dst_ip: str
    dst_port: int | None
    packets: int
    bytes: int
    direction: str
    client_ip: str


database_stub = types.ModuleType("database")
database_stub.WanFlowUsageRecord = StubWanFlowUsageRecord
sys.modules.setdefault("database", database_stub)

import config
import billing
import flow_import
import throttling_policy


class BillingTests(unittest.TestCase):
    def test_calculate_month_cost_cents_uses_configured_gb_rate(self) -> None:
        with patch.object(billing.cfg, "COST_IN_CENTS_PER_GB", 50):
            self.assertEqual(billing.calculate_month_cost_cents(0), 0)
            self.assertEqual(billing.calculate_month_cost_cents(1000), 50)
            self.assertEqual(billing.calculate_month_cost_cents(2500), 125)

    def test_calculate_month_cost_cents_preserves_fractional_usage(self) -> None:
        with patch.object(billing.cfg, "COST_IN_CENTS_PER_GB", 75):
            self.assertAlmostEqual(billing.calculate_month_cost_cents(333.333), 24.999975)


class ThrottlingPolicyTests(unittest.TestCase):
    def test_non_throttleable_vlan_never_gets_target_profile(self) -> None:
        with patch.object(
            throttling_policy,
            "THROTTLING_LEVELS",
            [config.ThrottleLevel(250, "slow")],
        ):
            self.assertIsNone(
                throttling_policy.target_profile_name_for_usage(
                    vlan_id="guest",
                    day_total_mb=10_000,
                    calendar_month_total_mb=10_000,
                    throttleable_vlan_ids=["basic"],
                )
            )

    def test_target_profile_advances_to_highest_reached_threshold(self) -> None:
        levels = [
            config.ThrottleLevel(250, "half"),
            config.ThrottleLevel(500, "quarter"),
            config.ThrottleLevel(1000, "eighth"),
        ]
        with (
            patch.object(throttling_policy, "THROTTLING_LEVELS", levels),
            patch.object(throttling_policy, "MONTHLY_USAGE_ADJUSTMENTS", []),
        ):
            self.assertIsNone(
                throttling_policy.target_profile_name_for_usage("basic", 249.99, 0, ["basic"])
            )
            self.assertEqual(
                throttling_policy.target_profile_name_for_usage("basic", 250, 0, ["basic"]),
                "half",
            )
            self.assertEqual(
                throttling_policy.target_profile_name_for_usage("basic", 999.99, 0, ["basic"]),
                "quarter",
            )
            self.assertEqual(
                throttling_policy.target_profile_name_for_usage("basic", 1000, 0, ["basic"]),
                "eighth",
            )

    def test_monthly_usage_adjustments_lower_daily_thresholds(self) -> None:
        levels = [config.ThrottleLevel(1000, "eighth")]
        adjustments = [
            config.MonthlyUsageAdjustment(2000, 0.75),
            config.MonthlyUsageAdjustment(4000, 0.50),
        ]
        with (
            patch.object(throttling_policy, "THROTTLING_LEVELS", levels),
            patch.object(throttling_policy, "MONTHLY_USAGE_ADJUSTMENTS", adjustments),
        ):
            self.assertIsNone(
                throttling_policy.target_profile_name_for_usage("basic", 499.99, 4000, ["basic"])
            )
            self.assertEqual(
                throttling_policy.target_profile_name_for_usage("basic", 500, 4000, ["basic"]),
                "eighth",
            )


class FlowImportParsingTests(unittest.TestCase):
    def test_parse_nfdump_line_reads_custom_csv_output(self) -> None:
        flow = flow_import.parse_nfdump_line(
            "2026-05-07 12:34:56.789,00:01:02.5,TCP,192.168.1.10,54321,8.8.8.8,443,1.5 K,2 M"
        )

        self.assertIsNotNone(flow)
        assert flow is not None
        self.assertEqual(flow.started_at, datetime(2026, 5, 7, 12, 34, 56, 789000))
        self.assertEqual(flow.duration_seconds, 62.5)
        self.assertEqual(flow.proto, "TCP")
        self.assertEqual(flow.src_ip, "192.168.1.10")
        self.assertEqual(flow.src_port, 54321)
        self.assertEqual(flow.dst_ip, "8.8.8.8")
        self.assertEqual(flow.dst_port, 443)
        self.assertEqual(flow.packets, 1500)
        self.assertEqual(flow.bytes, 2_000_000)
        self.assertEqual(flow.reverse_packets, 0)
        self.assertEqual(flow.reverse_bytes, 0)

    def test_parse_nfdump_line_reads_bidirectional_counters(self) -> None:
        flow = flow_import.parse_nfdump_line(
            "2026-05-07 12:34:56.789,00:01:02.5,TCP,192.168.1.10,54321,8.8.8.8,443,10,900,20,2 M"
        )

        self.assertIsNotNone(flow)
        assert flow is not None
        self.assertEqual(flow.packets, 10)
        self.assertEqual(flow.bytes, 900)
        self.assertEqual(flow.reverse_packets, 20)
        self.assertEqual(flow.reverse_bytes, 2_000_000)

    def test_parse_nfdump_line_skips_headers_and_malformed_rows(self) -> None:
        self.assertIsNone(flow_import.parse_nfdump_line("Summary: total flows"))
        self.assertIsNone(flow_import.parse_nfdump_line("Date first seen,duration,proto"))

    def test_classify_wan_flow_marks_internal_to_external_as_upload(self) -> None:
        internal_networks = [ipaddress.ip_network("192.168.0.0/16")]
        flow = flow_import.ParsedFlow(
            started_at=datetime(2026, 5, 7, 12, 0),
            duration_seconds=10.0,
            proto="TCP",
            src_ip="192.168.1.10",
            src_port=12345,
            dst_ip="8.8.8.8",
            dst_port=443,
            packets=5,
            bytes=900,
        )

        row = flow_import.classify_wan_flow(flow, "nfcapd.202605071200", internal_networks)

        self.assertIsNotNone(row)
        assert row is not None
        self.assertEqual(row.direction, "upload")
        self.assertEqual(row.client_ip, "192.168.1.10")
        self.assertEqual(row.ended_at, datetime(2026, 5, 7, 12, 0, 10))

    def test_classify_wan_flow_marks_external_to_internal_as_download(self) -> None:
        internal_networks = [ipaddress.ip_network("192.168.0.0/16")]
        flow = flow_import.ParsedFlow(
            started_at=datetime(2026, 5, 7, 12, 0),
            duration_seconds=3.0,
            proto="UDP",
            src_ip="1.1.1.1",
            src_port=53,
            dst_ip="192.168.1.10",
            dst_port=5353,
            packets=2,
            bytes=128,
        )

        row = flow_import.classify_wan_flow(flow, "nfcapd.202605071200", internal_networks)

        self.assertIsNotNone(row)
        assert row is not None
        self.assertEqual(row.direction, "download")
        self.assertEqual(row.client_ip, "192.168.1.10")

    def test_classify_wan_flow_rows_splits_bidirectional_record(self) -> None:
        internal_networks = [ipaddress.ip_network("192.168.0.0/16")]
        flow = flow_import.ParsedFlow(
            started_at=datetime(2026, 5, 7, 12, 0),
            duration_seconds=10.0,
            proto="TCP",
            src_ip="192.168.1.10",
            src_port=12345,
            dst_ip="8.8.8.8",
            dst_port=443,
            packets=5,
            bytes=900,
            reverse_packets=8,
            reverse_bytes=2_000_000,
        )

        rows = flow_import.classify_wan_flow_rows(flow, "nfcapd.202605071200", internal_networks)

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0].direction, "upload")
        self.assertEqual(rows[0].client_ip, "192.168.1.10")
        self.assertEqual(rows[0].bytes, 900)
        self.assertEqual(rows[1].direction, "download")
        self.assertEqual(rows[1].client_ip, "192.168.1.10")
        self.assertEqual(rows[1].src_ip, "8.8.8.8")
        self.assertEqual(rows[1].dst_ip, "192.168.1.10")
        self.assertEqual(rows[1].bytes, 2_000_000)

    def test_classify_wan_flow_ignores_lan_to_lan_and_external_to_external(self) -> None:
        internal_networks = [ipaddress.ip_network("192.168.0.0/16")]
        base_flow = {
            "started_at": datetime(2026, 5, 7, 12, 0),
            "duration_seconds": 1.0,
            "proto": "TCP",
            "src_port": 123,
            "dst_port": 456,
            "packets": 1,
            "bytes": 64,
        }

        lan_to_lan = flow_import.ParsedFlow(
            **base_flow,
            src_ip="192.168.1.10",
            dst_ip="192.168.1.11",
        )
        external_to_external = flow_import.ParsedFlow(
            **base_flow,
            src_ip="8.8.8.8",
            dst_ip="1.1.1.1",
        )

        self.assertIsNone(flow_import.classify_wan_flow(lan_to_lan, "source", internal_networks))
        self.assertIsNone(flow_import.classify_wan_flow(external_to_external, "source", internal_networks))

    def test_import_capture_file_skips_duplicate_wan_rows(self) -> None:
        internal_networks = [ipaddress.ip_network("192.168.0.0/16")]
        duplicate_row = (
            "2026-05-07 20:27:33.892,0.0,TCP,"
            "192.168.6.169,57158,17.57.144.26,5223,1,92"
        )
        captured_rows = []

        def fake_record_flow_import(source_file, rows, skipped_count):
            captured_rows.extend(rows)
            self.assertEqual(source_file, "nfcapd.202605072025")
            self.assertEqual(skipped_count, 1)
            return len(rows)

        with (
            patch.object(flow_import, "read_nfdump_file", return_value=f"{duplicate_row}\n{duplicate_row}\n"),
            patch.object(flow_import.db, "flow_import_exists", return_value=False, create=True),
            patch.object(flow_import.db, "record_flow_import", side_effect=fake_record_flow_import, create=True),
        ):
            imported_rows, skipped_rows = flow_import.import_capture_file(
                Path("nfcapd.202605072025"),
                internal_networks,
                "nfdump",
            )

        self.assertEqual(imported_rows, 1)
        self.assertEqual(skipped_rows, 1)
        self.assertEqual(len(captured_rows), 1)


if __name__ == "__main__":
    unittest.main()
