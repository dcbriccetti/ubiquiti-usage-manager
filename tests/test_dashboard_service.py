import sys
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

if "database" in sys.modules and not hasattr(sys.modules["database"], "WanIdentityUsageSummary"):
    del sys.modules["database"]

import dashboard_service


class DashboardActivitySpanTests(unittest.TestCase):
    def tearDown(self) -> None:
        dashboard_service.clear_dashboard_wan_cache()

    def test_wan_import_status_uses_just_now_for_zero_and_negative_age(self) -> None:
        self.assertEqual(
            dashboard_service.render_wan_import_status(None),
            "Internet data: none yet",
        )
        self.assertEqual(
            dashboard_service.render_wan_import_status(-1),
            "Internet data updated just now",
        )
        self.assertEqual(
            dashboard_service.render_wan_import_status(0),
            "Internet data updated just now",
        )
        self.assertEqual(
            dashboard_service.render_wan_import_status(1),
            "Internet data updated 1m ago",
        )
        self.assertEqual(
            dashboard_service.render_wan_import_status(2),
            "Internet data updated 2m ago",
        )

    def test_activity_span_normalization_accepts_current_and_legacy_values(self) -> None:
        self.assertEqual(dashboard_service.normalize_activity_span("1h"), "1h")
        self.assertEqual(dashboard_service.normalize_activity_span("6h"), "6h")
        self.assertEqual(dashboard_service.normalize_activity_span("24h"), "24h")
        self.assertEqual(dashboard_service.normalize_activity_span("7d"), "7d")

        self.assertEqual(dashboard_service.normalize_activity_span("12m"), "1h")
        self.assertEqual(dashboard_service.normalize_activity_span("12h"), "24h")
        self.assertEqual(dashboard_service.normalize_activity_span("12d"), "7d")
        self.assertEqual(dashboard_service.normalize_activity_span("unexpected"), "1h")

    def test_activity_sparkline_bucket_sizes_match_selected_span(self) -> None:
        rows: list[dashboard_service.DashboardRow] = [
            {
                "user_id": "",
                "name": "Test client",
                "ap_name": "",
                "ap_count": 0,
                "ap_breakdown": "",
                "mac": "aa:bb:cc:dd:ee:ff",
                "ip_prefix": "",
                "ip_half": "",
                "vlan_name": "",
                "frequency_band": "",
                "channel": "",
                "signal": None,
                "recent_activity": [],
                "recent_total_mb": 42.0,
                "last_5_min_mb": 2.0,
                "last_5_min_mbps": 2.0 * 8.0 / 300.0,
                "connection_duration": "",
                "day_total_mb": 0.0,
                "day_cost_cents": 0.0,
                "last_7_days_total_mb": 0.0,
                "last_7_days_cost_cents": 0.0,
                "calendar_month_total_mb": 0.0,
                "month_cost_cents": 0.0,
                "speed_limit_name": "",
                "speed_limit_up_kbps": None,
                "speed_limit_down_kbps": None,
            }
        ]

        expected = {
            "1h": (12, 300),
            "6h": (12, 1800),
            "24h": (24, 3600),
            "7d": (14, 43_200),
        }
        for span, (buckets, bucket_seconds) in expected.items():
            with self.subTest(span=span):
                with patch.object(
                    dashboard_service.db,
                    "get_wan_activity_series_by_mac",
                    return_value={"aa:bb:cc:dd:ee:ff": [1.0] * buckets},
                ) as get_series:
                    dashboard_service.add_recent_activity(rows, dashboard_service.normalize_activity_span(span))

                get_series.assert_called_once_with(
                    ["aa:bb:cc:dd:ee:ff"],
                    buckets=buckets,
                    bucket_seconds=bucket_seconds,
                )
                self.assertEqual(len(rows[0]["recent_activity"]), buckets)
                self.assertEqual(rows[0]["recent_total_mb"], 42.0)
                self.assertEqual(rows[0]["last_5_min_mb"], 2.0)
                self.assertAlmostEqual(rows[0]["last_5_min_mbps"], 2.0 * 8.0 / 300.0)

    def test_top_consumers_use_last_5_min_wan_rows(self) -> None:
        rows = [
            dashboard_service.db.WanIdentityUsageSummary(
                client_ip="192.168.4.10",
                mac="aa:bb:cc:dd:ee:01",
                name="Phone",
                user_id="",
                vlan="Basic",
                upload_bytes=250_000,
                download_bytes=750_000,
                flow_count=5,
            ),
            dashboard_service.db.WanIdentityUsageSummary(
                client_ip="192.168.4.11",
                mac="aa:bb:cc:dd:ee:02",
                name="Laptop",
                user_id="alex",
                vlan="Plus",
                upload_bytes=0,
                download_bytes=2_000_000,
                flow_count=3,
            ),
            dashboard_service.db.WanIdentityUsageSummary(
                client_ip="192.168.4.12",
                mac="aa:bb:cc:dd:ee:03",
                name="Idle",
                user_id="",
                vlan="Basic",
                upload_bytes=0,
                download_bytes=0,
                flow_count=1,
            ),
        ]

        consumers = dashboard_service.build_top_consumers_for_last_5_min(rows)

        self.assertEqual([consumer["label"] for consumer in consumers], ["alex", "Phone"])
        self.assertEqual([consumer["interval_mb"] for consumer in consumers], [2.0, 1.0])

    def test_dashboard_wan_cache_spans_normal_stream_tick(self) -> None:
        wan_data = dashboard_service.DashboardWanData(
            last_5_min_rows=[],
            recent_rows=[],
            today_rows=[],
            seven_day_rows=[],
            month_rows=[],
            last_5_min_totals_by_mac={},
            recent_totals_by_mac={},
            today_totals_by_mac={},
            seven_day_totals_by_mac={},
            month_totals_by_mac={},
            total_today_mb=0.0,
            total_last_7_days_mb=0.0,
            total_calendar_month_mb=0.0,
            last_5_min_mb=0.0,
            last_5_min_mbps=0.0,
            wan_import_status="Internet data updated just now",
            wan_import_stale=False,
        )

        with (
            patch.object(dashboard_service, "_build_dashboard_wan_data", return_value=wan_data) as build_wan_data,
            patch.object(dashboard_service.monotonic_time, "monotonic", side_effect=[0.0, 0.0, 0.0, 61.0]),
        ):
            first = dashboard_service.get_dashboard_wan_data(datetime(2026, 5, 22, 12, 0))
            second = dashboard_service.get_dashboard_wan_data(datetime(2026, 5, 22, 12, 1, 1))

        self.assertIs(first, wan_data)
        self.assertIs(second, wan_data)
        build_wan_data.assert_called_once()


if __name__ == "__main__":
    unittest.main()
