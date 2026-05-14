import sys
import unittest
from pathlib import Path
from unittest.mock import patch


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import dashboard_service


class DashboardActivitySpanTests(unittest.TestCase):
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
                "recent_total_mb": 0.0,
                "last_5_min_mb": 0.0,
                "last_5_min_mbps": 0.0,
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
                if span == "1h":
                    self.assertEqual(rows[0]["last_5_min_mb"], 1.0)
                    self.assertAlmostEqual(rows[0]["last_5_min_mbps"], 8.0 / 300.0)
                else:
                    self.assertEqual(rows[0]["last_5_min_mb"], 0.0)
                    self.assertEqual(rows[0]["last_5_min_mbps"], 0.0)


if __name__ == "__main__":
    unittest.main()
