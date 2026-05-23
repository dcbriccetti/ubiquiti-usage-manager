import sys
import unittest
from pathlib import Path
from unittest.mock import patch
from datetime import datetime


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import app


class DashboardRouteTests(unittest.TestCase):
    def test_dashboard_entrypoint_shows_loading_page_before_building_payload(self) -> None:
        flask_app = app.create_app()

        with (
            patch.object(app.cfg, "PLUS_ADMIN_IPS", {"127.0.0.1"}),
            patch.object(app, "build_live_dashboard_payload", side_effect=AssertionError),
            patch.object(app, "render_template", return_value="rendered") as render_template,
        ):
            response = flask_app.test_client().get(
                "/?window=today&activity_span=24h",
                environ_base={"REMOTE_ADDR": "127.0.0.1"},
            )

        self.assertEqual(response.status_code, 200)
        render_template.assert_called_once_with(
            "loading.html",
            loading_title="Loading Dashboard",
            loading_message="Collecting live clients and Internet activity...",
            target_url="/dashboard?window=today&activity_span=24h",
        )

    def test_dashboard_report_builds_payload(self) -> None:
        flask_app = app.create_app()

        with (
            patch.object(app.cfg, "PLUS_ADMIN_IPS", {"127.0.0.1"}),
            patch.object(app, "build_live_dashboard_payload", return_value={"rows": []}) as build_payload,
            patch.object(app, "render_template", return_value="rendered") as render_template,
        ):
            response = flask_app.test_client().get(
                "/dashboard?window=today&activity_span=24h",
                environ_base={"REMOTE_ADDR": "127.0.0.1"},
            )

        self.assertEqual(response.status_code, 200)
        build_payload.assert_called_once_with("today", "24h", 60)
        render_template.assert_called_once_with(
            "dashboard.html",
            initial_dashboard_payload={"rows": []},
        )

    def test_client_detail_prompts_before_loading_wan_details(self) -> None:
        flask_app = app.create_app()
        usage_record = app.UsageRecord(
            mac="aa:bb:cc:dd:ee:ff",
            user_id="1234",
            name="Pixel",
            vlan="Plus",
            mb_used=0.0,
        )
        context = {
            "mac": usage_record.mac,
            "latest_record": usage_record,
            "usage_history": [usage_record],
            "daily_total_mb": 0.0,
            "last_7_days_total_mb": 0.0,
            "calendar_month_total_mb": 0.0,
            "month_cost_cents": 0.0,
            "wan_client_ip": "",
            "wan_usage_available": False,
            "wan_identity_observed_at": None,
            "wan_today_download_mb": 0.0,
            "wan_today_upload_mb": 0.0,
            "wan_today_total_mb": 0.0,
            "wan_month_download_mb": 0.0,
            "wan_month_upload_mb": 0.0,
            "wan_month_total_mb": 0.0,
            "wan_import_usage_rows": [],
            "access_mode_usage_rows": [],
            "flow_activity_rows": [],
            "flow_activity_range_options": [],
            "selected_flow_activity_range": "this_month",
            "selected_flow_activity_range_label": "This month",
            "voucher_usage": None,
            "usage_scales": [],
            "current_month_label": "May 2026",
            "speed_limits_by_name": {},
        }

        with (
            patch.object(app.cfg, "PLUS_ADMIN_IPS", {"127.0.0.1"}),
            patch.object(app, "get_client_usage_context", return_value=context),
        ):
            response = flask_app.test_client().get("/clients/aa:bb:cc:dd:ee:ff")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Show Internet Details", body)
        self.assertIn("data-load-wan-details", body)
        self.assertIn("data-wan-details-url", body)
        self.assertNotIn("Loading Internet totals", body)
        self.assertNotIn("chart.umd.min.js", body)

    def test_client_wan_details_uses_lightweight_detail_context(self) -> None:
        flask_app = app.create_app()
        usage_record = app.UsageRecord(
            mac="aa:bb:cc:dd:ee:ff",
            user_id="1234",
            name="Pixel",
            vlan="Plus",
            mb_used=0.0,
            timestamp=datetime(2026, 5, 22, 12, 0),
        )
        base_context = {
            "mac": usage_record.mac,
            "latest_record": usage_record,
            "usage_history": [usage_record],
            "daily_total_mb": 0.0,
            "last_7_days_total_mb": 0.0,
            "calendar_month_total_mb": 0.0,
            "month_cost_cents": 0.0,
            "wan_client_ip": "",
            "wan_usage_available": False,
            "wan_identity_observed_at": None,
            "wan_today_download_mb": 0.0,
            "wan_today_upload_mb": 0.0,
            "wan_today_total_mb": 0.0,
            "wan_month_download_mb": 0.0,
            "wan_month_upload_mb": 0.0,
            "wan_month_total_mb": 0.0,
            "wan_import_usage_rows": [],
            "access_mode_usage_rows": [],
            "flow_activity_rows": [],
            "flow_activity_range_options": [],
            "selected_flow_activity_range": "this_month",
            "selected_flow_activity_range_label": "This month",
            "voucher_usage": None,
            "usage_scales": [],
            "current_month_label": "May 2026",
            "speed_limits_by_name": {},
        }
        detail_context = {
            "mac": usage_record.mac,
            "wan_import_usage_rows": [],
            "flow_activity_rows": [],
            "flow_activity_range_options": [
                {"key": "this_month", "label": "This month", "selected": True},
            ],
            "selected_flow_activity_range": "this_month",
            "selected_flow_activity_range_label": "This month",
            "current_month_label": "May 2026",
        }

        with (
            patch.object(app.cfg, "PLUS_ADMIN_IPS", {"127.0.0.1"}),
            patch.object(app, "get_client_usage_context", return_value=base_context) as usage_context,
            patch.object(app, "get_client_wan_detail_context", return_value=detail_context) as wan_context,
        ):
            response = flask_app.test_client().get("/clients/aa:bb:cc:dd:ee:ff/wan-details")

        self.assertEqual(response.status_code, 200)
        usage_context.assert_called_once_with(
            "aa:bb:cc:dd:ee:ff",
            include_wan_details=False,
        )
        wan_context.assert_called_once_with("aa:bb:cc:dd:ee:ff")
        body = response.get_data(as_text=True)
        self.assertIn("Top Internet Activities", body)
        self.assertIn("Recent Internet Usage", body)
        self.assertNotIn("Internet Today", body)


if __name__ == "__main__":
    unittest.main()
