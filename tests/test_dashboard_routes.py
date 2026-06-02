import sys
import unittest
from pathlib import Path
from unittest.mock import patch
from datetime import date, datetime


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import app


def admin_client(flask_app):
    client = flask_app.test_client()
    with client.session_transaction() as session:
        session["lan_admin_authenticated"] = True
    return client


def voucher_trend_fixture() -> app.db.PlusVoucherConsumptionTrend:
    return app.db.PlusVoucherConsumptionTrend(
        period_start=date(2026, 5, 20),
        period_end=date(2026, 5, 30),
        daily_usage=[],
        total_used_mb=0.0,
        total_remaining_mb=0.0,
        active_allocation_gb=0,
        activated_voucher_count=0,
        lifetime_average_daily_mb=0.0,
        recent_average_daily_mb=0.0,
        prior_average_daily_mb=0.0,
        today_mb=0.0,
        yesterday_mb=0.0,
        projected_days_remaining=None,
        projected_depletion_date=None,
        forecast_performance=app.db.PlusVoucherForecastPerformance(
            scored_forecast_count=0,
            mean_absolute_error_mb=None,
            baseline_mean_absolute_error_mb=None,
            improvement_pct=None,
            calibration_factor=1.0,
            baseline_daily_forecast_mb=0.0,
            learned_daily_forecast_mb=0.0,
            latest_scored_day=None,
        ),
    )


class DashboardRouteTests(unittest.TestCase):
    def test_dashboard_entrypoint_shows_loading_page_before_building_payload(self) -> None:
        flask_app = app.create_app()

        with (
            patch.object(app, "build_live_dashboard_payload", side_effect=AssertionError),
            patch.object(app, "render_template", return_value="rendered") as render_template,
        ):
            response = admin_client(flask_app).get(
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
            patch.object(app, "build_live_dashboard_payload", return_value={"rows": []}) as build_payload,
            patch.object(app, "render_template", return_value="rendered") as render_template,
        ):
            response = admin_client(flask_app).get(
                "/dashboard?window=today&activity_span=24h",
                environ_base={"REMOTE_ADDR": "127.0.0.1"},
            )

        self.assertEqual(response.status_code, 200)
        build_payload.assert_called_once_with("today", "24h", 60)
        render_template.assert_called_once_with(
            "dashboard.html",
            initial_dashboard_payload={"rows": []},
        )

    def test_vouchers_page_uses_isp_billing_cycle_and_basic_usage_baseline(self) -> None:
        class FrozenDateTime(datetime):
            @classmethod
            def now(cls, tz=None):
                return cls(2026, 5, 30, 12, 0, tzinfo=tz)

        flask_app = app.create_app()
        trend = app.db.PlusVoucherConsumptionTrend(
            period_start=date(2026, 5, 20),
            period_end=date(2026, 5, 30),
            daily_usage=[
                app.db.PlusVoucherDailyUsage(day=date(2026, 5, day), used_mb=1000.0)
                for day in range(20, 31)
            ],
            total_used_mb=20_000.0,
            total_remaining_mb=80_000.0,
            active_allocation_gb=100,
            activated_voucher_count=1,
            lifetime_average_daily_mb=1000.0,
            recent_average_daily_mb=2000.0,
            prior_average_daily_mb=1000.0,
            today_mb=1000.0,
            yesterday_mb=1000.0,
            projected_days_remaining=40.0,
            projected_depletion_date=date(2026, 7, 9),
            forecast_performance=app.db.PlusVoucherForecastPerformance(
                scored_forecast_count=0,
                mean_absolute_error_mb=None,
                baseline_mean_absolute_error_mb=None,
                improvement_pct=None,
                calibration_factor=1.0,
                baseline_daily_forecast_mb=2000.0,
                learned_daily_forecast_mb=2000.0,
                latest_scored_day=None,
            ),
        )

        with (
            patch.object(app, "datetime", FrozenDateTime),
            patch.object(app.cfg, "ISP_BILLING_CYCLE_START_DAY", 20),
            patch.object(app.cfg, "EXPECTED_BASIC_USAGE_GB_PER_CYCLE", 400.0),
            patch.object(app.db, "get_plus_vouchers", return_value=[]),
            patch.object(app.db, "get_active_plus_voucher_summaries", return_value=[]) as active_summaries,
            patch.object(app.db, "get_plus_voucher_consumption_trend", return_value=trend) as consumption_trend,
            patch.object(app.db, "get_unconsumed_plus_voucher_count", return_value=0),
            patch.object(app, "render_template", return_value="rendered") as render_template,
        ):
            response = admin_client(flask_app).get("/vouchers")

        self.assertEqual(response.status_code, 200)
        active_summaries.assert_called_once_with()
        consumption_trend.assert_called_once()
        self.assertEqual(consumption_trend.call_args.kwargs["lookback_days"], 11)
        self.assertEqual(consumption_trend.call_args.kwargs["period_end"], FrozenDateTime(2026, 5, 30, 12, 0))

        context = render_template.call_args.kwargs
        topoff = context["voucher_topoff_analysis"]
        self.assertEqual(topoff["billing_cycle_start"], date(2026, 5, 20))
        self.assertEqual(topoff["billing_cycle_end"], date(2026, 6, 20))
        self.assertEqual(topoff["expected_basic_usage_mb"], 400_000.0)
        self.assertEqual(topoff["voucher_cycle_used_mb"], 11_000.0)
        self.assertLess(topoff["cycle_forecast_mb"], 500_000.0)
        self.assertGreater(topoff["cycle_forecast_headroom_mb"], 0.0)
        self.assertEqual(topoff["recommendation"], "Wait")

    def test_vouchers_page_links_to_batch_and_single_printing(self) -> None:
        flask_app = app.create_app()
        voucher = app.db.PlusVoucherRecord(
            id=7,
            batch_id="batch-123",
            user_id=9123,
            password="pass-9123",
            allocation_gb=40,
            generated_at=datetime(2026, 5, 30, 12, 0),
            consumed_at=None,
        )

        with (
            patch.object(app.db, "get_plus_vouchers", return_value=[voucher]),
            patch.object(app.db, "get_active_plus_voucher_summaries", return_value=[]),
            patch.object(app.db, "get_plus_voucher_consumption_trend", return_value=voucher_trend_fixture()),
            patch.object(app.db, "get_unconsumed_plus_voucher_count", return_value=1),
        ):
            response = admin_client(flask_app).get("/vouchers")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn('href="/vouchers/batches/batch-123/print"', body)
        self.assertIn('href="/vouchers/7/print"', body)
        self.assertIn("Batch", body)
        self.assertIn("Single", body)
        self.assertNotIn("/vouchers/batches/batch-123/thermal", body)

    def test_voucher_batch_print_renders_for_brother_printer(self) -> None:
        flask_app = app.create_app()
        vouchers = [
            app.db.PlusVoucherRecord(
                id=7,
                batch_id="batch-123",
                user_id=9123,
                password="pass-9123",
                allocation_gb=40,
                generated_at=datetime(2026, 5, 30, 12, 0),
                consumed_at=None,
            ),
            app.db.PlusVoucherRecord(
                id=8,
                batch_id="batch-123",
                user_id=9124,
                password="pass-9124",
                allocation_gb=40,
                generated_at=datetime(2026, 5, 30, 12, 0),
                consumed_at=None,
            ),
        ]

        with patch.object(app.db, "get_plus_voucher_batch", return_value=vouchers):
            response = admin_client(flask_app).get("/vouchers/batches/batch-123/print")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Plus Vouchers", body)
        self.assertIn("Batch batch-123", body)
        self.assertIn("9123", body)
        self.assertIn("pass-9123", body)
        self.assertIn("9124", body)
        self.assertIn("pass-9124", body)
        self.assertIn("40 GB", body)
        self.assertIn("@page", body)
        self.assertIn("size: 62mm 90mm", body)
        self.assertNotIn("iPhone/iPad", body)

    def test_single_voucher_print_renders_one_voucher(self) -> None:
        flask_app = app.create_app()
        voucher = app.db.PlusVoucherRecord(
            id=7,
            batch_id="batch-123",
            user_id=9123,
            password="pass-9123",
            allocation_gb=40,
            generated_at=datetime(2026, 5, 30, 12, 0),
            consumed_at=None,
        )

        with patch.object(app.db, "get_plus_voucher", return_value=voucher):
            response = admin_client(flask_app).get("/vouchers/7/print")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Batch batch-123", body)
        self.assertIn("9123", body)
        self.assertIn("pass-9123", body)
        self.assertNotIn("9124", body)

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
            patch.object(app, "get_client_usage_context", return_value=context),
        ):
            response = admin_client(flask_app).get("/clients/aa:bb:cc:dd:ee:ff")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Show Internet Details", body)
        self.assertIn("data-load-wan-details", body)
        self.assertIn("data-wan-details-url", body)
        self.assertIn("Internet Today", body)
        self.assertNotIn("Loading Internet totals", body)

    def test_client_wan_details_loads_full_deferred_panels(self) -> None:
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

        with (
            patch.object(app, "get_client_usage_context", return_value=base_context) as usage_context,
        ):
            response = admin_client(flask_app).get("/clients/aa:bb:cc:dd:ee:ff/wan-details")

        self.assertEqual(response.status_code, 200)
        usage_context.assert_called_once_with("aa:bb:cc:dd:ee:ff")
        body = response.get_data(as_text=True)
        self.assertIn("Top Internet Activities", body)
        self.assertIn("Recent Internet Usage", body)
        self.assertIn("Internet Today", body)


if __name__ == "__main__":
    unittest.main()
