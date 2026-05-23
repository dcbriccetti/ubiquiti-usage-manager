import sys
import unittest
from pathlib import Path
from unittest.mock import patch


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


if __name__ == "__main__":
    unittest.main()
