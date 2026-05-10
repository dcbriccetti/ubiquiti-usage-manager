import os
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

if "database" in sys.modules and not hasattr(sys.modules["database"], "UsageRecord"):
    del sys.modules["database"]

import app


def usage_record(*, user_id: str | None, vlan: str | None) -> types.SimpleNamespace:
    return types.SimpleNamespace(
        user_id=user_id,
        vlan=vlan,
        mac="aa:bb:cc:dd:ee:ff",
        name="Test device",
        ap_name="Test AP",
    )


class PlusAdminAccessTests(unittest.TestCase):
    def setUp(self) -> None:
        os.environ.pop("DEV_REQUEST_IP", None)

    def test_live_plus_admin_overrides_stale_basic_usage_history(self) -> None:
        flask_app = app.create_app()
        live_clients = [
            {
                "mac": "aa:bb:cc:dd:ee:ff",
                "network": "Plus",
                "1x_identity": "daveb",
            }
        ]

        with (
            patch.object(app.cfg, "PLUS_ADMINS", {"daveb"}),
            patch.object(app, "find_client_mac_for_ip", return_value="aa:bb:cc:dd:ee:ff"),
            patch.object(
                app.db,
                "get_usage_history",
                return_value=[usage_record(user_id="", vlan="Basic")],
            ),
            patch.object(app.api, "get_api_data", return_value=live_clients),
            patch.object(app, "build_live_dashboard_payload", return_value={}),
        ):
            response = flask_app.test_client().get(
                "/api/dashboard-snapshot",
                environ_base={"REMOTE_ADDR": "192.168.1.22"},
            )

        self.assertEqual(response.status_code, 200)

    def test_configured_plus_admin_names_are_normalized(self) -> None:
        flask_app = app.create_app()
        live_clients = [
            {
                "mac": "aa:bb:cc:dd:ee:ff",
                "network": "Plus",
                "1x_identity": "daveb",
            }
        ]

        with (
            patch.object(app.cfg, "PLUS_ADMINS", {" DaveB "}),
            patch.object(app, "find_client_mac_for_ip", return_value="aa:bb:cc:dd:ee:ff"),
            patch.object(app.db, "get_usage_history", return_value=[]),
            patch.object(app.api, "get_api_data", return_value=live_clients),
            patch.object(app, "build_live_dashboard_payload", return_value={}),
        ):
            response = flask_app.test_client().get(
                "/api/dashboard-snapshot",
                environ_base={"REMOTE_ADDR": "192.168.1.22"},
            )

        self.assertEqual(response.status_code, 200)

    def test_plus_report_title_network_counts_as_plus_network(self) -> None:
        flask_app = app.create_app()
        live_clients = [
            {
                "mac": "aa:bb:cc:dd:ee:ff",
                "network": "Example Plus",
                "1x_identity": "president",
            }
        ]

        with (
            patch.object(app.cfg, "PLUS_REPORT_TITLE", "Example Plus"),
            patch.object(app.cfg, "PLUS_ADMINS", {"president", "it"}),
            patch.object(app, "find_client_mac_for_ip", return_value="aa:bb:cc:dd:ee:ff"),
            patch.object(app.db, "get_usage_history", return_value=[]),
            patch.object(app.api, "get_api_data", return_value=live_clients),
            patch.object(app, "build_live_dashboard_payload", return_value={}),
        ):
            response = flask_app.test_client().get(
                "/api/dashboard-snapshot",
                environ_base={"REMOTE_ADDR": "192.168.1.22"},
            )

        self.assertEqual(response.status_code, 200)

    def test_configured_plus_network_names_are_normalized(self) -> None:
        flask_app = app.create_app()
        live_clients = [
            {
                "mac": "aa:bb:cc:dd:ee:ff",
                "network": " Example Plus ",
                "1x_identity": "president",
            }
        ]

        with (
            patch.object(app.cfg, "PLUS_REPORT_TITLE", ""),
            patch.object(app.cfg, "PLUS_NETWORK_NAMES", {" example plus "}),
            patch.object(app.cfg, "PLUS_ADMINS", {"president"}),
            patch.object(app, "find_client_mac_for_ip", return_value="aa:bb:cc:dd:ee:ff"),
            patch.object(app.db, "get_usage_history", return_value=[]),
            patch.object(app.api, "get_api_data", return_value=live_clients),
            patch.object(app, "build_live_dashboard_payload", return_value={}),
        ):
            response = flask_app.test_client().get(
                "/api/dashboard-snapshot",
                environ_base={"REMOTE_ADDR": "192.168.1.22"},
            )

        self.assertEqual(response.status_code, 200)

    def test_live_basic_network_does_not_inherit_stale_plus_admin_status(self) -> None:
        flask_app = app.create_app()
        live_clients = [
            {
                "mac": "aa:bb:cc:dd:ee:ff",
                "network": "Basic",
                "1x_identity": "daveb",
            }
        ]

        with (
            patch.object(app.cfg, "PLUS_ADMINS", {"daveb"}),
            patch.object(app, "find_client_mac_for_ip", return_value="aa:bb:cc:dd:ee:ff"),
            patch.object(
                app.db,
                "get_usage_history",
                return_value=[usage_record(user_id="daveb", vlan="Plus")],
            ),
            patch.object(app.api, "get_api_data", return_value=live_clients),
        ):
            response = flask_app.test_client().get(
                "/api/dashboard-snapshot",
                environ_base={"REMOTE_ADDR": "192.168.1.22"},
            )

        self.assertEqual(response.status_code, 403)


if __name__ == "__main__":
    unittest.main()
