import os
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch

from werkzeug.security import generate_password_hash


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

    def test_configured_lan_admin_password_keeps_dashboard_self_service_fallback(self) -> None:
        with patch.object(app.cfg, "LAN_ADMIN_PASSWORD_HASH", generate_password_hash("secret")):
            flask_app = app.create_app()

        with (
            patch.object(app.api, "get_api_data", return_value=[]),
            patch.object(app, "find_client_mac_for_ip", return_value=None),
        ):
            response = flask_app.test_client().get(
                "/dashboard",
                environ_base={"REMOTE_ADDR": "10.8.0.12"},
            )

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.headers["Location"], "/my-usage")

    def test_configured_lan_admin_password_redirects_admin_only_pages_to_login(self) -> None:
        with patch.object(app.cfg, "LAN_ADMIN_PASSWORD_HASH", generate_password_hash("secret")):
            flask_app = app.create_app()

        with (
            patch.object(app.api, "get_api_data", return_value=[]),
            patch.object(app, "find_client_mac_for_ip", return_value=None),
        ):
            response = flask_app.test_client().get(
                "/wan",
                environ_base={"REMOTE_ADDR": "10.8.0.12"},
            )

        self.assertEqual(response.status_code, 302)
        self.assertIn("/admin/login", response.headers["Location"])
        self.assertIn("next=/wan", response.headers["Location"])

    def test_untrusted_forwarded_for_cannot_spoof_admin_ip(self) -> None:
        flask_app = app.create_app()

        with (
            patch.object(app.cfg, "PLUS_ADMIN_IPS", {"192.168.1.10"}),
            patch.object(app.api, "get_api_data", return_value=[]),
            patch.object(app, "find_client_mac_for_ip", return_value=None),
        ):
            response = flask_app.test_client().get(
                "/api/dashboard-snapshot",
                headers={"X-Forwarded-For": "192.168.1.10"},
                environ_base={"REMOTE_ADDR": "10.8.0.12"},
            )

        self.assertEqual(response.status_code, 403)

    def test_trusted_proxy_real_ip_can_match_admin_ip(self) -> None:
        flask_app = app.create_app()

        with (
            patch.object(app.cfg, "PLUS_ADMIN_IPS", {"192.168.1.10"}),
            patch.object(app, "build_live_dashboard_payload", return_value={}),
        ):
            response = flask_app.test_client().get(
                "/api/dashboard-snapshot",
                headers={
                    "X-Forwarded-For": "203.0.113.200, 192.168.1.10",
                    "X-Real-IP": "192.168.1.10",
                },
                environ_base={"REMOTE_ADDR": "127.0.0.1"},
            )

        self.assertEqual(response.status_code, 200)

    def test_lan_admin_password_login_allows_dashboard_without_plus_identity(self) -> None:
        with patch.object(app.cfg, "LAN_ADMIN_PASSWORD_HASH", generate_password_hash("secret")):
            flask_app = app.create_app()

        client = flask_app.test_client()
        login_response = client.post(
            "/admin/login",
            data={"password": "secret", "next": "/dashboard"},
            environ_base={"REMOTE_ADDR": "10.8.0.12"},
        )

        with (
            patch.object(app.api, "get_api_data", side_effect=AssertionError),
            patch.object(app, "build_live_dashboard_payload", return_value={"rows": []}) as build_payload,
            patch.object(app, "render_template", return_value="rendered"),
        ):
            dashboard_response = client.get(
                "/dashboard",
                environ_base={"REMOTE_ADDR": "10.8.0.12"},
            )

        self.assertEqual(login_response.status_code, 302)
        self.assertEqual(login_response.headers["Location"], "/dashboard")
        self.assertEqual(dashboard_response.status_code, 200)
        build_payload.assert_called_once()

    def test_lan_admin_password_login_rejects_wrong_password(self) -> None:
        with patch.object(app.cfg, "LAN_ADMIN_PASSWORD_HASH", generate_password_hash("secret")):
            flask_app = app.create_app()

        response = flask_app.test_client().post(
            "/admin/login",
            data={"password": "wrong", "next": "/dashboard"},
            environ_base={"REMOTE_ADDR": "10.8.0.12"},
        )

        self.assertEqual(response.status_code, 200)
        self.assertIn("Password was not accepted.", response.get_data(as_text=True))

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

    def test_successful_admin_check_is_cached_for_fast_followup_clicks(self) -> None:
        flask_app = app.create_app()
        live_clients = [
            {
                "mac": "aa:bb:cc:dd:ee:ff",
                "ip": "192.168.1.22",
                "network": "Plus",
                "1x_identity": "daveb",
            }
        ]

        with (
            patch.object(app.cfg, "PLUS_ADMINS", {"daveb"}),
            patch.object(app.db, "get_usage_history", return_value=[]),
            patch.object(app.api, "get_api_data", return_value=live_clients) as get_api_data,
            patch.object(app, "build_live_dashboard_payload", return_value={}),
        ):
            client = flask_app.test_client()
            first_response = client.get(
                "/api/dashboard-snapshot",
                environ_base={"REMOTE_ADDR": "192.168.1.22"},
            )
            second_response = client.get(
                "/api/dashboard-snapshot",
                environ_base={"REMOTE_ADDR": "192.168.1.22"},
            )

        self.assertEqual(first_response.status_code, 200)
        self.assertEqual(second_response.status_code, 200)
        self.assertEqual(get_api_data.call_count, 1)

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
