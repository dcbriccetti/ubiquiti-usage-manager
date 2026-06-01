import os
import sys
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

    def test_forwarded_for_cannot_grant_admin_access(self) -> None:
        flask_app = app.create_app()

        with (
            patch.object(app.api, "get_api_data", return_value=[]),
            patch.object(app, "find_client_mac_for_ip", return_value=None),
        ):
            response = flask_app.test_client().get(
                "/api/dashboard-snapshot",
                headers={"X-Forwarded-For": "192.168.1.10"},
                environ_base={"REMOTE_ADDR": "10.8.0.12"},
            )

        self.assertEqual(response.status_code, 403)

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

    def test_radius_identity_does_not_grant_admin_access(self) -> None:
        flask_app = app.create_app()

        with (
            patch.object(app.api, "get_api_data", side_effect=AssertionError),
            patch.object(app, "find_client_mac_for_ip", side_effect=AssertionError),
            patch.object(app.db, "get_usage_history", side_effect=AssertionError),
        ):
            response = flask_app.test_client().get(
                "/api/dashboard-snapshot",
                environ_base={"REMOTE_ADDR": "192.168.1.22"},
            )

        self.assertEqual(response.status_code, 403)

    def test_password_login_is_required_even_for_plus_radius_user(self) -> None:
        with patch.object(app.cfg, "LAN_ADMIN_PASSWORD_HASH", generate_password_hash("secret")):
            flask_app = app.create_app()

        with (
            patch.object(app.api, "get_api_data", side_effect=AssertionError),
            patch.object(app, "find_client_mac_for_ip", side_effect=AssertionError),
            patch.object(app.db, "get_usage_history", side_effect=AssertionError),
        ):
            response = flask_app.test_client().get(
                "/wan",
                environ_base={"REMOTE_ADDR": "192.168.1.22"},
            )

        self.assertEqual(response.status_code, 302)
        self.assertIn("/admin/login", response.headers["Location"])


if __name__ == "__main__":
    unittest.main()
