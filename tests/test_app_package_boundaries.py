import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import app
import lan_admin
from club_admin.app import create_app as create_club_app


class AppPackageBoundaryTests(unittest.TestCase):
    def test_lan_admin_package_uses_existing_lan_app_factory(self) -> None:
        self.assertIs(lan_admin.create_app, app.create_app)

    def test_club_admin_honors_reverse_proxy_url_prefix(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            with patch.dict("os.environ", {"USER_MANAGEMENT_URL_PREFIX": "/users"}):
                club_app = create_club_app(db_path)

            client = club_app.test_client()

            index_response = client.get("/users")
            self.assertEqual(index_response.status_code, 302)
            self.assertEqual(index_response.headers["Location"], "/users/self-checkin")

            response = client.get("/users/self-checkin")
            body = response.get_data(as_text=True)
            self.assertEqual(response.status_code, 200)
            self.assertIn('/users/static/club-admin.css', body)
            self.assertIn('action="/users/self-checkin"', body)
            self.assertIn('class="page self-checkin-page"', body)

    def test_club_admin_prefixed_admin_redirect_keeps_prefix_in_next_url(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "club-users.db"
            with patch.dict("os.environ", {"USER_MANAGEMENT_URL_PREFIX": "/users"}):
                club_app = create_club_app(db_path)

            response = club_app.test_client().get("/users/members")

        self.assertEqual(response.status_code, 302)
        self.assertEqual(
            response.headers["Location"],
            "/users/admin/login?next=/users/members",
        )
