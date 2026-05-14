import sys
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import app
import lan_admin


class AppPackageBoundaryTests(unittest.TestCase):
    def test_lan_admin_package_uses_existing_lan_app_factory(self) -> None:
        self.assertIs(lan_admin.create_app, app.create_app)
