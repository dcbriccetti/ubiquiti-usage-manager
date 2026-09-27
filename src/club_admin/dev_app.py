"""Run the users app on loopback with an isolated, fictional test database.

Usage: PYTHONPATH=src .venv/bin/python -m club_admin.dev_app
"""
from contextlib import closing
from pathlib import Path
import os

from werkzeug.security import generate_password_hash

import config as cfg
from club_admin import database, guest_registration_repository, member_repository
from club_admin.models import GuestRegistration, Member
from club_admin.visit_guests import today

ROOT = Path(__file__).resolve().parents[2]
TEST_DB = ROOT / "data" / "club_users.guest-test.db"


def create_test_app():
    from club_admin.app import create_app, _record_self_checkin

    # All overrides are confined to this process. Never open the normal database.
    os.environ["USER_MANAGEMENT_URL_PREFIX"] = "/"
    cfg.USER_MANAGEMENT_ORGANIZATION_NAME = "Sequoians · TEST DATA"
    cfg.USER_MANAGEMENT_ADMIN_PASSWORD_HASH = generate_password_hash("local-guests")
    cfg.USER_MANAGEMENT_DOCUMENTS_DIR = str(ROOT / "data" / "club_documents_guest_test")
    app = create_app(TEST_DB)
    app.jinja_env.auto_reload = True
    app.config["USER_MANAGEMENT_CHECKIN_MONITOR_TOKEN"] = ""
    with closing(database.connect(TEST_DB)) as conn:
        if not member_repository.get_member_by_card_number(conn, "TEST-HOST"):
            for card, first, last, phone, membership, screening in (
                ("TEST-HOST", "Dave", "Example", "2025550103", "Full Member", "safe"),
                ("TEST-ALICE", "Alice", "Rivera", "2025550148", "Visitor", "pending"),
                ("TEST-BOB", "Bob", "Chen", "2025550172", "Visitor", "safe"),
                ("TEST-CAROL", "Carol", "Example", "2025550115", "Associate Member", "safe"),
                ("TEST-BLOCKED", "Robin", "Example", "2025550199", "Visitor", "banned"),
            ):
                member_repository.insert_member(conn, Member(card_number=card, first_name=first,
                    last_name=last, cell_phone=phone, membership=membership, screening_status=screening))
            alice = member_repository.get_member_by_card_number(conn, "TEST-ALICE")
            guest_registration_repository.insert_guest_registration(conn,
                GuestRegistration(user_id=alice.id, visit_date=today()))
            _record_self_checkin(conn, alice, same_day=True)
            conn.commit()
    return app


if __name__ == "__main__":
    app = create_test_app()
    print(f"Test database: {TEST_DB}", flush=True)
    print("Open http://127.0.0.1:5052/self-checkin — staff password: local-guests", flush=True)
    app.run(host="127.0.0.1", port=5052, debug=False, use_reloader=False)
