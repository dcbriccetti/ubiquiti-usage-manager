"""Regression coverage for shared-kiosk application identity and cancellation."""

from contextlib import closing
from html.parser import HTMLParser
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from werkzeug.datastructures import MultiDict

from test_club_member_import import create_admin_app, application_session_token
from club_admin import database, member_repository, membership_application_repository
from club_admin.app import (
    MEMBERSHIP_APPLICATION_ACTIVITY_KEY,
    MEMBERSHIP_APPLICATION_IDLE_SECONDS,
    MEMBERSHIP_APPLICATION_SESSION_KEY,
    MEMBERSHIP_APPLICATION_TOKEN_KEY,
)
from club_admin.models import Member


class ApplicationControls(HTMLParser):
    """Collect successful controls for a Cancel click, including hidden inputs."""

    def __init__(self):
        super().__init__()
        self.controls = []
        self.in_application = False

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == "form":
            self.in_application = "data-application-session" in attrs
        if not self.in_application:
            return
        if (tag == "input" and attrs.get("type") == "hidden") or (
            tag == "button" and attrs.get("value") == "cancel"
        ):
            self.controls.append((attrs["name"], attrs.get("value", "")))

    def handle_endtag(self, tag):
        if tag == "form":
            self.in_application = False


class ClubKioskSessionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db_path = Path(self.temp.name) / "users.db"
        self.app = create_admin_app(self.db_path)
        self.client = self.app.test_client()
        with closing(database.connect(self.db_path)) as connection:
            for card, name, phone in (("1", "Alice", "5105551111"), ("2", "Bob", "5105552222")):
                member_repository.insert_member(connection, Member(
                    first_name=name, last_name="Example", membership="Visitor",
                    card_number=card, cell_phone=phone, address=f"{name} private address",
                ))
            connection.commit()
        self.now = 1_800_000_000
        clock_patch = patch("club_admin.app.time.time", return_value=self.now)
        self.clock = clock_patch.start()
        self.addCleanup(clock_patch.stop)

    def identify(self, name="Alice", phone="5105551111"):
        return self.client.post("/membership-application", data={"phone": phone, "initials": name})

    def advance(self, seconds):
        self.now += seconds
        self.clock.return_value = self.now

    def assert_session_cleared(self):
        with self.client.session_transaction() as browser_session:
            for key in (MEMBERSHIP_APPLICATION_SESSION_KEY, MEMBERSHIP_APPLICATION_ACTIVITY_KEY, MEMBERSHIP_APPLICATION_TOKEN_KEY):
                self.assertNotIn(key, browser_session)

    def assert_no_application(self):
        with closing(database.connect(self.db_path)) as connection:
            self.assertEqual(membership_application_repository.list_membership_application_records(connection), [])

    def valid_submission(self, token=None):
        return {
            "action": "submit",
            "application_token": token if token is not None else application_session_token(self.client),
            "requested_membership": "Associate Member", "gender": "female",
            "occupation": "Engineer", "driver_license_number": "EXAMPLE123",
            "driver_license_state": "CA", "driver_license_expires": "2030-01-01",
            "emergency_contact_name": "Example Contact", "emergency_contact_relationship": "Friend",
            "emergency_contact_phone": "5105553333", "convicted": "no",
            "club_news_name_permission": "no", "aanr_member": "no", "other_club_member": "no",
        }

    def test_navigation_clears_identity_and_blocks_old_submission(self):
        for path in ("/self-checkin", "/guest-registration", "/"):
            with self.subTest(path=path):
                self.identify()
                old_form = self.valid_submission()
                self.client.get(path)
                self.assert_session_cleared()
                body = self.client.get("/membership-application").get_data(as_text=True)
                self.assertNotIn("Alice private address", body)
                self.assertNotIn("data-application-session", body)
                self.assertEqual(self.client.post("/membership-application", data=old_form).status_code, 302)
                self.assert_no_application()

    def test_actual_cancel_controls_cancel_without_validation(self):
        parser = ApplicationControls()
        parser.feed(self.identify().get_data(as_text=True))
        self.assertEqual([value for name, value in parser.controls if name == "action"], ["cancel"])
        response = self.client.post("/membership-application", data=MultiDict(parser.controls))
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.location, "/self-checkin")
        self.assert_session_cleared()
        self.assert_no_application()

    def test_completed_legacy_form_cancel_does_not_submit(self):
        self.identify()
        data = MultiDict(self.valid_submission())
        data.add("action", "cancel")
        response = self.client.post("/membership-application", data=data)
        self.assertEqual(response.location, "/self-checkin")
        self.assert_session_cleared()
        self.assert_no_application()

    def test_cancel_works_when_form_definition_is_unavailable(self):
        self.identify()
        self.app.config["USER_MANAGEMENT_MEMBERSHIP_APPLICATION_DEFINITION_PATH"] = "missing.toml"
        response = self.client.post("/membership-application", data={"action": "cancel"})
        self.assertEqual(response.location, "/self-checkin")
        self.assert_session_cleared()

    def test_expired_get_hides_identity(self):
        self.identify()
        self.advance(MEMBERSHIP_APPLICATION_IDLE_SECONDS)
        response = self.client.get("/membership-application")
        self.assertNotIn("Alice private address", response.get_data(as_text=True))
        self.assert_session_cleared()

    def test_expired_submission_and_activity_cannot_revive_session(self):
        for action in ("submit", "activity"):
            with self.subTest(action=action):
                self.identify()
                data = self.valid_submission()
                data["action"] = action
                self.advance(MEMBERSHIP_APPLICATION_IDLE_SECONDS)
                response = self.client.post("/membership-application", data=data)
                self.assertEqual(response.status_code, 401 if action == "activity" else 302)
                self.assert_session_cleared()
                self.assert_no_application()

    def test_activity_extends_session_but_page_load_does_not(self):
        self.identify()
        token = application_session_token(self.client)
        self.advance(MEMBERSHIP_APPLICATION_IDLE_SECONDS - 1)
        response = self.client.post("/membership-application", data={"action": "activity", "application_token": token})
        self.assertEqual(response.status_code, 200)
        self.assertIn("no-store", response.headers["Cache-Control"])
        self.advance(MEMBERSHIP_APPLICATION_IDLE_SECONDS - 1)
        self.assertIn("Alice private address", self.client.get("/membership-application").get_data(as_text=True))
        self.advance(1)
        self.assertNotIn("Alice private address", self.client.get("/membership-application").get_data(as_text=True))
        self.assert_session_cleared()

    def test_stale_tab_cannot_submit_for_next_applicant(self):
        self.identify()
        old_form = self.valid_submission()
        self.client.get("/self-checkin")
        self.identify("Bob", "5105552222")
        self.assertNotEqual(old_form["application_token"], application_session_token(self.client))
        self.assertEqual(self.client.post("/membership-application", data=old_form).status_code, 302)
        self.assert_no_application()

    def test_missing_or_invalid_token_cannot_submit(self):
        for token in ("", "invalid", "non-ascii-\u00e9"):
            with self.subTest(token=token):
                self.identify()
                self.assertEqual(self.client.post("/membership-application", data=self.valid_submission(token)).status_code, 302)
                self.assert_no_application()

    def test_failed_identification_clears_previous_applicant(self):
        self.identify()
        self.identify("Nobody", "5105559999")
        self.assert_session_cleared()
        self.assertNotIn("Alice private address", self.client.get("/membership-application").get_data(as_text=True))

    def test_legacy_or_malformed_session_requires_identification(self):
        for timestamp in (None, "invalid", float("inf"), self.now + 100):
            with self.subTest(timestamp=timestamp):
                self.identify()
                with self.client.session_transaction() as browser_session:
                    if timestamp is None:
                        browser_session.pop(MEMBERSHIP_APPLICATION_ACTIVITY_KEY)
                    else:
                        browser_session[MEMBERSHIP_APPLICATION_ACTIVITY_KEY] = timestamp
                response = self.client.get("/membership-application")
                self.assertNotIn("Alice private address", response.get_data(as_text=True))
                self.assert_session_cleared()

    def test_valid_submission_still_succeeds_and_clears_identity(self):
        self.identify()
        response = self.client.post("/membership-application", data=self.valid_submission())
        self.assertEqual(response.location, "/membership-application/thanks")
        self.assert_session_cleared()
        with closing(database.connect(self.db_path)) as connection:
            records = membership_application_repository.list_membership_application_records(connection)
            self.assertEqual(len(records), 1)
            self.assertEqual(records[0].member.first_name, "Alice")


if __name__ == "__main__":
    unittest.main()
