"""Guest linking: daily identity, atomic writes, kiosk isolation, and staff edits."""
from contextlib import closing
from datetime import datetime, timedelta
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from test_club_member_import import create_admin_app, admin_client
from club_admin import database, member_repository, checkin_repository, visit_guest_repository
from club_admin.models import Member, CheckIn
from club_admin.visit_guests import SESSION_KEY, IDLE_SECONDS, today


class VisitGuestTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db = Path(self.tmp.name) / "test.db"
        self.app = create_admin_app(self.db)
        self.app.testing = True
        self.client = self.app.test_client()
        self.ids = {}
        with closing(database.connect(self.db)) as conn:
            for name, last, phone, membership, flag in (
                ('Dave', 'Example', '2025550103', 'Full Member', 'safe'),
                ('Alice', 'Rivera', '2025550148', 'Visitor', 'pending'),
                ('Bob', 'Chen', '2025550172', 'Visitor', 'safe'),
                ('Carol', 'Example', '2025550115', 'Associate Member', 'safe'),
                ('Robin', 'Example', '2025550199', 'Visitor', 'banned'),
            ):
                self.ids[name] = member_repository.insert_member(conn, Member(first_name=name,
                    last_name=last, cell_phone=phone, card_number=name, membership=membership, screening_status=flag))
            conn.commit()

    def identify(self, name='Dave', phone='2025550103', client=None):
        return (client or self.client).post('/self-checkin', data={'phone': phone, 'initials': name})

    def token(self, client=None):
        with (client or self.client).session_transaction() as s:
            return s[SESSION_KEY]['token']

    def action(self, action, client=None, **data):
        client = client or self.client
        return client.post('/self-checkin/guests', data={'action': action, 'token': self.token(client), **data})

    def add(self, name='Bob', phone='2025550172', client=None):
        return self.action('add', client=client, phone=phone, initials=name)

    def count(self, table, user=None):
        with closing(database.connect(self.db)) as conn:
            sql = f'SELECT COUNT(*) FROM {table}'
            return conn.execute(sql + (' WHERE user_id = ?' if user else ''), (self.ids[user],) if user else ()).fetchone()[0]

    def test_multiple_guests_saved_atomically_without_duplicate_registration_checkin(self):
        self.identify('Alice', '2025550148')
        self.identify()
        page = self.add('Alice', '2025550148').get_data(as_text=True)
        self.assertIn('Alice R.', page)
        self.assertNotIn('Rivera', page)
        self.assertIn('Already checked in today', page)
        self.add()
        self.assertEqual(self.count('visit_guest_links'), 0)
        self.assertEqual(self.count('checkins', 'Bob'), 0)
        result = self.action('done')
        self.assertEqual(result.status_code, 200)
        self.assertIn('Your guests are linked', result.get_data(as_text=True))
        self.assertEqual(self.count('checkins'), 3)
        self.assertEqual(self.count('visit_guest_links'), 2)
        self.action('done')
        self.assertEqual(self.count('checkins'), 3)
        self.assertEqual(self.count('visit_guest_links'), 2)
        self.identify()
        result = self.client.get('/self-checkin/guests')
        self.assertIn('Alice R.', result.get_data(as_text=True))
        self.assertIn('Bob C.', result.get_data(as_text=True))
        self.assertEqual(self.count('checkins', 'Dave'), 1)

    def test_repeat_after_hours_same_day_and_previous_day(self):
        self.identify()
        with closing(database.connect(self.db)) as conn:
            conn.execute("UPDATE checkins SET check_in_at = ?", (today().isoformat()+'T00:00:00',))
            conn.commit()
        self.assertIn('already checked in today', self.identify().get_data(as_text=True))
        self.assertEqual(self.count('checkins', 'Dave'), 1)
        with closing(database.connect(self.db)) as conn:
            conn.execute("UPDATE checkins SET check_in_at = ?", ((today()-timedelta(days=1)).isoformat()+'T23:59:00',))
            conn.commit()
        self.identify()
        self.assertEqual(self.count('checkins', 'Dave'), 2)

    def test_cancel_remove_and_navigation_discard_pending_guests(self):
        self.identify(); self.add()
        self.action('remove', guest_id=str(self.ids['Bob']))
        self.action('done')
        self.assertEqual(self.count('visit_guest_links'), 0)
        self.add(); self.action('cancel')
        self.assertEqual(self.count('checkins', 'Bob'), 0)
        self.assertEqual(self.client.get('/self-checkin/guests').status_code, 302)
        self.identify(); self.add(); self.client.get('/guest-registration')
        self.assertEqual(self.client.get('/self-checkin/guests').status_code, 302)

    def test_blocked_self_ambiguous_and_duplicate_guest(self):
        self.identify()
        self.assertIn('front desk', self.add('Robin','2025550199').get_data(as_text=True))
        self.assertIn('different guest', self.add('Dave','2025550103').get_data(as_text=True))
        self.add()
        self.assertIn('Already in your guest list', self.add().get_data(as_text=True))
        with closing(database.connect(self.db)) as conn:
            member_repository.insert_member(conn, Member(first_name='Bob', last_name='Other', cell_phone='2025550172', card_number='Other', membership='Visitor'))
            conn.commit()
        self.assertIn('No unique match', self.add().get_data(as_text=True))
        self.action('done')
        self.assertEqual(self.count('visit_guest_links'), 1)

    def test_other_host_conflict_after_staging_rolls_back_entire_group(self):
        self.identify(); self.add('Alice','2025550148'); self.add()
        other = self.app.test_client()
        self.identify('Carol','2025550115',other); self.add(client=other); self.action('done',client=other)
        response = self.action('done')
        self.assertIn('another host today', response.get_data(as_text=True))
        self.assertEqual(self.count('checkins','Alice'), 0)
        self.assertEqual(self.count('visit_guest_links'), 1)
        self.assertIn('another host today',self.add().get_data(as_text=True))

    def test_screening_rechecked_at_commit(self):
        self.identify(); self.add('Alice','2025550148'); self.add()
        with closing(database.connect(self.db)) as conn:
            conn.execute("UPDATE users SET screening_status = 'banned' WHERE id = ?", (self.ids['Bob'],))
            conn.commit()
        self.assertIn('front desk', self.action('done').get_data(as_text=True))
        self.assertEqual(self.count('visit_guest_links'), 0)
        self.assertEqual(self.count('checkins', 'Alice'), 0)

    def test_exception_during_group_write_rolls_back_prior_guest(self):
        self.identify(); self.add('Alice','2025550148'); self.add()
        original = visit_guest_repository.save_link
        def fail_second(conn, host, guest, day):
            if guest == self.ids['Bob']:
                raise ValueError('Simulated write failure')
            original(conn, host, guest, day)
        with patch('club_admin.visit_guest_repository.save_link', side_effect=fail_second):
            self.action('done')
        self.assertEqual(self.count('visit_guest_links'), 0)
        self.assertEqual(self.count('checkins','Alice'), 0)
        self.assertEqual(self.count('checkins','Bob'), 0)

    def test_session_expiry_tokens_and_no_cache(self):
        self.identify()
        token = self.token()
        self.assertEqual(self.client.post('/self-checkin/guests',data={'action':'done','token':'wrong'}).status_code,400)
        response = self.client.get('/self-checkin/guests')
        self.assertIn('no-store', response.headers['Cache-Control'])
        self.assertNotIn('Refresh', response.headers)
        with self.client.session_transaction() as s:
            state=s[SESSION_KEY];state['activity']-=IDLE_SECONDS;s[SESSION_KEY]=state
        self.assertEqual(self.client.post('/self-checkin/guests',data={'action':'activity','token':token}).status_code,401)
        self.identify(); token=self.token(); self.identify('Carol','2025550115')
        self.assertEqual(self.client.post('/self-checkin/guests',data={'action':'done','token':token}).status_code,400)
        with self.client.session_transaction() as s:
            state=s[SESSION_KEY];state['day']=(today()-timedelta(days=1)).isoformat();s[SESSION_KEY]=state
        self.assertEqual(self.client.get('/self-checkin/guests').status_code,302)

    def test_only_members_can_start_guest_flow(self):
        for name,phone in [('Alice','2025550148'),('Robin','2025550199')]:
            response=self.identify(name,phone)
            self.assertNotIn('Add guests to my visit',response.get_data(as_text=True))
            self.assertEqual(self.client.get('/self-checkin/guests').status_code,302)

    def test_staff_correction_reports_history_and_audits(self):
        self.identify();self.add();self.action('done')
        staff=admin_client(self.app)
        response=staff.get(f"/members/{self.ids['Bob']}")
        self.assertIn('Guest of:',response.get_data(as_text=True))
        self.assertIn('Dave Example',response.get_data(as_text=True))
        self.assertIn('Guest of <a',staff.get('/checkins/report').get_data(as_text=True))
        path=f"/members/{self.ids['Bob']}/visit-guests"
        self.assertEqual(self.client.get(path).status_code,302)
        self.assertEqual(staff.get(path).status_code,200)
        with staff.session_transaction() as s: token=s['visit_guest_admin_token']
        response=staff.post(path,data={'action':'save','token':token,'visit_date':today().isoformat(),'host_id':self.ids['Carol'],'guest_id':self.ids['Bob']})
        self.assertEqual(response.status_code,302)
        with closing(database.connect(self.db)) as conn:
            link=visit_guest_repository.guest_link(conn,self.ids['Bob'],today())
            self.assertEqual(link['host_user_id'],self.ids['Carol'])
            report=visit_guest_repository.report_links(conn,today(),today())
            self.assertEqual(report[(self.ids['Bob'],today().isoformat())][0]['name'],'Carol Example')
            self.assertGreater(conn.execute("SELECT COUNT(*) FROM audit_log WHERE field_name LIKE 'guest link %'").fetchone()[0],0)
        staff.post(path,data={'action':'remove','token':token,'visit_date':today().isoformat(),'link_id':link['id']})
        self.assertEqual(self.count('visit_guest_links'),0)
        self.assertEqual(self.count('checkins','Bob'),1)

    def test_real_new_visitor_registration_then_member_links(self):
        body=self.client.get('/guest-registration').get_data(as_text=True)
        self.assertNotIn('name="guest_of_member"',body)
        self.assertNotIn('name="member_name"',body)
        result=self.client.post('/guest-registration',data={
            'visit_date':today().isoformat(),'first_name':'New','last_name':'Guest','date_of_birth':'1990-06-15',
            'address':'123 Test St','city':'Example','state':'CA','zip':'94000',
            'cell_phone':'2025550123','marital_status':'single','heard_about':'Friend',
        })
        self.assertEqual(result.status_code,302)
        self.identify();self.add('New','2025550123');self.action('done')
        self.assertEqual(self.count('checkins'),2)
        self.assertEqual(self.count('visit_guest_links'),1)

    def test_guests_by_member_counts_people_across_hosts_and_dates(self):
        from datetime import date
        with closing(database.connect(self.db)) as conn:
            for host, guest, day in [
                ('Dave', 'Alice', '2026-09-01'), ('Dave', 'Bob', '2026-09-01'),
                ('Dave', 'Alice', '2026-09-02'), ('Carol', 'Alice', '2026-09-03'),
                ('Carol', 'Bob', '2026-08-31'), ('Dave', 'Bob', '2026-10-01'),
            ]:
                visit_guest_repository.save_link(conn, self.ids[host], self.ids[guest], date.fromisoformat(day))
            conn.commit()
            summary = visit_guest_repository.guests_by_member(conn, date(2026,9,1), date(2026,9,30))
        self.assertEqual((summary['guest_visits'], summary['different_guests'], summary['host_count']), (4,2,2))
        dave, carol = summary['hosts']
        self.assertEqual((dave['id'],dave['guest_visits'],dave['different_guests'],dave['days_hosting']), (self.ids['Dave'],3,2,2))
        self.assertEqual((carol['guest_visits'],carol['different_guests'],carol['days_hosting']), (1,1,1))
        self.assertEqual([visit['date'] for visit in dave['visits']], ['2026-09-02','2026-09-01'])
        self.assertEqual(len(dave['visits'][1]['guests']), 2)
        staff = admin_client(self.app)
        response = staff.get('/guests/report?start_date=2026-09-01&end_date=2026-09-30')
        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn('Alice R.',body)
        self.assertNotIn('Rivera',body)
        self.assertIn(f'aria-controls="host-visits-{self.ids["Dave"]}"',body)
        self.assertIn('2026-09-02',body)
        self.assertNotIn('2026-08-31',body)
        self.assertIn(f'/members/{self.ids["Alice"]}',body)

    def test_guest_report_access_season_empty_and_invalid_dates(self):
        from datetime import date
        self.assertEqual(self.client.get('/guests/report').status_code,302)
        staff = admin_client(self.app)
        # An unlinked visitor check-in must not become a hosted guest visit.
        self.identify('Bob','2025550172')
        with patch('club_admin.visit_guests.today',return_value=date(2026,9,27)):
            response = staff.get('/guests/report')
        self.assertEqual(response.status_code,200)
        body=response.get_data(as_text=True)
        self.assertIn('2026-04-01 to 2026-10-31',body)
        self.assertIn('No recorded guest links',body)
        for query in ['start_date=bad', 'start_date=2026-10-02&end_date=2026-10-01']:
            self.assertEqual(staff.get('/guests/report?'+query).status_code,400)


if __name__ == '__main__':
    unittest.main()
