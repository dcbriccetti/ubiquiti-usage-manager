UniFi usage dashboard + monitor for tracking client usage and applying policy-based speed limits.

## Documentation

- `README.md`: app structure, local run commands, and configuration notes for
  both the LAN dashboard and club user-management app.
- `deploy/README.md`: production paths, systemd units, routine deploys,
  backups, and maintenance commands.

## App Structure

This repo is being migrated gradually from one flat LAN-management app into
separate app packages:

```text
src/
  app.py                 # existing LAN management Flask app, kept for compatibility
  lan_admin/             # new LAN package boundary; currently wraps src/app.py
  club_admin/            # new club user management app
  shared/                # future shared code used by both apps
```

The LAN app still runs the same way as before, so existing scripts and imports
do not need to change yet. New club user-management code should go under
`src/club_admin/`.

Run the LAN app:

```bash
PYTHONPATH=src python src/app.py
```

Or through the new package entrypoint:

```bash
PYTHONPATH=src python -m lan_admin.app
```

Run the club user app:

```bash
PYTHONPATH=src python -m club_admin.app
```

By default the club app stores users in `data/club_users.db`. Override that
with `CLUB_ADMIN_DB_PATH=/path/to/club_users.db` when needed.
Set `USER_MANAGEMENT_ORGANIZATION_NAME` in `src/config_local.py` to change the visible
`<org name> User Management` title shown across club pages.
When deploying behind the same Nginx server as the LAN app, set
`USER_MANAGEMENT_URL_PREFIX = '/users'` so user-management links and static
assets stay under `/users/...`.
Self check-in and guest registration are public. User lists, details, reports,
exports, documents, notes, and edits require admin login. Set
`USER_MANAGEMENT_ADMIN_PASSWORD_HASH` in ignored
`src/config_local.py`; the app fails closed for admin pages when no hash is
configured. Generate a hash with:

```bash
PYTHONPATH=src .venv/bin/python -c 'from getpass import getpass; from secrets import token_urlsafe; from werkzeug.security import generate_password_hash; p=getpass("Admin password: "); q=getpass("Confirm password: "); assert p == q, "passwords did not match"; print("USER_MANAGEMENT_ADMIN_PASSWORD_HASH =", repr(generate_password_hash(p))); print("USER_MANAGEMENT_SESSION_SECRET =", repr(token_urlsafe(48)))'
```

Copy the printed `USER_MANAGEMENT_ADMIN_PASSWORD_HASH` and
`USER_MANAGEMENT_SESSION_SECRET` lines into `src/config_local.py`. Do not store
the plain admin password in any file.

Admin login uses a signed browser session cookie. A normal webapp restart does
not require admins to sign in again as long as `USER_MANAGEMENT_SESSION_SECRET`
stays the same and the browser still has the session cookie. Use the app logout
button, clear browser cookies, or rotate `USER_MANAGEMENT_SESSION_SECRET` when
you intentionally need to invalidate existing admin sessions.

The LAN usage dashboard can also use a password-backed admin session, which is
helpful from VPN or on-site networks when the browser is not connected to Plus
with a `PLUS_ADMINS` identity. Generate the LAN app hash and session secret with:

```bash
PYTHONPATH=src .venv/bin/python -c 'from getpass import getpass; from secrets import token_urlsafe; from werkzeug.security import generate_password_hash; p=getpass("LAN admin password: "); q=getpass("Confirm password: "); assert p == q, "passwords did not match"; print("LAN_ADMIN_PASSWORD_HASH =", repr(generate_password_hash(p))); print("LAN_ADMIN_SESSION_SECRET =", repr(token_urlsafe(48)))'
```

Copy the printed `LAN_ADMIN_PASSWORD_HASH` and `LAN_ADMIN_SESSION_SECRET` lines
into `src/config_local.py`.
LAN admin login uses the same signed-cookie model: restarting the service does
not clear authenticated browser sessions while `LAN_ADMIN_SESSION_SECRET`
remains unchanged.

Check-in procedure:

1. A user can check in on the public self check-in page with phone number plus
   initials, first name, or nickname.
2. After a successful phone/name check-in, the app shows a signed Code 128
   barcode the user can keep on their phone.
3. On later visits, the user can display that barcode. A barcode scanner should
   act like a keyboard, type the barcode into the focused barcode field, and
   send Enter/Return to submit the check-in.
4. Plain card numbers are not accepted as barcode check-ins.

Guest screening procedure:

1. New guest registrations are created with screening status `Needs review`.
2. A manager attaches the guest's ID image, checks the outside screening site,
   and clicks `Mark Safe` before printing the guest form.
3. Existing users have no screening flag by default and continue to check in
   normally unless a manager explicitly changes the status.
4. If a user must be banned, a manager edits the user, changes
   `Screening Status` to `Banned`, and adds a user note with the ban details.
5. Banned users cannot self check in or be checked in from the admin users page.
   The kiosk shows `Please see the front desk.`, and each blocked attempt is
   logged on that user.

To show scanned guest forms on user detail pages, set
`USER_MANAGEMENT_DOCUMENTS_DIR` in `src/config_local.py`. The app looks inside a
folder named with the user's card number for the first `.jpg` or `.jpeg` file
whose name starts with `Guest Form`.
To customize the printable first-time visitor form, set
`USER_MANAGEMENT_GUEST_FORM_DEFINITION_PATH` in `src/config_local.py` to a local
TOML file. When no form definition is configured, the app uses a generic
visitor registration form. Keep organization-specific form text in ignored
local files.

Example form definition:

```toml
title = "Guest Registration"
version = "ORG-1.0"

[labels]
name = "Name"
address = "Street Address"
cell_phone = "Cell Phone"

[agreement]
title = "Agreement"
paragraphs = [
  "I agree to follow the organization's rules and policies.",
  "I understand this form must be signed before access is granted.",
]
```
To enable the admin ZIP map, add ZIP centroid coordinates to ignored local
config:

```python
USER_MANAGEMENT_ZIP_COORDINATES = {
    "00000": (37.0000, -122.0000),
}
```

The map aggregates checked-in users by ZIP code for the selected check-in date
range. It does not call an external geocoding or map service for ZIPs with
configured or saved coordinates. ZIPs without local coordinates are looked up in
the browser through Zippopotam.us by ZIP code only, then saved transparently in
the local club-user SQLite database for future map loads.

The club app can export the current user list to CSV. Local CSV exports and
SQLite user databases are ignored by Git so user, member, and visitor data stays
off commits.
SQLite foreign-key enforcement is enabled by the app on every connection. If
you inspect the DB manually with `sqlite3`, run `PRAGMA foreign_keys=ON;` first;
the app also refuses to start if it detects broken club-user foreign keys.

## What This App Does

- Shows live and historical client usage in a Flask web dashboard.
- Polls UniFi clients continuously via a background monitor.
- Stores usage history in SQLite (`meter.db`).
- Applies throttle profiles by usage policy (configured in `src/config.py`).

## Current Dashboard Features

- Client table with live/historical windows: Active Now, Online Now, Today, 7 Days, and current month.
- Per-client usage details page for admins and self-service users with daily charts and usage history.
- Plus voucher workflow (admin-only):
  - `/vouchers` generates paper vouchers at preset dollar values,
  - each voucher stores a local user ID, password, and GB allocation,
  - generated vouchers create matching UniFi local RADIUS users via `rest/account`,
  - `/vouchers/batches/<batch_id>/print` renders print-ready voucher sheets.
- Organization-paid analytics split:
  - payer split (organization-paid vs user-paid),
  - organization-paid client list.
- Global month analytics:
  - Dashboard (`/`) keeps operational summaries (daily active clients, payer split, AP hotspots).
  - Insights (`/insights`) contains deeper month analytics:
    - daily MB and minutes split by Basic/Plus,
    - peak simultaneous users by day,
    - weekday/hour user-minutes heatmap.
    - throttling coverage metrics and profile-minutes chart.
    - top users and organization-paid client usage breakdown.
- Throttling coverage panel:
  - minutes throttled,
  - total active minutes,
  - percent time throttled,
  - minutes by profile chart (includes only profiles defined in `THROTTLING_LEVELS`).

## Prerequisites

- Python 3.11+ recommended
- Access to a UniFi controller/gateway API
- Valid UniFi API key in `src/keys.py`

## 1) Install Dependencies

```bash
python3 -m pip install -r requirements.txt
```

## 2) Configure The App

Edit `src/keys.py`:

```python
API_KEY = "your-unifi-api-key"
```

Review `src/config.py` for defaults and committed example values.
For private/local development settings, copy `src/config_local.py.example` to `src/config_local.py` and edit that file.
`src/config_local.py` is gitignored and automatically overrides uppercase values from `src/config.py` at runtime.

Config values:

- `THROTTLEABLE_VLAN_NAMES`
- `THROTTLING_LEVELS`
- `MONTHLY_USAGE_ADJUSTMENTS`
- `THROTTLING_ENABLED` (set `False` to disable policy-based throttling changes)
- `COST_IN_CENTS_PER_GB`
- `PLUS_REPORT_TITLE` (shown on printed Plus voucher instructions)
- `ORGANIZATION_TITLE`
- `PLUS_ADMINS`
- `LAN_ADMIN_PASSWORD_HASH` and `LAN_ADMIN_SESSION_SECRET` (optional password login for LAN app admins)
- `NFDUMP_DIR` (directory containing completed `nfcapd.*` flow capture files)
- `NFDUMP_BIN` (path/name for the `nfdump` command)
- `INTERNAL_NETWORKS` (CIDR ranges treated as LAN clients for WAN flow attribution)
- `FLOW_IMPORT_ENABLED`
- `FLOW_IMPORT_INTERVAL_SECONDS`
- `FLOW_IMPORT_WATCH_ENABLED`
- `FLOW_IMPORT_WATCH_POLL_SECONDS`
- `FLOW_IMPORT_SETTLE_SECONDS`
- `ORGANIZATION_PAID_DEVICE_MACS`
- `ORGANIZATION_PAID_USER_IDS`
- `ORGANIZATION_PAID_VLAN_NAMES`

Voucher behavior:

- Costs are calculated from `COST_IN_CENTS_PER_GB`.
- Voucher values are priced at `COST_IN_CENTS_PER_GB` and currently offered as $5, $10, $20, $50, and $100 batches.
- Creating vouchers also creates UniFi local RADIUS accounts, so test with a single voucher before generating a batch.

## 3) Start The Monitor (required for live data)

```bash
python3 src/monitor.py
```

The monitor:

- polls connected clients (`stat/sta`)
- writes usage intervals to `meter.db`
- can apply UniFi user-group changes when policy thresholds are reached

## 4) Start The Web App

In a second terminal:

```bash
python3 src/app.py
```

Open:

- `http://localhost:5051`

If your request is not recognized as an admin, `/` redirects to `/my-usage`.

## Helpful Environment Variables

- `LOG_LEVEL=DEBUG` to increase log detail
- `DEV_REQUEST_IP=<ip>` to force request IP resolution (testing)

Example:

```bash
LOG_LEVEL=DEBUG DEV_REQUEST_IP=<ip> python3 src/app.py
```

## WAN Flow Import

If UniFi exports NetFlow/IPFIX to this host and `nfcapd` writes completed capture files, the monitor imports newly completed captures shortly after they appear when `FLOW_IMPORT_WATCH_ENABLED` is enabled. It also performs a fallback scan every `FLOW_IMPORT_INTERVAL_SECONDS`. You can import manually with:

```bash
python3 src/flow_import.py
```

By default, the importer reads completed `nfcapd.YYYYMMDDHHMM` files from `/var/cache/nfdump`, ignores `nfcapd.current.*`, skips files already recorded in SQLite, and stores only flows crossing between `INTERNAL_NETWORKS` and external addresses.

## First-Time Tryout Checklist

1. Start monitor and web app.
2. Generate traffic from a test client.
3. Confirm dashboard rows appear and minute usage updates.
4. Open a client detail page from Name/MAC links.
5. Verify month cost values match `COST_IN_CENTS_PER_GB`.
6. Visit `/vouchers` and generate one test voucher.
7. Confirm the matching UniFi local RADIUS user is created.
8. Print the generated voucher batch page and verify the SSID/cost details.

## Notes

- `meter.db` is created in the repo root on first run.
- Throttling actions are live changes against UniFi groups, so test carefully.
- Main files:
  - `src/app.py` web routes/UI
  - `src/monitor.py` polling + enforcement loop
  - `src/dashboard_service.py` dashboard row shaping
  - `src/database.py` persistence/query layer
