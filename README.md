UniFi usage dashboard + monitor for tracking client usage and applying policy-based speed limits.

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
- `PLUS_ADMIN_IPS` (exact IPs or CIDR ranges that bypass UniFi client lookup for admin access, useful for VPN admin clients)
- `NFDUMP_DIR` (directory containing completed `nfcapd.*` flow capture files)
- `NFDUMP_BIN` (path/name for the `nfdump` command)
- `INTERNAL_NETWORKS` (CIDR ranges treated as LAN clients for WAN flow attribution)
- `FLOW_IMPORT_ENABLED`
- `FLOW_IMPORT_INTERVAL_SECONDS`
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

If UniFi exports NetFlow/IPFIX to this host and `nfcapd` writes completed capture files, the monitor imports completed captures every `FLOW_IMPORT_INTERVAL_SECONDS`. You can also import manually with:

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
