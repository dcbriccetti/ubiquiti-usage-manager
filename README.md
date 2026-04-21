UniFi usage dashboard + monitor for tracking client usage and applying policy-based speed limits.

## What This App Does

- Shows live and historical client usage in a Flask web dashboard.
- Polls UniFi clients continuously via a background monitor.
- Stores usage history in SQLite (`meter.db`).
- Applies throttle profiles by usage policy (configured in `src/config.py`).

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

Review `src/config.py` for your environment:

- `THROTTLEABLE_VLAN_NAMES`
- `THROTTLING_LEVELS`
- `MONTHLY_USAGE_ADJUSTMENTS`
- `COST_IN_CENTS_PER_GB`
- `PLUS_ADMINS`

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
- `DEV_FORCE_PLUS_ADMIN=1` to bypass admin check (testing)

Example:

```bash
LOG_LEVEL=DEBUG DEV_FORCE_PLUS_ADMIN=1 python3 src/app.py
```

## First-Time Tryout Checklist

1. Start monitor and web app.
2. Generate traffic from a test client.
3. Confirm dashboard rows appear and minute usage updates.
4. Open a client detail page from Name/MAC links.
5. Verify month cost values match `COST_IN_CENTS_PER_GB`.

## Notes

- `meter.db` is created in the repo root on first run.
- Throttling actions are live changes against UniFi groups, so test carefully.
- Main files:
  - `src/app.py` web routes/UI
  - `src/monitor.py` polling + enforcement loop
  - `src/dashboard_service.py` dashboard row shaping
  - `src/database.py` persistence/query layer
