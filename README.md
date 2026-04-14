An application to control and bill for usage in a Ubiquiti local area network.

## Branch Direction

The `codex-flask-webapp` branch starts the move from a single long-running script
to a small Flask web application plus a separate background monitor.

## Running The Web App

Install dependencies:

```bash
python3 -m pip install -r requirements.txt
```

Start the Flask dashboard:

```bash
python3 src/app.py
```

## Running The Monitor

Run the polling/throttling worker separately:

```bash
python3 src/monitor.py
```

## Safe Mode

`src/config.py` now includes `SAFE_MODE = True` by default.

In safe mode:

- No database write calls are executed from the monitor/web startup path.
- No UniFi group changes are sent (`set_user_group` / `release_all_from_limit` are skipped).

Set `SAFE_MODE = False` only when you are ready to allow live network changes.

## Current Shape

- `src/app.py` serves the Flask dashboard.
- `src/monitor.py` owns the background usage polling and throttling loop.
- `src/database.py` stores usage records and provides dashboard queries.
