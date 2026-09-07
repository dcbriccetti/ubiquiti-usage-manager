# Production Deployment

Production currently runs from:

```text
/home/daveb/devel/ubiquiti-usage-manager
```

The checked-in systemd units keep the existing process model:

- `ubiquiti-usage-monitor.service` runs `src/monitor.py`
- `ubiquiti-usage-lan.service` runs the LAN dashboard on `127.0.0.1:5051`
- `ubiquiti-usage-club.service` runs the user-management app on `127.0.0.1:5052`
- `ubiquiti-usage-backup.timer` runs a weekly SQLite backup on Mondays at 2 AM
- `ubiquiti-usage-nfdump-prune.timer` prunes imported nfdump capture files daily

These files are repo artifacts only until explicitly installed on production.

## First Systemd Install

From production, after pulling this commit:

```bash
cd /home/daveb/devel/ubiquiti-usage-manager
sudo deploy/scripts/install-systemd-units.sh
sudo systemctl start ubiquiti-usage-monitor.service
sudo systemctl start ubiquiti-usage-lan.service
sudo systemctl start ubiquiti-usage-club.service
sudo systemctl start ubiquiti-usage-backup.timer
sudo systemctl start ubiquiti-usage-nfdump-prune.timer
```

Verify before stopping the old `./run` process:

```bash
systemctl status ubiquiti-usage-monitor.service --no-pager
systemctl status ubiquiti-usage-lan.service --no-pager
systemctl status ubiquiti-usage-club.service --no-pager
ss -ltnp | grep -E ':(5051|5052)'
curl -fsS http://127.0.0.1:5051/my-usage >/dev/null
curl -fsS http://127.0.0.1:5052/self-checkin >/dev/null
```

After the services are healthy, stop the old interactive runner:

```bash
pkill -f 'python3 src/monitor.py'
pkill -f 'python3 src/app.py'
pkill -f 'python3 -m club_admin.app'
pkill -f './run'
```

Then enable boot startup:

```bash
sudo systemctl enable ubiquiti-usage-monitor.service
sudo systemctl enable ubiquiti-usage-lan.service
sudo systemctl enable ubiquiti-usage-club.service
sudo systemctl enable ubiquiti-usage-backup.timer
sudo systemctl enable ubiquiti-usage-nfdump-prune.timer
```

Rollback is to stop the new units and run the old script again:

```bash
sudo systemctl stop ubiquiti-usage-monitor.service ubiquiti-usage-lan.service ubiquiti-usage-club.service
cd /home/daveb/devel/ubiquiti-usage-manager
./run
```

## Routine Deploy

Normal deployment is:

```bash
git push origin main
ssh daveb@seqserver.local
cd /home/daveb/devel/ubiquiti-usage-manager
deploy/scripts/deploy-prod.sh
```

When VPN DNS is not available, SSH to the server IP instead:

```bash
ssh daveb@192.168.2.30
```

Once the systemd units are installed, the server-side deploy command is:

```bash
cd /home/daveb/devel/ubiquiti-usage-manager
deploy/scripts/deploy-prod.sh
```

The script pulls with `--ff-only`, updates Python dependencies, restarts the
three services, waits for the local LAN and club health endpoints, and skips
database backups by default. Set `BACKUP_BEFORE_DEPLOY=1` when you want one:

```bash
BACKUP_BEFORE_DEPLOY=1 deploy/scripts/deploy-prod.sh
```

The deploy health checks retry for up to 30 seconds after service restart so a
normal Flask startup delay does not fail the deploy.

Admin browser sessions are cookie-based and survive service restarts while the
configured session secrets stay the same. Restarting the services should not by
itself prompt already-authenticated admins to log in again.

## Backups

The backup timer writes SQLite backups under:

```text
/home/daveb/devel/ubiquiti-usage-manager-backups
```

Run a backup manually:

```bash
deploy/scripts/backup-prod-databases.sh
```

Adjust retention with `BACKUP_RETENTION_DAYS`; the default is 30 days.

## nfdump Capture Pruning

The nfdump prune timer deletes only completed capture files under
`/var/cache/nfdump` that are older than 3 days and whose filenames already exist
in the `meter.db` `flow_imports` table. It never deletes `nfcapd.current*`
files.

Report only:

```bash
deploy/scripts/prune-nfdump-captures.py
```

Apply manually:

```bash
sudo deploy/scripts/prune-nfdump-captures.py --apply
```

The systemd service runs as root because `/var/cache/nfdump` is owned by the
capture service, not by the app user. It does not restart or signal the running
LAN, monitor, or club apps.

## Meter Database Pruning

### Offline summary validation (Stage 1)

`deploy/scripts/validate-meter-rollups.py` is an offline prototype, not an app
migration or production accounting writer. Run it on an extracted consistent
SQLite backup, with a new output directory:

```bash
python3 deploy/scripts/validate-meter-rollups.py \
  --source /path/to/extracted-backup.db \
  --output /path/to/new-validation-directory
```

The source is opened read-only. The tool builds separate hourly/daily WAN
summaries and daily active-voucher charges, then writes `report.json`. It checks
integer-byte and flow-count conservation, identity-group totals for each month,
today and the last seven days, and each active voucher's daily bytes and first
usage time. Reporting dates are anchored to the backup's latest flow. Partial
hours use raw edge queries. Exit status 1 means a comparison failed; existing
output directories are refused. Output may contain client identities; keep it
private. Rebuild after an interrupted run into another new directory.

The reference calculations stream the existing start-time query semantics to
avoid loading millions of flow rows into memory. Tests compare those semantics
with application functions. The proposed canonical identity timeline can differ
from the current report-window-dependent attribution; any difference is a
failure to investigate, not an acceptable rounding discrepancy.

This stage does not implement live imports, retries, late-arrival corrections,
end-time client details, consumed-voucher reconstruction, or pruning. A passing
offline report alone is not permission to delete raw history. Run large backup
checks at reduced CPU/I/O priority if sharing the production host. Timings from
sequential offline runs include cache effects and are not live request timings.

### Existing raw-flow cleanup

Use the prune tool to remove old raw WAN-flow rows while preserving active Plus
voucher accounting. The cutoff is the oldest unconsumed voucher generation time.
If there are no active vouchers, it falls back to 90 days by default.

Report only:

```bash
deploy/scripts/prune-meter-db.py
```

Apply after taking a backup:

```bash
deploy/scripts/backup-prod-databases.sh
sudo systemctl stop ubiquiti-usage-monitor.service ubiquiti-usage-lan.service ubiquiti-usage-club.service
deploy/scripts/prune-meter-db.py --apply --vacuum --yes-i-have-a-backup
sudo systemctl start ubiquiti-usage-monitor.service ubiquiti-usage-lan.service ubiquiti-usage-club.service
```
