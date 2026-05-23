# Production Deployment

Production currently runs from:

```text
/home/daveb/devel/ubiquiti-usage-manager
```

The checked-in systemd units keep the existing process model:

- `ubiquiti-usage-monitor.service` runs `src/monitor.py`
- `ubiquiti-usage-lan.service` runs the LAN dashboard on `127.0.0.1:5051`
- `ubiquiti-usage-club.service` runs the user-management app on `127.0.0.1:5052`
- `ubiquiti-usage-backup.timer` runs a daily SQLite backup

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
```

Rollback is to stop the new units and run the old script again:

```bash
sudo systemctl stop ubiquiti-usage-monitor.service ubiquiti-usage-lan.service ubiquiti-usage-club.service
cd /home/daveb/devel/ubiquiti-usage-manager
./run
```

## Routine Deploy

Once the units are installed, routine deploys can use:

```bash
cd /home/daveb/devel/ubiquiti-usage-manager
deploy/scripts/deploy-prod.sh
```

The script pulls with `--ff-only`, updates Python dependencies, restarts the
three services, and checks the local health endpoints.

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

## Meter Database Pruning

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
