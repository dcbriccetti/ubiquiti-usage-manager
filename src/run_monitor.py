'CLI entry point for starting the continuous usage monitor.'

from datetime import datetime

import database as db
from monitor import UsageMonitor
from monitor_report import print_snapshot_report


if __name__ == "__main__":
    db.init_db()
    UsageMonitor().run_forever(
        on_cycle=lambda snapshots: print_snapshot_report(snapshots, datetime.now())
    )
