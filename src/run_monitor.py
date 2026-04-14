'CLI entry point for starting the continuous usage monitor.'

import database as db
from monitor import UsageMonitor


if __name__ == "__main__":
    db.init_db()
    UsageMonitor().run_forever()
