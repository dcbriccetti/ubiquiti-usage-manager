'CLI entry point for starting the continuous usage monitor.'

from monitor import UsageMonitor


if __name__ == "__main__":
    UsageMonitor().run_forever()
