#!/usr/bin/env bash
set -euo pipefail

APP_DIR=${APP_DIR:-/home/daveb/devel/ubiquiti-usage-manager}
SYSTEMD_DIR=${SYSTEMD_DIR:-/etc/systemd/system}

cd "${APP_DIR}"

if [[ ! -d .git ]]; then
    echo "This does not look like the production checkout: ${APP_DIR}" >&2
    exit 1
fi

install -m 0644 deploy/systemd/ubiquiti-usage-monitor.service "${SYSTEMD_DIR}/ubiquiti-usage-monitor.service"
install -m 0644 deploy/systemd/ubiquiti-usage-lan.service "${SYSTEMD_DIR}/ubiquiti-usage-lan.service"
install -m 0644 deploy/systemd/ubiquiti-usage-club.service "${SYSTEMD_DIR}/ubiquiti-usage-club.service"
install -m 0644 deploy/systemd/ubiquiti-usage-backup.service "${SYSTEMD_DIR}/ubiquiti-usage-backup.service"
install -m 0644 deploy/systemd/ubiquiti-usage-backup.timer "${SYSTEMD_DIR}/ubiquiti-usage-backup.timer"

systemctl daemon-reload
systemctl list-unit-files 'ubiquiti-usage-*' --no-pager
