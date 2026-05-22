#!/usr/bin/env bash
set -euo pipefail

APP_DIR=${APP_DIR:-/home/daveb/devel/ubiquiti-usage-manager}
PYTHON=${PYTHON:-${APP_DIR}/venv/bin/python}

services=(
    ubiquiti-usage-monitor.service
    ubiquiti-usage-lan.service
    ubiquiti-usage-club.service
)

cd "${APP_DIR}"

if [[ ! -d .git ]]; then
    echo "This does not look like the production checkout: ${APP_DIR}" >&2
    exit 1
fi

git pull --ff-only origin main
"${PYTHON}" -m pip install -r requirements.txt

deploy/scripts/backup-prod-databases.sh

sudo systemctl restart "${services[@]}"

for service in "${services[@]}"; do
    sudo systemctl is-active --quiet "${service}"
    systemctl --no-pager status "${service}"
done

curl --max-time 10 -fsS http://127.0.0.1:5051/my-usage >/dev/null
curl --max-time 10 -fsS http://127.0.0.1:5052/self-checkin >/dev/null

echo "Production deploy completed."
