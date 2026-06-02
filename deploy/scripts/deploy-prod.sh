#!/usr/bin/env bash
set -euo pipefail

APP_DIR=${APP_DIR:-/home/daveb/devel/ubiquiti-usage-manager}
PYTHON=${PYTHON:-${APP_DIR}/venv/bin/python}

services=(
    ubiquiti-usage-monitor.service
    ubiquiti-usage-lan.service
    ubiquiti-usage-club.service
)

wait_for_http() {
    local name=$1
    local url=$2
    local deadline=$((SECONDS + 30))

    echo "Waiting for ${name} at ${url}"
    until curl --max-time 2 -fsS "${url}" >/dev/null; do
        if (( SECONDS >= deadline )); then
            echo "${name} health check failed after 30 seconds: ${url}" >&2
            return 1
        fi
        sleep 1
    done
    echo "${name} health check passed."
}

cd "${APP_DIR}"

if [[ ! -d .git ]]; then
    echo "This does not look like the production checkout: ${APP_DIR}" >&2
    exit 1
fi

git pull --ff-only origin main
"${PYTHON}" -m pip install -r requirements.txt

if [[ "${BACKUP_BEFORE_DEPLOY:-0}" =~ ^(1|true|yes|on)$ ]]; then
    deploy/scripts/backup-prod-databases.sh
else
    echo "Skipping database backup. Set BACKUP_BEFORE_DEPLOY=1 to run one."
fi

sudo systemctl restart "${services[@]}"

for service in "${services[@]}"; do
    sudo systemctl is-active --quiet "${service}"
    systemctl --no-pager status "${service}"
done

wait_for_http "LAN dashboard" "http://127.0.0.1:5051/my-usage"
wait_for_http "club user app" "http://127.0.0.1:5052/self-checkin"

echo "Production deploy completed."
