#!/usr/bin/env bash
set -euo pipefail

APP_DIR=${APP_DIR:-/home/daveb/devel/ubiquiti-usage-manager}
BACKUP_DIR=${BACKUP_DIR:-/home/daveb/devel/ubiquiti-usage-manager-backups}
BACKUP_RETENTION_DAYS=${BACKUP_RETENTION_DAYS:-30}

timestamp=$(date +%Y%m%d-%H%M%S)
destination="${BACKUP_DIR}/${timestamp}"

backup_sqlite_db() {
    local source_db=$1
    local output_name=$2

    if [[ ! -f "${source_db}" ]]; then
        echo "Skipping missing database: ${source_db}"
        return 0
    fi

    sqlite3 "${source_db}" ".backup '${destination}/${output_name}'"
    sqlite3 "${destination}/${output_name}" "PRAGMA integrity_check;" | grep -qx "ok"
    gzip -9 "${destination}/${output_name}"
}

if [[ ! -d "${APP_DIR}/.git" ]]; then
    echo "APP_DIR does not look like a checkout: ${APP_DIR}" >&2
    exit 1
fi

command -v sqlite3 >/dev/null
command -v gzip >/dev/null

umask 077
mkdir -p "${destination}"

backup_sqlite_db "${APP_DIR}/meter.db" "meter.db"
backup_sqlite_db "${APP_DIR}/data/club_users.db" "club_users.db"

find "${BACKUP_DIR}" -mindepth 1 -maxdepth 1 -type d -mtime +"${BACKUP_RETENTION_DAYS}" -print -exec rm -rf {} +

echo "Backups written to ${destination}"
