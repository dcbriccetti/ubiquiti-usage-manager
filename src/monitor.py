import time
import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import Event, Lock, Thread

import config as cfg
import database as db
import unifi_api as api
from clientinfo import ClientInfo
from dashboard_events import notify_dashboard_data_changed
from flow_import import completed_capture_files, import_completed_captures, parse_internal_networks
from logging_config import configure_logging
from speedlimit import SpeedLimit
from throttling_policy import target_profile_name_for_usage
from throttling_runtime import (
    build_throttling_levels,
    enforce_target_limit,
    get_throttling_limit_ids,
    is_speed_limit_throttled,
    release_configured_limits,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True, kw_only=True)
class ClientSnapshot:
    "Snapshot of one client's current usage totals and throttle state."
    # Immutable view-model used by both CLI output and Flask templates.
    client: ClientInfo
    interval_mb: float
    day_total_mb: float
    last_7_days_total_mb: float
    calendar_month_total_mb: float
    effective_speed_limit: SpeedLimit | None
    is_throttled: bool


def get_connected_clients() -> list[ClientSnapshot]:
    'Fetch connected clients and return lightweight usage snapshots for the UI.'
    speed_limits = api.get_speed_limits()
    speed_limits_by_id = {limit.id: limit for limit in speed_limits}
    throttling_limit_ids: set[str] = set()
    if cfg.THROTTLING_ENABLED:
        throttling_levels = build_throttling_levels(speed_limits)
        throttling_limit_ids = get_throttling_limit_ids(throttling_levels)
    ap_names_by_mac = api.get_ap_names_by_mac()
    recent_interval_by_mac = db.get_recent_interval_totals()

    snapshots: list[ClientSnapshot] = []
    for raw_client in api.get_api_data("stat/sta"):
        client = ClientInfo.create(raw_client, speed_limits_by_id, ap_names_by_mac)
        is_throttled = is_speed_limit_throttled(client.speed_limit, throttling_limit_ids)
        snapshots.append(
            ClientSnapshot(
                client=client,
                interval_mb=recent_interval_by_mac.get(client.mac, 0.0),
                day_total_mb=db.get_daily_total(client.mac),
                last_7_days_total_mb=db.get_last_7_days_total(client.mac),
                calendar_month_total_mb=db.get_calendar_month_total(client.mac),
                effective_speed_limit=client.speed_limit,
                is_throttled=is_throttled,
            )
        )

    return snapshots


class UsageMonitor:
    'Poll UniFi clients, compute usage deltas, and enforce throttling rules.'

    def __init__(self) -> None:
        'Initialize runtime caches and load throttle-related controller metadata.'
        self.last_totals_by_client_mac: dict[str, float] = {}
        self.current_day = datetime.now().date()
        self.speed_limits_by_id: dict[str, SpeedLimit] = {}
        self.speed_limits_by_name: dict[str, SpeedLimit] = {}
        self.throttling_levels: list[tuple[int, SpeedLimit]] = []
        self.throttling_limit_ids: set[str] = set()
        self.throttleable_vlan_ids: list[str] = []
        self.last_flow_import_monotonic = 0.0
        self._flow_import_lock = Lock()
        self._flow_watch_stop = Event()
        self._flow_watch_thread: Thread | None = None
        self.refresh_runtime_state()

        startup_release_limit_ids = self.throttling_limit_ids
        if not cfg.THROTTLING_ENABLED:
            startup_release_limit_ids = self._configured_throttling_limit_ids_for_release()
            logger.info("Throttling disabled: releasing configured throttle limits at startup only")
        release_configured_limits(startup_release_limit_ids, "startup")
        self._start_flow_import_watcher()

    def refresh_runtime_state(self) -> None:
        'Reload speed-limit groups and throttleable VLAN IDs from the controller.'
        speed_limits = api.get_speed_limits()
        self.speed_limits_by_id = {limit.id: limit for limit in speed_limits}
        self.speed_limits_by_name = {limit.name: limit for limit in speed_limits}
        if cfg.THROTTLING_ENABLED:
            self.throttling_levels = build_throttling_levels(speed_limits)
            self.throttling_limit_ids = get_throttling_limit_ids(self.throttling_levels)
            self.throttleable_vlan_ids = api.get_vlan_ids_for_names(cfg.THROTTLEABLE_VLAN_NAMES)
        else:
            self.throttling_levels = []
            self.throttling_limit_ids = set()
            self.throttleable_vlan_ids = []

    def process_connected_clients(self) -> list[ClientSnapshot]:
        'Process all connected clients for one cycle and return current snapshots.'
        snapshots: list[ClientSnapshot] = []
        observed_clients: list[ClientInfo] = []
        ap_names_by_mac = api.get_ap_names_by_mac()

        for raw_client in api.get_api_data("stat/sta"):
            client = ClientInfo.create(raw_client, self.speed_limits_by_id, ap_names_by_mac)
            observed_clients.append(client)
            interval_mb = self._update_client_usage(client)
            interval_kb = interval_mb * 1000

            if interval_kb >= cfg.IGNORE_BELOW_KB:
                db.log_usage(client, interval_mb)

            day_total_mb = db.get_daily_total(client.mac)
            last_7_days_total_mb = db.get_last_7_days_total(client.mac)
            calendar_month_total_mb = db.get_calendar_month_total(client.mac)
            is_throttled, effective_speed_limit = self._enforce_limit_if_needed(client, day_total_mb, calendar_month_total_mb)
            snapshots.append(
                ClientSnapshot(
                    client=client,
                    interval_mb=interval_mb,
                    day_total_mb=day_total_mb,
                    last_7_days_total_mb=last_7_days_total_mb,
                    calendar_month_total_mb=calendar_month_total_mb,
                    effective_speed_limit=effective_speed_limit,
                    is_throttled=is_throttled,
                )
            )

        db.record_client_ip_identities(observed_clients)
        return snapshots

    def run_forever(self, poll_interval_seconds: int = 60, on_cycle: Callable[[list[ClientSnapshot]], None] | None = None) -> None:
        'Run the monitor loop continuously at the configured poll interval.'
        first_cycle = True
        while True:
            if first_cycle:
                first_cycle = False
            else:
                self._sleep_until_next_poll_boundary(poll_interval_seconds)
            try:
                self._handle_day_transition()
                snapshots = self.process_connected_clients()
                self._import_flows_if_due()
                if on_cycle:
                    on_cycle(snapshots)
            except Exception as exc:
                logger.exception("Monitor cycle failed: %s", exc)

    @staticmethod
    def _sleep_until_next_poll_boundary(poll_interval_seconds: int) -> None:
        'Sleep until the next wall-clock poll boundary to keep timestamps aligned.'
        if poll_interval_seconds <= 0:
            return

        now = time.time()
        next_boundary = (int(now // poll_interval_seconds) + 1) * poll_interval_seconds
        sleep_seconds = max(0.0, next_boundary - now)
        time.sleep(sleep_seconds)

    def _handle_day_transition(self) -> None:
        now_date = datetime.now().date()
        if now_date > self.current_day:
            logger.info("Midnight reset date=%s", now_date)
            if cfg.THROTTLING_ENABLED:
                release_configured_limits(self.throttling_limit_ids, "midnight")
            self.current_day = now_date

    def _import_flows_if_due(self) -> None:
        'Periodically import completed nfdump captures without interrupting monitoring.'
        if not getattr(cfg, 'FLOW_IMPORT_ENABLED', True):
            return

        interval_seconds = int(getattr(cfg, 'FLOW_IMPORT_INTERVAL_SECONDS', 300) or 0)
        if interval_seconds <= 0:
            return

        now_monotonic = time.monotonic()
        if self.last_flow_import_monotonic and now_monotonic - self.last_flow_import_monotonic < interval_seconds:
            return
        self.last_flow_import_monotonic = now_monotonic

        self._import_flows_now('scheduled')

    def _import_flows_now(self, reason: str) -> tuple[int, int, int]:
        'Import completed nfdump captures immediately and notify dashboards.'
        if not getattr(cfg, 'FLOW_IMPORT_ENABLED', True):
            return 0, 0, 0

        if not self._flow_import_lock.acquire(blocking=False):
            return 0, 0, 0

        try:
            internal_networks = parse_internal_networks(getattr(cfg, 'INTERNAL_NETWORKS', set()))
            if not internal_networks:
                logger.warning('Flow import skipped: no valid INTERNAL_NETWORKS configured')
                return 0, 0, 0

            files, rows, skipped = import_completed_captures(
                capture_dir=Path(str(getattr(cfg, 'NFDUMP_DIR', '/var/cache/nfdump'))),
                internal_networks=internal_networks,
                nfdump_bin=str(getattr(cfg, 'NFDUMP_BIN', 'nfdump')),
            )
            if files or rows or skipped:
                logger.info(
                    'Flow import complete (%s): files=%s imported_rows=%s skipped_rows=%s',
                    reason,
                    files,
                    rows,
                    skipped,
                )
                notify_dashboard_data_changed()
            return files, rows, skipped
        except Exception as exc:
            logger.exception('Flow import failed: %s', exc)
            return 0, 0, 0
        finally:
            self._flow_import_lock.release()

    def _start_flow_import_watcher(self) -> None:
        'Start a lightweight poll watcher for newly completed nfcapd files.'
        if not getattr(cfg, 'FLOW_IMPORT_ENABLED', True):
            return
        if not getattr(cfg, 'FLOW_IMPORT_WATCH_ENABLED', True):
            return
        if self._flow_watch_thread is not None:
            return

        self._flow_watch_thread = Thread(
            target=self._watch_flow_import_dir,
            name='flow-import-watch',
            daemon=True,
        )
        self._flow_watch_thread.start()

    def _watch_flow_import_dir(self) -> None:
        'Import shortly after a new completed nfcapd capture appears.'
        capture_dir = Path(str(getattr(cfg, 'NFDUMP_DIR', '/var/cache/nfdump')))
        poll_seconds = float(getattr(cfg, 'FLOW_IMPORT_WATCH_POLL_SECONDS', 1.0) or 1.0)
        settle_seconds = float(getattr(cfg, 'FLOW_IMPORT_SETTLE_SECONDS', 1.0) or 0.0)
        known_files = self._completed_capture_file_names(capture_dir)
        while not self._flow_watch_stop.wait(max(0.1, poll_seconds)):
            current_files = self._completed_capture_file_names(capture_dir)
            if not current_files.difference(known_files):
                known_files = current_files
                continue

            known_files = current_files
            if settle_seconds > 0:
                self._flow_watch_stop.wait(settle_seconds)
            self.last_flow_import_monotonic = time.monotonic()
            self._import_flows_now('watch')

    @staticmethod
    def _completed_capture_file_names(capture_dir: Path) -> set[str]:
        'Return completed nfcapd capture filenames currently visible.'
        try:
            return {path.name for path in completed_capture_files(capture_dir)}
        except OSError as exc:
            logger.warning('Flow import watch skipped directory scan: %s', exc)
            return set()

    def _update_client_usage(self, client: ClientInfo) -> float:
        previous_total = self.last_totals_by_client_mac.get(client.mac, client.mb_used_since_connection)
        # Connection reset/device reconnect can roll counters backward.
        if client.mb_used_since_connection < previous_total:
            self.last_totals_by_client_mac[client.mac] = client.mb_used_since_connection
            return 0.0

        interval_mb = client.mb_used_since_connection - previous_total
        self.last_totals_by_client_mac[client.mac] = client.mb_used_since_connection
        return interval_mb

    def _enforce_limit_if_needed(self, client: ClientInfo, day_total_mb: float, calendar_month_total_mb: float) -> tuple[bool, SpeedLimit | None]:
        if not cfg.THROTTLING_ENABLED:
            return False, client.speed_limit
        target_profile_name = target_profile_name_for_usage(client.vlan_id, day_total_mb, calendar_month_total_mb, self.throttleable_vlan_ids)
        target_limit = self.speed_limits_by_name.get(target_profile_name) if target_profile_name else None
        return enforce_target_limit(client.name, client.unifi_client_id, client.speed_limit, target_limit, self.throttling_limit_ids)

    def _configured_throttling_limit_ids_for_release(self) -> set[str]:
        'Resolve configured throttle profile IDs for one-time release paths.'
        try:
            configured_levels = build_throttling_levels(list(self.speed_limits_by_id.values()))
        except ValueError as exc:
            logger.warning("Skipping startup release for throttling profiles: %s", exc)
            return set()
        return get_throttling_limit_ids(configured_levels)

if __name__ == "__main__":
    configure_logging()
    db.init_db()
    UsageMonitor().run_forever()
