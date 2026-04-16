import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime

import config as cfg
import database as db
import unifi_api as api
from clientinfo import ClientInfo
from speedlimit import SpeedLimit
from throttling_policy import target_profile_name_for_usage
from throttling_runtime import (
    build_throttling_levels,
    enforce_target_limit,
    get_throttling_limit_ids,
    is_speed_limit_throttled,
    release_configured_limits,
)


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
        self.refresh_runtime_state()

        release_configured_limits(self.throttling_limit_ids, "startup")

    def refresh_runtime_state(self) -> None:
        'Reload speed-limit groups and throttleable VLAN IDs from the controller.'
        speed_limits = api.get_speed_limits()
        self.speed_limits_by_id = {limit.id: limit for limit in speed_limits}
        self.speed_limits_by_name = {limit.name: limit for limit in speed_limits}
        self.throttling_levels = build_throttling_levels(speed_limits)
        self.throttling_limit_ids = get_throttling_limit_ids(self.throttling_levels)

        self.throttleable_vlan_ids = api.get_vlan_ids_for_names(
            cfg.THROTTLEABLE_VLAN_NAMES
        )

    def process_connected_clients(self) -> list[ClientSnapshot]:
        'Process all connected clients for one cycle and return current snapshots.'
        snapshots: list[ClientSnapshot] = []
        ap_names_by_mac = api.get_ap_names_by_mac()

        for raw_client in api.get_api_data("stat/sta"):
            client = ClientInfo.create(
                raw_client, self.speed_limits_by_id, ap_names_by_mac
            )
            interval_mb = self._update_client_usage(client)
            interval_kb = interval_mb * 1000

            if interval_kb >= cfg.IGNORE_BELOW_KB:
                db.log_usage(client, interval_mb)

            day_total_mb = db.get_daily_total(client.mac)
            last_7_days_total_mb = db.get_last_7_days_total(client.mac)
            calendar_month_total_mb = db.get_calendar_month_total(client.mac)
            is_throttled, effective_speed_limit = self._enforce_limit_if_needed(
                client,
                day_total_mb,
                calendar_month_total_mb,
            )
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

        return snapshots

    def run_forever(
        self,
        poll_interval_seconds: int = 60,
        on_cycle: Callable[[list[ClientSnapshot]], None] | None = None,
    ) -> None:
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
                if on_cycle:
                    on_cycle(snapshots)
            except Exception as exc:
                print(f"Error: {exc}")

    def _sleep_until_next_poll_boundary(self, poll_interval_seconds: int) -> None:
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
            print(f"Midnight Reset: {now_date}")
            release_configured_limits(
                self.throttling_limit_ids, "midnight"
            )
            self.current_day = now_date

    def _update_client_usage(self, client: ClientInfo) -> float:
        previous_total = self.last_totals_by_client_mac.get(
            client.mac, client.mb_used_since_connection
        )
        # Connection reset/device reconnect can roll counters backward.
        if client.mb_used_since_connection < previous_total:
            previous_total = 0

        interval_mb = client.mb_used_since_connection - previous_total
        self.last_totals_by_client_mac[client.mac] = client.mb_used_since_connection
        return interval_mb

    def _enforce_limit_if_needed(
        self,
        client: ClientInfo,
        day_total_mb: float,
        calendar_month_total_mb: float,
    ) -> tuple[bool, SpeedLimit | None]:
        target_profile_name = target_profile_name_for_usage(
            client.vlan_id,
            day_total_mb,
            calendar_month_total_mb,
            self.throttleable_vlan_ids,
        )
        target_limit = (
            self.speed_limits_by_name.get(target_profile_name)
            if target_profile_name
            else None
        )

        return enforce_target_limit(
            client.name,
            client.unifi_client_id,
            client.speed_limit,
            target_limit,
            self.throttling_limit_ids,
        )


if __name__ == "__main__":
    db.init_db()
    UsageMonitor().run_forever()
