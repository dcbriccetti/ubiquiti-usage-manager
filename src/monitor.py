import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime

import config as cfg
import database as db
import unifi_api as api
from clientinfo import ClientInfo
from speedlimit import SpeedLimit

def _get_throttling_policy() -> list[tuple[float, str]]:
    'Return sorted threshold/profile-name pairs from config with legacy fallback.'
    levels = getattr(cfg, "THROTTLING_LEVELS", None)
    if levels:
        return sorted(
            [
                (float(level.threshold_mb), level.profile_name)
                for level in levels
            ],
            key=lambda pair: pair[0],
        )

    legacy_policy = getattr(cfg, "THROTTLING_SPEED_LIMITS", None)
    if legacy_policy:
        return sorted(
            [(float(threshold_mb), profile_name) for threshold_mb, profile_name in legacy_policy],
            key=lambda pair: pair[0],
        )

    return [(float(cfg.DATA_LIMIT_MB), cfg.SLOW_SPEED_LIMIT_NAME)]


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
    speed_limits_by_name = {limit.name: limit for limit in speed_limits}
    throttling_profile_names = [profile_name for _, profile_name in _get_throttling_policy()]
    throttling_limit_ids = {
        speed_limits_by_name[name].id for name in throttling_profile_names if name in speed_limits_by_name
    }
    ap_names_by_mac = api.get_ap_names_by_mac()

    snapshots: list[ClientSnapshot] = []
    for raw_client in api.get_api_data("stat/sta"):
        client = ClientInfo.create(raw_client, speed_limits_by_id, ap_names_by_mac)
        is_throttled = bool(
            client.speed_limit
            and client.speed_limit.id in throttling_limit_ids
        )
        snapshots.append(
            ClientSnapshot(
                client=client,
                interval_mb=0,
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
        self.throttling_levels: list[tuple[float, SpeedLimit]] = []
        self.throttling_limit_ids: set[str] = set()
        self.throttleable_vlan_ids: list[str] = []
        self.refresh_runtime_state()

        self._release_configured_limits("startup")

    def refresh_runtime_state(self) -> None:
        'Reload speed-limit groups and throttleable VLAN IDs from the controller.'
        speed_limits = api.get_speed_limits()
        self.speed_limits_by_id = {limit.id: limit for limit in speed_limits}
        speed_limits_by_name = {limit.name: limit for limit in speed_limits}
        policy = _get_throttling_policy()

        self.throttling_levels = []
        for threshold_mb, profile_name in policy:
            limit = speed_limits_by_name.get(profile_name)
            if not limit:
                raise ValueError(f"Could not find speed limit named: {profile_name}")
            self.throttling_levels.append((threshold_mb, limit))

        self.throttling_limit_ids = {limit.id for _, limit in self.throttling_levels}

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
            is_throttled, effective_speed_limit = self._enforce_limit_if_needed(client, day_total_mb)
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
        while True:
            try:
                self._handle_day_transition()
                snapshots = self.process_connected_clients()
                db.update_monitor_heartbeat()
                if on_cycle:
                    on_cycle(snapshots)
            except Exception as exc:
                print(f"Error: {exc}")

            time.sleep(poll_interval_seconds)

    def _handle_day_transition(self) -> None:
        now_date = datetime.now().date()
        if now_date > self.current_day:
            print(f"Midnight Reset: {now_date}")
            self._release_configured_limits("midnight")
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
        self, client: ClientInfo, day_total_mb: float
    ) -> tuple[bool, SpeedLimit | None]:
        # Return both throttled state and the effective speed limit for current-cycle display.
        current_limit = client.speed_limit
        target_limit = self._target_limit_for_usage(client, day_total_mb)

        if target_limit and (not current_limit or current_limit.id != target_limit.id):
            if cfg.SAFE_MODE:
                print(f"SAFE_MODE: would throttle {client.name} to {target_limit.name}")
            else:
                print(f"LIMIT REACHED: Throttling {client.name} to {target_limit.name}")
                if api.set_user_group(client.unifi_client_id, target_limit.id):
                    current_limit = target_limit

        is_throttled = bool(
            current_limit
            and current_limit.id in self.throttling_limit_ids
        )
        return is_throttled, current_limit

    def _target_limit_for_usage(
        self, client: ClientInfo, day_total_mb: float
    ) -> SpeedLimit | None:
        if client.vlan_id not in self.throttleable_vlan_ids:
            return None

        target: SpeedLimit | None = None
        for threshold_mb, limit in self.throttling_levels:
            if day_total_mb >= threshold_mb:
                target = limit
        return target

    def _release_configured_limits(self, context: str) -> None:
        if not self.throttling_limit_ids:
            return

        if cfg.SAFE_MODE:
            print(f"SAFE_MODE: skipping release_all_from_limit during {context}.")
            return

        for limit_id in self.throttling_limit_ids:
            api.release_all_from_limit(limit_id)


if __name__ == "__main__":
    db.init_db()
    UsageMonitor().run_forever()
