import time
from dataclasses import dataclass
from datetime import datetime

import config as cfg
import database as db
import unifi_api as api
from clientinfo import ClientInfo
from speedlimit import SpeedLimit


@dataclass(frozen=True, kw_only=True)
class ClientSnapshot:
    client: ClientInfo
    interval_mb: float
    day_total_mb: float
    is_throttled: bool


def get_connected_clients() -> list[ClientSnapshot]:
    speed_limits = api.get_speed_limits()
    speed_limits_by_id = {limit.id: limit for limit in speed_limits}
    slow_speed_limit = next(
        (limit for limit in speed_limits if limit.name == cfg.SLOW_SPEED_LIMIT_NAME),
        None,
    )
    ap_names_by_mac = api.get_ap_names_by_mac()

    snapshots: list[ClientSnapshot] = []
    for raw_client in api.get_api_data("stat/sta"):
        client = ClientInfo.create(raw_client, speed_limits_by_id, ap_names_by_mac)
        is_throttled = bool(
            slow_speed_limit
            and client.speed_limit
            and client.speed_limit.id == slow_speed_limit.id
        )
        snapshots.append(
            ClientSnapshot(
                client=client,
                interval_mb=0,
                day_total_mb=db.get_daily_total(client.mac),
                is_throttled=is_throttled,
            )
        )

    return snapshots


class UsageMonitor:
    def __init__(self) -> None:
        # db.init_db()  # Temporarily disabled: avoid schema writes.
        self.last_totals_by_client_mac: dict[str, float] = {}
        self.current_day = datetime.now().date()
        self.speed_limits_by_id: dict[str, SpeedLimit] = {}
        self.slow_speed_limit: SpeedLimit | None = None
        self.throttleable_vlan_ids: list[str] = []
        self.refresh_runtime_state()

        if self.slow_speed_limit:
            if cfg.SAFE_MODE:
                print("SAFE_MODE: skipping release_all_from_limit during startup.")
            else:
                api.release_all_from_limit(self.slow_speed_limit.id)

    def refresh_runtime_state(self) -> None:
        speed_limits = api.get_speed_limits()
        self.speed_limits_by_id = {limit.id: limit for limit in speed_limits}
        self.slow_speed_limit = next(
            (limit for limit in speed_limits if limit.name == cfg.SLOW_SPEED_LIMIT_NAME),
            None,
        )
        if not self.slow_speed_limit:
            raise ValueError(
                f"Could not find speed limit named: {cfg.SLOW_SPEED_LIMIT_NAME}"
            )

        self.throttleable_vlan_ids = api.get_vlan_ids_for_names(
            cfg.THROTTLEABLE_VLAN_NAMES
        )

    def process_connected_clients(self) -> list[ClientSnapshot]:
        snapshots: list[ClientSnapshot] = []
        ap_names_by_mac = api.get_ap_names_by_mac()

        print(f"\n--- Update: {datetime.now().strftime('%H:%M:%S')} ---")
        header = (
            f"{'User ID':<13} | {'Name':<20} | {'MAC':<17} | {'VLAN':<10} | "
            f"{'AP':<16} | {'Sig':<3} | {'Last Min':>11} | {'Day Total':>11} | "
            f"{'Speed Limit (kbps up/down)':<20}"
        )
        print(header)
        print("-" * len(header))

        for raw_client in api.get_api_data("stat/sta"):
            client = ClientInfo.create(
                raw_client, self.speed_limits_by_id, ap_names_by_mac
            )
            interval_mb = self._update_client_usage(client)
            interval_kb = interval_mb * 1024

            if interval_kb >= cfg.IGNORE_BELOW_KB:
                # db.log_usage(client, interval_mb)  # Temporarily disabled: no DB writes.
                pass

            day_total_mb = db.get_daily_total(client.mac)
            is_throttled = self._enforce_limit_if_needed(client, day_total_mb)
            snapshots.append(
                ClientSnapshot(
                    client=client,
                    interval_mb=interval_mb,
                    day_total_mb=day_total_mb,
                    is_throttled=is_throttled,
                )
            )

            self._print_snapshot_row(client, interval_kb, day_total_mb)

        return snapshots

    def run_forever(self, poll_interval_seconds: int = 60) -> None:
        while True:
            try:
                self._handle_day_transition()
                self.process_connected_clients()
            except Exception as exc:
                print(f"Error: {exc}")

            time.sleep(poll_interval_seconds)

    def _handle_day_transition(self) -> None:
        now_date = datetime.now().date()
        if now_date > self.current_day and self.slow_speed_limit:
            print(f"Midnight Reset: {now_date}")
            if cfg.SAFE_MODE:
                print("SAFE_MODE: skipping release_all_from_limit at midnight.")
            else:
                api.release_all_from_limit(self.slow_speed_limit.id)
            self.current_day = now_date

    def _update_client_usage(self, client: ClientInfo) -> float:
        previous_total = self.last_totals_by_client_mac.get(
            client.mac, client.mb_used_since_connection
        )
        if client.mb_used_since_connection < previous_total:
            previous_total = 0

        interval_mb = client.mb_used_since_connection - previous_total
        self.last_totals_by_client_mac[client.mac] = client.mb_used_since_connection
        return interval_mb

    def _enforce_limit_if_needed(self, client: ClientInfo, day_total_mb: float) -> bool:
        should_throttle = (
            client.vlan_id in self.throttleable_vlan_ids
            and day_total_mb > cfg.DATA_LIMIT_MB
            and self.slow_speed_limit is not None
        )

        already_throttled = bool(
            self.slow_speed_limit
            and client.speed_limit
            and client.speed_limit.id == self.slow_speed_limit.id
        )

        if should_throttle and not already_throttled and self.slow_speed_limit:
            if cfg.SAFE_MODE:
                print(f"SAFE_MODE: would throttle {client.name}")
                return False

            print(f"LIMIT REACHED: Throttling {client.name}")
            if api.set_user_group(client.unifi_client_id, self.slow_speed_limit.id):
                return True

        return should_throttle or already_throttled

    @staticmethod
    def _print_snapshot_row(
        client: ClientInfo, interval_kb: float, day_total_mb: float
    ) -> None:
        drop_suffix = " AP"
        ap_str = (
            client.ap_name[:-len(drop_suffix)]
            if client.ap_name.endswith(drop_suffix)
            else client.ap_name
        )
        signal_str = " " * 3 if client.signal == 0 else f"{client.signal:<3}"
        interval_str = " " * 11 if interval_kb < 1.0 else f"{interval_kb:>8,.0f} KB"
        total_str = " " * 11 if day_total_mb < 0.01 else f"{day_total_mb:>8,.0f} MB"
        speed_limit_str = str(client.speed_limit) if client.speed_limit else ""

        print(
            f"{client.user_id:<13} | {client.name[:20]:<20} | {client.mac:<17} | "
            f"{client.vlan_name:<10} | {ap_str:<16} | {signal_str} | {interval_str} | "
            f"{total_str} | {speed_limit_str:<20}"
        )


if __name__ == "__main__":
    UsageMonitor().run_forever()
