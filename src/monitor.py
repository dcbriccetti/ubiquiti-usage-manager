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
    last_7_days_total_mb: float
    calendar_month_total_mb: float
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
                last_7_days_total_mb=db.get_last_7_days_total(client.mac),
                calendar_month_total_mb=db.get_calendar_month_total(client.mac),
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
        report_rows: list[str] = []

        print(f"\n--- Update: {datetime.now().strftime('%H:%M:%S')} ---")
        header_top, header_bottom, divider, usage_col_width, speed_limit_col_width = self._build_report_headers()

        for raw_client in api.get_api_data("stat/sta"):
            client = ClientInfo.create(
                raw_client, self.speed_limits_by_id, ap_names_by_mac
            )
            interval_mb = self._update_client_usage(client)
            interval_kb = interval_mb * 1000

            if interval_kb >= cfg.IGNORE_BELOW_KB:
                # db.log_usage(client, interval_mb)  # Temporarily disabled: no DB writes.
                pass

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
                    is_throttled=is_throttled,
                )
            )

            report_rows.append(
                self._build_snapshot_row(
                    client,
                    interval_mb,
                    day_total_mb,
                    last_7_days_total_mb,
                    calendar_month_total_mb,
                    effective_speed_limit,
                    usage_col_width,
                    speed_limit_col_width,
                )
            )

        print(header_top)
        print(header_bottom)
        print(divider)
        for row in report_rows:
            print(row)

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

    def _enforce_limit_if_needed(
        self, client: ClientInfo, day_total_mb: float
    ) -> tuple[bool, SpeedLimit | None]:
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
                return False, client.speed_limit

            print(f"LIMIT REACHED: Throttling {client.name}")
            if api.set_user_group(client.unifi_client_id, self.slow_speed_limit.id):
                return True, self.slow_speed_limit

        return should_throttle or already_throttled, client.speed_limit

    @staticmethod
    def _build_report_headers() -> tuple[str, str, str, int, int]:
        base_header = (
            f"{'User ID':<13} | {'Name':<20} | {'MAC':<17} | {'VLAN':<10} | {'AP':<16} | "
            f"{'Sig':<3}"
        )
        usage_col_width = 10
        speed_limit_header = "Speed Limit (kbps up/down)"
        speed_limit_col_width = len(speed_limit_header)
        usage_columns = (
            f"{'Minute':>{usage_col_width}} | {'Today':>{usage_col_width}} | "
            f"{'7 Days':>{usage_col_width}} | {'This Month':>{usage_col_width}}"
        )
        usage_group_label = " Usage (MB) "
        header_top = (
            f"{' ' * len(base_header)} | {usage_group_label.center(len(usage_columns), '-')} | "
        )
        header_bottom = (
            f"{base_header} | {usage_columns} | {speed_limit_header:<{speed_limit_col_width}} |"
        )
        divider = "-" * len(header_bottom)
        return header_top, header_bottom, divider, usage_col_width, speed_limit_col_width

    @staticmethod
    def _build_snapshot_row(
        client: ClientInfo,
        interval_mb: float,
        day_total_mb: float,
        last_7_days_total_mb: float,
        calendar_month_total_mb: float,
        effective_speed_limit: SpeedLimit | None,
        usage_col_width: int,
        speed_limit_col_width: int,
    ) -> str:
        drop_suffix = " AP"
        ap_str = (
            client.ap_name[:-len(drop_suffix)]
            if client.ap_name.endswith(drop_suffix)
            else client.ap_name
        )
        signal_str = " " * 3 if client.signal == 0 else f"{client.signal:<3}"
        interval_str = (
            " " * usage_col_width
            if interval_mb < 0.01
            else f"{interval_mb:>{usage_col_width},.2f}"
        )
        total_str = (
            " " * usage_col_width
            if day_total_mb < 0.01
            else f"{day_total_mb:>{usage_col_width},.1f}"
        )
        last_7_days_str = (
            " " * usage_col_width
            if last_7_days_total_mb < 0.01
            else f"{last_7_days_total_mb:>{usage_col_width},.1f}"
        )
        calendar_month_str = (
            " " * usage_col_width
            if calendar_month_total_mb < 0.01
            else f"{calendar_month_total_mb:>{usage_col_width},.1f}"
        )
        speed_limit_str = str(effective_speed_limit) if effective_speed_limit else ""

        return (
            f"{client.user_id:<13} | {client.name[:20]:<20} | {client.mac:<17} | "
            f"{client.vlan_name:<10} | {ap_str:<16} | {signal_str} | {interval_str} | "
            f"{total_str} | {last_7_days_str} | {calendar_month_str} | "
            f"{speed_limit_str:<{speed_limit_col_width}} |"
        )


if __name__ == "__main__":
    UsageMonitor().run_forever()
