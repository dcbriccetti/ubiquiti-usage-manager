import time
from datetime import datetime
import unifi_api as api
import database as db
import config as cfg
from speedlimit import SpeedLimit
from clientinfo import ClientInfo

def process_connected_clients() -> None:
    ap_names_by_mac: dict[str, str] = api.get_ap_names_by_mac()
    print(f'\n--- Update: {datetime.now().strftime('%H:%M:%S')} ---')
    header = f"{'User ID':<13} | {'Name':<20} | {'MAC':<17} | {'VLAN':<10} | {'AP':<16} | {'Sig':<3} | {'Last Min':>11} | {'Day Total':>11} | {'Speed Limit (kbps up/down)':<20}"
    print(header)
    print("-" * len(header))

    clients = [ClientInfo.create(c, speed_limits_by_id, ap_names_by_mac) for c in api.get_api_data('stat/sta')]

    for c in clients:
        if c.mac not in last_totals_by_client_mac:
            last_totals_by_client_mac[c.mac] = c.mb_used_since_connection

        if c.mb_used_since_connection < last_totals_by_client_mac[c.mac]:
            last_totals_by_client_mac[c.mac] = 0

        interval_mb = c.mb_used_since_connection - last_totals_by_client_mac[c.mac]
        interval_kb = interval_mb * 1024
        last_totals_by_client_mac[c.mac] = c.mb_used_since_connection

        if interval_kb >= cfg.IGNORE_BELOW_KB:
            db.log_usage(c, interval_mb)

            day_total_mb = db.get_daily_total(c.mac)

            if c.vlan_id in throttleable_vlan_ids and day_total_mb > cfg.DATA_LIMIT_MB:
                if c.speed_limit is None or c.speed_limit.id != slow_speed_limit.id:
                    print(f'⚠️ LIMIT REACHED: Throttling {c.name}')
                    api.set_user_group(c.unifi_client_id, slow_speed_limit.id)

            drop_suffix = ' AP'  # No need for suffix in a column labeled "Access Point"
            ap_str = c.ap_name[:-len(drop_suffix)] if c.ap_name.endswith(drop_suffix) else c.ap_name
            signal_str = ' ' * 3 if c.signal == 0 else f'{c.signal:<3}'
            interval_str = ' ' * 11 if interval_kb < 1.0   else f'{interval_kb:>8,.0f} KB'
            total_str    = ' ' * 11 if day_total_mb < 0.01 else f'{day_total_mb:>8,.0f} MB'
            speed_limit_str = str(c.speed_limit) if c.speed_limit else ''

            print(f'{c.user_id:<13} | {c.name[:20]:<20} | {c.mac:<17} | {c.vlan_name:<10} | {ap_str:<16} | {signal_str} | {interval_str} | {total_str} | {speed_limit_str:<20}')

if __name__ == "__main__":
    db.init_db()

    speed_limits: list[SpeedLimit] = api.get_speed_limits()
    speed_limits_by_id: dict[str, SpeedLimit]  = {limit.id: limit for limit in speed_limits}
    slow_speed_limit: SpeedLimit | None = next((limit for limit in speed_limits if limit.name == cfg.SLOW_SPEED_LIMIT_NAME), None)
    if not slow_speed_limit:
        raise ValueError(f"Could not find speed limit named: {cfg.SLOW_SPEED_LIMIT_NAME}")

    throttleable_vlan_ids = api.get_vlan_ids_for_names(cfg.THROTTLEABLE_VLAN_NAMES)
    last_totals_by_client_mac: dict[str, float] = {}
    api.release_all_from_limit(slow_speed_limit.id)
    current_day = datetime.now().date()

    while True:
        try:
            # Check for midnight transition
            now_date = datetime.now().date()
            if now_date > current_day:
                print(f"🕛 Midnight Reset: {now_date}")
                api.release_all_from_limit(slow_speed_limit.id)
                current_day = now_date

            process_connected_clients()
        except Exception as e:
            print(f"Error: {e}")

        time.sleep(60)
