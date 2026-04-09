import time
from datetime import datetime
import unifi_api as api
import database as db
import config as cfg
from structures import SpeedLimit

def process_connected_clients() -> None:
    ap_names_by_mac: dict[str, str] = api.get_ap_names_by_mac()
    clients:         list           = api.get_api_data('stat/sta')

    print(f'\n--- Update: {datetime.now().strftime('%H:%M:%S')} ---')
    header = f"{'Name':<18} | {'MAC':<17} | {'VLAN':<10} | {'AP':<16} | {'Sig':<3} | {'Last Min':>11} | {'Day Total':>11} | {'Speed Limit (kbps up/down)':<30}"
    print(header)
    print("-" * len(header))

    for c in clients:
        mac            = c.get('mac')
        name           = c.get('name') or c.get('hostname') or c.get('dev_name') or mac
        vlan_id        = c.get('network_id')
        vlan_name      = c.get('network', '')
        speed_limit_id: str | None = c.get('usergroup_id')
        speed_limit: SpeedLimit | None = speed_limits_by_id[speed_limit_id] if speed_limit_id else None
        ap_mac         = c.get('ap_mac', '')
        ap_name        = ap_names_by_mac.get(ap_mac, '')
        signal         = c.get('signal', 0)
        curr_total_mb  = (c.get('tx_bytes', 0) + c.get('rx_bytes', 0)) / (1024 * 1024)

        if mac not in last_totals_by_client_mac:
            last_totals_by_client_mac[mac] = curr_total_mb

        if curr_total_mb < last_totals_by_client_mac[mac]:
            last_totals_by_client_mac[mac] = 0

        interval_mb = curr_total_mb - last_totals_by_client_mac[mac]
        interval_kb = interval_mb * 1024
        last_totals_by_client_mac[mac] = curr_total_mb

        if interval_kb >= cfg.IGNORE_BELOW_KB:
            db.log_usage(mac, name, vlan_name, interval_mb, speed_limit.name if speed_limit else '', ap_name, signal)

            day_total_mb = db.get_daily_total(mac)

            if vlan_id in throttleable_vlan_ids and day_total_mb > cfg.DATA_LIMIT_MB:
                if speed_limit_id != slow_speed_limit.id:
                    print(f'⚠️ LIMIT REACHED: Throttling {name}')
                    api.set_user_group(c.get('_id'), slow_speed_limit.id)

            drop_suffix = ' AP'  # No need for suffix in a column labeled "Access Point"
            ap_str = ap_name[:-len(drop_suffix)] if ap_name.endswith(drop_suffix) else ap_name
            signal_str = ' ' * 3 if signal == 0 else f'{signal:<3}'
            interval_str = ' ' * 11 if interval_kb < 1.0   else f'{interval_kb:>8,.0f} KB'
            total_str    = ' ' * 11 if day_total_mb < 0.01 else f'{day_total_mb:>8,.0f} MB'
            speed_limit_str = str(speed_limit) if speed_limit else ''

            print(f'{name:<18} | {mac:<17} | {vlan_name:<10} | {ap_str:<16} | {signal_str} | {interval_str} | {total_str} | {speed_limit_str:<30}')

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
