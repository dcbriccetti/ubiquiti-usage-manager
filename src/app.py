import time
from datetime import datetime
import unifi_api as api
import database as db
import config as cfg

def process_connected_clients() -> None:
    speed_limit_names_by_id: dict[str, str]  = dict(api.get_speed_limit_names())
    ap_names_by_mac:         dict[str, str]  = api.get_ap_names_map()
    clients:                 list            = api.get_api_data('stat/sta')

    print(f'\n--- Update: {datetime.now().strftime('%H:%M:%S')} ---')
    header = f"{'Name':<18} | {'MAC':<17} | {'VLAN':<10} | {'AP':<16} | {'Sig':<3} | {'Last Min':>11} | {'Day Total':>11} | Speed Limit"
    print(header)
    print("-" * len(header))

    for c in clients:
        mac            = c.get('mac')
        name           = c.get('name') or c.get('hostname') or c.get('dev_name') or mac
        vlan_id        = c.get('network_id')
        vlan_name      = c.get('network', '')
        speed_limit_id = c.get('usergroup_id')
        speed_limit    = speed_limit_names_by_id.get(speed_limit_id, '')
        ap_mac         = c.get('ap_mac', '')
        ap_name        = ap_names_by_mac.get(ap_mac, '')
        signal         = c.get('signal', 0)
        curr_total_mb  = api.bytes_to_mb(c.get('tx_bytes', 0) + c.get('rx_bytes', 0))

        if mac not in last_totals_by_client_mac:
            last_totals_by_client_mac[mac] = curr_total_mb

        if curr_total_mb < last_totals_by_client_mac[mac]:
            last_totals_by_client_mac[mac] = 0

        interval_mb = curr_total_mb - last_totals_by_client_mac[mac]
        interval_kb = interval_mb * 1024
        last_totals_by_client_mac[mac] = curr_total_mb

        if interval_kb >= cfg.IGNORE_BELOW_KB:
            db.log_usage(mac, name, vlan_name, interval_mb, speed_limit, ap_name, signal)

            day_total_mb = db.get_daily_total(mac)

            if vlan_id in THROTTLEABLE_VLAN_IDS and day_total_mb > cfg.DATA_LIMIT_MB:
                if speed_limit_id != SLOW_GROUP_ID:
                    print(f'⚠️ LIMIT REACHED: Throttling {name}')
                    api.set_user_group(c.get('_id'), SLOW_GROUP_ID)

            drop_suffix = ' AP'  # No need for suffix in a column labeled "Access Point"
            ap_str = ap_name[:-len(drop_suffix)] if ap_name.endswith(drop_suffix) else ap_name
            interval_str = ' ' * 11 if interval_kb < 1.0   else f'{interval_kb:>8.2f} KB'
            total_str    = ' ' * 11 if day_total_mb < 0.01 else f'{day_total_mb:>8.2f} MB'

            print(f'{name:<18} | {mac:<17} | {vlan_name:<10} | {ap_str:<16} | {signal:<3} | {interval_str} | {total_str} | {speed_limit}')

def resolve_config_ids():
    """
    Look up the IDs for the names specified in config.py
    """
    # Get all user groups from the controller
    user_groups: list[tuple[str, str]] = api.get_speed_limit_names()

    # Resolve SLOW_GROUP_ID
    slow_group = next((g for g in user_groups if g[1] == cfg.SLOW_GROUP_NAME), None)
    if not slow_group:
        raise ValueError(f"Could not find User Group named: {cfg.SLOW_GROUP_NAME}")

    vlan_ids: list[str] = api.get_vlan_ids_for_names(cfg.THROTTLEABLE_VLAN_NAMES)

    return slow_group[0], vlan_ids

if __name__ == "__main__":
    db.init_db()

    SLOW_GROUP_ID, THROTTLEABLE_VLAN_IDS = resolve_config_ids()
    last_totals_by_client_mac: dict[str, float] = {}
    api.release_all_from_limit(SLOW_GROUP_ID)
    current_day = datetime.now().date()

    while True:
        try:
            # Check for midnight transition
            now_date = datetime.now().date()
            if now_date > current_day:
                print(f"🕛 Midnight Reset: {now_date}")
                api.release_all_from_limit(SLOW_GROUP_ID)
                current_day = now_date

            process_connected_clients()
        except Exception as e:
            print(f"Error: {e}")

        time.sleep(60)
