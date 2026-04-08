import time
from datetime import datetime
import unifi_api as api
import database as db

IGNORE_BELOW_KB = 10
BASIC_VLAN_ID = '68b2649affd51d6829ce7225'
DATA_LIMIT_MB = 5_000

stats_db: dict = {}

def update_monitor() -> None:
    speed_limit_names_by_id: dict[str, str]  = api.get_speed_limit_names_map()
    ap_names_by_mac:         dict[str, str]  = api.get_ap_names_map()
    clients:                 list            = api.get_api_data('stat/sta')

    print(f'\n--- Update: {datetime.now().strftime('%H:%M:%S')} ---')
    header = f"{'VLAN':<10} | {'Name':<18} | {'AP':<16} | {'Sig':<5} | {'Last Min':<11} | {'Day Total':<11} | Speed Limit"
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
        signal         = c.get('rssi', 0)
        sig_icon       = api.get_signal_emoji(signal)
        curr_total_mb  = api.bytes_to_mb(c.get('tx_bytes', 0) + c.get('rx_bytes', 0))

        if mac not in stats_db:
            stats_db[mac] = {'last_total': curr_total_mb}
        if curr_total_mb < stats_db[mac]['last_total']:
            stats_db[mac]['last_total'] = 0

        interval_mb = curr_total_mb - stats_db[mac]['last_total']
        interval_kb = interval_mb * 1024
        stats_db[mac]['last_total'] = curr_total_mb

        if interval_kb >= IGNORE_BELOW_KB:
            db.log_usage(mac, name, vlan_name, interval_mb, speed_limit, ap_name, signal)

            day_total_mb = db.get_daily_total(mac)

            if vlan_id == BASIC_VLAN_ID and day_total_mb > DATA_LIMIT_MB:
                if speed_limit_id != api.SLOW_GROUP_ID:
                    print(f'⚠️ LIMIT REACHED: Throttling {name}')
                    # api.set_user_group(c.get('_id'), api.SLOW_GROUP_ID)

            drop_suffix = ' AP'  # No need for suffix in a column labeled "Access Point"
            ap_str = ap_name[:-len(drop_suffix)] if ap_name.endswith(drop_suffix) else ap_name
            interval_str = ' ' * 11 if interval_kb < 1.0   else f'{interval_kb:>8.2f} KB'
            total_str    = ' ' * 11 if day_total_mb < 0.01 else f'{day_total_mb:>8.2f} MB'
            signal_str   = ' ' *  5 if signal <= 0         else f'{sig_icon}{signal:<3}'

            print(f'{vlan_name:<10} | {name:<18} | {ap_str:<16} | {signal_str} | {interval_str} | {total_str} | {speed_limit}')

if __name__ == '__main__':
    db.init_db()
    while True:
        try:
            update_monitor()
        except Exception as e:
            print(f'Error: {e}')

        time.sleep(60)
