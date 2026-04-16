'''UniFi controller HTTP helpers.

Provides small, typed wrappers around the UniFi Network API endpoints used by
monitoring and dashboard modules.
'''

from typing import Any

import requests
import urllib3
from keys import API_KEY
from speedlimit import SpeedLimit

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- SHARED CONFIG ---
BASE_URL = "https://192.168.0.1/proxy/network/api/s/default"
HEADERS = {"X-API-KEY": API_KEY, "Accept": "application/json"}

def get_api_data(endpoint: str) -> list[dict[str, Any]]:
    'Fetch a UniFi endpoint and return its data array.'
    try:
        response = requests.get(f"{BASE_URL}/{endpoint}", headers=HEADERS, verify=False, timeout=10)
        response.raise_for_status()
        payload = response.json()
        data = payload.get('data', []) if isinstance(payload, dict) else []
        return [item for item in data if isinstance(item, dict)]
    except Exception as e:
        print(f"⚠️ UniFi API Error ({endpoint}): {e}")
        return []

def get_speed_limits() -> list[SpeedLimit]:
    'Return configured UniFi user groups as SpeedLimit objects.'
    return [
        SpeedLimit(
            id=str(g.get('_id', '')),
            name=str(g.get('name', '')),
            up_kbps=g.get('qos_rate_max_up'),
            down_kbps=g.get('qos_rate_max_down')
        )
        for g in get_api_data('list/usergroup')
        if g.get('_id') and g.get('name')
    ]

def get_ap_names_by_mac() -> dict[str, str]:
    'Return a mapping of AP MAC address to AP display name/model.'
    devices = get_api_data('stat/device')
    return {
        str(d.get('mac', '')): str(d.get('name') or d.get('model') or '')
        for d in devices
        if d.get('mac')
    }

def set_user_group(user_id: str, group_id: str | None) -> bool:
    "Update one client's UniFi group/profile."
    url = f"{BASE_URL}/upd/user/{user_id}"
    try:
        res = requests.post(
            url,
            json={"usergroup_id": group_id},
            headers=HEADERS,
            verify=False,
            timeout=10,
        )
        return res.status_code == 200
    except Exception as e:
        print(f"⚠️ UniFi API Error (upd/user/{user_id}): {e}")
        return False

def get_vlan_ids_for_names(names: list[str]) -> list[str]:
    'Resolve network names to UniFi VLAN/network IDs.'
    networks = get_api_data('rest/networkconf')
    return [str(n.get('_id')) for n in networks if n.get('name') in names and n.get('_id')]

def release_all_from_limit(slow_group_id: str) -> None:
    'Move all currently throttled clients back to the default group.'
    clients = get_api_data('stat/sta')
    count = 0
    for c in clients:
        user_id = c.get('_id')
        if c.get('usergroup_id') == slow_group_id and user_id:
            if set_user_group(str(user_id), ""): # "" usually resets to Default
                count += 1
    if count > 0:
        print(f"✅ Successfully released {count} user(s) from the speed limit.")

def get_group_id_by_name(group_name: str) -> str | None:
    'Look up the UniFi group ID for a group name.'
    groups = get_api_data('list/usergroup')
    for group in groups:
        name = group.get('name')
        group_id = group.get('_id')
        if isinstance(name, str) and isinstance(group_id, str) and name.lower() == group_name.lower():
            return group_id
    return None
