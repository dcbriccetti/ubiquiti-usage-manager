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
        SpeedLimit(id=g['_id'], name=g['name'],
                   up_kbps=g.get('qos_rate_max_up'), down_kbps=g.get('qos_rate_max_down'))
        for g in get_api_data('list/usergroup')
        if '_id' in g and 'name' in g and isinstance(g['_id'], str) and isinstance(g['name'], str)
    ]

def get_ap_names_by_mac() -> dict[str, str]:
    'Return a mapping of AP MAC address to AP display name/model.'
    devices = get_api_data('stat/device')
    return {
        str(d['mac']): str(d.get('name') or d.get('model') or '')
        for d in devices
        if d.get('mac')
    }

def set_user_group(user_id: str, group_id: str | None) -> bool:
    "Update one client's UniFi group/profile."
    url = f"{BASE_URL}/upd/user/{user_id}"
    try:
        res = requests.post(url, json={"usergroup_id": group_id or ''}, headers=HEADERS, verify=False, timeout=10)
        return res.status_code == 200
    except Exception as e:
        print(f"⚠️ UniFi API Error (upd/user/{user_id}): {e}")
        return False

def get_vlan_ids_for_names(names: list[str]) -> list[str]:
    'Resolve network names to UniFi VLAN/network IDs.'
    networks = get_api_data('rest/networkconf')
    return [str(n.get('_id')) for n in networks if n.get('name') in names and n.get('_id')]

def release_all_from_limits(throttling_group_ids: set[str]) -> None:
    'Move all clients assigned to any configured throttling group back to default.'
    if not throttling_group_ids:
        return

    clients = get_api_data('stat/sta')
    count = 0
    for c in clients:
        user_id = c.get('_id')
        group_id = c.get('usergroup_id')
        if group_id in throttling_group_ids and user_id:
            if set_user_group(str(user_id), None):
                count += 1
    if count > 0:
        print(f"✅ Successfully released {count} user(s) from throttling speed limits.")

def get_group_id_by_name(group_name: str) -> str | None:
    'Look up the UniFi group ID for a group name.'
    groups = get_api_data('list/usergroup')
    for group in groups:
        name = group.get('name')
        group_id = group.get('_id')
        if isinstance(name, str) and isinstance(group_id, str) and name.lower() == group_name.lower():
            return group_id
    return None
