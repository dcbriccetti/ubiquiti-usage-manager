import requests
import urllib3
from keys import API_KEY

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- SHARED CONFIG ---
BASE_URL      = "https://192.168.0.1/proxy/network/api/s/default"
HEADERS       = {"X-API-KEY": API_KEY, "Accept": "application/json"}

def get_api_data(endpoint: str) -> list:
    """Generic fetcher for UniFi data endpoints."""
    try:
        response = requests.get(f"{BASE_URL}/{endpoint}", headers=HEADERS, verify=False, timeout=10)
        response.raise_for_status()
        return response.json().get('data', [])
    except Exception as e:
        print(f"⚠️ UniFi API Error ({endpoint}): {e}")
        return []

def get_speed_limit_names() -> list[tuple[str, str]]:
    groups = get_api_data('list/usergroup')
    return [(g['_id'], g['name']) for g in groups]

def get_ap_names_map() -> dict[str, str]:
    """Returns a dictionary mapping AP MACs to their Names/Models."""
    devices = get_api_data('stat/device')
    return {d['mac']: (d.get('name') or d.get('model')) for d in devices}

def set_user_group(user_id: str, group_id: str | None) -> bool:
    """Updates a user's speed profile/group."""
    url = f"{BASE_URL}/upd/user/{user_id}"
    try:
        res = requests.post(url, json={"usergroup_id": group_id}, headers=HEADERS, verify=False)
        return res.status_code == 200
    except:
        return False

def get_vlan_ids_for_names(names: list[str]) -> list[str]:
    networks = get_api_data('rest/networkconf')
    return [n['_id'] for n in networks if n.get('name') in names]

def release_all_from_limit(slow_group_id: str) -> None:
    """Finds everyone currently throttled and moves them to the Default group."""
    clients = get_api_data('stat/sta')
    count = 0
    for c in clients:
        if c.get('usergroup_id') == slow_group_id:
            if set_user_group(c.get('_id'), ""): # "" usually resets to Default
                count += 1
    if count > 0:
        print(f"✅ Successfully released {count} user(s) from the speed limit.")

def get_group_id_by_name(group_name: str) -> str | None:
    """
    Looks up the 24-character hex ID for a given group name (e.g., 'Slow').
    Returns None if the group is not found.
    """
    groups = get_api_data('list/usergroup')
    # Case-insensitive search for the group name
    target = next((g for g in groups if g['name'].lower() == group_name.lower()), None)

    return target['_id'] if target else None

def bytes_to_mb(num_bytes: int) -> float:
    return num_bytes / (1024 * 1024)
