import requests
import urllib3
from keys import API_KEY

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- SHARED CONFIG ---
BASE_URL      = "https://192.168.0.1/proxy/network/api/s/default"
HEADERS       = {"X-API-KEY": API_KEY, "Accept": "application/json"}
SLOW_GROUP_ID = '69cad159178e6c2c87e7fdae'

def get_api_data(endpoint: str) -> list:
    """Generic fetcher for UniFi data endpoints."""
    try:
        response = requests.get(f"{BASE_URL}/{endpoint}", headers=HEADERS, verify=False, timeout=10)
        response.raise_for_status()
        return response.json().get('data', [])
    except Exception as e:
        print(f"⚠️ UniFi API Error ({endpoint}): {e}")
        return []

def get_speed_limit_names_map() -> dict[str, str]:
    """Returns a dictionary mapping Group IDs to their human Names."""
    groups = get_api_data('list/usergroup')
    return {g['_id']: g['name'] for g in groups}

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

def get_group_id_by_name(group_name: str) -> str | None:
    """
    Looks up the 24-character hex ID for a given group name (e.g., 'Slow').
    Returns None if the group is not found.
    """
    groups = get_api_data('list/usergroup')
    # Case-insensitive search for the group name
    target = next((g for g in groups if g['name'].lower() == group_name.lower()), None)

    return target['_id'] if target else None

def bytes_to_mb(bytes: int) -> float:
    return bytes / (1024 * 1024)

def get_signal_emoji(rssi: int) -> str:
    if rssi <= 0:  return ""
    if rssi >= 40: return "🟢"
    if rssi >= 25: return "🟡"
    if rssi >= 15: return "🟠"
    return "🔴"
