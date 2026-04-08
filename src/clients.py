import requests
import urllib3
from keys import API_KEY, SITE_ID

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
GATEWAY_IP = "192.168.0.1" # Change if your local subnet is different
URL = f"https://{GATEWAY_IP}/proxy/network/integration/v1/sites/{SITE_ID}/clients"

headers = {
    "X-API-KEY": API_KEY,
    "Accept": "application/json"
}

def get_clients():
    try:
        # verify=False is usually needed for local UniFi SSL certs
        response = requests.get(URL, headers=headers, verify=False)

        if response.status_code == 200:
            data = response.json()
            # The official API returns a list under the 'data' key
            clients = data.get('data', [])
            print(f"--- Found {len(clients)} Clients ---")
            for c in clients:
                name = c.get('name') or c.get('hostname') or "Unknown"
                ip = c.get('ipAddress')
                mac = c.get('macAddress')
                print(f"[{name}] IP: {ip} | MAC: {mac}")
        else:
            print(f"Error {response.status_code}: {response.text}")

    except Exception as e:
        print(f"Connection Failed: {e}")

if __name__ == "__main__":
    get_clients()
