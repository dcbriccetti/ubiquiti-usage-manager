'Helpers for mapping an HTTP requester on LAN to a UniFi client MAC address.'

from flask import Request

import unifi_api as api


def get_request_ip(request: Request) -> str | None:
    'Return the best client IP candidate from proxy headers or direct connection.'
    # If the app is behind a reverse proxy, first forwarded IP is the client.
    forwarded_for = request.headers.get('X-Forwarded-For', '')
    if forwarded_for:
        first_ip = forwarded_for.split(',')[0].strip()
        if first_ip:
            return first_ip
    return request.remote_addr


def find_client_mac_for_ip(ip_address: str) -> str | None:
    'Resolve a LAN IP to a UniFi client MAC using active and recently-seen client endpoints.'
    # stat/sta covers currently active stations; stat/alluser broadens coverage for recent/wired clients.
    for endpoint in ('stat/sta', 'stat/alluser'):
        for client in api.get_api_data(endpoint):
            client_ip = client.get('ip') or client.get('last_ip')
            client_mac = client.get('mac')
            if isinstance(client_ip, str) and client_ip == ip_address and isinstance(client_mac, str):
                return client_mac.lower()
    return None
