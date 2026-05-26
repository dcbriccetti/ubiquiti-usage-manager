'Helpers for mapping an HTTP requester on LAN to a UniFi client MAC address.'

import ipaddress

from flask import Request

import unifi_api as api


def _request_came_from_trusted_proxy(request: Request) -> bool:
    remote_addr = request.remote_addr
    if not remote_addr:
        return False
    try:
        return ipaddress.ip_address(remote_addr).is_loopback
    except ValueError:
        return False


def get_request_ip(request: Request) -> str | None:
    'Return the best client IP candidate from proxy headers or direct connection.'
    if _request_came_from_trusted_proxy(request):
        real_ip = request.headers.get('X-Real-IP', '').strip()
        if real_ip:
            return real_ip

        forwarded_for = request.headers.get('X-Forwarded-For', '')
        if forwarded_for:
            forwarded_ips = [
                ip.strip()
                for ip in forwarded_for.split(',')
                if ip.strip()
            ]
            if forwarded_ips:
                return forwarded_ips[-1]

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
