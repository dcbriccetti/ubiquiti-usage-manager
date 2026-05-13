'''Best-effort reverse-DNS labels for flow endpoint display.'''

from concurrent.futures import Future, ThreadPoolExecutor, TimeoutError
from datetime import datetime, timedelta
import ipaddress
import socket
from threading import Lock

import config as cfg


POSITIVE_TTL = timedelta(hours=12)
NEGATIVE_TTL = timedelta(hours=1)
_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix='reverse-dns')
_cache_lock = Lock()
_cache: dict[str, tuple[datetime, str | None]] = {}
_pending: dict[str, Future[str | None]] = {}
HOSTNAME_CATEGORY_LABELS = (
    (('amazonaws.com',), 'Amazon cloud host'),
    (('cloudfront.net',), 'Amazon CDN host'),
    (('akamaitechnologies.com', 'akamai.net', 'akadns.net'), 'Akamai CDN host'),
    (('1e100.net', 'googleusercontent.com', 'googlevideo.com'), 'Google host'),
    (('cloudflare.com', 'cloudflare.net'), 'Cloudflare host'),
    (('fastly.net',), 'Fastly CDN host'),
    (('azure.com', 'azureedge.net', 'trafficmanager.net', 'windows.net'), 'Microsoft cloud host'),
    (('microsoft.com', 'msn.com', 'office.com'), 'Microsoft host'),
    (('apple.com', 'icloud.com'), 'Apple host'),
    (('facebook.com', 'fbcdn.net', 'instagram.com', 'whatsapp.net', 'messenger.com'), 'Meta host'),
    (('nflxvideo.net', 'netflix.com'), 'Streaming service host'),
)
IP_NETWORK_CATEGORY_LABELS = (
    (ipaddress.ip_network('17.0.0.0/8'), 'Apple host'),
    (ipaddress.ip_network('1.0.0.0/24'), 'Cloudflare DNS'),
    (ipaddress.ip_network('1.1.1.0/24'), 'Cloudflare DNS'),
    (ipaddress.ip_network('8.8.4.0/24'), 'Google DNS'),
    (ipaddress.ip_network('8.8.8.0/24'), 'Google DNS'),
    (ipaddress.ip_network('9.9.9.0/24'), 'Quad9 DNS'),
    (ipaddress.ip_network('149.112.112.0/24'), 'Quad9 DNS'),
    (ipaddress.ip_network('208.67.220.0/24'), 'Cisco DNS'),
    (ipaddress.ip_network('208.67.222.0/24'), 'Cisco DNS'),
)


def shorten_hostname(hostname: str) -> str:
    'Return a compact hostname label for display.'
    parts = [part for part in hostname.rstrip('.').split('.') if part]
    if len(parts) <= 3:
        return '.'.join(parts)
    if len(parts[-3]) > 12:
        return '.'.join(parts[-2:])
    return '.'.join(parts[-3:])


def safe_hostname_label(hostname: str) -> str | None:
    'Return a report-safe hostname label, or None to keep the IP visible.'
    normalized = shorten_hostname(hostname).lower()
    for suffixes, label in HOSTNAME_CATEGORY_LABELS:
        if any(normalized == suffix or normalized.endswith(f'.{suffix}') for suffix in suffixes):
            return label
    return None


def safe_ip_label(ip_address: str) -> str | None:
    'Return a report-safe provider label for known public IP ranges.'
    try:
        parsed_ip = ipaddress.ip_address(ip_address)
    except ValueError:
        return None

    for network, label in IP_NETWORK_CATEGORY_LABELS:
        if parsed_ip in network:
            return label
    return None


def _lookup_hostname(ip_address: str) -> str | None:
    try:
        hostname, _, _ = socket.gethostbyaddr(ip_address)
    except (OSError, socket.herror):
        return None
    return hostname.rstrip('.') or None


def _cache_result(ip_address: str, hostname: str | None) -> None:
    ttl = POSITIVE_TTL if hostname else NEGATIVE_TTL
    with _cache_lock:
        _cache[ip_address] = (datetime.now() + ttl, hostname)
        _pending.pop(ip_address, None)


def _valid_public_ip(ip_address: str) -> bool:
    try:
        parsed = ipaddress.ip_address(ip_address)
    except ValueError:
        return False
    return not (
        parsed.is_private
        or parsed.is_loopback
        or parsed.is_link_local
        or parsed.is_multicast
        or parsed.is_unspecified
    )


def _schedule_lookup(ip_address: str) -> Future[str | None] | None:
    if not _valid_public_ip(ip_address):
        return None

    with _cache_lock:
        cached = _cache.get(ip_address)
        if cached and cached[0] > datetime.now():
            return None
        pending = _pending.get(ip_address)
        if pending:
            return pending

        future = _executor.submit(_lookup_hostname, ip_address)
        _pending[ip_address] = future

    def remember_result(completed: Future[str | None]) -> None:
        try:
            hostname = completed.result()
        except Exception:
            hostname = None
        _cache_result(ip_address, hostname)

    future.add_done_callback(remember_result)
    return future


def resolve_host_labels(ip_addresses: list[str], wait: bool = True) -> dict[str, str]:
    'Return cached reverse-DNS labels and queue missing lookups.'
    timeout_seconds = float(getattr(cfg, 'REVERSE_DNS_LOOKUP_TIMEOUT_SECONDS', 0.05))
    labels: dict[str, str] = {}
    futures: list[tuple[str, Future[str | None]]] = []
    now = datetime.now()
    unique_ips = list(dict.fromkeys(ip for ip in ip_addresses if ip))
    for ip_address in unique_ips:
        if label := safe_ip_label(ip_address):
            labels[ip_address] = label

    if not bool(getattr(cfg, 'ENABLE_REVERSE_DNS', True)):
        return labels

    with _cache_lock:
        for ip_address in unique_ips:
            if ip_address in labels:
                continue
            cached = _cache.get(ip_address)
            if cached and cached[0] > now and cached[1]:
                if label := safe_hostname_label(cached[1]):
                    labels[ip_address] = label

    for ip_address in unique_ips:
        if ip_address in labels:
            continue
        future = _schedule_lookup(ip_address)
        if future is not None:
            futures.append((ip_address, future))

    if not wait:
        return labels

    deadline = datetime.now() + timedelta(seconds=timeout_seconds)
    for ip_address, future in futures:
        remaining = (deadline - datetime.now()).total_seconds()
        if remaining <= 0:
            break
        try:
            hostname = future.result(timeout=remaining)
        except TimeoutError:
            continue
        except Exception:
            continue
        if hostname and (label := safe_hostname_label(hostname)):
            labels[ip_address] = label
    return labels
