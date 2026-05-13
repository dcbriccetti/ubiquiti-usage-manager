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
    if not bool(getattr(cfg, 'ENABLE_REVERSE_DNS', True)):
        return {}

    timeout_seconds = float(getattr(cfg, 'REVERSE_DNS_LOOKUP_TIMEOUT_SECONDS', 0.05))
    labels: dict[str, str] = {}
    futures: list[tuple[str, Future[str | None]]] = []
    now = datetime.now()
    unique_ips = list(dict.fromkeys(ip for ip in ip_addresses if ip))
    with _cache_lock:
        for ip_address in unique_ips:
            cached = _cache.get(ip_address)
            if cached and cached[0] > now and cached[1]:
                labels[ip_address] = cached[1]

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
        if hostname:
            labels[ip_address] = hostname
    return labels
