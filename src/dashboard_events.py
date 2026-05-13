'''Lightweight dashboard refresh notifications shared by in-process workers.'''

from threading import Condition


_condition = Condition()
_version = 0


def notify_dashboard_data_changed() -> None:
    'Wake dashboard streams in this process after data changes.'
    global _version
    with _condition:
        _version += 1
        _condition.notify_all()


def current_dashboard_data_version() -> int:
    'Return the current in-process dashboard data version.'
    with _condition:
        return _version


def wait_for_dashboard_data_change(last_seen_version: int, timeout_seconds: float) -> int:
    'Wait until dashboard data changes or timeout expires, then return the version.'
    with _condition:
        _condition.wait_for(lambda: _version != last_seen_version, timeout=max(0.0, timeout_seconds))
        return _version
