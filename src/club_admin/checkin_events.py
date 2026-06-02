'''Lightweight check-in refresh notifications shared by in-process requests.'''

from threading import Condition


_condition = Condition()
_version = 0


def notify_checkins_changed() -> None:
    '''Wake check-in report streams in this process after check-ins change.'''
    global _version
    with _condition:
        _version += 1
        _condition.notify_all()


def current_checkins_version() -> int:
    '''Return the current in-process check-in data version.'''
    with _condition:
        return _version


def wait_for_checkins_change(last_seen_version: int, timeout_seconds: float) -> int:
    '''Wait until check-ins change or timeout expires, then return the version.'''
    with _condition:
        _condition.wait_for(
            lambda: _version != last_seen_version,
            timeout=max(0.0, timeout_seconds),
        )
        return _version
