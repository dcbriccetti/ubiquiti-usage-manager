from dataclasses import dataclass


@dataclass(frozen=True)
class ThrottleLevel:
    threshold_mb: int
    profile_name: str


IGNORE_BELOW_KB = 10
THROTTLEABLE_VLAN_NAMES = ['Basic']
THROTTLING_LEVELS = [
    ThrottleLevel(250, 'Half speed'),
    ThrottleLevel(500, 'Quarter speed'),
    ThrottleLevel(1_000, 'Eighth speed'),
]
SAFE_MODE = False
