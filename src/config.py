from dataclasses import dataclass
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path


@dataclass(frozen=True)
class ThrottleLevel:
    threshold_mb: int
    profile_name: str


@dataclass(frozen=True)
class MonthlyUsageAdjustment:
    month_total_mb: int
    daily_threshold_multiplier: float


IGNORE_BELOW_KB = 10
THROTTLEABLE_VLAN_NAMES = ['Basic']
THROTTLING_LEVELS = [  # Ascending order
    ThrottleLevel(  250, '1/2 speed'),
    ThrottleLevel(  500, '1/4 speed'),
    ThrottleLevel(1_000, '1/8 speed'),
    ThrottleLevel(1_500, '1/16 speed'),
]
# As month-to-date usage grows, daily thresholds are lowered by these multipliers.
MONTHLY_USAGE_ADJUSTMENTS = [  # Ascending order
    MonthlyUsageAdjustment(2_000, 0.75),
    MonthlyUsageAdjustment(4_000, 0.50),
    MonthlyUsageAdjustment(6_000, 0.35),
]

COST_IN_CENTS_PER_GB = 50
PLUS_ADMINS: set[str] = set()
SELF_SERVICE_SPEED_LIMIT_ENABLED = False

# Devices/users whose usage is paid by the organization (for global analytics split).
# Use VLAN names only (not SSID names).
ORGANIZATION_PAID_DEVICE_MACS: set[str] = set()
ORGANIZATION_PAID_USER_IDS: set[str] = set()
ORGANIZATION_PAID_VLAN_NAMES: set[str] = set()


def _apply_local_overrides() -> None:
    'Load uppercase config values from config_local.py when present.'
    local_config_path = Path(__file__).with_name('config_local.py')
    if not local_config_path.exists():
        return

    module_spec = spec_from_file_location('config_local', local_config_path)
    if not module_spec or not module_spec.loader:
        return

    local_module = module_from_spec(module_spec)
    module_spec.loader.exec_module(local_module)
    for name in dir(local_module):
        if name.isupper():
            globals()[name] = getattr(local_module, name)


_apply_local_overrides()
