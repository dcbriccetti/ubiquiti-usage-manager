from dataclasses import dataclass


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
PLUS_ADMINS = {'plus_admin_1', 'plus_admin_2', 'plus_admin_3'}
SELF_SERVICE_SPEED_LIMIT_ENABLED = False
