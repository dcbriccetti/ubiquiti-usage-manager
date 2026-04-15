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
    ThrottleLevel(  250, 'Half speed'),
    ThrottleLevel(  500, 'Quarter speed'),
    ThrottleLevel(1_000, 'Eighth speed'),
    ThrottleLevel(1_500, 'Sixteenth speed'),
]
# As month-to-date usage grows, daily thresholds are lowered by these multipliers.
MONTHLY_USAGE_ADJUSTMENTS = [  # Ascending order
    MonthlyUsageAdjustment(2_000, 0.75),
    MonthlyUsageAdjustment(4_000, 0.50),
    MonthlyUsageAdjustment(6_000, 0.35),
]
