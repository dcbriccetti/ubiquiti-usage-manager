'Throttle policy helpers for selecting speed-limit targets from usage.'

from config import MONTHLY_USAGE_ADJUSTMENTS, THROTTLING_LEVELS


def get_daily_threshold_multiplier(calendar_month_total_mb: float) -> float:
    'Return a daily-threshold multiplier based on month-to-date usage.'
    multiplier = 1.0
    for adjustment in MONTHLY_USAGE_ADJUSTMENTS:
        if calendar_month_total_mb >= adjustment.month_total_mb:
            multiplier = adjustment.daily_threshold_multiplier
    return multiplier


def target_profile_name_for_usage(
    vlan_id: str,
    day_total_mb: float,
    calendar_month_total_mb: float,
    throttleable_vlan_ids: list[str],
) -> str | None:
    'Return the configured target profile name for this usage level, or None when no throttle should apply.'
    if vlan_id not in throttleable_vlan_ids:
        return None

    multiplier = get_daily_threshold_multiplier(calendar_month_total_mb)
    target_name: str | None = None
    for level in THROTTLING_LEVELS:
        adjusted_threshold = level.threshold_mb * multiplier
        if day_total_mb >= adjusted_threshold:
            target_name = level.profile_name

    return target_name
