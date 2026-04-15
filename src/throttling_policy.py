'Throttle policy helpers for selecting speed-limit targets from usage.'

from config import THROTTLING_LEVELS


def target_profile_name_for_usage(vlan_id: str, day_total_mb: float, throttleable_vlan_ids: list[str]) -> str | None:
    'Return the configured target profile name for this usage level, or None when no throttle should apply.'
    if vlan_id not in throttleable_vlan_ids:
        return None

    target_name: str | None = None
    for level in THROTTLING_LEVELS:
        if day_total_mb >= level.threshold_mb:
            target_name = level.profile_name

    return target_name
