'Throttle policy helpers for selecting speed-limit targets from usage.'

import config as cfg


def get_throttling_policy() -> list[cfg.ThrottleLevel]:
    'Return throttling levels sorted by usage threshold.'
    return sorted(cfg.THROTTLING_LEVELS, key=lambda level: level.threshold_mb)


def target_profile_name_for_usage(
    vlan_id: str,
    day_total_mb: float,
    throttleable_vlan_ids: list[str],
    policy: list[cfg.ThrottleLevel],
) -> str | None:
    'Return the target profile name for this usage level, or None when no throttle should apply.'
    if vlan_id not in throttleable_vlan_ids:
        return None

    target_name: str | None = None
    for level in policy:
        if day_total_mb >= level.threshold_mb:
            target_name = level.profile_name

    return target_name
