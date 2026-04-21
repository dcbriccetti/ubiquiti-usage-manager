'Throttle runtime helpers for resolving and applying UniFi speed-limit profiles.'

import logging

import unifi_api as api
from config import THROTTLING_LEVELS
from speedlimit import SpeedLimit

logger = logging.getLogger(__name__)


def build_throttling_levels(speed_limits: list[SpeedLimit]) -> list[tuple[int, SpeedLimit]]:
    'Resolve configured policy profile names to concrete SpeedLimit objects.'
    speed_limits_by_name = {limit.name: limit for limit in speed_limits}

    throttling_levels: list[tuple[int, SpeedLimit]] = []
    for level in THROTTLING_LEVELS:
        limit = speed_limits_by_name.get(level.profile_name)
        if not limit:
            raise ValueError(f"Could not find speed limit named: {level.profile_name}")
        throttling_levels.append((level.threshold_mb, limit))

    return throttling_levels


def get_throttling_limit_ids(throttling_levels: list[tuple[int, SpeedLimit]]) -> set[str]:
    'Return the set of speed-limit IDs considered throttled states.'
    return {limit.id for _, limit in throttling_levels}


def is_speed_limit_throttled(speed_limit: SpeedLimit | None, throttling_limit_ids: set[str]) -> bool:
    'Return True when the given speed limit is one of the configured throttling profiles.'
    return bool(speed_limit and speed_limit.id in throttling_limit_ids)


def enforce_target_limit(client_name: str, unifi_client_id: str, current_limit: SpeedLimit | None, target_limit: SpeedLimit | None, throttling_limit_ids: set[str]) -> tuple[bool, SpeedLimit | None]:
    'Apply target speed limit when needed and return throttled state plus effective limit.'
    effective_limit = current_limit

    if target_limit and (not effective_limit or effective_limit.id != target_limit.id):
        logger.info("Limit reached: throttling client=%s target=%s", client_name, target_limit.name)
        if api.set_user_group(unifi_client_id, target_limit.id):
            effective_limit = target_limit

    is_throttled = is_speed_limit_throttled(effective_limit, throttling_limit_ids)
    return is_throttled, effective_limit


def release_configured_limits(throttling_limit_ids: set[str], context: str) -> None:
    'Release all clients currently assigned to configured throttling profiles.'
    if not throttling_limit_ids:
        return

    logger.info("Releasing throttled clients during context=%s", context)
    api.release_all_from_limits(throttling_limit_ids)
