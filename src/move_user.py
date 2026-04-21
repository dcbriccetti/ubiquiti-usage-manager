import logging
import sys
from typing import Any

import unifi_api as api
from logging_config import configure_logging

logger = logging.getLogger(__name__)

def move_user(mac: str, target_group_name: str) -> None:
    'Move one active client into a specified UniFi group.'
    # 1. Resolve the Group Name to an ID
    group_id = api.get_group_id_by_name(target_group_name)

    if not group_id and target_group_name.lower() != "default":
        logger.error("Group '%s' not found on the UniFi controller", target_group_name)
        return

    # UniFi 'Default' is usually represented by an empty string or None
    if target_group_name.lower() == "default":
        group_id = ""

    # 2. Find the active client
    clients: list[dict[str, Any]] = api.get_api_data('stat/sta')
    target: dict[str, Any] | None = next(
        (c for c in clients if c.get('mac') == mac.lower() and c.get('_id')),
        None
    )

    if not target:
        logger.error("Could not find active client: %s", mac)
        return

    user_id = target.get('_id')
    if not user_id:
        logger.error("Client record for %s is missing _id", mac)
        return

    name = target.get('name') or mac

    # 3. Execute the move
    logger.info("Moving %s to group '%s'", name, target_group_name)
    if api.set_user_group(str(user_id), group_id):
        logger.info("Success: %s is now in group '%s'", name, target_group_name)
    else:
        logger.error("Failed to update %s", name)

if __name__ == "__main__":
    configure_logging()
    if len(sys.argv) < 3:
        logger.info("Usage: python3 move_user.py <MAC_ADDRESS> <GROUP_NAME>")
        logger.info("Example: python3 move_user.py 00:11:22:33:44:55 Slow")
    else:
        move_user(sys.argv[1], sys.argv[2])
