import sys
import unifi_api as api

def move_user(mac: str, target_group_name: str) -> None:
    # 1. Resolve the Group Name to an ID
    group_id = api.get_group_id_by_name(target_group_name)

    if not group_id and target_group_name.lower() != "default":
        print(f"❌ Error: Group '{target_group_name}' not found on the UniFi controller.")
        return

    # UniFi 'Default' is usually represented by an empty string or None
    if target_group_name.lower() == "default":
        group_id = ""

    # 2. Find the active client
    clients = api.get_api_data('stat/sta')
    target = next((c for c in clients if c.get('mac') == mac.lower()), None)

    if not target:
        print(f"❌ Error: Could not find active client: {mac}")
        return

    user_id = target.get('_id')
    name = target.get('name') or mac

    # 3. Execute the move
    print(f"🔄 Moving {name} to group '{target_group_name}'...")
    if api.set_user_group(user_id, group_id):
        print(f"✅ Success: {name} is now in the {target_group_name} group.")
    else:
        print(f"❌ Failed to update {name}.")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python3 move_user.py <MAC_ADDRESS> <GROUP_NAME>")
        print("Example: python3 move_user.py 00:11:22:33:44:55 Slow")
    else:
        move_user(sys.argv[1], sys.argv[2])
