from dataclasses import dataclass
from speedlimit import SpeedLimit

@dataclass(frozen=True, kw_only=True)
class ClientInfo:
    'Normalized client fields derived from UniFi station payloads.'
    unifi_client_id: str
    mac: str
    ip_address: str
    name: str
    user_id: str | None
    vlan_id: str
    vlan_name: str
    speed_limit: SpeedLimit | None
    ap_name: str
    signal: int
    mb_used_since_connection: float
    assoc_time_seconds: int | None

    @staticmethod
    def _resolve_ap_name(c: dict, ap_names_by_mac: dict[str, str]) -> str:
        'Resolve best available AP display name from current and fallback fields.'
        ap_mac = c.get('ap_mac', '')
        ap_name = ap_names_by_mac.get(ap_mac, '') if ap_mac else ''
        if not ap_name:
            last_uplink_name = c.get('last_uplink_name')
            if isinstance(last_uplink_name, str):
                ap_name = last_uplink_name
        if not ap_name:
            last_uplink_mac = c.get('last_uplink_mac')
            if isinstance(last_uplink_mac, str):
                ap_name = ap_names_by_mac.get(last_uplink_mac, '')
        if c.get('is_wired'):
            ap_name = ''
        return ap_name

    @classmethod
    def create(cls, c: dict, speed_limits_by_id: dict[str, SpeedLimit], ap_names_by_mac: dict[str, str]):
        'Build a ClientInfo instance from raw UniFi client data.'
        def parse_positive_int(value: object) -> int | None:
            if isinstance(value, bool):
                return None
            if isinstance(value, int):
                return value if value >= 0 else None
            if isinstance(value, float):
                if value < 0:
                    return None
                return int(value)
            if isinstance(value, str):
                text = value.strip()
                if not text:
                    return None
                try:
                    parsed = int(float(text))
                except ValueError:
                    return None
                return parsed if parsed >= 0 else None
            return None

        speed_limit_id: str | None = c.get('usergroup_id')
        speed_limit = speed_limits_by_id.get(speed_limit_id) if speed_limit_id else None
        ap_name = cls._resolve_ap_name(c, ap_names_by_mac)
        user_id = c.get('1x_identity') or c.get('identity') or c.get('last_1x_identity')
        if not user_id:
            last_identities = c.get('last_1x_identities')
            if isinstance(last_identities, list) and last_identities:
                first_identity = last_identities[0]
                user_id = first_identity if isinstance(first_identity, str) else None
        assoc_time_seconds = parse_positive_int(c.get('assoc_time'))
        if assoc_time_seconds is None:
            assoc_time_seconds = parse_positive_int(c.get('uptime'))

        raw_ip = c.get('ip') or c.get('last_ip') or ''
        ip_address = raw_ip if isinstance(raw_ip, str) else ''

        return cls(
            unifi_client_id=c.get('_id', ''),
            mac=c.get('mac', ''),
            ip_address=ip_address,
            name=c.get('name') or c.get('hostname') or c.get('dev_name') or c.get('mac') or '',
            user_id=user_id,
            vlan_id=c.get('network_id', ''),
            vlan_name=c.get('network', ''),
            speed_limit     = speed_limit,
            ap_name=ap_name,
            signal=c.get('signal', 0),
            mb_used_since_connection=(c.get('tx_bytes', 0) + c.get('rx_bytes', 0)) / (1000 * 1000),
            assoc_time_seconds=assoc_time_seconds,
        )
