from dataclasses import dataclass
from speedlimit import SpeedLimit

@dataclass(frozen=True, kw_only=True)
class ClientInfo:
    'Normalized client fields derived from UniFi station payloads.'
    unifi_client_id: str
    mac: str
    name: str
    user_id: str | None
    vlan_id: str
    vlan_name: str
    speed_limit: SpeedLimit | None
    ap_name: str
    signal: int
    mb_used_since_connection: float

    @classmethod
    def create(cls, c: dict, speed_limits_by_id: dict[str, SpeedLimit], ap_names_by_mac: dict[str, str]):
        'Build a ClientInfo instance from raw UniFi client data.'
        speed_limit_id: str | None = c.get('usergroup_id')
        speed_limit = speed_limits_by_id.get(speed_limit_id) if speed_limit_id else None
        ap_mac = c.get('ap_mac', '')

        return cls(
            unifi_client_id =c.get('_id', ''),
            mac             = c.get('mac', ''),
            name            = c.get('name') or c.get('hostname') or c.get('dev_name') or c.get('mac') or '',
            user_id         = c.get('1x_identity', ''),
            vlan_id         = c.get('network_id', ''),
            vlan_name       = c.get('network', ''),
            speed_limit     = speed_limit,
            ap_name         = ap_names_by_mac.get(ap_mac, ''),
            signal          = c.get('signal', 0),
            mb_used_since_connection = (c.get('tx_bytes', 0) + c.get('rx_bytes', 0)) / (1000 * 1000)
        )
