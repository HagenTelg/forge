import typing
import ipaddress
from forge.crypto import PublicKey
from forge.vis.access import wildcard_match_level
from . import CONFIGURATION
from .storage import DashboardInterface


_match_access = wildcard_match_level

_ADDRESS_AUTH: typing.List[typing.Tuple[typing.Union[ipaddress.IPv4Network, ipaddress.IPv6Network], str, str]] = list()
for config in CONFIGURATION.get('DASHBOARD.ACCESS.STATIC', []):
    station = config.get('station').lower()
    code = config.get('code')
    if not code:
        code = config.get('mode')
    code = code.lower()
    if isinstance(config["ip"], str):
        addr = ipaddress.ip_network(config["ip"])
        _ADDRESS_AUTH.append((addr, station, code))
    else:
        for add in config["ip"]:
            addr = ipaddress.ip_network(add)
            _ADDRESS_AUTH.append((addr, station, code))


def check_address(address: typing.Union[ipaddress.IPv4Address, ipaddress.IPv6Address],
                  station: typing.Optional[str], entry_code: str) -> bool:
    for net, access_station, access_code in _ADDRESS_AUTH:
        if address not in net:
            continue
        if access_station != station and access_station != '*':
            continue
        if not _match_access(access_code, entry_code):
            continue
        return True
    return False


async def check_key(db: DashboardInterface, public_key: PublicKey,
                    station: typing.Optional[str], entry_code: str) -> bool:
    return await db.check_access_key(public_key, station, entry_code)


async def check_bearer(db: DashboardInterface, bearer_token: str,
                       station: typing.Optional[str], entry_code: str) -> bool:
    return await db.check_access_bearer(bearer_token, station, entry_code)

