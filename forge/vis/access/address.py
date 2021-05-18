import typing
import logging
import ipaddress
from . import BaseAccessUser, BaseAccessController, Request
from forge.vis.util import name_to_initials
from forge.const import STATIONS


_LOGGER = logging.getLogger(__name__)


class AccessController(BaseAccessController):
    def __init__(self, config: dict):
        self.networks: typing.List[typing.Union[ipaddress.IPv4Network, ipaddress.IPv6Network]] = list()
        if isinstance(config["ip"], str):
            self.networks.append(ipaddress.ip_network(config["ip"]))
        else:
            for add in config["ip"]:
                self.networks.append(ipaddress.ip_network(add))

        self.station = config.get("station", "").lower()
        self.mode = config.get("mode", "*").lower()
        self.write = config.get("write", False)
        self.name = config.get("name", "")
        self.initials = config.get("initials", name_to_initials(self.name))

    async def authenticate(self, request: Request) -> typing.Optional[BaseAccessUser]:
        try:
            origin = ipaddress.ip_address(request.client.host)
        except ValueError:
            return None
        for net in self.networks:
            if origin not in net:
                continue
            _LOGGER.debug(f"Using address authentication for {origin}")
            return AccessUser(self, origin)
        return None


class AccessUser(BaseAccessUser):
    def __init__(self, controller: AccessController, origin: typing.Union[ipaddress.IPv4Address, ipaddress.IPv6Address]):
        self.controller = controller
        self.origin = origin

        self.username = controller.name
        if len(self.username) == 0:
            self.username = controller.initials
        if len(self.username) == 0:
            self.username = f"{origin}:{controller.station}:{controller.mode}"

    @property
    def is_authenticated(self) -> bool:
        return True

    @property
    def display_name(self) -> str:
        return self.username

    @property
    def initials(self) -> str:
        return self.controller.initials

    @property
    def display_id(self) -> str:
        return str(self.origin)

    @property
    def visible_stations(self) -> typing.List[str]:
        if self.controller.station == "*":
            return sorted(STATIONS)
        return [self.controller.station]

    def allow_station(self, station: str) -> bool:
        if self.controller.station == "*":
            return True
        return self.controller.station == station

    def allow_mode(self, station: str, mode: str, write=False) -> bool:
        if not self.allow_station(station):
            return False
        if not self.matches_mode(self.controller.mode, mode):
            return False
        return not write or self.controller.write
