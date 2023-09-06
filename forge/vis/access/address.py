import typing
import logging
import ipaddress
from . import BaseAccessLayer, BaseAccessController, Request
from forge.vis.util import name_to_initials
from forge.const import DISPLAY_STATIONS


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
        self.authenticated = config.get("authenticated", True)
        self.name = config.get("name", "")
        self.initials = config.get("initials", name_to_initials(self.name))

    async def authenticate(self, request: Request) -> typing.Optional[BaseAccessLayer]:
        try:
            origin = ipaddress.ip_address(request.client.host)
        except ValueError:
            return None
        for net in self.networks:
            if origin not in net:
                continue
            _LOGGER.debug(f"Using address authentication for {origin}")
            return AccessLayer(self, origin)
        return None


class AccessLayer(BaseAccessLayer):
    def __init__(self, controller: AccessController, origin: typing.Union[ipaddress.IPv4Address, ipaddress.IPv6Address]):
        self.controller = controller
        self.origin = origin

        self.username = controller.name
        if len(self.username) == 0:
            self.username = controller.initials
        if len(self.username) == 0:
            self.username = f"{origin}:{controller.station}:{controller.mode}"

    def is_authenticated(self, lower: typing.Sequence[BaseAccessLayer]) -> bool:
        if self.controller.authenticated:
            return True
        return super().is_authenticated(lower)

    def initials(self, _lower: typing.Sequence[BaseAccessLayer]) -> str:
        return self.controller.initials

    def display_id(self, _lower: typing.Sequence[BaseAccessLayer]) -> str:
        return str(self.origin)

    def display_name(self, _lower: typing.Sequence[BaseAccessLayer]) -> str:
        return self.username

    def visible_stations(self, lower: typing.Sequence[BaseAccessLayer]) -> typing.Set[str]:
        if self.controller.station == "*":
            stations = set(DISPLAY_STATIONS)
        else:
            stations = {self.controller.station}
        if lower:
            stations |= lower[0].visible_stations(lower[1:])
        return stations

    def allow_station(self, station: str, lower: typing.Sequence[BaseAccessLayer]) -> bool:
        if self.controller.station == "*":
            return True
        if self.controller.station == station:
            return True
        return lower and lower[0].allow_station(station, lower[1:])

    def allow_mode(self, station: str, mode: str, write: bool, lower: typing.Sequence[BaseAccessLayer]) -> bool:
        if self.allow_station(station, lower) and self.matches_mode(self.controller.mode, mode):
            if not write or self.controller.write:
                return True
        return lower and lower[0].allow_mode(station, mode, write, lower[1:])

    def allow_global(self, mode: str, write: bool, lower: typing.Sequence[BaseAccessLayer]) -> bool:
        if self.matches_mode(self.controller.mode, mode):
            if not write or self.controller.write:
                return True
        return lower and lower[0].allow_global(mode, write, lower[1:])
