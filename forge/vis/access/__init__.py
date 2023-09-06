import typing
from abc import ABC, abstractmethod
from starlette.requests import Request
from starlette.authentication import BaseUser


def wildcard_match_level(access: str, mode: str) -> typing.Optional[int]:
    if len(access) == 0:
        return None

    access_parts = access.split('-')
    mode_parts = mode.split('-')
    if len(access_parts) != len(mode_parts):
        if access_parts[-1] != "*":
            return None

    level = 0
    for i in range(min(len(access_parts), len(mode_parts))):
        check = access_parts[i]
        if check == "*":
            continue
        if len(check) == 0:
            return None
        if access_parts[i] != mode_parts[i]:
            return None
        level += 1
    return level


class AccessUser(BaseUser):
    def __init__(self, layers: typing.Sequence["BaseAccessLayer"]):
        self._layers = layers

    def identity(self) -> str:
        pass

    @property
    def is_authenticated(self) -> bool:
        return self._layers[0].is_authenticated(self._layers[1:])

    @property
    def initials(self) -> str:
        return self._layers[0].initials(self._layers[1:])

    @property
    def display_id(self) -> str:
        return self._layers[0].display_id(self._layers[1:])

    @property
    def display_name(self) -> str:
        return self._layers[0].display_name(self._layers[1:])

    @property
    def can_request_access(self) -> bool:
        return self._layers[0].can_request_access(self._layers[1:])

    @property
    def visible_stations(self) -> typing.List[str]:
        stations = list(self._layers[0].visible_stations(self._layers[1:]))
        stations.sort()
        return stations

    def allow_station(self, station: str) -> bool:
        return self._layers[0].allow_station(station, self._layers[1:])

    def allow_mode(self, station: str, mode: str, write=False) -> bool:
        return self._layers[0].allow_mode(station, mode, write, self._layers[1:])

    def allow_global(self, mode: str, write=False) -> bool:
        return self._layers[0].allow_global(mode, write, self._layers[1:])

    def find_layer(self, test: typing.Callable[["BaseAccessLayer"], bool]) -> typing.Optional["BaseAccessLayer"]:
        for l in self._layers:
            if test(l):
                return l
        return None

    def layer_type(self, t: typing.Type["BaseAccessLayer"]) -> typing.Optional["BaseAccessLayer"]:
        return self.find_layer(lambda x: isinstance(x, t))


class BaseAccessLayer(ABC):
    @staticmethod
    def matches_mode(access: str, mode: str) -> bool:
        return wildcard_match_level(access, mode) is not None

    def is_authenticated(self, lower: typing.Sequence["BaseAccessLayer"]) -> bool:
        return lower and lower[0].is_authenticated(lower[1:])

    @abstractmethod
    def initials(self, lower: typing.Sequence["BaseAccessLayer"]) -> str:
        pass

    @abstractmethod
    def display_id(self, lower: typing.Sequence["BaseAccessLayer"]) -> str:
        pass

    def display_name(self, lower: typing.Sequence["BaseAccessLayer"]) -> str:
        return self.display_id(lower)

    def can_request_access(self, lower: typing.Sequence["BaseAccessLayer"]) -> bool:
        return lower and lower[0].can_request_access(lower[1:])

    @abstractmethod
    def visible_stations(self, lower: typing.Sequence["BaseAccessLayer"]) -> typing.Set[str]:
        pass

    @abstractmethod
    def allow_station(self, station: str, lower: typing.Sequence["BaseAccessLayer"]) -> bool:
        pass

    @abstractmethod
    def allow_mode(self, station: str, mode: str, write: bool, lower: typing.Sequence["BaseAccessLayer"]) -> bool:
        pass

    @abstractmethod
    def allow_global(self, mode: str, write: bool, lower: typing.Sequence["BaseAccessLayer"]) -> bool:
        pass


class BaseAccessController(ABC):
    @abstractmethod
    async def authenticate(self, request: Request) -> typing.Optional[BaseAccessLayer]:
        pass

    @property
    def sort_order(self) -> int:
        return 0

