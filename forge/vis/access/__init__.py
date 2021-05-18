import typing
from abc import ABC, abstractmethod
from starlette.requests import Request
from starlette.authentication import BaseUser


class BaseAccessUser(BaseUser, ABC):
    def identity(self) -> str:
        pass

    @property
    def initials(self) -> str:
        raise NotImplementedError

    @property
    def display_id(self) -> str:
        raise NotImplementedError

    @property
    def display_name(self) -> str:
        return self.display_id

    @staticmethod
    def matches_mode(access: str, mode: str) -> bool:
        if len(access) == 0:
            return False

        access_parts = access.split('-')
        mode_parts = mode.split('-')
        if len(access_parts) != len(mode_parts):
            if access_parts[-1] != "*":
                return False

        for i in range(min(len(access_parts), len(mode_parts))):
            check = access_parts[i]
            if check == "*":
                continue
            if len(check) == 0:
                return False
            if access_parts[i] != mode_parts[i]:
                return False
        return True

    @property
    def can_request_access(self) -> bool:
        return False

    @property
    def visible_stations(self) -> typing.List[str]:
        raise NotImplementedError

    @abstractmethod
    def allow_station(self, station: str) -> bool:
        pass

    @abstractmethod
    def allow_mode(self, station: str, mode: str, write=False) -> bool:
        pass


class BaseAccessController(ABC):
    @abstractmethod
    async def authenticate(self, request: Request) -> typing.Optional[BaseAccessUser]:
        pass

