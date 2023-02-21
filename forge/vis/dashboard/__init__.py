import typing
from abc import ABC, abstractmethod
from starlette.requests import Request
from starlette.responses import Response

if typing.TYPE_CHECKING:
    from .entry import Entry
    from .status import Status
    from .email import EmailContents


class Record(ABC):
    @abstractmethod
    async def entry(self, **kwargs) -> typing.Optional["Entry"]:
        pass

    @abstractmethod
    async def status(self, **kwargs) -> typing.Optional["Status"]:
        pass

    @abstractmethod
    async def details(self, request: Request, **kwargs) -> Response:
        pass

    @abstractmethod
    async def badge_json(self, request: Request, **kwargs) -> Response:
        pass

    @abstractmethod
    async def badge_svg(self, request: Request, **kwargs) -> Response:
        pass

    @abstractmethod
    async def email(self, **kwargs) -> typing.Optional["EmailContents"]:
        pass

