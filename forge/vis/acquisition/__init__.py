import typing
from abc import ABC, abstractmethod
from starlette.requests import Request
from starlette.responses import Response


class Display(ABC):
    @abstractmethod
    async def __call__(self, request: Request, **kwargs) -> Response:
        pass


class SummaryItem(ABC):
    @abstractmethod
    async def __call__(self, request: Request, **kwargs) -> Response:
        pass


class Translator(ABC):
    pass