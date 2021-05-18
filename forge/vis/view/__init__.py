import typing
from abc import ABC, abstractmethod
from starlette.requests import Request
from starlette.responses import Response


class View(ABC):
    @abstractmethod
    async def __call__(self, request: Request, **kwargs) -> Response:
        pass
