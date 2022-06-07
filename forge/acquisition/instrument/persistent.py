import typing
import asyncio
from .base import BasePersistentInterface


class PersistentInterface(BasePersistentInterface):
    def load(self, name: str) -> typing.Tuple[typing.Any, typing.Optional[float]]:
        return None, None

    async def save(self, name: str, value: typing.Any, effective_time: typing.Optional[float]) -> None:
        raise NotImplementedError
