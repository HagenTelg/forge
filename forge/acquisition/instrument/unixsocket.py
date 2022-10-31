import typing
import asyncio
from forge.acquisition import LayeredConfiguration
from .streaming import StreamingContext
from .base import BaseDataOutput, BasePersistentInterface, BaseBusInterface


class UnixSocketContext(StreamingContext):
    def __init__(self, config: LayeredConfiguration, data: BaseDataOutput, bus: BaseBusInterface,
                 persistent: BasePersistentInterface, path: str, always_reset: bool = False):
        super().__init__(config, data, bus, persistent)

        self._path = path
        self.always_reset_stream = always_reset

    async def open_stream(self) -> typing.Tuple[typing.Optional[asyncio.StreamReader],
                                                typing.Optional[asyncio.StreamWriter]]:
        return await asyncio.open_unix_connection(path=self._path)
