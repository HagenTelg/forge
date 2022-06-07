import typing
import asyncio
from forge.acquisition import LayeredConfiguration
from .streaming import StreamingContext
from .base import BaseDataOutput, BasePersistentInterface, BaseBusInterface


class TCPContext(StreamingContext):
    def __init__(self, config: LayeredConfiguration, data: BaseDataOutput, bus: BaseBusInterface,
                 persistent: BasePersistentInterface, host: str, port: int, ssl: bool = None):
        super().__init__(config, data, bus, persistent)

        self._host = host
        self._port = port
        self._ssl = ssl

    async def open_stream(self) -> typing.Tuple[typing.Optional[asyncio.StreamReader],
                                                typing.Optional[asyncio.StreamWriter]]:
        return await asyncio.open_connection(host=self._host, port=self._port, ssl=self._ssl)
