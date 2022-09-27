import typing
import asyncio
from abc import ABC, abstractmethod
from ..bus.client import AcquisitionBusClient


class BaseControl:
    CONTROL_TYPE: str = None

    class BusClient(AcquisitionBusClient):
        def __init__(self, control: "BaseControl", reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
            super().__init__(control.CONTROL_TYPE, reader, writer)
            self.control = control

        async def incoming_message(self, source: str, record: str, message: typing.Any) -> None:
            await self.control.bus_message(source, record, message)

    def __init__(self):
        self.bus: typing.Optional[BaseControl.BusClient] = None

    async def initialize(self):
        pass

    async def finish(self):
        pass

    async def bus_message(self, source: str, record: str, message: typing.Any) -> None:
        pass

    @abstractmethod
    async def run(self):
        pass