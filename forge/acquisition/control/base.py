import typing
import asyncio
from enum import Enum
from abc import ABC, abstractmethod
from ..bus.client import AcquisitionBusClient, PersistenceLevel


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

    def log(self, message: str, auxiliary: typing.Dict[str, typing.Any] = None,
            is_error: bool = False) -> None:
        message: typing.Dict[str, typing.Any] = {
            'message': message,
            'type': ('error' if is_error else 'info'),
        }
        if auxiliary:
            message['auxiliary'] = auxiliary
        self.bus.send_message(PersistenceLevel.DATA, 'event_log', message)

    @abstractmethod
    async def run(self):
        pass