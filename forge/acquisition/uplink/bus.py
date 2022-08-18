import typing
import asyncio
import logging
from abc import ABC, abstractmethod
from forge.acquisition.bus.protocol import PersistenceLevel


_LOGGER = logging.getLogger(__name__)


class BusInterface:
    @abstractmethod
    async def send_message(self, level: PersistenceLevel, record: str, message: typing.Any) -> None:
        pass
