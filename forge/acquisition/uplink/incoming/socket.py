import typing
import asyncio
import logging
import struct
import io
from starlette.websockets import WebSocket
from forge.authsocket import WebsocketBinary as AuthSocket
from forge.acquisition.bus.protocol import PersistenceLevel
from .protocol import PROTOCOL_VERSION, UplinkSerializer


_LOGGER = logging.getLogger(__name__)


class BusSocket(AuthSocket):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.has_instantaneous_data: bool = None
        self.websocket: WebSocket = None
        self.bus_serializer: UplinkSerializer = None

    async def handshake(self, websocket: WebSocket, data: bytes) -> bool:
        if len(data) < 2:
            _LOGGER.debug(f"Invalid handshake data for {self.display_id}")
            return False
        version, has_instant = struct.unpack('<BB', data[:2])
        if version != PROTOCOL_VERSION:
            _LOGGER.debug(f"Incompatible protocol version for {self.display_id}")
            return False
        self.has_instantaneous_data = (has_instant != 0)

        self.websocket = websocket
        self.bus_serializer = UplinkSerializer()
        return True

    async def websocket_data(self, websocket: WebSocket, data: bytes) -> None:
        reader = io.BytesIO(data)
        source = await self.bus_serializer.deserialize_string_lookup(reader)
        record = await self.bus_serializer.deserialize_string_lookup(reader)
        message = await self.bus_serializer.deserialize_message(reader)
        await self.incoming_message(source, record, message)

    async def send_message(self, level: PersistenceLevel, record: str, message: typing.Any) -> None:
        if not self.websocket:
            return

        data = io.BytesIO()
        data.write(struct.pack('<B', level.value))
        self.bus_serializer.serialize_string_lookup(data, record)
        self.bus_serializer.serialize_message(data, message)
        await self.websocket.send_bytes(bytes(data.getbuffer()))
        data.close()

    async def on_disconnect(self, websocket: WebSocket, close_code):
        self.websocket = None
        await super().on_disconnect(websocket, close_code)

    async def incoming_message(self, source: str, record: str, message: typing.Any) -> None:
        pass
