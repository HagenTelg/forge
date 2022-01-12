import typing
import asyncio
import logging
import struct
import starlette.status
from starlette.websockets import WebSocket
from starlette.exceptions import HTTPException
from forge.const import STATIONS
from forge.authsocket import WebsocketBinary as AuthSocket, PublicKey, key_to_bytes, public_key_display
from .tunnel.protocol import ServerConnectionType, InitiateConnectionStatus, FromRemotePacketType, ToRemotePacketType
from . import CONFIGURATION


_LOGGER = logging.getLogger(__name__)


class TunnelSocket(AuthSocket):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.server_reader: asyncio.StreamReader = None
        self.server_writer: asyncio.StreamWriter = None
        self._server_read_task: typing.Optional[asyncio.Task] = None

    async def handshake(self, websocket: WebSocket, data: bytes) -> bool:
        self.server_reader, self.server_writer = await asyncio.open_unix_connection(
            CONFIGURATION.get('TELEMETRY.TUNNEL.SOCKET', '/run/forge-telemetry-tunnel.socket'))

        self.server_writer.write(struct.pack('<B', ServerConnectionType.TO_REMOTE.value))
        self.server_writer.write(key_to_bytes(self.public_key))
        await self.server_writer.drain()

        _LOGGER.info(f"Tunnel connected to {self.display_id}")

        self._server_read_task = asyncio.ensure_future(self.read_from_server(websocket))
        return True

    async def read_from_server(self, websocket: WebSocket) -> None:
        try:
            while True:
                packet_type = await self.server_reader.read(1)
                if not packet_type:
                    try:
                        await websocket.close()
                    except:
                        pass
                    return

                packet_type = ToRemotePacketType(struct.unpack('<B', packet_type)[0])
                if packet_type == ToRemotePacketType.DATA:
                    connection_id, data_length = struct.unpack('<HH', await self.server_reader.readexactly(4))
                    packet_data = await self.server_reader.readexactly(data_length)

                    header = struct.pack('<BH', packet_type.value, connection_id)
                    await websocket.send_bytes(header + packet_data)
                elif packet_type == ToRemotePacketType.SSH_CONNECTION_OPEN or packet_type == ToRemotePacketType.CONNECTION_CLOSE:
                    connection_id = struct.unpack('<H', await self.server_reader.readexactly(2))[0]
                    await websocket.send_bytes(struct.pack('<BH', packet_type.value, connection_id))
                else:
                    raise ValueError("Invalid packet type")
        finally:
            if self.server_writer:
                try:
                    self.server_writer.close()
                except OSError:
                    pass
                self.server_writer = None
            try:
                await websocket.close()
            except:
                pass

    async def on_disconnect(self, websocket: WebSocket, close_code):
        if self._server_read_task:
            t = self._server_read_task
            self._server_read_task = None
            try:
                t.cancel()
            except:
                pass
            try:
                await t
            except asyncio.CancelledError:
                pass
        if self.server_writer:
            try:
                self.server_writer.close()
            except OSError:
                pass
            self.server_writer = None
        return await super().on_disconnect(websocket, close_code)

    async def websocket_data(self, websocket: WebSocket, data: bytes) -> None:
        packet_type = FromRemotePacketType(struct.unpack('<B', data[:1])[0])

        if packet_type == FromRemotePacketType.DATA:
            data_length = len(data) - 3
            if data_length <= 0 or data_length > 0xFFFF:
                raise ValueError(f"Data packet length ({data_length}) invalid")
            connection_id = struct.unpack('<H', data[1:3])[0]

            self.server_writer.write(struct.pack('<BHH', packet_type.value, connection_id,  data_length))
            self.server_writer.write(data[3:])
            await self.server_writer.drain()
        elif packet_type == FromRemotePacketType.CONNECTION_CLOSED:
            if len(data) != 3:
                raise ValueError("Invalid close packet")

            self.server_writer.write(data)
            await self.server_writer.drain()
        elif packet_type == FromRemotePacketType.CONNECTION_OPEN:
            if len(data) != 3:
                raise ValueError("Invalid open packet")

            self.server_writer.write(data)
            await self.server_writer.drain()
        else:
            raise ValueError(f"Invalid packet type {packet_type}")


class ConnectionSocket(AuthSocket):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.server_reader: asyncio.StreamReader = None
        self.server_writer: asyncio.StreamWriter = None
        self._server_read_task: typing.Optional[asyncio.Task] = None

    async def complete_handshake(self, websocket: WebSocket, target: PublicKey) -> bool:
        self.display_id = f"{self.display_id} to {public_key_display(target)}"

        self.server_reader, self.server_writer = await asyncio.open_unix_connection(
            CONFIGURATION.get('TELEMETRY.TUNNEL.SOCKET', '/run/forge-telemetry-tunnel.socket'))

        _LOGGER.debug(f"Starting forwarding for {self.display_id}")

        self.server_writer.write(struct.pack('<B', ServerConnectionType.INITIATE_CONNECTION.value))
        self.server_writer.write(key_to_bytes(target))
        await self.server_writer.drain()

        try:
            status = await self.server_reader.readexactly(1)
        except:
            _LOGGER.debug(f"Error getting status for {self.display_id}", exc_info=True)
            return False

        status = InitiateConnectionStatus(struct.unpack('<B', status)[0])
        if status != InitiateConnectionStatus.OK:
            _LOGGER.debug(f"Error forwarding {self.display_id} ({status.value})")
            await websocket.send_bytes(struct.pack('<B', status.value))
            return False

        await websocket.send_bytes(struct.pack('<B', InitiateConnectionStatus.OK.value))

        self._server_read_task = asyncio.ensure_future(self.read_from_server(websocket))
        return True

    async def handshake(self, websocket: WebSocket, data: bytes) -> bool:
        target = PublicKey.from_public_bytes(data[:32])
        if not await websocket.scope['telemetry'].tunnel_connection_authorized(self.public_key, target):
            _LOGGER.debug(f"Rejected forwarding for {self.display_id} to {public_key_display(target)}")
            await websocket.send_bytes(struct.pack('<B', InitiateConnectionStatus.PERMISSION_DENIED.value))
            return False
        return await self.complete_handshake(websocket, target)

    async def read_from_server(self, websocket: WebSocket) -> None:
        try:
            while True:
                data = await self.server_reader.read(4096)
                if not data:
                    return
                await websocket.send_bytes(data)
        finally:
            if self.server_writer:
                try:
                    self.server_writer.close()
                except OSError:
                    pass
                self.server_writer = None
            try:
                await websocket.close()
            except:
                pass

    async def on_disconnect(self, websocket: WebSocket, close_code):
        if self._server_read_task:
            t = self._server_read_task
            self._server_read_task = None
            try:
                t.cancel()
            except:
                pass
            try:
                await t
            except asyncio.CancelledError:
                pass
        if self.server_writer:
            try:
                self.server_writer.close()
            except OSError:
                pass
            self.server_writer = None
        return await super().on_disconnect(websocket, close_code)

    async def websocket_data(self, websocket: WebSocket, data: bytes) -> None:
        self.server_writer.write(data)
        await self.server_writer.drain()


class StationConnectionSocket(ConnectionSocket):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.station: str = None

    async def on_connect(self, websocket: WebSocket):
        self.station = websocket.path_params['station'].lower()
        if self.station not in STATIONS:
            raise HTTPException(starlette.status.HTTP_404_NOT_FOUND, detail="Invalid station")
        await super().on_connect(websocket)

    async def handshake(self, websocket: WebSocket, data: bytes) -> bool:
        target = await websocket.scope['telemetry'].tunnel_station_target(self.public_key, self.station)
        if not target:
            _LOGGER.debug(f"Rejected forwarding for {self.display_id} to {self.station}")
            await websocket.send_bytes(struct.pack('<B', InitiateConnectionStatus.PERMISSION_DENIED.value))
            return False
        return await self.complete_handshake(websocket, target)
