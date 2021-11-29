import typing
import asyncio
import logging
import ipaddress
import time
from enum import Enum
from base64 import b64decode, b64encode
from secrets import token_bytes
from starlette.endpoints import WebSocketEndpoint
from starlette.websockets import WebSocket
from cryptography.exceptions import InvalidSignature
from . import PublicKey
from .storage import Interface as TelemetryInterface

_LOGGER = logging.getLogger(__name__)


class _HandshakeSocket(WebSocketEndpoint):
    encoding = 'json'

    class _ConnectionState(Enum):
        CLOSED = 0
        RECEIVE_PUBLIC_KEY = 1
        RECEIVE_CHALLENGE_RESPONSE = 2
        ACCEPTED = 3

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.telemetry: TelemetryInterface = None

        self.public_key: typing.Optional[PublicKey] = None
        self._challenge_token: typing.Optional[bytes] = None
        self._state = self._ConnectionState.RECEIVE_PUBLIC_KEY
        self._handshake_timeout_task: typing.Optional[asyncio.Task] = None

    def _receive_key(self, data: typing.Dict[str, typing.Any]) -> bool:
        try:
            self.public_key = PublicKey.from_public_bytes(b64decode(data['public_key']))
        except:
            return False
        return True

    def _verify_challenge(self, data: typing.Dict[str, typing.Any]) -> bool:
        try:
            signature = b64decode(data['signature'])
        except:
            return False
        try:
            self.public_key.verify(signature, self._challenge_token)
        except InvalidSignature:
            return False
        self._challenge_token = None
        return True

    async def _reject_connection(self, websocket: WebSocket) -> None:
        self._state = self._ConnectionState.CLOSED
        self.public_key = None
        self._challenge_token = None
        await self._stop_handshake_timeout()
        try:
            await websocket.close()
        except:
            pass

    async def _stop_handshake_timeout(self):
        if not self._handshake_timeout_task:
            return
        t = self._handshake_timeout_task
        self._handshake_timeout_task = None
        try:
            t.cancel()
            await t
        except:
            pass

    async def on_connect(self, websocket: WebSocket):
        async def timeout():
            await asyncio.sleep(30)
            _LOGGER.debug("Handshake timeout")
            await websocket.close()

        self._handshake_timeout_task = asyncio.ensure_future(timeout())
        await super().on_connect(websocket)

    async def on_receive(self, websocket: WebSocket, data: typing.Dict[str, typing.Any]):
        if self._state == self._ConnectionState.CLOSED:
            return
        elif self._state == self._ConnectionState.RECEIVE_PUBLIC_KEY:
            if not self._receive_key(data):
                return await self._reject_connection(websocket)

            self._challenge_token = token_bytes(32)
            self._state = self._ConnectionState.RECEIVE_CHALLENGE_RESPONSE
            await websocket.send_json({
                'token': b64encode(self._challenge_token).decode('ascii'),
            })
            return
        elif self._state == self._ConnectionState.RECEIVE_CHALLENGE_RESPONSE:
            if not self._verify_challenge(data):
                return await self._reject_connection(websocket)

            self._state = self._ConnectionState.ACCEPTED
            self.telemetry = websocket.scope['telemetry']

            if not await self.handshake(websocket, data):
                await self._reject_connection(websocket)
                return

            await self._stop_handshake_timeout()
            return

        await self.websocket_data(websocket, data)

    async def on_disconnect(self, websocket: WebSocket, close_code):
        self._state = self._ConnectionState.CLOSED
        await self._stop_handshake_timeout()
        if self._ping_task:
            try:
                self._ping_task.cancel()
                await self._ping_task
            except:
                pass
            self._ping_task = None

    async def handshake(self, websocket: WebSocket, data: typing.Dict[str, typing.Any]) -> bool:
        return True

    async def websocket_data(self, websocket: WebSocket, data: typing.Dict[str, typing.Any]) -> None:
        pass


class TelemetrySocket(_HandshakeSocket):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.station: typing.Optional[str] = None
        self.origin: typing.Optional[str] = None
        self._ping_task: typing.Optional[asyncio.Task] = None

    async def handshake(self, websocket: WebSocket, data: typing.Dict[str, typing.Any]) -> bool:
        try:
            self.origin = str(ipaddress.ip_address(websocket.client.host))
        except ValueError:
            self.origin = None

        self.station = data.get('station')
        if self.station is not None:
            self.station = str(self.station).strip()
            self.station = self.station[:32].lower()
            if self.station == '*':
                return False

        await self.telemetry.ping_host(self.public_key, self.origin, self.station)
        self._ping_task = asyncio.ensure_future(self._ping())
        return True

    async def websocket_data(self, websocket: WebSocket, data: typing.Dict[str, typing.Any]) -> None:
        request = data['request']
        if request == 'update':
            await self.telemetry.connected_update(self.public_key, self.origin, self.station,
                                                  data.get('telemetry', {}))
        elif request == 'partial':
            await self.telemetry.connected_update(self.public_key, self.origin, self.station,
                                                  data.get('telemetry', {}), partial=True)
        elif request == 'log':
            log_type = data['log']
            log_events = data['events']
            if log_type == 'kernel':
                await self.telemetry.append_log_kernel(self.public_key, self.origin, self.station,
                                                       log_events)
            elif log_type == 'acquisition':
                await self.telemetry.append_log_acquisition(self.public_key, self.origin, self.station,
                                                            log_events)
        elif request == 'get_time':
            await websocket.send_json({
                'response': 'server_time',
                'server_time': round(time.time()),
            })

    async def _ping(self):
        while True:
            await asyncio.sleep(60)
            await self.telemetry.ping_host(self.public_key, self.origin, self.station)
