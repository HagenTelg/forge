import typing
import asyncio
import logging
import aiohttp
from base64 import b64encode, b64decode
from secrets import token_bytes
from enum import Enum
from starlette.endpoints import WebSocketEndpoint
from starlette.websockets import WebSocket
from .crypto import PublicKey, PrivateKey, InvalidSignature


_LOGGER = logging.getLogger(__name__)


def key_to_bytes(key: typing.Union[PrivateKey, PublicKey]) -> bytes:
    from cryptography.hazmat.primitives.serialization import Encoding, PrivateFormat, PublicFormat, NoEncryption
    if isinstance(key, PrivateKey):
        return key.private_bytes(Encoding.Raw, PrivateFormat.Raw, NoEncryption())
    return key.public_bytes(Encoding.Raw, PublicFormat.Raw)


def public_key_display(key: PublicKey) -> str:
    return b64encode(key_to_bytes(key)).decode('ascii')


class _ConnectionState(Enum):
    CLOSED = 0
    RECEIVE_PUBLIC_KEY = 1
    RECEIVE_CHALLENGE_RESPONSE = 2
    ACCEPTED = 3


class WebsocketBinary(WebSocketEndpoint):
    encoding = 'bytes'
    HANDSHAKE_TIMEOUT = 30.0

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.display_id: str = None

        self.public_key: typing.Optional[PublicKey] = None
        self._challenge_token: typing.Optional[bytes] = None
        self._state = _ConnectionState.RECEIVE_PUBLIC_KEY
        self._handshake_timeout_task: typing.Optional[asyncio.Task] = None

    @classmethod
    async def client_handshake(cls, websocket: "aiohttp.client.ClientWebSocketResponse", key: PrivateKey,
                               extra_data: bytes = None):
        await websocket.send_bytes(key_to_bytes(key.public_key()))
        challenge = await websocket.receive_bytes()
        signature = key.sign(challenge)
        if extra_data:
            signature = signature + extra_data
        await websocket.send_bytes(signature)

    def _receive_key(self, data: bytes) -> bool:
        try:
            self.public_key = PublicKey.from_public_bytes(data)
        except:
            return False
        return True

    def _verify_challenge(self, data: bytes) -> bool:
        try:
            self.public_key.verify(data, self._challenge_token)
        except InvalidSignature:
            return False
        self._challenge_token = None
        return True

    async def _reject_connection(self, websocket: WebSocket) -> None:
        self._state = _ConnectionState.CLOSED
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
            await asyncio.sleep(self.HANDSHAKE_TIMEOUT)
            _LOGGER.debug("Handshake timeout")
            await websocket.close()

        self._handshake_timeout_task = asyncio.ensure_future(timeout())
        await super().on_connect(websocket)

    async def on_receive(self, websocket: WebSocket, data: bytes):
        if self._state == _ConnectionState.CLOSED:
            return
        elif self._state == _ConnectionState.RECEIVE_PUBLIC_KEY:
            self.display_id = websocket.client.host
            if not self._receive_key(data):
                return await self._reject_connection(websocket)

            self._challenge_token = token_bytes(32)
            self._state = _ConnectionState.RECEIVE_CHALLENGE_RESPONSE
            await websocket.send_bytes(self._challenge_token)
            return
        elif self._state == _ConnectionState.RECEIVE_CHALLENGE_RESPONSE:
            if not self._verify_challenge(data[:64]):
                return await self._reject_connection(websocket)

            self._state = _ConnectionState.ACCEPTED
            self.display_id = public_key_display(self.public_key) + " (" + self.display_id + ")"

            data = data[64:]
            if not await self.handshake(websocket, data):
                await self._reject_connection(websocket)
                return

            await self._stop_handshake_timeout()
            return

        await self.websocket_data(websocket, data)

    async def on_disconnect(self, websocket: WebSocket, close_code):
        self._state = _ConnectionState.CLOSED
        await self._stop_handshake_timeout()
        if self.display_id:
            _LOGGER.debug(f"Connection {self.display_id} closed")
            self.display_id = None

    async def handshake(self, websocket: WebSocket, data: bytes) -> bool:
        return True

    async def websocket_data(self, websocket: WebSocket, data: bytes) -> None:
        pass


class WebsocketJSON(WebSocketEndpoint):
    encoding = 'json'
    HANDSHAKE_TIMEOUT = 30.0

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.display_id: str = None

        self.public_key: typing.Optional[PublicKey] = None
        self._challenge_token: typing.Optional[bytes] = None
        self._state = _ConnectionState.RECEIVE_PUBLIC_KEY
        self._handshake_timeout_task: typing.Optional[asyncio.Task] = None

    @classmethod
    async def client_handshake(cls, websocket: "aiohttp.client.ClientWebSocketResponse", key: PrivateKey,
                               extra_data: typing.Dict[str, typing.Any] = None):
        await websocket.send_json({
            'public_key': b64encode(key_to_bytes(key.public_key())).decode('ascii'),
        })
        challenge = await websocket.receive_json()
        token = b64decode(challenge['token'])
        signature = key.sign(token)
        response = {}
        if extra_data:
            response.update(extra_data)
        response['signature'] = b64encode(signature).decode('ascii')
        await websocket.send_json(response)

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
        self._state = _ConnectionState.CLOSED
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
        except:
            pass
        try:
            await t
        except:
            pass

    async def on_connect(self, websocket: WebSocket):
        async def timeout():
            await asyncio.sleep(self.HANDSHAKE_TIMEOUT)
            _LOGGER.debug("Handshake timeout")
            await websocket.close()

        self._handshake_timeout_task = asyncio.ensure_future(timeout())
        await super().on_connect(websocket)

    async def on_receive(self, websocket: WebSocket, data: typing.Dict[str, typing.Any]):
        if self._state == _ConnectionState.CLOSED:
            return
        elif self._state == _ConnectionState.RECEIVE_PUBLIC_KEY:
            self.display_id = websocket.client.host
            if not self._receive_key(data):
                return await self._reject_connection(websocket)

            self._challenge_token = token_bytes(32)
            self._state = _ConnectionState.RECEIVE_CHALLENGE_RESPONSE
            await websocket.send_json({
                'token': b64encode(self._challenge_token).decode('ascii'),
            })
            return
        elif self._state == _ConnectionState.RECEIVE_CHALLENGE_RESPONSE:
            if not self._verify_challenge(data):
                return await self._reject_connection(websocket)

            self._state = _ConnectionState.ACCEPTED
            self.display_id = public_key_display(self.public_key) + " (" + self.display_id + ")"

            if not await self.handshake(websocket, data):
                await self._reject_connection(websocket)
                return

            await self._stop_handshake_timeout()
            return

        await self.websocket_data(websocket, data)

    async def on_disconnect(self, websocket: WebSocket, close_code):
        self._state = _ConnectionState.CLOSED
        await self._stop_handshake_timeout()
        if self.display_id:
            _LOGGER.debug(f"Connection {self.display_id} closed")
            self.display_id = None

    async def handshake(self, websocket: WebSocket, data: typing.Dict[str, typing.Any]) -> bool:
        return True

    async def websocket_data(self, websocket: WebSocket, data: typing.Union[typing.Dict, typing.List]) -> None:
        pass
