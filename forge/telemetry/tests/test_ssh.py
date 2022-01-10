import pytest
import asyncio
import typing
import struct
from base64 import b64encode
from starlette.testclient import TestClient
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.routing import WebSocketRoute
from starlette.types import ASGIApp, Receive, Scope, Send
from starlette.websockets import WebSocketDisconnect
from forge.telemetry import PrivateKey, PublicKey, key_to_bytes, CONFIGURATION
from forge.telemetry.ssh import TunnelSocket, ConnectionSocket, StationConnectionSocket
from forge.telemetry.storage import Interface as TelemetryInterface
from forge.telemetry.tunnel.protocol import ServerConnectionType, FromRemotePacketType, ToRemotePacketType, InitiateConnectionStatus


class _DatabaseMiddleware:
    def __init__(self, app: ASGIApp, interface: TelemetryInterface):
        self.app = app
        self.db = interface

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        scope['telemetry'] = self.db
        await self.app(scope, receive, send)


@pytest.fixture
def interface():
    result = TelemetryInterface("sqlite+pysqlite:///:memory:")
    result.db.foreground_only = True
    return result


@pytest.fixture
def app(interface):
    routes = [
        WebSocketRoute('/tunnel', TunnelSocket),
        WebSocketRoute('/connection/{station}', StationConnectionSocket),
        WebSocketRoute('/connection', ConnectionSocket),
    ]
    middleware = [
        Middleware(_DatabaseMiddleware, interface=interface),
    ]
    app = Starlette(routes=routes, middleware=middleware)
    return app


@pytest.fixture
def client(app):
    return TestClient(app)


async def prepare_backend_socket(tmp_path) -> typing.Tuple[asyncio.Future, asyncio.Future]:
    reader = asyncio.get_event_loop().create_future()
    writer = asyncio.get_event_loop().create_future()

    async def connection(r: asyncio.StreamReader, w: asyncio.StreamWriter) -> None:
        reader.set_result(r)
        writer.set_result(w)

    socket_location = str(tmp_path / 'backend.sock')

    await asyncio.start_unix_server(connection, path=socket_location)
    CONFIGURATION.set('TELEMETRY.TUNNEL.SOCKET', socket_location)

    return reader, writer


@pytest.mark.asyncio
async def test_tunnel(client, interface, tmp_path):
    reader, writer = await prepare_backend_socket(tmp_path)

    key = PrivateKey.generate()
    with client.websocket_connect("/tunnel") as ws:
        ws.send_bytes(key_to_bytes(key.public_key()))
        token = ws.receive_bytes()
        assert len(token) > 0
        signature = key.sign(token)
        ws.send_bytes(signature)

        reader = await reader
        writer = await writer

        packet = await reader.readexactly(1)
        data = struct.pack('<B', ServerConnectionType.TO_REMOTE.value)
        assert packet == data

        packet = await reader.readexactly(32)
        assert packet == key_to_bytes(key.public_key())

        data = struct.pack('<BH', FromRemotePacketType.CONNECTION_CLOSED.value, 123)
        ws.send_bytes(data)
        packet = await reader.readexactly(3)
        assert packet == data

        data = struct.pack('<BH', FromRemotePacketType.DATA.value, 456) + (b'5' * 10)
        ws.send_bytes(data)
        packet = await reader.readexactly(10 + 5)
        data = struct.pack('<BHH', FromRemotePacketType.DATA.value, 456, 10) + (b'5' * 10)
        assert packet == data

        data = struct.pack('<BH', ToRemotePacketType.SSH_CONNECTION_OPEN.value, 120)
        writer.write(data)
        await writer.drain()
        packet = ws.receive_bytes()
        assert packet == data

        data = struct.pack('<BH', ToRemotePacketType.CONNECTION_CLOSE.value, 340)
        writer.write(data)
        await writer.drain()
        packet = ws.receive_bytes()
        assert packet == data

        data = struct.pack('<BHH', FromRemotePacketType.DATA.value, 560, 10) + (b'6' * 10)
        writer.write(data)
        await writer.drain()
        packet = ws.receive_bytes()
        data = struct.pack('<BH', FromRemotePacketType.DATA.value, 560) + (b'6' * 10)
        assert packet == data


@pytest.mark.asyncio
async def test_signature_fail(client, interface, tmp_path):
    with client.websocket_connect("/tunnel") as ws:
        ws.send_bytes(b'1' * 32)
        token = ws.receive_bytes()
        assert len(token) > 0
        ws.send_bytes(b'1' * 64)

        try:
            ws.send_bytes(bytes(4))
            ws.receive_bytes()
            raise RuntimeError
        except WebSocketDisconnect:
            pass


@pytest.mark.asyncio
async def test_connection(client, interface, tmp_path):
    reader, writer = await prepare_backend_socket(tmp_path)

    key = PrivateKey.generate()

    def set_auth(engine):
        key_string = b64encode(key_to_bytes(key.public_key())).decode('ascii')
        with engine.connect() as conn:
            conn.execute(f'INSERT INTO access_station(public_key, station) VALUES ("{key_string}", "*")')

    interface.db.sync(set_auth)

    with client.websocket_connect("/connection") as ws:
        ws.send_bytes(key_to_bytes(key.public_key()))
        token = ws.receive_bytes()
        assert len(token) > 0
        signature = key.sign(token)
        target_key = b'1' * 32
        ws.send_bytes(signature + target_key)

        reader = await reader
        writer = await writer

        packet = await reader.readexactly(1)
        data = struct.pack('<B', ServerConnectionType.INITIATE_CONNECTION.value)
        assert packet == data

        packet = await reader.readexactly(32)
        assert packet == target_key

        data = struct.pack('<B', InitiateConnectionStatus.OK.value)
        writer.write(data)
        await writer.drain()
        packet = ws.receive_bytes()
        assert packet == data

        data = b'2' * 10
        ws.send_bytes(data)
        packet = await reader.readexactly(10)
        assert packet == data

        data = b'3' * 11
        writer.write(data)
        await writer.drain()
        packet = ws.receive_bytes()
        assert packet == data


@pytest.mark.asyncio
async def test_rejected_connection(client, interface, tmp_path):
    await prepare_backend_socket(tmp_path)

    key = PrivateKey.generate()

    def set_auth(engine):
        key_string = b64encode(key_to_bytes(key.public_key())).decode('ascii')
        with engine.connect() as conn:
            conn.execute(f'INSERT INTO access_station(public_key, station) VALUES ("{key_string}", "nil")')

    interface.db.sync(set_auth)

    with client.websocket_connect("/connection") as ws:
        ws.send_bytes(key_to_bytes(key.public_key()))
        token = ws.receive_bytes()
        assert len(token) > 0
        signature = key.sign(token)
        target_key = b'1' * 32
        ws.send_bytes(signature + target_key)

        data = struct.pack('<B', InitiateConnectionStatus.PERMISSION_DENIED.value)
        packet = ws.receive_bytes()
        assert packet == data


@pytest.mark.asyncio
async def test_restricted_connection(client, interface, tmp_path):
    reader, writer = await prepare_backend_socket(tmp_path)

    key = PrivateKey.generate()
    target_key = b'1' * 32

    def set_auth(engine):
        key_string = b64encode(key_to_bytes(key.public_key())).decode('ascii')
        with engine.connect() as conn:
            conn.execute(f'INSERT INTO access_station(public_key, station) VALUES ("{key_string}", "nil")')

    interface.db.sync(set_auth)
    await interface.ping_host(PublicKey.from_public_bytes(target_key), '127.0.0.1', 'nil')

    with client.websocket_connect("/connection") as ws:
        ws.send_bytes(key_to_bytes(key.public_key()))
        token = ws.receive_bytes()
        assert len(token) > 0
        signature = key.sign(token)
        ws.send_bytes(signature + target_key)

        reader = await reader
        writer = await writer

        packet = await reader.readexactly(1)
        data = struct.pack('<B', ServerConnectionType.INITIATE_CONNECTION.value)
        assert packet == data

        packet = await reader.readexactly(32)
        assert packet == target_key

        data = struct.pack('<B', InitiateConnectionStatus.OK.value)
        writer.write(data)
        await writer.drain()
        packet = ws.receive_bytes()
        assert packet == data

        data = b'2' * 10
        ws.send_bytes(data)
        packet = await reader.readexactly(10)
        assert packet == data

        data = b'3' * 11
        writer.write(data)
        await writer.drain()
        packet = ws.receive_bytes()
        assert packet == data


@pytest.mark.asyncio
async def test_station(client, interface, tmp_path):
    reader, writer = await prepare_backend_socket(tmp_path)

    key = PrivateKey.generate()
    target_key = b'1' * 32

    def set_auth(engine):
        key_string = b64encode(key_to_bytes(key.public_key())).decode('ascii')
        with engine.connect() as conn:
            conn.execute(f'INSERT INTO access_station(public_key, station) VALUES ("{key_string}", "*")')

    interface.db.sync(set_auth)
    await interface.ping_host(PublicKey.from_public_bytes(target_key), '127.0.0.1', 'nil')

    with client.websocket_connect("/connection/nil") as ws:
        ws.send_bytes(key_to_bytes(key.public_key()))
        token = ws.receive_bytes()
        assert len(token) > 0
        signature = key.sign(token)
        ws.send_bytes(signature + target_key)

        reader = await reader
        writer = await writer

        packet = await reader.readexactly(1)
        data = struct.pack('<B', ServerConnectionType.INITIATE_CONNECTION.value)
        assert packet == data

        packet = await reader.readexactly(32)
        assert packet == target_key

        data = struct.pack('<B', InitiateConnectionStatus.OK.value)
        writer.write(data)
        await writer.drain()
        packet = ws.receive_bytes()
        assert packet == data

        data = b'2' * 10
        ws.send_bytes(data)
        packet = await reader.readexactly(10)
        assert packet == data

        data = b'3' * 11
        writer.write(data)
        await writer.drain()
        packet = ws.receive_bytes()
        assert packet == data


@pytest.mark.asyncio
async def test_rejected_station(client, interface, tmp_path):
    await prepare_backend_socket(tmp_path)

    key = PrivateKey.generate()
    target_key = b'1' * 32

    await interface.ping_host(PublicKey.from_public_bytes(target_key), '127.0.0.1', 'nil')

    with client.websocket_connect("/connection/nil") as ws:
        ws.send_bytes(key_to_bytes(key.public_key()))
        token = ws.receive_bytes()
        assert len(token) > 0
        signature = key.sign(token)
        target_key = b'1' * 32
        ws.send_bytes(signature + target_key)

        data = struct.pack('<B', InitiateConnectionStatus.PERMISSION_DENIED.value)
        packet = ws.receive_bytes()
        assert packet == data


@pytest.mark.asyncio
async def test_restricted_station(client, interface, tmp_path):
    reader, writer = await prepare_backend_socket(tmp_path)

    key = PrivateKey.generate()
    target_key = b'1' * 32

    def set_auth(engine):
        key_string = b64encode(key_to_bytes(key.public_key())).decode('ascii')
        with engine.connect() as conn:
            conn.execute(f'INSERT INTO access_station(public_key, station) VALUES ("{key_string}", "nil")')

    interface.db.sync(set_auth)
    await interface.ping_host(PublicKey.from_public_bytes(target_key), '127.0.0.1', 'nil')

    with client.websocket_connect("/connection/nil") as ws:
        ws.send_bytes(key_to_bytes(key.public_key()))
        token = ws.receive_bytes()
        assert len(token) > 0
        signature = key.sign(token)
        ws.send_bytes(signature + target_key)

        reader = await reader
        writer = await writer

        packet = await reader.readexactly(1)
        data = struct.pack('<B', ServerConnectionType.INITIATE_CONNECTION.value)
        assert packet == data

        packet = await reader.readexactly(32)
        assert packet == target_key

        data = struct.pack('<B', InitiateConnectionStatus.OK.value)
        writer.write(data)
        await writer.drain()
        packet = ws.receive_bytes()
        assert packet == data

        data = b'2' * 10
        ws.send_bytes(data)
        packet = await reader.readexactly(10)
        assert packet == data

        data = b'3' * 11
        writer.write(data)
        await writer.drain()
        packet = ws.receive_bytes()
        assert packet == data