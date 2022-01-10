import pytest
import asyncio
import typing
import time
from base64 import b64encode, b64decode
from json import loads as from_json
from starlette.testclient import TestClient
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.routing import WebSocketRoute
from starlette.types import ASGIApp, Receive, Scope, Send
from starlette.websockets import WebSocketDisconnect
from forge.authsocket import PrivateKey, key_to_bytes
from forge.telemetry.connected import TelemetrySocket
from forge.telemetry.storage import Interface as TelemetryInterface


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
        WebSocketRoute('/update', TelemetrySocket)
    ]
    middleware = [
        Middleware(_DatabaseMiddleware, interface=interface),
    ]
    app = Starlette(routes=routes, middleware=middleware)
    return app


@pytest.fixture
def client(app):
    return TestClient(app)


def test_basic(client, interface):
    key = PrivateKey.generate()
    with client.websocket_connect("/update") as ws:
        public_key = b64encode(key_to_bytes(key.public_key())).decode('ascii')
        ws.send_json({
            'public_key': public_key,
        })
        data = ws.receive_json()
        token = data['token']
        assert isinstance(token, str)
        token = b64decode(token)
        assert len(token) > 0
        ws.send_json({
            'signature': b64encode(key.sign(token)).decode('ascii'),
            'station': 'nil',
        })

        ws.send_json({
            'request': 'get_time',
        })
        data = ws.receive_json()
        assert data['response'] == 'server_time'
        assert int(time.time()+1) >= int(data['server_time'])

        host_id = None
        host_key = None

        def fetch_host_id(engine):
            nonlocal host_id
            nonlocal host_key
            with engine.connect() as conn:
                row = conn.execute('SELECT id, public_key FROM host_data WHERE station = "nil"').one()
                host_id = int(row[0])
                host_key = str(row[1])

        interface.db.sync(fetch_host_id)
        assert host_id is not None
        assert host_key == public_key

        ws.send_json({
            'request': 'update',
            'telemetry': {'test_key': 'test_value'}
        })
        ws.send_json({
            'request': 'get_time',
        })
        data = ws.receive_json()
        assert data['response'] == 'server_time'
        assert int(time.time()+1) >= int(data['server_time'])

        telemetry_json = None

        def fetch_host_telemetry(engine):
            nonlocal telemetry_json
            with engine.connect() as conn:
                row = conn.execute(f'SELECT telemetry FROM telemetry WHERE host_data = {host_id}').one()
                telemetry_json = row[0]

        interface.db.sync(fetch_host_telemetry)
        assert telemetry_json is not None
        telemetry = from_json(telemetry_json)
        assert telemetry == {'test_key': 'test_value'}

        ws.send_json({
            'request': 'update',
            'telemetry': {'test_key': 'test_value2'}
        })
        ws.send_json({
            'request': 'get_time',
        })
        data = ws.receive_json()
        assert data['response'] == 'server_time'
        assert int(time.time() + 1) >= int(data['server_time'])

        interface.db.sync(fetch_host_telemetry)
        assert telemetry_json is not None
        telemetry = from_json(telemetry_json)
        assert telemetry == {'test_key': 'test_value2'}


def test_signature_fail(client, interface):
    with client.websocket_connect("/update") as ws:
        ws.send_json({
            'public_key': b64encode(b'1' * 32).decode('ascii'),
        })
        data = ws.receive_json()
        token = data['token']
        assert isinstance(token, str)
        token = b64decode(token)
        assert len(token) > 0
        ws.send_json({
            'signature': b64encode(b'1' * 64).decode('ascii'),
            'station': 'nil',
        })

        try:
            ws.send_json({
                'request': 'get_time',
            })
            result = ws.receive_text()
            raise RuntimeError(result)
        except WebSocketDisconnect:
            pass

        host_id = None

        def fetch_host_id(engine):
            nonlocal host_id
            with engine.connect() as conn:
                row = conn.execute('SELECT id FROM host_data WHERE station = "nil"').one_or_none()
                if row is None:
                    return
                host_id = int(row[0])

        interface.db.sync(fetch_host_id)
        assert host_id is None
