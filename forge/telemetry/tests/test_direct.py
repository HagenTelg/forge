import pytest
import asyncio
import typing
import time
from base64 import b64encode, urlsafe_b64encode
from json import loads as from_json
from json import dumps as to_json
from starlette.testclient import TestClient
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.routing import Route
from starlette.types import ASGIApp, Receive, Scope, Send
from forge.authsocket import PrivateKey, key_to_bytes
from forge.telemetry.direct import update
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
        Route('/update', update, methods=['POST'])
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
    telemetry = to_json({
        'sequence_number': 500,
        'station': 'nil',
        'test_key': 'test_value',
    }).encode('utf-8')
    public_key = b64encode(key_to_bytes(key.public_key())).decode('ascii')
    signature = b64encode(key.sign(telemetry)).decode('ascii')
    response = client.post('/update', headers={
        'X-HostID': f'{public_key} {signature}'
    }, data=telemetry)
    assert response.status_code == 200
    response = response.json()

    assert response['status'] == 'ok'
    assert int(time.time()+1) >= int(response['server_time'])

    host_id = None
    host_key = None
    telemetry_json = None

    def fetch_info(engine):
        nonlocal host_id
        nonlocal host_key
        nonlocal telemetry_json
        with engine.connect() as conn:
            row = conn.execute('SELECT id, public_key FROM host_data WHERE station = "nil"').one()
            host_id = int(row[0])
            host_key = str(row[1])
            row = conn.execute(f'SELECT telemetry FROM telemetry WHERE host_data = {host_id}').one()
            telemetry_json = row[0]

    interface.db.sync(fetch_info)
    assert host_id is not None
    assert host_key == public_key
    assert telemetry_json is not None
    telemetry = from_json(telemetry_json)
    assert telemetry == {'test_key': 'test_value'}

    telemetry = to_json({
        'sequence_number': 501,
        'station': 'nil',
        'test_key': 'test_value2',
    }).encode('utf-8')
    public_key = urlsafe_b64encode(key_to_bytes(key.public_key())).decode('ascii')
    signature = urlsafe_b64encode(key.sign(telemetry)).decode('ascii')
    response = client.post(f'/update?publickey={public_key}&signature={signature}', data=telemetry)
    assert response.status_code == 200
    response = response.json()

    assert response['status'] == 'ok'
    assert int(time.time() + 1) >= int(response['server_time'])

    interface.db.sync(fetch_info)
    assert telemetry_json is not None
    telemetry = from_json(telemetry_json)
    assert telemetry == {'test_key': 'test_value2'}


def test_signature_fail(client, interface):
    telemetry = to_json({
        'sequence_number': 500,
        'station': 'nil',
        'test_key': 'test_value',
    }).encode('utf-8')
    public_key = b64encode(b'1' * 32).decode('ascii')
    signature = b64encode(b'1' * 64).decode('ascii')
    response = client.post('/update', headers={
        'X-HostID': f'{public_key} {signature}'
    }, data=telemetry)
    assert response.status_code != 200

    response = client.post(f'/update?publickey={public_key}&signature={signature}', data=telemetry)
    assert response.status_code != 200


def test_duplicate_reject(client, interface):
    key = PrivateKey.generate()
    telemetry = to_json({
        'sequence_number': 500,
        'station': 'nil',
        'test_key': 'test_value',
    }).encode('utf-8')
    public_key = b64encode(key_to_bytes(key.public_key())).decode('ascii')
    signature = b64encode(key.sign(telemetry)).decode('ascii')
    response = client.post('/update', headers={
        'X-HostID': f'{public_key} {signature}'
    }, data=telemetry)
    assert response.status_code == 200
    response = response.json()

    assert response['status'] == 'ok'
    assert int(time.time()+1) >= int(response['server_time'])

    response = client.post('/update', headers={
        'X-HostID': f'{public_key} {signature}'
    }, data=telemetry)
    assert response.status_code == 200
    response = response.json()
    assert response['status'] == 'rejected'
