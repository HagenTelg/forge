import asyncio
import typing
from starlette.applications import Starlette
from starlette.testclient import TestClient
from starlette.routing import Mount
from starlette.authentication import AuthenticationBackend, AuthCredentials
from starlette.requests import Request
from starlette.middleware import Middleware
from starlette.middleware.sessions import SessionMiddleware
from starlette.middleware.authentication import AuthenticationMiddleware
from forge.vis.access import BaseAccessUser
from forge.vis.data.server import sockets


class StubUser(BaseAccessUser):
    @property
    def is_authenticated(self) -> bool:
        return True

    @property
    def display_name(self) -> str:
        return "ABC"

    @property
    def initials(self) -> str:
        return "ABC"

    @property
    def display_id(self) -> str:
        return "ABC"

    @property
    def visible_stations(self) -> typing.List[str]:
        raise ["brw"]

    def allow_station(self, station: str) -> bool:
        return True

    def allow_mode(self, station: str, mode: str, write=False) -> bool:
        return True


class StubAuthBackend(AuthenticationBackend):
    async def authenticate(self, request: Request):
        return AuthCredentials(['authenticated']), StubUser()


def create_app():
    routes = [
        Mount('/data', routes=sockets),
    ]
    middleware = [
        Middleware(SessionMiddleware, secret_key='test'),
        Middleware(AuthenticationMiddleware, backend=StubAuthBackend())
    ]
    app = Starlette(routes=routes, middleware=middleware)
    return app


def test_basic_stream():
    app = create_app()
    client = TestClient(app)

    with client.websocket_connect("/data/brw") as ds:
        ds.send_json({
            'action': 'start',
            'stream': 1,
            'data': 'example-timeseries',
            'start_epoch_ms': 1609459200000,
            'end_epoch_ms': 1609545600000,
        })
        data = ds.receive_json()
        assert data['type'] == 'data'
        assert data['stream'] == 1
        assert data['content']['time']['origin'] == 1609459200000
        while True:
            data = ds.receive_json()
            if data['type'] == 'end':
                assert data['stream'] == 1
                break

            assert data['type'] == 'data'
            assert data['stream'] == 1


def test_stream_stop():
    app = create_app()
    client = TestClient(app)

    with client.websocket_connect("/data/brw") as ds:
        ds.send_json({
            'action': 'start',
            'stream': 42,
            'data': 'example-timeseries',
            'start_epoch_ms': 1609459200000,
            'end_epoch_ms': 1609545600000,
        })
        ds.send_json({
            'action': 'stop',
            'stream': 42,
        })
        while True:
            data = ds.receive_json()
            if data['type'] == 'end':
                assert data['stream'] == 42
                break

            assert data['type'] == 'data'
            assert data['stream'] == 42


