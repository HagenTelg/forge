import asyncio
from starlette.applications import Starlette
from starlette.testclient import TestClient
from starlette.routing import Route, Mount
from starlette.authentication import requires, AuthenticationBackend, AuthCredentials
from starlette.requests import Request
from starlette.responses import Response, JSONResponse
from starlette.middleware import Middleware
from starlette.middleware.sessions import SessionMiddleware
from starlette.middleware.authentication import AuthenticationMiddleware
from forge.vis.access.database import AccessController, ControlInterface


async def no_auth(request: Request) -> Response:
    return JSONResponse({'ok': True})


@requires('authenticated')
async def required_auth(request: Request) -> Response:
    return JSONResponse({'ok': True})


class StubAuthBackend(AuthenticationBackend):
    def __init__(self, controller: AccessController):
        self.controller = controller

    async def authenticate(self, request: Request):
        user = await self.controller.authenticate(request)
        if user is not None:
            return AuthCredentials(['authenticated']), user


def create_app():
    controller = AccessController("sqlite+pysqlite:///:memory:")
    controller.db.foreground_only = True
    routes = [
        Route('/no_auth', endpoint=no_auth, name='root'),
        Route('/required_auth', endpoint=required_auth),
        Mount('/auth', routes=controller.routes),
    ]
    middleware = [
        Middleware(SessionMiddleware, secret_key='test'),
        Middleware(AuthenticationMiddleware, backend=StubAuthBackend(controller))
    ]
    app = Starlette(routes=routes, middleware=middleware)
    return app, controller


def test_basic_access():
    app, _ = create_app()
    client = TestClient(app)

    response = client.get('/no_auth')
    assert response.json() == {'ok': True}

    response = client.get('/required_auth')
    assert response.status_code != 200


def test_user_operations():
    app, _ = create_app()
    client = TestClient(app)

    response = client.get('/required_auth')
    assert response.status_code != 200

    client.post('/auth/password/create', data={
        'email': 'test@example.com',
        'password': 'testtesttest',
    })

    response = client.get('/required_auth')
    assert response.json() == {"ok": True}

    client.get('/auth/logout')
    response = client.get('/required_auth')
    assert response.status_code != 200

    client.post('/auth/password/login', data={
        'email': 'test2@example.com',
        'password': 'testtesttest',
    })
    response = client.get('/required_auth')
    assert response.status_code != 200

    client.post('/auth/password/login', data={
        'email': 'test@example.com',
        'password': 'testtesttest',
    })
    response = client.get('/required_auth')
    assert response.json() == {'ok': True}

    response = client.post('/auth/change_info', json={
        'email': 'test2@example.com',
        'name': 'Test User',
        'initials': 'TU2',
    })
    assert response.json() == {
        'status': 'ok',
        'email': 'test2@example.com',
        'name': 'Test User',
        'initials': 'TU2',
    }

    response = client.post('/auth/password/change', json={
        'password': 'test2testtest',
    })
    assert response.json() == {'status': 'ok'}

    client.get('/auth/logout')
    response = client.get('/required_auth')
    assert response.status_code != 200

    response = client.post('/auth/password/login', data={
        'email': 'test@example.com',
        'password': 'testtesttest',
    })
    assert response.status_code == 401
    response = client.get('/required_auth')
    assert response.status_code != 200

    client.post('/auth/password/login', data={
        'email': 'test2@example.com',
        'password': 'test2testtest',
    })
    response = client.get('/required_auth')
    assert response.json() == {'ok': True}


def test_password_reset():
    app, controller = create_app()
    client = TestClient(app)

    response = client.get('/required_auth')
    assert response.status_code != 200

    client.post('/auth/password/create', data={
        'email': 'test@example.com',
        'password': 'testtesttest',
    })

    client.get('/auth/logout')

    response = client.post('/auth/password/reset_issue', data={
        'email': 'test@example.com',
    })
    assert response.status_code == 200

    reset_token = None

    def fetch_token(engine):
        nonlocal reset_token
        with engine.connect() as conn:
            row = conn.execute('SELECT token FROM password_reset').one()
            reset_token = row[0]

    controller.db.sync(fetch_token)
    assert reset_token is not None

    response = client.get(f'/auth/password/reset?token={reset_token}')
    assert response.status_code == 200

    response = client.get('/required_auth')
    assert response.json() == {"ok": True}


def test_external_interface():
    app, controller = create_app()
    client = TestClient(app)
    interface = ControlInterface("sqlite+pysqlite:///:memory:")
    interface.db = controller.db

    loop = asyncio.get_event_loop()
    loop.run_until_complete(interface.add_user('test@example.com', 'testtesttest'))

    client.post('/auth/password/login', data={
        'email': 'test@example.com',
        'password': 'testtesttest',
    })
    response = client.get('/required_auth')
    assert response.json() == {'ok': True}

    client.get('/auth/logout')

    loop.run_until_complete(interface.modify_user(set_email='test@example2.com', set_password='testtesttest2', email='test@example.com'))

    client.post('/auth/password/login', data={
        'email': 'test@example2.com',
        'password': 'testtesttest2',
    })
    response = client.get('/required_auth')
    assert response.json() == {'ok': True}

    loop.close()
