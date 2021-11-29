import typing
import logging
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.middleware.trustedhost import TrustedHostMiddleware
from starlette.routing import Route, WebSocketRoute
from starlette.types import ASGIApp, Receive, Scope, Send
from . import CONFIGURATION
from .storage import Interface as TelemetryInterface
from .direct import update
from .connected import TelemetrySocket
from .ssh import ConnectionSocket, TunnelSocket, StationConnectionSocket


_LOGGER = logging.getLogger(__name__)


routes = [
    Route('/update', endpoint=update, methods=['POST']),
    WebSocketRoute('/socket/update', TelemetrySocket),
    WebSocketRoute('/socket/ssh/open', TunnelSocket),
    WebSocketRoute('/socket/ssh/connect/{station}', StationConnectionSocket),
    WebSocketRoute('/socket/ssh/connect', ConnectionSocket),
]

middleware: typing.List[Middleware] = list()

middleware.append(Middleware(TrustedHostMiddleware,
                             allowed_hosts=CONFIGURATION.get('TELEMETRY.TRUSTED_HOSTS', ["*"])))


class _DatabaseMiddleware:
    def __init__(self, app: ASGIApp, database_uri: str):
        self.app = app
        self.db = TelemetryInterface(database_uri)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        scope['telemetry'] = self.db
        await self.app(scope, receive, send)


middleware.append(Middleware(_DatabaseMiddleware, database_uri=CONFIGURATION.TELEMETRY.DATABASE))

app = Starlette(routes=routes, middleware=middleware)
