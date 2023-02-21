import typing
import logging
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.middleware.trustedhost import TrustedHostMiddleware
from starlette.routing import Route, WebSocketRoute
from starlette.types import ASGIApp, Receive, Scope, Send
from . import CONFIGURATION
from .storage import DashboardInterface
from .update import update, DashboardSocket


_LOGGER = logging.getLogger(__name__)


routes = [
    Route('/update', endpoint=update, methods=['POST']),
    WebSocketRoute('/socket/update', DashboardSocket),
]

middleware: typing.List[Middleware] = list()

middleware.append(Middleware(TrustedHostMiddleware,
                             allowed_hosts=CONFIGURATION.get('DASHBOARD.TRUSTED_HOSTS', ["*"])))


class _DatabaseMiddleware:
    def __init__(self, app: ASGIApp, database_uri: str):
        self.app = app
        self.db = DashboardInterface(database_uri)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        scope['dashboard'] = self.db
        await self.app(scope, receive, send)


middleware.append(Middleware(_DatabaseMiddleware, database_uri=CONFIGURATION.DASHBOARD.DATABASE))

app = Starlette(routes=routes, middleware=middleware)
