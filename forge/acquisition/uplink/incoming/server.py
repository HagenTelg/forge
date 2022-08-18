import typing
import logging
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.middleware.trustedhost import TrustedHostMiddleware
from starlette.routing import WebSocketRoute
from starlette.types import ASGIApp, Receive, Scope, Send
from forge.processing.control.access import AccessInterface
from .. import CONFIGURATION
from .downlink import DownlinkSocket


_LOGGER = logging.getLogger(__name__)


routes = [
    WebSocketRoute('/{station}', DownlinkSocket),
]

middleware: typing.List[Middleware] = list()

middleware.append(Middleware(TrustedHostMiddleware,
                             allowed_hosts=CONFIGURATION.get('ACQUISITION.UPLINK.TRUSTED_HOSTS', ["*"])))


class _DatabaseMiddleware:
    def __init__(self, app: ASGIApp, database_uri: str):
        self.app = app
        self.db = AccessInterface(database_uri)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        scope['processing'] = self.db
        await self.app(scope, receive, send)


middleware.append(Middleware(_DatabaseMiddleware, database_uri=CONFIGURATION.PROCESSING.CONTROL.DATABASE))

app = Starlette(routes=routes, middleware=middleware)
