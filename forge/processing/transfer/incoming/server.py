import typing
import logging
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.middleware.trustedhost import TrustedHostMiddleware
from starlette.routing import Route
from starlette.types import ASGIApp, Receive, Scope, Send
from forge.processing.control.access import AccessInterface
from .. import CONFIGURATION
from .receive import receive_data, receive_backup, receive_auxiliary


_LOGGER = logging.getLogger(__name__)


routes = [
    Route('/{station}/data', endpoint=receive_data, methods=['POST']),
    Route('/{station}/backup', endpoint=receive_backup, methods=['POST']),
    Route('/{station}/auxiliary', endpoint=receive_auxiliary, methods=['POST']),
]

middleware: typing.List[Middleware] = list()

middleware.append(Middleware(TrustedHostMiddleware,
                             allowed_hosts=CONFIGURATION.get('PROCESSING.INCOMING.TRUSTED_HOSTS', ["*"])))


class _DatabaseMiddleware:
    def __init__(self, app: ASGIApp, database_uri: str):
        self.app = app
        self.db = AccessInterface(database_uri)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        scope['processing'] = self.db
        await self.app(scope, receive, send)


middleware.append(Middleware(_DatabaseMiddleware, database_uri=CONFIGURATION.PROCESSING.CONTROL.DATABASE))

app = Starlette(routes=routes, middleware=middleware)
