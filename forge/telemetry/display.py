import typing
from starlette.types import ASGIApp, Receive, Scope, Send
from .storage import DisplayInterface


class DatabaseMiddleware:
    def __init__(self, app: ASGIApp, database_uri: str):
        self.app = app
        self.db = DisplayInterface(database_uri)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        scope['telemetry'] = self.db
        await self.app(scope, receive, send)
