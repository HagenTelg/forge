import typing
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session
from starlette.types import ASGIApp, Receive, Scope, Send
from forge.crypto import PublicKey
from .database import Interface, AccessStation, AccessData


class DisplayInterface(Interface):
    async def get_data_processing_keys(self, station: str) -> typing.List[PublicKey]:
        def execute(engine: Engine):
            with Session(engine) as orm_session:
                query = orm_session.query(AccessStation).filter_by(station=station.lower())
                query = query.join(AccessData)

                result: typing.List[PublicKey] = list()
                for access in query:
                    result.append(self.key_from_column(access.public_key))
                return result

        return await self.db.execute(execute)


class DatabaseMiddleware:
    def __init__(self, app: ASGIApp, database_uri: str):
        self.app = app
        self.db = DisplayInterface(database_uri)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        scope['processing'] = self.db
        await self.app(scope, receive, send)
