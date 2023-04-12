import typing
import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor, Future
from sqlite3 import Connection as SQLiteConnection
from sqlalchemy.engine import Engine
from sqlalchemy import event, create_engine
from sqlalchemy.pool import SingletonThreadPool
from sqlalchemy.orm import DeclarativeMeta
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.types import Text


_LOGGER = logging.getLogger(__name__)


@event.listens_for(Engine, "connect")
def _set_sqlite_pragma(dbapi_connection, connection_record):
    if isinstance(dbapi_connection, SQLiteConnection):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()


@compiles(Text, "mysql")
def _mysql_text(type_, compiler, **kw):
    return "LONGTEXT"


@compiles(Text, "mariadb")
def _mariadb_text(type_, compiler, **kw):
    return "LONGTEXT"


class ORMDatabase:
    def __init__(self, uri: str, orm_base: DeclarativeMeta):
        self._engine = create_engine(uri)
        self.foreground_only = False
        if isinstance(self._engine.pool, SingletonThreadPool):
            self._pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="Database")
            self.sync(orm_base.metadata.create_all)
        else:
            orm_base.metadata.create_all(self._engine)
            self._engine.dispose()
            self._pool = ThreadPoolExecutor(thread_name_prefix="Database")

    def future(self, call: typing.Callable[[Engine], typing.Any]) -> Future:
        return self._pool.submit(call, self._engine)

    async def execute(self, call: typing.Callable[[Engine], typing.Any]):
        return await asyncio.wrap_future(self.future(call))

    def background(self, call: typing.Callable[[Engine], None]) -> None:
        if self.foreground_only:
            self.sync(call)
            return
        self.future(call)

    def sync(self, call: typing.Callable[[Engine], typing.Any]):
        return self.future(call).result()
