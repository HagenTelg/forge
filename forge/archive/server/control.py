import typing
import asyncio
import logging
from pathlib import Path
from forge.service import get_writer_fileno
from forge.tasks import wait_cancelable
from .connection import Connection
from .transaction import ReadTransaction, WriteTransaction
from .storage import Storage
from .lock import ArchiveLocker
from .notify import NotificationDispatch
from .intent import IntentTracker

_LOGGER = logging.getLogger(__name__)


class Controller:
    def __init__(self, storage_root: typing.Optional[Path] = None):
        self.storage = Storage(root_directory=storage_root)
        self.locker = ArchiveLocker()
        self.notify = NotificationDispatch()
        self.intent = IntentTracker()
        self._next_connection_uid: int = 1
        self.active_connections: typing.Dict[int, Connection] = dict()

    async def initialize(self) -> None:
        self.storage.initialize()

    def shutdown(self) -> None:
        self.storage.shutdown()
        for connection in self.active_connections.values():
            try:
                connection.writer.close()
            except:
                pass
        self.active_connections.clear()

    @staticmethod
    def _construct_identifier(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> str:
        identifier = None

        peername = writer.get_extra_info('peername')
        if peername:
            try:
                if isinstance(peername, tuple):
                    identifier = "[" + str(peername[0]) + "]:" + peername[1]
                else:
                    identifier = str(peername)
            except:
                pass

        if not identifier:
            fileno = get_writer_fileno(writer)
            if fileno is not None:
                identifier = "fd:" + str(fileno)

        if not identifier:
            identifier = "%X" % id(reader)

        systemd_name = getattr(reader, "systemd_name", None)
        if systemd_name:
            identifier = identifier + "@" + systemd_name

        return identifier

    async def connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        connection_id = self._construct_identifier(reader, writer)
        _LOGGER.debug("Initializing connection %s", connection_id)
        connection = Connection(reader, writer, connection_id)
        await wait_cancelable(connection.initialize(self), 30.0)

        uid = self._next_connection_uid
        self._next_connection_uid += 1
        self.active_connections[uid] = connection
        try:
            await connection.run()
        finally:
            self.active_connections.pop(uid, None)
            await connection.shutdown()

    async def read_transaction(self) -> ReadTransaction:
        tr = ReadTransaction(self.storage, self.locker, self.intent)
        await tr.begin()
        return tr

    async def write_transaction(self) -> WriteTransaction:
        tr = WriteTransaction(self.storage, self.locker, self.notify, self.intent)
        await tr.begin()
        return tr
