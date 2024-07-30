import typing
import asyncio
import logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
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
    class StorageAccess:
        def __init__(self, control: "Controller"):
            self._control = control

        async def __aenter__(self) -> Storage:
            await self._control._storage_lock.acquire()
            return self._control._storage

        async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
            self._control._storage_lock.release()

        def __enter__(self) -> Storage:
            if self._control._storage_lock.locked():
                raise BlockingIOError
            return self._control._storage

        def __exit__(self, exc_type, exc_val, exc_tb) -> None:
            pass

        async def begin_read(self) -> "Controller.StorageHandle":
            async with self as storage:
                return self._control.StorageHandle(self, storage.begin_read())

        async def begin_write(self) -> "Controller.StorageHandle":
            async with self as storage:
                return self._control.StorageHandle(self, storage.begin_write())

        async def list_files(self, path: str, modified_after: float = 0) -> typing.List[str]:
            async with self as storage:
                return await asyncio.get_event_loop().run_in_executor(
                    None,
                    storage.list_files, path, modified_after
                )

    class StorageHandle:
        def __init__(self, storage: "Controller.StorageAccess",
                     handle: typing.Union[Storage.ReadHandle, Storage.WriteHandle]):
            self._control: "Controller" = storage._control
            self._handle = handle

        async def __aenter__(self) -> typing.Union[Storage.ReadHandle, Storage.WriteHandle]:
            await self._control._storage_lock.acquire()
            return self._handle

        async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
            self._control._storage_lock.release()

        def __enter__(self) -> typing.Union[Storage.ReadHandle, Storage.WriteHandle]:
            if self._control._storage_lock.locked():
                raise BlockingIOError
            return self._handle

        def __exit__(self, exc_type, exc_val, exc_tb) -> None:
            pass

        @property
        def generation(self) -> int:
            return self._handle.generation

        async def release(self) -> None:
            async with self as handle:
                await asyncio.get_event_loop().run_in_executor(None, handle.release)

        async def commit(self, progress: typing.Optional[typing.Callable[[int, int], None]] = None) -> None:
            async with self as handle:
                await asyncio.get_event_loop().run_in_executor(None, handle.commit, progress)

        async def abort(self) -> None:
            async with self as handle:
                await asyncio.get_event_loop().run_in_executor(None, handle.abort)

    def __init__(self, storage_root: typing.Optional[Path] = None):
        self._storage = Storage(root_directory=storage_root)
        self.locker = ArchiveLocker()
        self.notify = NotificationDispatch()
        self.intent = IntentTracker()
        self._next_connection_uid: int = 1
        self.active_connections: typing.Dict[int, Connection] = dict()
        self._storage_lock: asyncio.Lock = None

    async def initialize(self) -> None:
        self._storage.initialize()
        self._storage_lock = asyncio.Lock()

    def shutdown(self) -> None:
        self._storage.shutdown()
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
                    identifier = "[" + str(peername[0]) + "]:" + str(peername[1])
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

    @property
    def storage(self) -> "Controller.StorageAccess":
        return self.StorageAccess(self)

    async def read_transaction(self) -> ReadTransaction:
        tr = ReadTransaction(self.storage, self.locker, self.intent)
        await tr.begin()
        return tr

    async def write_transaction(self) -> WriteTransaction:
        tr = WriteTransaction(self.storage, self.locker, self.notify, self.intent)
        await tr.begin()
        return tr
