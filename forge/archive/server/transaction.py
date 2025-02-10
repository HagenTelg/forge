import typing
import asyncio
import logging
import time
from abc import ABC, abstractmethod
from .lock import ArchiveLocker, LockDenied
from .notify import NotificationDispatch
from .intent import IntentTracker

_LOGGER = logging.getLogger(__name__)

if typing.TYPE_CHECKING:
    from .connection import Connection
    from .control import Controller


class _BaseTransaction(ABC):
    def __init__(self, storage: "Controller.StorageAccess", locker: ArchiveLocker, intent: IntentTracker):
        self.storage = storage
        self.locker = locker
        self.intent = intent
        self.status: str = None
        self.begin_time: float = None
        self.storage_handle: "Controller.StorageHandle" = None
        self.intent_origin: "Connection" = None
        self.locks: typing.List[ArchiveLocker.Lock] = list()

    def __del__(self):
        if self.storage_handle:
            try:
                with self.storage_handle as handle:
                    _LOGGER.error("Leaked storage handle in transaction (%d)", handle.generation)
                    handle.release()
            except BlockingIOError:
                _LOGGER.error("Unable to release leaked handle due to lock contention")
            self.storage_handle = None
        if self.locks:
            _LOGGER.error("Leaked %d locks in transaction", len(self.locks))
            for lock in self.locks:
                lock.release()
            self.locks.clear()

    def __str__(self) -> str:
        return self.status

    @abstractmethod
    async def begin(self) -> None:
        pass

    @abstractmethod
    async def commit(self) -> None:
        pass

    @abstractmethod
    async def abort(self) -> None:
        pass

    @property
    def generation(self) -> int:
        return self.storage_handle.generation

    async def read_file(self, name: str) -> typing.BinaryIO:
        async with self.storage_handle as handle:
            return handle.read_file(name)

    async def write_file(self, name: str) -> typing.BinaryIO:
        raise NotImplementedError

    def remove_file(self, name: str) -> None:
        raise NotImplementedError

    def _check_intent_conflict(self, key: str, start: int, end: int) -> typing.Optional[str]:
        conflicting = self.intent.get_conflicting(self.intent_origin, key, start, end)
        if not conflicting:
            return None

        message = "Waiting for update"
        for c in conflicting:
            if not c:
                continue
            c.queue_unsolicited(c.write_intent_hit, key, start, end)
            message = c.intent_status
        return message

    def lock_read(self, key: str, start: int, end: int) -> typing.Optional[str]:
        conflicting = self._check_intent_conflict(key, start, end)
        if conflicting:
            return conflicting

        try:
            lock = self.locker.acquire_read(self.generation, self, key, start, end)
        except LockDenied as e:
            return str(e.blocked)
        self.locks.append(lock)
        return None

    def lock_write(self, key: str, start: int, end: int) -> typing.Optional[str]:
        raise NotImplementedError

    def send_notification(self, key: str, start: int, end: int) -> None:
        raise NotImplementedError

    def acquire_intent(self, uid: int, key: str, start: int, end: int) -> None:
        raise NotImplementedError

    def release_intent(self, uid: int) -> None:
        raise NotImplementedError


class ReadTransaction(_BaseTransaction):
    async def begin(self) -> None:
        self.storage_handle = await self.storage.begin_read()
        self.status = str(self.generation)
        self.begin_time = time.time()

    async def _end_transaction(self) -> None:
        await self.storage_handle.release()
        self.storage_handle = None
        for lock in self.locks:
            lock.release()
        self.locks.clear()

    async def commit(self) -> None:
        await self._end_transaction()

    async def abort(self) -> None:
        await self._end_transaction()


class WriteTransaction(_BaseTransaction):
    def __init__(self, storage: "Controller.StorageAccess", locker: ArchiveLocker, notify: NotificationDispatch,
                 intent: IntentTracker):
        super().__init__(storage, locker, intent)
        self.notify = notify
        self.queued_notifications: typing.List[NotificationDispatch.Queued] = list()
        self.intents_to_acquire: typing.Dict[int, typing.Tuple[str, int, int]] = dict()
        self.intents_to_release: typing.Set[int] = set()

    async def begin(self) -> None:
        self.storage_handle = await self.storage.begin_write()
        self.status = str(self.generation)
        self.begin_time = time.time()

    async def commit(self) -> None:
        try:
            for uid in self.intents_to_release:
                self.intent.release(self.intent_origin, uid)
            for uid, args in self.intents_to_acquire.items():
                self.intent.acquire(self.intent_origin, uid, *args)
        except:
            await self.abort()
            raise

        progress_fraction: float = 0.0
        progress_updated = asyncio.Event()
        commit_loop = asyncio.get_event_loop()

        def storage_progress(completed: int, total: int) -> None:
            nonlocal progress_fraction
            # Informational only, so just rely on the GIL for locking
            progress_fraction = float(completed) / float(total)
            commit_loop.call_soon_threadsafe(progress_updated.set)

        async def set_status():
            while True:
                await progress_updated.wait()
                progress_updated.clear()
                self.status = f"Data updating, {progress_fraction * 100.0:.0f}% done"

        status_task = commit_loop.create_task(set_status())
        await self.storage_handle.commit(storage_progress)
        try:
            status_task.cancel()
        except:
            pass
        try:
            await status_task
        except:
            pass

        self.storage_handle = None
        for lock in self.locks:
            lock.release()
        self.locks.clear()
        await self.notify.dispatch(self.queued_notifications)
        self.queued_notifications.clear()

    async def abort(self) -> None:
        await self.storage_handle.abort()
        self.storage_handle = None
        for lock in self.locks:
            lock.release()
        self.locks.clear()
        self.queued_notifications.clear()
        self.intents_to_acquire.clear()
        self.intents_to_release.clear()

    async def write_file(self, name: str) -> typing.BinaryIO:
        async with self.storage_handle as handle:
            return handle.write_file(name)

    async def remove_file(self, name: str) -> None:
        async with self.storage_handle as handle:
            return handle.remove_file(name)

    def lock_write(self, key: str, start: int, end: int) -> typing.Optional[str]:
        conflicting = self._check_intent_conflict(key, start, end)
        if conflicting:
            return conflicting

        try:
            lock = self.locker.acquire_write(self.generation, self, key, start, end)
        except LockDenied as e:
            return str(e.blocked)
        self.locks.append(lock)
        return None

    def send_notification(self, key: str, start: int, end: int) -> None:
        self.queued_notifications.append(self.notify.queue_notification(key, start, end))

    def acquire_intent(self, uid: int, key: str, start: int, end: int) -> None:
        self.intents_to_acquire[uid] = (key, start, end)
        self.intents_to_release.discard(uid)

    def release_intent(self, uid: int) -> None:
        self.intents_to_acquire.pop(uid, None)
        self.intents_to_release.add(uid)
