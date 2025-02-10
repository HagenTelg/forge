import typing
import asyncio
import logging
import struct
import os
import time
from forge.tasks import wait_cancelable
from forge.service import send_file_contents
from ..protocol import ProtocolError, Handshake, ClientPacket, ServerPacket, read_string, write_string
from .transaction import ReadTransaction, WriteTransaction

_LOGGER = logging.getLogger(__name__)


if typing.TYPE_CHECKING:
    from .control import Controller
    from .lock import ArchiveLocker


class _ConnectionLogger(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        return '[%s-"%s"] %s' % (self.extra['connection'].identifier, self.extra['connection'].name or "", msg), kwargs


class Connection:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, identifier: str):
        self.reader = reader
        self.writer = writer
        self.identifier = identifier
        self.control: Controller = None
        self.name: str = None
        self._logger = _ConnectionLogger(_LOGGER, {'connection': self})
        self._unsolicited = asyncio.Queue()

        self._transaction: typing.Optional[typing.Union[ReadTransaction, WriteTransaction]] = None
        self.next_notification_uid: int = 1
        self.next_intent_uid: int = 1

    def __repr__(self) -> str:
        return f"Connection({repr(self.identifier)}, {repr(self.name)})"

    @property
    def intent_status(self) -> str:
        if not self._transaction:
            return f"Waiting for {self.name or self.identifier}"
        return self._transaction.status

    @property
    def diagnostic_transaction_status(self) -> typing.Optional[typing.Dict]:
        if not self._transaction:
            return None
        if not self._transaction.storage_handle:
            return None
        result = {
            'status': self._transaction.status,
            'begin': int(self._transaction.begin_time * 1000),
            'generation': self._transaction.generation,
            'lock_count': len(self._transaction.locks),
        }
        if isinstance(self._transaction, ReadTransaction):
            result['type'] = 'read'
        else:
            result['type'] = 'write'
        return result

    @property
    def diagnostic_transaction_locks(self) -> typing.List["ArchiveLocker.Lock"]:
        if not self._transaction:
            return []
        return self._transaction.locks

    @property
    def diagnostic_transaction_details(self):
        if not self._transaction:
            return None
        result = {
            'status': self._transaction.status,
            'begin': int(self._transaction.begin_time * 1000),
            'generation': self._transaction.generation,
            'locks': [
                {
                    'key': lock.key,
                    'start': lock.start,
                    'end': lock.end,
                    'type': 'write' if lock.write else 'read',
                } for lock in self._transaction.locks
            ],
        }
        if isinstance(self._transaction, ReadTransaction):
            result['type'] = 'read'
        else:
            result['type'] = 'write'
            result['notifications'] = [
                {
                    'key': notification.key,
                    'start': notification.start,
                    'end': notification.end,
                } for notification in self._transaction.queued_notifications
            ]
            result['intent_acquire'] = [
                {
                    'key': intent[0],
                    'start': intent[1],
                    'end': intent[2],
                } for intent in self._transaction.intents_to_acquire.values()
            ]
            held = self._transaction.intent.get_held(self)
            release = list()
            for intent_uid in self._transaction.intents_to_release:
                intent = held.get(intent_uid)
                if not intent:
                    continue
                release.append({
                    'key': intent.key,
                    'start': intent.start,
                    'end': intent.end,
                })
            result['intent_release'] = release
        return result

    async def _drain_writer(self) -> None:
        await wait_cancelable(self.writer.drain(), 65.0)

    async def initialize(self, controller: "Controller") -> None:
        check = struct.unpack('<I', await self.reader.readexactly(4))[0]
        if check != Handshake.CLIENT_TO_SERVER.value:
            raise ProtocolError(f"Invalid handshake 0x{check:08X}")
        self.writer.write(struct.pack('<II', Handshake.SERVER_TO_CLIENT.value,
                                      Handshake.PROTOCOL_VERSION.value))
        await self._drain_writer()
        check = struct.unpack('<I', await self.reader.readexactly(4))[0]
        if check != Handshake.PROTOCOL_VERSION.value:
            raise ProtocolError(f"Invalid protocol version {check}")

        client_name = await read_string(self.reader)
        if not client_name:
            raise ProtocolError("No client name supplied")
        self.name = client_name

        check = struct.unpack('<I', await self.reader.readexactly(4))[0]
        if check != Handshake.CLIENT_READY:
            raise ProtocolError(f"Invalid ready handshake 0x{check:08X}")

        self.writer.write(struct.pack('<I', Handshake.SERVER_READY.value))
        await self._drain_writer()

        self.control = controller

    def queue_unsolicited(self, attach: "typing.Callable[..., typing.Awaitable]", *args, **kwargs) -> None:
        self._unsolicited.put_nowait((attach, args, kwargs))

    async def _process_packet(self, packet_type: ClientPacket) -> None:
        if packet_type == ClientPacket.HEARTBEAT:
            self.writer.write(struct.pack('<B', ServerPacket.HEARTBEAT.value))
            await self._drain_writer()
        elif packet_type == ClientPacket.TRANSACTION_BEGIN and not self._transaction:
            write = struct.unpack('<B', await self.reader.readexactly(1))[0]
            self.writer.write(struct.pack('<B', ServerPacket.TRANSACTION_STARTED.value))
            if write:
                self._transaction = await self.control.write_transaction()
                self._logger.debug("Write transaction (%d) started", self._transaction.generation)
            else:
                self._transaction = await self.control.read_transaction()
                self._logger.debug("Read transaction (%d) started", self._transaction.generation)
            self._transaction.intent_origin = self
            self._transaction.status = f"Waiting for {self.name or self.identifier}"
            self.writer.write(struct.pack('<B', 1))
        elif packet_type == ClientPacket.TRANSACTION_COMMIT and self._transaction:
            self._logger.debug("Committing transaction (%d)", self._transaction.generation)
            self.writer.write(struct.pack('<B', ServerPacket.TRANSACTION_COMPLETE.value))
            await self._transaction.commit()
            self._transaction = None
            self.writer.write(struct.pack('<B', 1))
            await self._drain_writer()
        elif packet_type == ClientPacket.TRANSACTION_ABORT and self._transaction:
            self._logger.debug("Aborting transaction (%d)", self._transaction.generation)
            self.writer.write(struct.pack('<B', ServerPacket.TRANSACTION_ABORTED.value))
            await self._transaction.abort()
            self._transaction = None
            self.writer.write(struct.pack('<B', 1))
            await self._drain_writer()
        elif packet_type == ClientPacket.SET_TRANSACTION_STATUS:
            status = await read_string(self.reader)
            if not self._transaction:
                self._logger.debug("Ignored non-transaction status set: %s", status)
            else:
                self._logger.debug("Transaction (%d) status set to: %s", self._transaction.generation, status)
                self._transaction.status = status
        elif packet_type == ClientPacket.READ_FILE and self._transaction:
            name = await read_string(self.reader)
            self.writer.write(struct.pack('<B', ServerPacket.READ_FILE_DATA.value))
            source = await self._transaction.read_file(name)
            if not source:
                self._logger.debug("File %s not found", name)
                self.writer.write(struct.pack('<B', 0))
                return
            try:
                st = os.stat(source.fileno())
                total_size = st.st_size
                self._logger.debug("Sending %d bytes from file %s", total_size, name)
                self.writer.write(struct.pack('<BQ', 1, total_size))
                await send_file_contents(source, self.writer, total_size)
            finally:
                source.close()
        elif packet_type == ClientPacket.WRITE_FILE and self._transaction:
            name = await read_string(self.reader)
            total_size = struct.unpack('<Q', await self.reader.readexactly(8))[0]
            self._logger.debug("Receiving %d bytes to file %s", total_size, name)
            self.writer.write(struct.pack('<B', ServerPacket.WRITE_FILE_DATA.value))
            destination = await self._transaction.write_file(name)
            try:
                while total_size > 0:
                    chunk = await self.reader.readexactly(min(total_size, 64 * 1024))
                    total_size -= len(chunk)
                    destination.write(chunk)
            finally:
                destination.close()
            self.writer.write(struct.pack('<B', 1))
        elif packet_type == ClientPacket.REMOVE_FILE and self._transaction:
            name = await read_string(self.reader)
            self._logger.debug("Removing file %s", name)
            self.writer.write(struct.pack('<B', ServerPacket.REMOVE_FILE_OK.value))
            await self._transaction.remove_file(name)
            self.writer.write(struct.pack('<B', 1))
        elif packet_type == ClientPacket.LOCK_READ and self._transaction:
            key = await read_string(self.reader)
            (start, end) = struct.unpack('<qq', await self.reader.readexactly(16))
            held = self._transaction.lock_read(key, start, end)
            if held is not None:
                self.writer.write(struct.pack('<B', ServerPacket.READ_LOCK_DENIED.value))
                write_string(self.writer, held or "")
            else:
                self.writer.write(struct.pack('<B', ServerPacket.READ_LOCK_ACQUIRED.value))
        elif packet_type == ClientPacket.LOCK_WRITE and self._transaction:
            key = await read_string(self.reader)
            (start, end) = struct.unpack('<qq', await self.reader.readexactly(16))
            held = self._transaction.lock_write(key, start, end)
            if held is not None:
                self.writer.write(struct.pack('<B', ServerPacket.WRITE_LOCK_DENIED.value))
                write_string(self.writer, held or "")
            else:
                self.writer.write(struct.pack('<B', ServerPacket.WRITE_LOCK_ACQUIRED.value))
        elif packet_type == ClientPacket.SEND_NOTIFICATION and self._transaction:
            key = await read_string(self.reader)
            (start, end) = struct.unpack('<qq', await self.reader.readexactly(16))
            self._transaction.send_notification(key, start, end)
            self.writer.write(struct.pack('<B', ServerPacket.NOTIFICATION_QUEUED.value))
        elif packet_type == ClientPacket.LISTEN_NOTIFICATION:
            key = await read_string(self.reader)
            synchronous = struct.unpack('<B', await self.reader.readexactly(1))[0]
            self.control.notify.listen(self, key, synchronous)
            self.writer.write(struct.pack('<B', ServerPacket.NOTIFICATION_LISTENING.value))
        elif packet_type == ClientPacket.ACKNOWLEDGE_NOTIFICATION:
            uid = struct.unpack('<Q', await self.reader.readexactly(8))[0]
            await self.control.notify.acknowledge(self, uid)
        elif packet_type == ClientPacket.ACQUIRE_INTENT:
            key = await read_string(self.reader)
            (start, end, immediate) = struct.unpack('<qqB', await self.reader.readexactly(17))
            uid = self.next_intent_uid
            self.next_intent_uid += 1
            if immediate == 0 and self._transaction:
                self._transaction.acquire_intent(uid, key, start, end)
            else:
                self.control.intent.acquire(self, uid, key, start, end)
            self.writer.write(struct.pack('<BQ', ServerPacket.INTENT_ACQUIRED.value, uid))
        elif packet_type == ClientPacket.RELEASE_INTENT:
            (uid, immediate) = struct.unpack('<QB', await self.reader.readexactly(9))
            if immediate == 0 and self._transaction:
                self._transaction.release_intent(uid)
            else:
                self.control.intent.release(self, uid)
            self.writer.write(struct.pack('<B', ServerPacket.INTENT_RELEASED.value))
        elif packet_type == ClientPacket.LIST_FILES:
            name = await read_string(self.reader)
            modified_after = struct.unpack('<d', await self.reader.readexactly(8))[0]
            self.writer.write(struct.pack('<B', ServerPacket.LIST_RESULT.value))
            contents = await self.control.storage.list_files(name, modified_after)
            _LOGGER.debug("Sending %d file entries from %s", len(contents), name)
            self.writer.write(struct.pack('<I', len(contents)))
            for c in contents:
                write_string(self.writer, c)
        else:
            raise ProtocolError(f"Invalid client packet type {packet_type}")

    async def run(self) -> None:
        self._logger.debug("Connection ready")
        tasks = set()
        heartbeat_receive_time = time.monotonic()
        try:
            packet_begin = None
            unsolicited_available = None
            while True:
                if not packet_begin:
                    packet_begin = asyncio.ensure_future(wait_cancelable(self.reader.readexactly(1), 65.0))
                    tasks.add(packet_begin)

                if not unsolicited_available:
                    unsolicited_available = asyncio.ensure_future(self._unsolicited.get())
                    tasks.add(unsolicited_available)

                done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                tasks = set(pending)

                if packet_begin in done:
                    try:
                        packet_type = ClientPacket(struct.unpack('<B', packet_begin.result())[0])
                        if packet_type == ClientPacket.CLOSE:
                            try:
                                self.writer.close()
                            except OSError:
                                pass
                            heartbeat_receive_time = time.monotonic()
                            raise EOFError
                    except asyncio.TimeoutError:
                        self._logger.warning("Connection timeout")
                        return
                    except (IOError, EOFError, ConnectionResetError, asyncio.IncompleteReadError):
                        self._logger.debug("Connection closed")
                        return
                    packet_begin = None
                    if packet_type == ClientPacket.HEARTBEAT:
                        heartbeat_receive_time = time.monotonic()
                    await self._process_packet(packet_type)

                if unsolicited_available in done:
                    send, args, kwargs = unsolicited_available.result()
                    unsolicited_available = None
                    await send(*args, **kwargs)
        finally:
            if time.monotonic() - heartbeat_receive_time > 15.0:
                self._logger.warning("Heartbeat lag detected")
            for c in tasks:
                try:
                    c.cancel()
                except:
                    pass
                try:
                    await c
                except:
                    pass

    def write_notification(self, key: str, start: int, end: int, synchronous: bool) -> int:
        if synchronous:
            uid = self.next_notification_uid
            self.next_notification_uid += 1
        else:
            uid = 0
        self.writer.write(struct.pack('<B', ServerPacket.NOTIFICATION_RECEIVED.value))
        write_string(self.writer, key)
        self.writer.write(struct.pack('<qqQ', start, end, uid))
        return uid

    def write_intent_hit(self, key: str, start: int, end: int) -> None:
        self.writer.write(struct.pack('<B', ServerPacket.INTENT_HIT.value))
        write_string(self.writer, key)
        self.writer.write(struct.pack('<qq', start, end))

    async def shutdown(self) -> None:
        self._logger.debug("Connection shutting down")

        await self.control.notify.disconnect(self)

        if self._transaction:
            self._logger.debug("Aborting active transaction (%d)", self._transaction.generation)
            await self._transaction.abort()
            self._transaction = None

        self.control.intent.disconnect(self)

        self.control = None
