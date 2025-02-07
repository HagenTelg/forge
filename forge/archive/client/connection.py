import typing
import asyncio
import logging
import struct
import os
import random
import time
from forge.tasks import wait_cancelable
from forge.service import send_file_contents
from ..protocol import ProtocolError, Handshake, ServerPacket, ClientPacket, read_string, write_string

_LOGGER = logging.getLogger(__name__)


class LockDenied(Exception):
    def __init__(self, status: str):
        self.status = status


class Connection:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, name: str):
        self.reader = reader
        self.writer = writer
        self.heartbeat_received = asyncio.Event()
        self.name = name
        self.log_extra: typing.Dict[str, typing.Any] = {
            'client_name': name
        }
        info = writer.get_extra_info('socket')
        if info:
            self.log_extra['fileno'] = info.fileno()
        info = writer.get_extra_info('sockname')
        if info:
            self.log_extra['socket'] = info

        self._request_queue = asyncio.Queue()
        self._callback_queue = asyncio.Queue()
        self._closed = asyncio.Event()
        self._response_handler: "typing.Optional[typing.Tuple[typing.Callable[[Connection, ServerPacket, ...], typing.Awaitable], typing.Tuple, typing.Dict, asyncio.Future]]" = None
        self._notification_handlers: "typing.Dict[str, typing.List[typing.Tuple[typing.Callable[[str, int, int, ...], typing.Awaitable], typing.Tuple, typing.Dict]]]" = dict()
        self._intent_handlers: "typing.Dict[str, typing.List[typing.Tuple[typing.Callable[[str, int, int, ...], typing.Awaitable], typing.Tuple, typing.Dict]]]" = dict()
        self._transaction_intents: "typing.Optional[typing.Dict[Connection.IntentHandle, bool]]" = None
        self._internal_run: typing.Optional[asyncio.Task] = None

    @classmethod
    async def default_connection(cls, name: str, use_environ: bool = True) -> "Connection":
        from forge.archive import CONFIGURATION, DEFAULT_ARCHIVE_TCP_PORT
        from starlette.datastructures import URL

        def try_url(url: URL) -> typing.Optional[typing.Awaitable[typing.Tuple[asyncio.StreamReader, asyncio.StreamWriter]]]:
            if url.path and url.scheme == 'unix':
                return asyncio.open_unix_connection(url.path)
            elif url.hostname or url.scheme == 'tcp':
                return asyncio.open_connection(
                    url.hostname,
                    url.port or int(CONFIGURATION.get("ARCHIVE.PORT", DEFAULT_ARCHIVE_TCP_PORT))
                )
            return None

        def try_direct(server: typing.Optional[str]) -> typing.Optional[typing.Awaitable[typing.Tuple[asyncio.StreamReader, asyncio.StreamWriter]]]:
            if not server:
                return None
            if server.startswith('/'):
                return asyncio.open_unix_connection(server)
            else:
                return asyncio.open_connection(
                    server,
                    int(CONFIGURATION.get("ARCHIVE.PORT", DEFAULT_ARCHIVE_TCP_PORT))
                )

        def find_connection() -> typing.Awaitable[typing.Tuple[asyncio.StreamReader, asyncio.StreamWriter]]:
            try:
                url = URL(url=CONFIGURATION.ARCHIVE.URL)
                connect = try_url(url)
                if connect:
                    return connect
            except AttributeError:
                pass

            try:
                server = CONFIGURATION.ARCHIVE.SERVER
                connect = try_url(URL(url=server))
                if connect:
                    return connect
                connect = try_direct(server)
                if connect:
                    return connect
            except AttributeError:
                pass

            try:
                socket = CONFIGURATION.ARCHIVE.SOCKET
                if socket:
                    return asyncio.open_unix_connection(socket)
            except AttributeError:
                pass

            if use_environ:
                server = os.environ.get("FORGE_ARCHIVE")
                connect = try_url(URL(url=server))
                if connect:
                    return connect
                connect = try_direct(server)
                if connect:
                    return connect

            return asyncio.open_unix_connection('/run/forge-archive.socket')

        reader, writer = await find_connection()
        return cls(reader, writer, name)

    def __repr__(self) -> str:
        return f"Connection({repr(self.name)})"

    async def _drain_writer(self) -> None:
        await wait_cancelable(self.writer.drain(), 30.0)

    async def _initialize(self) -> None:
        self.writer.write(struct.pack('<I', Handshake.CLIENT_TO_SERVER.value))
        await self._drain_writer()

        (check, version) = struct.unpack('<II', await self.reader.readexactly(8))
        if check != Handshake.SERVER_TO_CLIENT.value:
            raise ProtocolError(f"Invalid handshake 0x{check:08X}")
        if version != Handshake.PROTOCOL_VERSION.value:
            raise ProtocolError(f"Invalid protocol version {version}")

        self.writer.write(struct.pack('<I', Handshake.PROTOCOL_VERSION.value))
        write_string(self.writer, self.name)
        self.writer.write(struct.pack('<I', Handshake.CLIENT_READY.value))
        await self._drain_writer()

        check = struct.unpack('<I', await self.reader.readexactly(4))[0]
        if check != Handshake.SERVER_READY:
            raise ProtocolError(f"Invalid ready handshake 0x{check:08X}")

    async def startup(self) -> None:
        await self._initialize()
        self._internal_run = asyncio.get_event_loop().create_task(self.run())

    async def _request_response(self,
                                request: "typing.Callable[[Connection, ...], typing.Awaitable]",
                                response: "typing.Optional[typing.Callable[[Connection, ServerPacket, ...], typing.Awaitable]]",
                                *args, **kwargs) -> typing.Any:
        completed = asyncio.Future()

        await self._request_queue.put((
            request,
            response,
            args, kwargs,
            completed
        ))

        wait_closed = asyncio.ensure_future(self._closed.wait())
        tasks = [completed, wait_closed]
        try:
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        except asyncio.CancelledError:
            for t in tasks:
                try:
                    t.cancel()
                except:
                    pass
            for t in tasks:
                try:
                    await t
                except:
                    pass
            raise

        for c in pending:
            try:
                c.cancel()
            except:
                pass
            try:
                await c
            except asyncio.CancelledError:
                pass
            except:
                _LOGGER.debug("Error in request pending task", extra=self.log_extra, exc_info=True)
                raise
        for d in done:
            try:
                await d
            except:
                _LOGGER.debug("Error in request completed task", extra=self.log_extra, exc_info=True)
                raise

        if completed not in done:
            raise EOFError

        return completed.result()

    def _callback(self, call: typing.Callable[..., typing.Awaitable], *args, **kwargs) -> None:
        self._callback_queue.put_nowait((call, args, kwargs))

    async def _process_packet(self, packet_type: ServerPacket) -> None:
        if self._response_handler:
            response, args, kwargs, completed = self._response_handler
            try:
                fut = asyncio.ensure_future(response(self, packet_type, *args, **kwargs))
                try:
                    data = await fut
                    fut = None
                finally:
                    if fut is not None:
                        try:
                            fut.cancel()
                        except:
                            pass
                        try:
                            await fut
                        except asyncio.CancelledError:
                            pass
                        except:
                            _LOGGER.debug("Exception in response cancellation", exc_info=True, extra=self.log_extra)
                            pass
                if data is not None:
                    if not completed.done():
                        # Might have been canceled due to the caller being canceled itself
                        completed.set_result(data)
                    self._response_handler = None
                    return
            except Exception as e:
                if not completed.done():
                    # Might have been canceled due to the caller being canceled itself
                    completed.set_exception(e)
                _LOGGER.debug("Exception in response handler", exc_info=True, extra=self.log_extra)
                raise

        if packet_type == ServerPacket.HEARTBEAT:
            self.heartbeat_received.set()
        elif packet_type == ServerPacket.NOTIFICATION_RECEIVED:
            key = await read_string(self.reader)
            (start, end, uid) = struct.unpack('<qqQ', await self.reader.readexactly(8 * 3))
            self._callback(self._process_notification, key, start, end, uid)
        elif packet_type == ServerPacket.INTENT_HIT:
            key = await read_string(self.reader)
            (start, end) = struct.unpack('<qq', await self.reader.readexactly(16))
            targets = self._intent_handlers.get(key)
            if targets:
                for call, args, kwargs in targets:
                    self._callback(call, key, start, end, *args, **kwargs)
        else:
            raise ProtocolError(f"Invalid client packet type {packet_type}")

    async def _process_notification(self, key: str, start: int, end: int, uid: int) -> None:
        targets = self._notification_handlers.get(key)
        if targets:
            for call, args, kwargs in list(targets):
                await call(key, start, end, *args, **kwargs)

        async def send_ack(connection: "Connection", uid: int):
            connection.writer.write(struct.pack('<BQ', ClientPacket.ACKNOWLEDGE_NOTIFICATION.value, uid))

        if uid != 0:
            await self._request_response(send_ack, None, uid)

    async def run(self) -> None:
        _LOGGER.debug("Connection ready", extra=self.log_extra)
        tasks = set()
        heartbeat_send_time = time.monotonic()
        try:
            packet_begin = None
            request_available = None
            callback_available = None
            callback_running = None
            send_heartbeat = None
            awaiting_heartbeat: int = 0
            while True:
                if not packet_begin:
                    packet_begin = asyncio.ensure_future(wait_cancelable(self.reader.readexactly(1), 30.0))
                    tasks.add(packet_begin)

                if not request_available and not self._response_handler:
                    request_available = asyncio.ensure_future(self._request_queue.get())
                    tasks.add(request_available)

                if not callback_available and not callback_running:
                    callback_available = asyncio.ensure_future(self._callback_queue.get())
                    tasks.add(callback_available)

                if not send_heartbeat:
                    send_heartbeat = asyncio.ensure_future(asyncio.sleep(10.0))
                    tasks.add(send_heartbeat)

                done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                tasks = set(pending)

                if packet_begin in done:
                    try:
                        packet_type = ServerPacket(struct.unpack('<B', packet_begin.result())[0])
                    except asyncio.TimeoutError:
                        _LOGGER.error("Archive server timeout", extra=self.log_extra)
                        return
                    except (IOError, EOFError, ConnectionResetError, asyncio.IncompleteReadError):
                        _LOGGER.debug("Connection closed", extra=self.log_extra)
                        return
                    packet_begin = None
                    await self._process_packet(packet_type)
                    if packet_type == ServerPacket.HEARTBEAT:
                        awaiting_heartbeat = 0

                if request_available in done:
                    request, response, args, kwargs, completed = request_available.result()
                    request_available = None
                    try:
                        await request(self, *args, **kwargs)
                        if response:
                            self._response_handler = (response, args, kwargs, completed)
                        else:
                            completed.set_result(None)
                    except (IOError, EOFError, ConnectionResetError) as e:
                        completed.set_exception(e)
                        _LOGGER.debug("Connection closed", extra=self.log_extra)
                        return
                    except Exception as e:
                        completed.set_exception(e)
                        _LOGGER.debug("Exception in request handler", exc_info=True, extra=self.log_extra)
                        raise

                if callback_available in done:
                    cb, args, kwargs = callback_available.result()
                    callback_available = None
                    callback_running = asyncio.ensure_future(cb(*args, **kwargs))
                    tasks.add(callback_running)

                if callback_running in done:
                    callback_running = None

                if send_heartbeat in done:
                    send_heartbeat = None
                    if awaiting_heartbeat < 10:
                        self.writer.write(struct.pack('<B', ClientPacket.HEARTBEAT.value))
                        await self._drain_writer()
                        awaiting_heartbeat += 1
                        heartbeat_send_time = time.monotonic()
        except asyncio.CancelledError:
            raise
        except:
            _LOGGER.debug("Error in connection processing", extra=self.log_extra, exc_info=True)
            raise
        finally:
            if time.monotonic() - heartbeat_send_time > 15.0:
                _LOGGER.warning("Heartbeat lag detected", extra=self.log_extra)
            for c in tasks:
                try:
                    c.cancel()
                except:
                    pass
                try:
                    await c
                except:
                    pass
            _LOGGER.debug("Connection processing ended", extra=self.log_extra)
            self._closed.set()

    async def shutdown(self) -> None:
        _LOGGER.debug("Connection shutting down", extra=self.log_extra)

        async def request(connection: "Connection"):
            try:
                connection.writer.write(struct.pack('<B', ClientPacket.CLOSE.value))
            except:
                try:
                    connection.writer.close()
                except:
                    pass
                raise

            try:
                await connection._drain_writer()
            except:
                try:
                    connection.writer.close()
                except:
                    pass
                raise

            connection.writer.close()
            raise EOFError

        completed = asyncio.Future()
        await self._request_queue.put((
            request,
            None,
            [], {},
            completed
        ))
        wait_closed = asyncio.ensure_future(self._closed.wait())
        done, pending = await asyncio.wait([completed, wait_closed], return_when=asyncio.FIRST_COMPLETED)
        for c in pending:
            try:
                c.cancel()
            except:
                pass
            try:
                await c
            except (asyncio.CancelledError, IOError, EOFError, ConnectionResetError):
                pass
            except:
                _LOGGER.debug("Error in shutdown pending task", extra=self.log_extra, exc_info=True)
        for d in done:
            try:
                await d
            except (IOError, EOFError, ConnectionResetError):
                pass
            except:
                _LOGGER.debug("Error in shutdown task", extra=self.log_extra, exc_info=True)

        if self._internal_run:
            try:
                await self._internal_run
            except (asyncio.CancelledError, IOError, EOFError, ConnectionResetError):
                pass
            except:
                _LOGGER.debug("Exception in run shutdown", extra=self.log_extra, exc_info=True)
            self._internal_run = None

    def abort(self) -> None:
        if self._internal_run:
            try:
                self._internal_run.cancel()
            except:
                pass
            self._internal_run = None
        try:
            self.writer.close()
        except:
            pass

    async def list_files(self, path: str, modified_after: float = 0) -> typing.List[str]:
        async def request(connection: "Connection", path: str, modified_after: float):
            connection.writer.write(struct.pack('<B', ClientPacket.LIST_FILES))
            write_string(connection.writer, path)
            connection.writer.write(struct.pack('<d', modified_after))

        async def response(connection: "Connection", packet_type: ServerPacket, path: str, modified_after: float):
            if packet_type != ServerPacket.LIST_RESULT:
                return None
            result_length = struct.unpack('<I', await connection.reader.readexactly(4))[0]
            result_list: typing.List[str] = list()
            for _ in range(result_length):
                result_list.append(await read_string(connection.reader))
            return result_list

        return await self._request_response(request, response, path, modified_after)

    async def transaction_begin(self, write: bool) -> None:
        assert self._transaction_intents is None

        async def request(connection: "Connection"):
            connection.writer.write(struct.pack('<BB', ClientPacket.TRANSACTION_BEGIN.value, write and 1 or 0))

        async def response(connection: "Connection", packet_type: ServerPacket):
            if packet_type != ServerPacket.TRANSACTION_STARTED:
                return None
            return struct.unpack('<B', await connection.reader.readexactly(1))[0]

        if await self._request_response(request, response) != 1:
            raise IOError
        self._transaction_intents = dict()

    async def transaction_commit(self) -> typing.List["Connection.IntentHandle"]:
        assert self._transaction_intents is not None

        async def request(connection: "Connection"):
            connection.writer.write(struct.pack('<B', ClientPacket.TRANSACTION_COMMIT.value))

        async def response(connection: "Connection", packet_type: ServerPacket):
            if packet_type != ServerPacket.TRANSACTION_COMPLETE:
                return None
            return struct.unpack('<B', await connection.reader.readexactly(1))[0]

        if await self._request_response(request, response) != 1:
            intents = self._transaction_intents
            self._transaction_intents = None
            unreleased = list()
            for intent, acquire in intents.items():
                if not acquire and intent._realized:
                    unreleased.append(intent)
            raise IOError
        intents = self._transaction_intents
        self._transaction_intents = None
        acquired = list()
        for intent, acquire in intents.items():
            if acquire:
                intent._realized = True
                acquired.append(intent)
            else:
                intent._realized = False
        return acquired

    async def transaction_abort(self) -> typing.List["Connection.IntentHandle"]:
        assert self._transaction_intents is not None

        async def request(connection: "Connection"):
            connection.writer.write(struct.pack('<B', ClientPacket.TRANSACTION_ABORT.value))

        async def response(connection: "Connection", packet_type: ServerPacket):
            if packet_type != ServerPacket.TRANSACTION_ABORTED:
                return None
            return struct.unpack('<B', await connection.reader.readexactly(1))[0]

        r = await self._request_response(request, response)
        intents = self._transaction_intents
        self._transaction_intents = None
        unreleased = list()
        for intent, acquire in intents.items():
            if not acquire and intent._realized:
                unreleased.append(intent)
        if r != 1:
            raise IOError
        return unreleased

    class _TransactionContext:
        def __init__(self, connection: "Connection", write: bool):
            self.connection = connection
            self.write = write

        async def __aenter__(self) -> "Connection":
            await self.connection.transaction_begin(self.write)
            return self.connection

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            if exc_type is not None:
                await self.connection.transaction_abort()
            else:
                await self.connection.transaction_commit()

    def transaction(self, write: bool = False) -> "Connection._TransactionContext":
        return self._TransactionContext(self, write)

    async def __aenter__(self) -> "Connection":
        await self.startup()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.shutdown()

    async def set_transaction_status(self, status: str):
        async def request(connection: "Connection", status: str):
            connection.writer.write(struct.pack('<B', ClientPacket.SET_TRANSACTION_STATUS.value))
            write_string(connection.writer, status)

        await self._request_response(request, None, status)

    async def read_data(self, name: str, writer: typing.Callable[[bytes], typing.Awaitable]) -> None:
        async def request(connection: "Connection"):
            connection.writer.write(struct.pack('<B', ClientPacket.READ_FILE.value))
            write_string(connection.writer, name)

        async def response(connection: "Connection", packet_type: ServerPacket):
            if packet_type != ServerPacket.READ_FILE_DATA:
                return None
            status = struct.unpack('<B', await connection.reader.readexactly(1))[0]
            if status == 1:
                total_size = struct.unpack('<Q', await connection.reader.readexactly(8))[0]
                while total_size > 0:
                    chunk = await connection.reader.readexactly(min(total_size, 64 * 1024))
                    total_size -= len(chunk)
                    await writer(chunk)
                return 1
            else:
                return 2

        r = await self._request_response(request, response)
        if r == 2:
            raise FileNotFoundError

    async def read_file(self, name: str, destination: typing.BinaryIO) -> None:
        async def writer(data: bytes) -> None:
            destination.write(data)

        await self.read_data(name, writer)

    async def read_bytes(self, name: str) -> bytes:
        result = bytearray()

        async def writer(data: bytes) -> None:
            nonlocal result
            result += data

        await self.read_data(name, writer)
        return bytes(result)

    async def write_data(self, name: str, total_size: int,
                         reader: typing.Callable[[int], typing.Awaitable[bytes]]) -> None:
        async def request(connection: "Connection"):
            connection.writer.write(struct.pack('<B', ClientPacket.WRITE_FILE.value))
            write_string(connection.writer, name)
            connection.writer.write(struct.pack('<Q', total_size))
            remaining = total_size
            while remaining > 0:
                chunk = await reader(remaining)
                remaining -= len(chunk)
                connection.writer.write(chunk)

        async def response(connection: "Connection", packet_type: ServerPacket):
            if packet_type != ServerPacket.WRITE_FILE_DATA:
                return None
            return struct.unpack('<B', await connection.reader.readexactly(1))[0]

        if await self._request_response(request, response) != 1:
            raise IOError

    async def write_file(self, name: str, source: typing.BinaryIO) -> None:
        async def request(connection: "Connection"):
            st = os.stat(source.fileno())
            total_size = st.st_size
            source.seek(0)

            connection.writer.write(struct.pack('<B', ClientPacket.WRITE_FILE.value))
            write_string(connection.writer, name)
            connection.writer.write(struct.pack('<Q', total_size))
            await send_file_contents(source, connection.writer, total_size)

        async def response(connection: "Connection", packet_type: ServerPacket):
            if packet_type != ServerPacket.WRITE_FILE_DATA:
                return None
            return struct.unpack('<B', await connection.reader.readexactly(1))[0]

        if await self._request_response(request, response) != 1:
            raise IOError

    async def write_bytes(self, name: str, source: typing.Union[bytes, bytearray]):
        async def request(connection: "Connection"):
            connection.writer.write(struct.pack('<B', ClientPacket.WRITE_FILE.value))
            write_string(connection.writer, name)
            connection.writer.write(struct.pack('<Q', len(source)))
            connection.writer.write(source)

        async def response(connection: "Connection", packet_type: ServerPacket):
            if packet_type != ServerPacket.WRITE_FILE_DATA:
                return None
            return struct.unpack('<B', await connection.reader.readexactly(1))[0]

        if await self._request_response(request, response) != 1:
            raise IOError

    async def remove_file(self, name: str) -> None:
        async def request(connection: "Connection"):
            connection.writer.write(struct.pack('<B', ClientPacket.REMOVE_FILE.value))
            write_string(connection.writer, name)

        async def response(connection: "Connection", packet_type: ServerPacket):
            if packet_type != ServerPacket.REMOVE_FILE_OK:
                return None
            return struct.unpack('<B', await connection.reader.readexactly(1))[0]

        if await self._request_response(request, response) != 1:
            raise IOError

    async def lock_read(self, key: str, start: int, end: int) -> None:
        async def request(connection: "Connection"):
            connection.writer.write(struct.pack('<B', ClientPacket.LOCK_READ.value))
            write_string(connection.writer, key)
            connection.writer.write(struct.pack('<qq', start, end))

        async def response(connection: "Connection", packet_type: ServerPacket):
            if packet_type == ServerPacket.READ_LOCK_ACQUIRED:
                return 1
            if packet_type == ServerPacket.READ_LOCK_DENIED:
                return await read_string(connection.reader)
            return None

        r = await self._request_response(request, response)
        if r != 1:
            raise LockDenied(r)

    async def lock_write(self, key: str, start: int, end: int) -> None:
        async def request(connection: "Connection"):
            connection.writer.write(struct.pack('<B', ClientPacket.LOCK_WRITE.value))
            write_string(connection.writer, key)
            connection.writer.write(struct.pack('<qq', start, end))

        async def response(connection: "Connection", packet_type: ServerPacket):
            if packet_type == ServerPacket.WRITE_LOCK_ACQUIRED:
                return 1
            if packet_type == ServerPacket.WRITE_LOCK_DENIED:
                return await read_string(connection.reader)
            return None

        r = await self._request_response(request, response)
        if r != 1:
            raise LockDenied(r)

    class IntentHandle:
        def __init__(self, connection: "Connection", uid: int):
            self._connection = connection
            self._uid = uid
            self._realized = False

        async def release(self, immediate: bool = False) -> None:
            async def request(connection: "Connection", intent: "Connection.IntentHandle"):
                connection.writer.write(struct.pack('<BQB', ClientPacket.RELEASE_INTENT.value,
                                                    intent._uid, 1 if immediate else 0))

            async def response(connection: "Connection", packet_type: ServerPacket, intent: "Connection.IntentHandle"):
                return packet_type == ServerPacket.INTENT_RELEASED or None

            await self._connection._request_response(request, response, self)

            if not immediate and self._connection._transaction_intents is not None:
                self._connection._transaction_intents[self] = False
                return
            if not self._realized:
                _LOGGER.debug("Duplicate intent release (%d)", self._uid, extra=self._connection.log_extra)
                return
            self._realized = False

        def __del__(self):
            if self._realized:
                _LOGGER.error("Leaked intent (%d)", self._uid, extra=self._connection.log_extra)
                self._realized = False

    async def acquire_intent(self, key: str, start: int, end: int, immediate: bool = False) -> "Connection.IntentHandle":
        async def request(connection: "Connection"):
            connection.writer.write(struct.pack('<B', ClientPacket.ACQUIRE_INTENT.value))
            write_string(connection.writer, key)
            connection.writer.write(struct.pack('<qqB', start, end, 1 if immediate else 0))

        async def response(connection: "Connection", packet_type: ServerPacket):
            if packet_type != ServerPacket.INTENT_ACQUIRED:
                return None
            uid = struct.unpack('<Q', await connection.reader.readexactly(8))[0]

            intent = connection.IntentHandle(connection, uid)
            if not immediate and connection._transaction_intents is not None:
                connection._transaction_intents[intent] = True
            else:
                intent._realized = True
            return intent

        return await self._request_response(request, response)

    async def send_notification(self, key: str, start: int, end: int) -> None:
        async def request(connection: "Connection"):
            connection.writer.write(struct.pack('<B', ClientPacket.SEND_NOTIFICATION.value))
            write_string(connection.writer, key)
            connection.writer.write(struct.pack('<qq', start, end))

        async def response(connection: "Connection", packet_type: ServerPacket) -> bool:
            return packet_type == ServerPacket.NOTIFICATION_QUEUED or None

        await self._request_response(request, response)

    async def listen_notification(self, key: str, handler: "typing.Callable[[str, int, int, ...], typing.Awaitable]",
                                  *args, synchronous: bool = True, **kwargs) -> None:
        target = self._notification_handlers.get(key)
        if target is not None:
            target.append((handler, args, kwargs))
            return
        target = list()
        self._notification_handlers[key] = target
        target.append((handler, args, kwargs))

        async def request(connection: "Connection"):
            connection.writer.write(struct.pack('<B', ClientPacket.LISTEN_NOTIFICATION.value))
            write_string(connection.writer, key)
            connection.writer.write(struct.pack('<B', 1 if synchronous else 0))

        async def response(connection: "Connection", packet_type: ServerPacket):
            return packet_type == ServerPacket.NOTIFICATION_LISTENING or None

        await self._request_response(request, response)

    async def listen_intent(self, key: str, handler: "typing.Callable[[str, int, int, ...], typing.Awaitable]",
                            *args, **kwargs) -> None:
        target = self._intent_handlers.get(key)
        if target is None:
            target = list()
            self._intent_handlers[key] = target
        target.append((handler, args, kwargs))

    async def periodic_watchdog(self, interval: float,
                                heartbeat_timeout: typing.Union[float, typing.Callable[[], float]] = 30,
                                request_timeout: typing.Union[float, typing.Callable[[], float]] = 600,
                                immediate: bool = False) -> typing.AsyncIterable[None]:
        assert interval > 0.001

        def to_timeout(t: typing.Union[float, typing.Callable[[], float]]) -> float:
            if callable(t):
                return t()
            return t

        now = time.monotonic()
        last_connection_heartbeat = now
        next_returned_heartbeat = now
        if not immediate:
            next_returned_heartbeat += interval
        while True:
            now = time.monotonic()

            if self._response_handler:
                connection_heartbeat_timeout = last_connection_heartbeat + to_timeout(request_timeout)
            else:
                connection_heartbeat_timeout = last_connection_heartbeat + to_timeout(heartbeat_timeout)

            heartbeat_wait_time = connection_heartbeat_timeout - now
            if heartbeat_wait_time < 0.001:
                _LOGGER.warning("Watchdog heartbeat timeout, stalling for heartbeat response")
                await self.heartbeat_received.wait()
                self.heartbeat_received.clear()

                now = time.monotonic()
                last_connection_heartbeat = now
                if now < next_returned_heartbeat:
                    yield None
                    next_returned_heartbeat = now + interval
                continue

            returned_wait_time = next_returned_heartbeat - now
            if returned_wait_time < 0.001:
                yield None
                next_returned_heartbeat = now + interval
                returned_wait_time = interval

            total_wait_time = max(min(heartbeat_wait_time, returned_wait_time), 0.001)
            try:
                await wait_cancelable(self.heartbeat_received.wait(), total_wait_time)
                self.heartbeat_received.clear()
                last_connection_heartbeat = time.monotonic()
            except asyncio.TimeoutError:
                pass


class LockBackoff:
    def __init__(self):
        self.failure_count: int = 0

    @property
    def minimum_wait(self) -> float:
        if self.failure_count <= 10:
            return 0.0
        return max(self.maximum_wait * 0.1, 0.1)

    @property
    def maximum_wait(self) -> float:
        if self.failure_count <= 10:
            return 0.25
        return min(self.failure_count * 0.1, 5.0)

    @property
    def has_failed(self) -> bool:
        return self.failure_count == 0

    def failed(self) -> None:
        self.failure_count += 1

    def clear(self) -> None:
        self.failure_count = 0

    async def __call__(self) -> None:
        self.failed()
        await asyncio.sleep(self.wait_time)

    @property
    def wait_time(self) -> float:
        lower_bound = self.minimum_wait
        upper_bound = self.maximum_wait
        if upper_bound <= lower_bound:
            return lower_bound
        return random.uniform(lower_bound, upper_bound)
