import typing
import asyncio
import logging
import time
import datetime
import os
import re
from collections import OrderedDict
from pathlib import Path
from json import load as from_json, dump as to_json
from math import floor, ceil
from forge.range import FindIntersecting, Merge as RangeMerge
from forge.const import STATIONS, MAX_I64
from forge.logicaltime import year_bounds_ms
from forge.tasks import wait_cancelable
from forge.dashboard.report import report_ok, report_failed
from ..client.connection import Connection, LockDenied, LockBackoff

_LOGGER = logging.getLogger(__name__)


class UpdateManager:
    STATE_VERSION = 1

    class _Pending:
        def __init__(self, manager: "UpdateManager", start: int, end: int):
            self.manager = manager
            self.start = start
            self.end = end
            self.intents: typing.List[Connection.IntentHandle] = list()

        async def acquire(self) -> None:
            for key in self.manager.intent_keys:
                self.intents.append(await self.manager.connection.acquire_intent(key, self.start, self.end, True))

    class _Intersecting(FindIntersecting):
        def __init__(self, manager: "UpdateManager"):
            self.manager = manager

        @property
        def canonical(self) -> bool:
            return True

        def __len__(self) -> int:
            return len(self.manager._pending)

        def get_start(self, index: int) -> typing.Union[int, float]:
            return self.manager._pending[index].start

        def get_end(self, index: int) -> typing.Union[int, float]:
            return self.manager._pending[index].end

    def __init__(self, connection: Connection):
        self.connection = connection
        self._pending: typing.List[UpdateManager._Pending] = list()
        self._do_update: typing.Dict[UpdateManager._Pending, int] = OrderedDict()
        self._lock: asyncio.Lock = None

        self._intersecting = self._Intersecting(self)
        self._shutdown_in_progress: bool = False

    @property
    def state_file(self) -> Path:
        raise NotImplementedError

    @property
    def listen_keys(self) -> typing.Iterable[str]:
        raise NotImplementedError

    @property
    def intent_keys(self) -> typing.Iterable[str]:
        raise NotImplementedError

    def round_notification(self, key: str, start: int, end: int) -> typing.Tuple[int, int]:
        return start, end

    async def get_modified(self, modified_after: float) -> typing.List[typing.Tuple[int, int]]:
        return []

    def notify_update_ready(self) -> None:
        pass

    async def initialize(self) -> None:
        self._lock = asyncio.Lock()

        for key in self.listen_keys:
            await self.connection.listen_notification(key, self._notification_received)
        for key in self.intent_keys:
            await self.connection.listen_intent(key, self._intent_hit)

    async def load_existing(self) -> None:
        try:
            with open(self.state_file, "r") as f:
                if os.fstat(f.fileno()).st_size == 0:
                    raise FileNotFoundError
                state = from_json(f)
        except FileNotFoundError:
            state = None
        state_modified: typing.Optional[float] = None
        if state:
            state_version = state.get('version')
            if state.get('version') != self.STATE_VERSION:
                raise RuntimeError(f"Unsupported state version {state_version} vs {self.STATE_VERSION}")
            state_modified = state['modified']

            async with self._lock:
                _LOGGER.debug("Found %d state pending", len(state['pending']))
                for p in state['pending']:
                    await self._install_pending(p[0], p[1], save_state=False)

        modified = await self.get_modified(state_modified or 0)
        async with self._lock:
            _LOGGER.debug("Found %d modified pending", len(modified))
            for start, end in modified:
                await self._install_pending(start, end, save_state=False)

            _LOGGER.debug("Finished loading with %d pending updates", len(self._pending))

        # Now with any updates from merges or modified files
        self._save_state()

    async def shutdown(self) -> None:
        async with self._lock:
            self._shutdown_in_progress = True
            self._save_state()

            for p in self._pending:
                for i in p.intents:
                    await i.release(True)
                p.intents.clear()

    async def flush(self, start: int = -MAX_I64, end: int = MAX_I64) -> None:
        any_hit = False
        async with self._lock:
            for hit in self._intersecting(start, end):
                self._do_update[self._pending[hit]] = hit
                any_hit = True
        if any_hit:
            self.notify_update_ready()

    class _NextUpdate:
        def __init__(self, manager: "UpdateManager", active: "UpdateManager._Pending"):
            self._manager = manager
            self._active: typing.Optional["UpdateManager._Pending"] = active

        async def __aenter__(self) -> typing.Tuple[int, int]:
            return self._active.start, self._active.end

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            active = self._active
            self._active = None
            if not active:
                raise RuntimeError("Duplicate context exit")

            if exc_type is not None:
                # If we've failed, re-install the pending then release the replaced intents
                async with self._manager._lock:
                    await self._manager._install_pending(active.start, active.end, force_update=True)
                for i in active.intents:
                    await i.release(True)
                active.intents.clear()
                return

            # Active intents cleared by transaction exit, and pending itself no longer relevant (update completed)
            active.intents.clear()

            async with self._manager._lock:
                if not self._manager._shutdown_in_progress:
                    self._manager._save_state()

        def __del__(self):
            if self._active:
                raise RuntimeError("Leaked pending update (no context manger entry)")

    async def has_update(self) -> bool:
        async with self._lock:
            return bool(self._pending)

    async def next_update(self) -> typing.Optional["UpdateManager._NextUpdate"]:
        async with self._lock:
            try:
                active, idx = next(iter(self._do_update.items()))
            except StopIteration:
                return None
            update = self._NextUpdate(self, active)
            self._do_update.pop(active)

            # Only search the whole pending if it has moved since being queued
            if idx >= len(self._pending) or self._pending[idx] != active:
                idx = self._pending.index(active)
            del self._pending[idx]

            # Renumber any remaining queued (otherwise the above optimization may only work once)
            for key, update_idx in reversed(list(self._do_update.items())):
                if update_idx > idx:
                    update_idx -= 1
                self._do_update[key] = update_idx

            # We're in a transaction, so queue intents for release
            for i in active.intents:
                await i.release()

            return update

    def _save_state(self, sync: bool = False) -> None:
        state_contents: typing.Dict[str, typing.Any] = {
            'version': self.STATE_VERSION,
            'modified': int(floor(time.time())),
            'pending': [[p.start, p.end] for p in self._pending],
        }

        with open(self.state_file, "w") as f:
            to_json(state_contents, f)

            if sync:
                try:
                    f.flush()
                    os.fdatasync(f.fileno())
                except AttributeError:
                    os.fsync(f.fileno())

    async def _install_pending(self, start: int, end: int, save_state: bool = True, force_update: bool = False) -> None:
        class Merge(RangeMerge):
            def __init__(self, manager: "UpdateManager"):
                self.manager = manager
                self.to_release: typing.List["UpdateManager._Pending"] = list()
                self.inserted_index: int = None

            @property
            def canonical(self) -> bool:
                return True

            def __len__(self) -> int:
                return len(self.manager._pending)

            def __delitem__(self, key: typing.Union[slice, int]) -> None:
                if isinstance(key, slice):
                    self.to_release.extend(self.manager._pending[key])
                else:
                    self.to_release.append(self.manager._pending[key])
                del self.manager._pending[key]

            def get_start(self, index: int) -> typing.Union[int, float]:
                return self.manager._pending[index].start

            def get_end(self, index: int) -> typing.Union[int, float]:
                return self.manager._pending[index].end

            def insert(self, index: int, start: int, end: int) -> typing.Any:
                new_pending = self.manager._Pending(self.manager, start, end)
                self.manager._pending.insert(index, new_pending)
                self.inserted_index = index
                return new_pending

            def merge_contained(self, index: int) -> typing.Any:
                return None

        merge = Merge(self)
        new_pending = merge(start, end)
        if not new_pending:
            _LOGGER.debug("Already have pending containing %d,%d", start, end)
            return
        if save_state:
            self._save_state(sync=True)

        _LOGGER.debug("Acquiring pending %d,%d that replaces %d existing", start, end, len(merge.to_release))

        # Acquire new intents, then release the replaced ones
        await new_pending.acquire()
        for p in merge.to_release:
            for i in p.intents:
                await i.release(True)
            p.intents.clear()
            was_updated = self._do_update.pop(p, None)
            if was_updated:
                self._do_update[new_pending] = merge.inserted_index
        if force_update:
            self._do_update[new_pending] = merge.inserted_index

    async def _notification_received(self, key: str, start: int, end: int) -> None:
        async with self._lock:
            if self._shutdown_in_progress:
                _LOGGER.debug(f"Ignoring notification during shutdown %s,%d,%d", key, start, end)
                return

            _LOGGER.debug(f"Received notification for %s,%d,%d", key, start, end)
            start, end = self.round_notification(key, start, end)

            # Connection callbacks are not re-entrant and the connection can process normal requests
            # (intent acquisition), so this is safe.
            await self._install_pending(start, end)

    async def _intent_hit(self, key: str, start: int, end: int) -> None:
        async with self._lock:
            if self._shutdown_in_progress:
                _LOGGER.debug(f"Ignoring intent hit during shutdown %s,%d,%d", key, start, end)
                return

            _LOGGER.debug(f"Received intent hit for %s,%d,%d", key, start, end)

            for idx in self._intersecting(start, end):
                add_pending = self._pending[idx]
                self._do_update.pop(add_pending, None)
                self._do_update[add_pending] = idx
                _LOGGER.debug(f"Intent pending %d,%d", add_pending.start, add_pending.end)
        self.notify_update_ready()


class StationsController:
    class Manager(UpdateManager):
        def __init__(self, controller: "StationsController", station: str):
            super().__init__(controller.connection)
            self.controller = controller
            self.station = station

        def notify_update_ready(self) -> None:
            self.controller._update_ready.set()

        async def perform_update(self, start: int, end: int) -> None:
            raise NotImplementedError

        async def scan_modified_files(
                self, path: str, modified_after: float,
                convert: typing.Callable[[str], typing.Optional[typing.Tuple[int, int]]]
        ) -> typing.List[typing.Tuple[int, int]]:
            result: typing.List[typing.Tuple[int, int]] = list()
            for file in await self.connection.list_files(path, modified_after):
                file_path = Path(file)
                add = convert(file_path.name)
                if add:
                    result.append(add)
            return result

        DAY_FILE_MATCH = re.compile(
            r'[A-Z][0-9A-Z_]{0,31}-[A-Z][A-Z0-9]*_'
            r's((\d{4})(\d{2})(\d{2}))\.nc',
        )

        @classmethod
        def convert_day_file(cls, file_name: str) -> typing.Optional[typing.Tuple[int, int]]:
            match = cls.DAY_FILE_MATCH.fullmatch(file_name)
            if not match:
                return None
            file_start = int(floor(datetime.datetime(
                int(match.group(2)), int(match.group(3)), int(match.group(4)),
                tzinfo=datetime.timezone.utc
            ).timestamp() * 1000.0))
            return file_start, file_start + (24 * 60 * 60 * 1000)

        YEAR_FILE_MATCH = re.compile(
            r'[A-Z][0-9A-Z_]{0,31}-[A-Z][A-Z0-9]*_'
            r's(\d{4})0101\.nc',
        )

        @classmethod
        def convert_year_file(cls, file_name: str) -> typing.Optional[typing.Tuple[int, int]]:
            match = cls.YEAR_FILE_MATCH.fullmatch(file_name)
            if not match:
                return None
            return year_bounds_ms(int(match.group(1)))

    class ManagerPerDay(Manager):
        def round_notification(self, key: str, start: int, end: int) -> typing.Tuple[int, int]:
            if start <= 0:
                start = 0
            else:
                start = int(floor(start / (24 * 60 * 60 * 1000))) * (24 * 60 * 60 * 1000)

            next_day = int(ceil(time.time() / (24 * 60 * 60))) * (24 * 60 * 60 * 1000)
            if end >= next_day:
                end = next_day
            else:
                end = int(ceil(end / (24 * 60 * 60 * 1000))) * (24 * 60 * 60 * 1000)
            if start >= end:
                end = start + 24 * 60 * 60 * 1000
            return start, end

    def __init__(self, connection: Connection):
        self.connection = connection
        self._update_ready = asyncio.Event()
        self.stations: typing.Dict[str, "StationsController.Manager"] = OrderedDict()
        self._in_transaction: bool = False
        self._shutdown_in_progress: bool = False

    async def initialize(self) -> None:
        for station in sorted(STATIONS):
            _LOGGER.debug(f"Initializing {station.upper()}")
            manager = self.Manager(self, station.lower())
            self.stations[station] = manager
            await manager.initialize()
        for station in self.stations.values():
            _LOGGER.debug(f"Loading {station.station.upper()}")
            await station.load_existing()

    async def _do_any_update(self) -> bool:
        # First stage check, so to avoid unneeded transactions
        for station in self.stations.values():
            if not await station.has_update():
                continue
            break
        else:
            return False

        station_order = [s for s in self.stations.values()]
        backoff = LockBackoff()
        while True:
            station_busy: typing.Optional[int] = None
            try:
                self._in_transaction = True
                async with self.connection.transaction(True):
                    for station_idx in range(len(station_order)):
                        station = station_order[station_idx]
                        update_context = await station.next_update()
                        if not update_context:
                            continue
                        try:
                            async with update_context as (start, end):
                                _LOGGER.debug(f"Starting update for {station.station.upper()} on {start},{end}")
                                await station.perform_update(start, end)
                        except LockDenied:
                            station_busy = station_idx
                            raise

                        # Station success, so reset wait time and finish the transaction
                        _LOGGER.debug("Update completed")
                        backoff.clear()
                        break
                    else:
                        # No stations have any work to do, so back to sleeping
                        return False
            except LockDenied as ld:
                _LOGGER.debug("Archive busy: %s", ld.status)
                # If a station was busy, put it at the back so that we try others first
                if station_busy is not None:
                    station = station_order[station_busy]
                    del station_order[station_busy]
                    station_order.append(station)
                self._in_transaction = False
                await backoff()
            finally:
                self._in_transaction = False

    async def run(self, before_idle: typing.Optional[typing.Callable[[], typing.Awaitable]] = None) -> None:
        while True:
            self._update_ready.clear()
            if not await self._do_any_update():
                if not before_idle:
                    await self._update_ready.wait()
                    continue
                while True:
                    wait_time = await before_idle()
                    if wait_time is None:
                        await self._update_ready.wait()
                        break
                    wait_time = max(wait_time, 0.001)
                    try:
                        await wait_cancelable(self._update_ready.wait(), wait_time)
                        break
                    except asyncio.TimeoutError:
                        pass

    async def shutdown(self) -> None:
        self._shutdown_in_progress = True
        for station in self.stations.values():
            await station.shutdown()

    async def flush(self, start: int = -MAX_I64, end: int = MAX_I64) -> None:
        if self._shutdown_in_progress:
            return
        _LOGGER.debug(f"Flushing all data in {start if start > -MAX_I64 else '-INF'},{end if end < MAX_I64 else 'INF'}")
        for station in self.stations.values():
            await station.flush(start, end)

    def request_timeout(self) -> float:
        if self._in_transaction:
            return 6 * 60 * 60
        return 10 * 60

    @classmethod
    def create_updater(cls, connection: Connection, args) -> "StationsController":
        return cls(connection)

    @classmethod
    def updater_control_socket(cls) -> typing.Optional[str]:
        raise NotImplementedError

    @classmethod
    def add_updater_arguments(cls, parser) -> None:
        pass

    UPDATER_DESCRIPTION = ""
    UPDATER_CONNECTION_NAME = ""
    FLUSH_DESCRIPTION = ""

    @classmethod
    def run_updater(cls) -> None:
        import argparse
        import signal
        import struct
        from forge.archive import CONFIGURATION

        parser = argparse.ArgumentParser(description=cls.UPDATER_DESCRIPTION)

        parser.add_argument('--debug',
                            dest='debug', action='store_true',
                            help="enable debug output")
        parser.add_argument('--systemd',
                            dest='systemd', action='store_true',
                            help="enable systemd integration")
        parser.add_argument('--dashboard',
                            dest='dashboard', type=str,
                            help="dashboard notification code")
        group = parser.add_mutually_exclusive_group()
        group.add_argument('--server-host',
                           dest='tcp_server',
                           help="archive server host")
        group.add_argument('--server-socket',
                           dest='unix_socket',
                           help="archive server Unix socket")
        parser.add_argument('--server-port',
                            dest='tcp_port',
                            type=int,
                            default=CONFIGURATION.get("ARCHIVE.PORT"),
                            help="archive server port")

        parser.add_argument('--control-socket',
                            dest='control_socket', default=cls.updater_control_socket(),
                            help="override destination station")

        cls.add_updater_arguments(parser)

        args = parser.parse_args()
        if args.tcp_server and not args.tcp_port:
            parser.error("Both a server host and port must be specified")

        if args.debug:
            from forge.log import set_debug_logger
            set_debug_logger()

        loop = asyncio.new_event_loop()

        connection: Connection = None
        controller: StationsController = None
        control_server: "asyncio.Server" = None
        controller_run: asyncio.Task = None

        async def control_connection(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            try:
                command = struct.unpack('<B', await reader.readexactly(1))[0]
                if command == 0:
                    flush_start, flush_end = struct.unpack('<qq', await reader.readexactly(16))
                    if controller:
                        await controller.flush(flush_start, flush_end)
                        writer.write(struct.pack('<B', 0))
                    else:
                        writer.write(struct.pack('<B', 1))
                    await writer.drain()
                else:
                    _LOGGER.error(f"Invalid updater command {command}")
            finally:
                try:
                    writer.close()
                except:
                    pass

        class ArchiveConnection(Connection):
            async def run(self) -> None:
                try:
                    await super().run()
                finally:
                    if controller_run:
                        try:
                            controller_run.cancel()
                        except:
                            pass

        async def initialize():
            nonlocal connection
            nonlocal controller
            nonlocal control_server

            if args.tcp_server and args.tcp_port:
                _LOGGER.debug(f"Connecting to archive TCP socket {args.tcp_server}:{args.tcp_port}")
                reader, writer = await asyncio.open_connection(args.tcp_server, int(args.tcp_port))
                connection = ArchiveConnection(reader, writer, cls.UPDATER_CONNECTION_NAME)
            elif args.unix_socket:
                _LOGGER.debug(f"Connecting to archive Unix socket {args.unix_socket}")
                reader, writer = await asyncio.open_unix_connection(args.unix_socket)
                connection = ArchiveConnection(reader, writer, cls.UPDATER_CONNECTION_NAME)
            else:
                connection = await ArchiveConnection.default_connection(cls.UPDATER_CONNECTION_NAME)

            await connection.startup()

            _LOGGER.debug("Initializing station update controller")
            controller = cls.create_updater(connection, args)

            if args.systemd:
                import systemd.daemon
                _LOGGER.debug("Started startup keepalive")

                async def send_keepalive() -> None:
                    async for _ in connection.periodic_watchdog(10, request_timeout=3600):
                        systemd.daemon.notify("EXTEND_TIMEOUT_USEC=30000000")
                        _LOGGER.debug("Startup keepalive sent")

                keepalive = loop.create_task(send_keepalive())
            else:
                keepalive = None

            await controller.initialize()

            if keepalive:
                _LOGGER.debug("Shutting down startup keepalive")
                t = keepalive
                keepalive = None
                try:
                    t.cancel()
                except:
                    pass
                try:
                    await t
                except:
                    pass

            control_socket = args.control_socket
            if control_socket:
                _LOGGER.info(f"Binding to external control socket {control_socket}")
                try:
                    os.unlink(control_socket)
                except OSError:
                    pass
                control_server = await asyncio.start_unix_server(control_connection, path=control_socket)

        async def shutdown():
            nonlocal connection
            nonlocal controller
            nonlocal control_server

            if control_server:
                _LOGGER.debug("Shutting down external control socket")
                try:
                    control_server.close()
                except:
                    pass
                try:
                    await wait_cancelable(control_server.wait_closed(), 2.0)
                except:
                    pass
            control_server = None

            _LOGGER.debug("Shutting down station update controller")
            do_shutdown = controller
            controller = None
            await do_shutdown.shutdown()

            _LOGGER.debug("Shutting down archive connection")
            do_shutdown = connection
            connection = None
            await do_shutdown.shutdown()

        loop.run_until_complete(initialize())

        dashboard: typing.Optional[typing.Callable[[], typing.Awaitable]] = None
        if args.dashboard:
            last_notification: typing.Optional[float] = None

            async def notify_dashboard() -> float:
                nonlocal last_notification

                now = time.monotonic()
                if not last_notification or (now - last_notification) >= 300:
                    last_notification = now
                    await report_ok(args.dashboard)
                return 300 - (now - last_notification)

            dashboard = notify_dashboard

        heartbeat: typing.Optional[asyncio.Task] = None
        if args.systemd:
            import systemd.daemon
            systemd.daemon.notify("READY=1")

            _LOGGER.debug("Starting systemd heartbeat")

            async def send_heartbeat() -> typing.NoReturn:
                async for _ in connection.periodic_watchdog(10, heartbeat_timeout=controller.request_timeout):
                    systemd.daemon.notify("WATCHDOG=1")

            heartbeat = loop.create_task(send_heartbeat())

        _LOGGER.info("Update controller ready")

        controller_run = loop.create_task(controller.run(before_idle=dashboard))
        loop.add_signal_handler(signal.SIGINT, controller_run.cancel)
        loop.add_signal_handler(signal.SIGTERM, controller_run.cancel)
        try:
            loop.run_until_complete(controller_run)
        except asyncio.CancelledError:
            pass
        except:
            if args.dashboard:
                loop.run_until_complete(report_failed(args.dashboard, exc_info=True))
            raise
        finally:
            controller_run = None

        if heartbeat:
            _LOGGER.debug("Shutting down heartbeat")
            t = heartbeat
            heartbeat = None
            try:
                t.cancel()
            except:
                pass
            try:
                loop.run_until_complete(t)
            except:
                pass

        loop.run_until_complete(shutdown())
        loop.close()

    @classmethod
    def run_flush(cls) -> None:
        import argparse
        import struct
        from forge.timeparse import parse_time_argument

        parser = argparse.ArgumentParser(description=cls.FLUSH_DESCRIPTION)

        parser.add_argument('--debug',
                            dest='debug', action='store_true',
                            help="enable debug output")
        parser.add_argument('--dashboard',
                            dest='dashboard', type=str,
                            help="dashboard notification code")
        parser.add_argument('--control-socket',
                            dest='control_socket', default=cls.updater_control_socket(),
                            help="override destination station")

        parser.add_argument('--before',
                            dest='before',
                            help="flush data before a time, or 'today' for the start of the current UTC day")

        args = parser.parse_args()
        if not args.control_socket:
            parser.error("No control socket available")

        if args.debug:
            from forge.log import set_debug_logger
            set_debug_logger()

        before = MAX_I64
        if args.before:
            if args.before.lower() == "today":
                before = int(floor(time.time() / (24 * 60 * 60))) * 24 * 60 * 60 * 1000
            else:
                before = int(ceil(parse_time_argument(args.before).timestamp() * 1000))

        loop = asyncio.new_event_loop()

        async def run():
            _LOGGER.debug(f"Connecting to control socket {args.control_socket}")
            reader, writer = await asyncio.open_unix_connection(args.control_socket)

            writer.write(struct.pack('<Bqq', 0, -MAX_I64, before))
            result = struct.unpack('<B', await reader.readexactly(1))[0]

            writer.close()
            return result

        try:
            result = loop.run_until_complete(run())
        except:
            if args.dashboard:
                loop.run_until_complete(report_failed(args.dashboard, exc_info=True))
            raise

        if result != 0:
            print("Error sending flush")

            if args.dashboard:
                loop.run_until_complete(report_failed(args.dashboard, notifications=[{
                    "code": "",
                    "severity": "error",
                    "data": "Error sending archive update flush."
                }]))
        else:
            if args.dashboard:
                loop.run_until_complete(report_ok(args.dashboard))

        loop.close()
        exit(result)
