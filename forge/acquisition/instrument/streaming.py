import typing
import asyncio
import logging
import time
import traceback
from forge.tasks import wait_cancelable
from forge.acquisition import LayeredConfiguration
from .base import BaseSimulator, BaseContext, BaseDataOutput, BasePersistentInterface, BaseBusInterface, CommunicationsError
from .standard import StandardInstrument

_LOGGER = logging.getLogger(__name__)


class StreamingContext(BaseContext):
    def __init__(self, config: LayeredConfiguration, data: BaseDataOutput, bus: BaseBusInterface,
                 persistent: BasePersistentInterface):
        super().__init__(config, data, bus, persistent)
        self.always_reset_stream: bool = False

    async def open_stream(self) -> typing.Tuple[typing.Optional[asyncio.StreamReader],
                                                typing.Optional[asyncio.StreamWriter]]:
        raise NotImplementedError

    async def close_stream(self, reader: typing.Optional[asyncio.StreamReader],
                           writer: typing.Optional[asyncio.StreamWriter]) -> None:
        if writer:
            writer.close()


class StreamingInstrument(StandardInstrument):
    def __init__(self, context: StreamingContext):
        super().__init__(context)
        self.context = context

        self.reader: typing.Optional[asyncio.StreamReader] = None
        self.writer: typing.Optional[asyncio.StreamWriter] = None

        self._stream_need_reset: bool = False

    async def drain_reader(self, delay: float) -> None:
        now = time.monotonic()
        end_time = now + delay
        while True:
            remaining = end_time - now
            if remaining < 0.0:
                break
            try:
                await wait_cancelable(self.reader.read(4096), max(remaining, 0.01))
            except asyncio.TimeoutError:
                break
            now = time.monotonic()

    async def read_line(self) -> bytes:
        line = bytearray()
        while len(line) < 65536:
            d = await self.reader.read(1)
            if not d:
                break
            if d == b'\r' or d == b'\n':
                line = line.strip()
                if line:
                    break
                line.clear()
                continue
            line += d
        return bytes(line)

    async def read_multiple_lines(self, maximum_count: typing.Optional[int] = None,
                                  total: typing.Optional[float] = None,
                                  first: typing.Optional[float] = None,
                                  tail: typing.Optional[float] = None) -> typing.List[bytes]:
        result_lines: typing.List[bytes] = list()

        async def do_read():
            if first:
                line = await wait_cancelable(self.read_line(), first)
            else:
                line = await self.read_line()
            result_lines.append(line)

            while not maximum_count or len(result_lines) < maximum_count:
                if tail:
                    try:
                        line = await wait_cancelable(self.read_line(), tail)
                    except asyncio.TimeoutError:
                        break
                else:
                    line = await self.read_line()
                result_lines.append(line)

        if total:
            try:
                await wait_cancelable(do_read(), total)
            except asyncio.TimeoutError:
                if not result_lines:
                    raise
        else:
            await do_read()

        return result_lines

    async def start_communications(self) -> None:
        pass

    async def communicate(self) -> None:
        raise NotImplementedError

    async def reset_stream(self) -> None:
        self._stream_need_reset = False
        r = self.reader
        w = self.writer
        self.reader = None
        self.writer = None
        if r or w:
            await self.context.close_stream(r, w)
            await asyncio.sleep(1.0)
        self.reader, self.writer = await self.context.open_stream()

    async def run(self) -> typing.NoReturn:
        # Send initial information and state
        await self.emit()

        async def establish_communications() -> bool:
            if self._stream_need_reset or not self.reader:
                _LOGGER.debug("Resetting stream after communications start failure")
                try:
                    await self.reset_stream()
                except IOError:
                    _LOGGER.warning("Error resetting stream", exc_info=True)
                    return False
                if not self.reader:
                    _LOGGER.warning("No read stream available")
                    return False

            try:
                await self.start_communications()
            except (TimeoutError, asyncio.TimeoutError):
                _LOGGER.debug("Timeout waiting for response in start communications", exc_info=True)
                if self.context.always_reset_stream:
                    self._stream_need_reset = True
                return False
            except CommunicationsError:
                _LOGGER.debug("Invalid response in start communications", exc_info=True)
                if self.context.always_reset_stream:
                    self._stream_need_reset = True
                return False
            except (IOError, EOFError, asyncio.IncompleteReadError):
                _LOGGER.warning("Stream IO error during start communications", exc_info=True)
                self._stream_need_reset = True
                return False

            _LOGGER.debug("Communications established")
            self.context.bus.log("Communications established",
                                 type=BaseBusInterface.LogType.COMMUNICATIONS_ESTABLISHED)
            return True

        async def process() -> bool:
            try:
                await self.communicate()
            except (TimeoutError, asyncio.TimeoutError):
                _LOGGER.info("Timeout waiting for response", exc_info=True)
                self.context.bus.log("Timeout waiting for response", {
                    "exception": traceback.format_exc(),
                }, type=BaseBusInterface.LogType.COMMUNICATIONS_LOST)
                if self.context.always_reset_stream:
                    self._stream_need_reset = True
                return False
            except CommunicationsError:
                _LOGGER.info("Invalid response received", exc_info=True)
                self.context.bus.log("Invalid response received", {
                    "exception": traceback.format_exc(),
                }, type=BaseBusInterface.LogType.COMMUNICATIONS_LOST)
                if self.context.always_reset_stream:
                    self._stream_need_reset = True
                return False
            except (IOError, EOFError, asyncio.IncompleteReadError):
                _LOGGER.warning("Stream IO error", exc_info=True)
                self.context.bus.log("Stream IO error", {
                    "exception": traceback.format_exc(),
                }, type=BaseBusInterface.LogType.COMMUNICATIONS_LOST)
                self._stream_need_reset = True
                return False

            return True

        if not self.reader:
            try:
                self.reader, self.writer = await self.context.open_stream()

                if not self.reader:
                    _LOGGER.warning("No read stream returned from initial open")
                    self._stream_need_reset = True
                    await asyncio.sleep(10)
            except IOError:
                _LOGGER.warning("Error opening stream", exc_info=True)
                await asyncio.sleep(10)

        while True:
            while not await establish_communications():
                self.is_communicating = False
                await asyncio.sleep(10)
                self._stream_need_reset = True

            self.is_communicating = True
            while await process():
                await self.emit()
            self.is_communicating = False
            await asyncio.sleep(1.0)


class StreamingSimulator(BaseSimulator):
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        super().__init__()
        self.reader = reader
        self.writer = writer


def launch(instrument: typing.Type[StreamingInstrument]) -> None:
    from .run import run, arguments, average_config, instrument_config, cutsize_config, \
        data_output, bus_interface, persistent_interface

    args = arguments()

    args.add_argument('--serial',
                      dest="serial",
                      help="connect to a serial port")
    args.add_argument('--control',
                      dest="control",
                      help="auxiliary serial control socket")

    args = args.parse_args()

    bus = bus_interface(args)
    data = data_output(args)
    persistent = persistent_interface(args)

    instrument_config = instrument_config(args)

    def context() -> StreamingContext:
        if args.serial:
            from .serial import SerialPortContext
            serial_args = getattr(instrument, 'SERIAL_PORT', {})
            return SerialPortContext(instrument_config, data, bus, persistent, args.serial, serial_args, args.control)

        serial = instrument_config.section_or_constant("SERIAL_PORT")
        if serial:
            from .serial import SerialPortContext
            serial_args = getattr(instrument, 'SERIAL_PORT', {})
            return SerialPortContext(instrument_config, data, bus, persistent, serial, serial_args, args.control)

        tcp = instrument_config.section_or_constant("TCP")
        if tcp:
            from .tcp import TCPContext
            if isinstance(tcp, str):
                (host, port) = tcp.split(':')
                ssl = None
                always_reset = True
            else:
                host = str(tcp.get("HOST"))
                port = int(tcp.get("PORT"))
                ssl = tcp.get("SSL") or None
                retain = tcp.get("RETRY_RETAIN_CONNECTION")
                if retain is None:
                    always_reset = True
                else:
                    always_reset = not retain
            host = host.strip()
            if not host:
                raise ValueError(f"invalid TCP target host: {host}")
            if port <= 0 or port > 65535:
                raise ValueError(f"invalid TCP target port: {port}")
            return TCPContext(instrument_config, data, bus, persistent, host, port, ssl, always_reset)

        unix = instrument_config.section_or_constant("UNIX_SOCKET")
        if unix:
            from .unixsocket import UnixSocketContext
            if isinstance(unix, str):
                path = unix
                always_reset = True
            else:
                path = str(unix.get("PATH"))
                retain = tcp.get("RETRY_RETAIN_CONNECTION")
                if retain is None:
                    always_reset = True
                else:
                    always_reset = not retain
            path = path.strip()
            if not path:
                raise ValueError(f"invalid Unix socket path: {path}")
            return UnixSocketContext(instrument_config, data, bus, persistent, path, always_reset)

        raise ValueError("no serial port or streaming device defined")

    ctx = context()
    ctx.average_config = average_config(args)
    ctx.cutsize_config = cutsize_config(args)
    run(instrument(ctx), args.systemd)
