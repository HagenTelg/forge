import typing
import asyncio
import serial
import termios
from forge.acquisition import LayeredConfiguration
from forge.tasks import background_task
from .streaming import StreamingContext
from .base import BaseDataOutput, BasePersistentInterface, BaseBusInterface


_DATA_BITS = {
    5: serial.FIVEBITS,
    6: serial.SIXBITS,
    7: serial.SEVENBITS,
    8: serial.SEVENBITS,
}
_STOP_BITS = {
    1: serial.STOPBITS_ONE,
    1.5: serial.STOPBITS_ONE_POINT_FIVE,
    2: serial.STOPBITS_TWO,
}
_PARITY = {
    False: serial.PARITY_NONE,
    'N': serial.PARITY_NONE,
    'NONE': serial.PARITY_NONE,

    'E': serial.PARITY_EVEN,
    'EVENT': serial.PARITY_EVEN,

    'O': serial.PARITY_ODD,
    'ODD': serial.PARITY_ODD,

    'M': serial.PARITY_MARK,
    'MARK': serial.PARITY_MARK,

    'S': serial.PARITY_SPACE,
    'SPACE': serial.PARITY_SPACE,
}


class SerialTransport(asyncio.Transport):
    def __init__(self, loop: asyncio.AbstractEventLoop, protocol: asyncio.Protocol, port: serial.Serial,
                 extra=None):
        super().__init__(extra=extra)
        self._loop = loop
        self._protocol = protocol
        self._port: typing.Optional[serial.Serial] = port

        self._is_reading: bool = False

        self._high_water: int = 64 * 1024
        self._low_water = self._high_water // 4
        self._write_buffer = bytearray()
        self._is_writing: bool = False
        self._write_paused: bool = False

        self._is_closing: bool = False
        self._close_event: typing.Optional[asyncio.Event] = None

        self._loop.call_soon(self._protocol.connection_made, self)
        self._loop.call_soon(self.resume_reading)

    @property
    def port(self) -> serial.Serial:
        return self._port

    def get_extra_info(self, name, default=None):
        if name == 'port':
            return self._port
        return super().get_extra_info(name, default=default)

    def is_reading(self) -> bool:
        return self._is_reading

    def pause_reading(self) -> None:
        if not self._is_reading:
            return
        if self.is_closing():
            return
        self._is_reading = False
        self._loop.remove_reader(self._port.fileno())

    def resume_reading(self) -> None:
        if self._is_reading:
            return
        if self.is_closing():
            return
        self._is_reading = True
        self._loop.add_reader(self._port.fileno(), self._do_read)

    def _do_read(self) -> None:
        if self.is_closing():
            return
        try:
            data = self._port.read(1024)
        except serial.SerialException as e:
            self._handle_close(e)
            return
        if data:
            self._protocol.data_received(data)

    def set_write_buffer_limits(self, high: int = None, low: int = None):
        if high is None:
            if low is not None:
                high = low * 4
            else:
                high = 64 * 1024
        if low is None:
            low = high // 4

        if low < 0 or high < low:
            raise ValueError(f"high ({high}) must >= low ({low}) and low must be >= 0")
        self._high_water = high
        self._low_water = low
        self._maybe_pause_write()

    def _maybe_pause_write(self) -> None:
        if self._write_paused:
            return
        if self.get_write_buffer_size() <= self._high_water:
            return
        self._write_paused = True
        self._protocol.pause_writing()

    def _maybe_resume_write(self) -> None:
        if not self._write_paused:
            return
        if self.get_write_buffer_size() > self._low_water:
            return
        self._write_paused = False
        self._protocol.resume_writing()

    def get_write_buffer_size(self) -> int:
        return len(self._write_buffer)

    def get_write_buffer_limits(self) -> typing.Tuple[int, int]:
        return self._low_water, self._high_water

    def _start_writing(self) -> None:
        if self._is_writing:
            return
        if self.is_closing():
            return
        self._is_writing = True
        self._loop.add_writer(self._port.fileno(), self._do_write)

    def _stop_writing(self) -> None:
        if not self._is_writing:
            return
        if self.is_closing():
            return
        self._is_writing = False
        self._loop.remove_writer(self._port.fileno())

    def _do_write(self) -> None:
        if not self._port:
            return

        try:
            n = self._port.write(self._write_buffer)
        except (BlockingIOError, InterruptedError):
            return
        except serial.SerialException as e:
            self._loop.call_exception_handler({
                'message': "error writing to serial port",
                'exception': e,
                'transport': self,
                'protocol': self._protocol,
            })
            self._handle_close(e)
            return

        if n >= len(self._write_buffer):
            self._write_buffer.clear()
            self._stop_writing()
            self._maybe_resume_write()
            if self._is_closing and len(self._write_buffer) == 0:
                self._handle_close()
            return

        del self._write_buffer[:n]
        self._maybe_resume_write()

    def write(self, data: typing.Union[bytes, bytearray]) -> None:
        if self.is_closing():
            return
        if not data:
            return
        should_start_writer = self.get_write_buffer_size() == 0
        self._write_buffer += data
        if should_start_writer:
            self._start_writing()
        self._maybe_pause_write()

    def can_write_eof(self) -> bool:
        return False

    def is_closing(self) -> bool:
        if not self._port:
            return True
        return self._is_closing

    async def wait_for_closed(self) -> None:
        if not self._port:
            return
        if not self._is_closing:
            return
        if not self._close_event:
            self._close_event = asyncio.Event()
        await self._close_event.wait()
        self._close_event = None

    def _disconnect_io(self) -> None:
        if not self._port:
            return
        if self._is_reading:
            self._is_reading = False
            self._loop.remove_reader(self._port.fileno())
        if self._is_writing:
            self._is_writing = False
            self._loop.remove_writer(self._port.fileno())

    def _handle_close(self, e: typing.Optional[Exception] = None) -> None:
        self._is_closing = True
        self._disconnect_io()
        if not self._close_event:
            self._close_event = asyncio.Event()
        background_task(self._handle_connection_lost(e))

    async def _handle_connection_lost(self, e: typing.Optional[Exception] = None) -> None:
        try:
            if self._loop:
                await self._loop.run_in_executor(None, self._port.flush)
        except (IOError, termios.error):
            pass

        try:
            if self._protocol:
                self._protocol.connection_lost(e)
        finally:
            try:
                if self._loop and self._port:
                    await self._loop.run_in_executor(None, self._port.close)
            except (IOError, termios.error):
                pass
            self._write_buffer.clear()
            self._port = None
            self._protocol = None
            self._loop = None
            if self._close_event:
                self._close_event.set()

    def close(self) -> None:
        if self._is_closing:
            return
        self._is_closing = True
        if self.get_write_buffer_size() == 0:
            self._handle_close()

    def abort(self):
        self._write_buffer.clear()
        if self._is_closing:
            return
        self._handle_close()


async def open_serial(limit: int = None, **kwargs) -> typing.Tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    port = serial.Serial(**kwargs)

    if limit is None:
        limit = 64 * 1024

    loop = asyncio.get_event_loop()
    reader = asyncio.StreamReader(limit=limit, loop=loop)
    protocol = asyncio.StreamReaderProtocol(reader, loop=loop)
    transport = SerialTransport(loop, protocol, port)
    reader.serial_transport = transport
    writer = asyncio.StreamWriter(transport, protocol, reader, loop)
    return reader, writer


class SerialPortContext(StreamingContext):
    def __init__(self, config: LayeredConfiguration, data: BaseDataOutput, bus: BaseBusInterface,
                 persistent: BasePersistentInterface, serial: typing.Union[str, LayeredConfiguration],
                 serial_args: typing.Dict[str, typing.Any] = None):
        super().__init__(config, data, bus, persistent)

        self._serial_args: typing.Dict[str, typing.Any] = dict()
        if serial_args:
            self._serial_args.update(serial_args)

        if isinstance(serial, str):
            self._serial_args['port'] = serial
        else:
            self._serial_args['port'] = serial["PORT"]

            baud = serial.get("BAUD")
            if baud:
                self._serial_args['baudrate'] = int(baud)

            data_bits = serial.get("DATA_BITS")
            if data_bits:
                self._serial_args['bytesize'] = _DATA_BITS[int(data_bits)]

            stop_bits = serial.get("STOP_BITS")
            if stop_bits:
                self._serial_args['stopbits'] = _STOP_BITS[float(stop_bits)]

            parity = serial.get("PARITY")
            if parity:
                if isinstance(parity, str):
                    parity = parity.upper()
                self._serial_args['parity'] = _PARITY[parity]

            xonxoff = serial.get("XON_XOFF")
            if xonxoff is not None:
                self._serial_args['xonxoff'] = bool(xonxoff)

            rtscts = serial.get("RTS_CTS")
            if rtscts is not None:
                self._serial_args['rtscts'] = bool(rtscts)

            dsrdtr = serial.get("DSR_DTR")
            if rtscts is not None:
                self._serial_args['dsrdtr'] = bool(dsrdtr)

        self.read_only = config.get("READ_ONLY", default=False)

        self._serial_args['timeout'] = 0
        self._serial_args['inter_byte_timeout'] = 0
        self._serial_args['write_timeout'] = 0
        self._serial_args['exclusive'] = False

    async def open_stream(self) -> typing.Tuple[typing.Optional[asyncio.StreamReader],
                                                typing.Optional[asyncio.StreamWriter]]:
        return await open_serial(**self._serial_args)

    async def close_stream(self, reader: typing.Optional[asyncio.StreamReader],
                           writer: typing.Optional[asyncio.StreamWriter]) -> None:
        if writer:
            writer.transport.abort()
            await writer.transport.wait_for_closed()
        elif reader:
            reader.serial_transport.abort()
            await reader.serial_transport.wait_for_closed()
