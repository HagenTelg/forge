import typing
import asyncio
import logging
import serial
import socket
import struct
from serial.rs485 import RS485Settings
from forge.acquisition import LayeredConfiguration
from forge.tasks import background_task
from forge.acquisition.serial.multiplexer.protocol import ControlOperation, Parity as ControlParity
from .streaming import StreamingContext
from .base import BaseDataOutput, BasePersistentInterface, BaseBusInterface

have_termios = False
try:
    import termios
    have_termios = True
except ImportError:
    termios = object()
    class FakeError(Exception):
        pass
    termios.error = FakeError


_LOGGER = logging.getLogger(__name__)


_DATA_BITS = {
    5: serial.FIVEBITS,
    6: serial.SIXBITS,
    7: serial.SEVENBITS,
    8: serial.EIGHTBITS,
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
    'EVEN': serial.PARITY_EVEN,

    'O': serial.PARITY_ODD,
    'ODD': serial.PARITY_ODD,

    'M': serial.PARITY_MARK,
    'MARK': serial.PARITY_MARK,

    'S': serial.PARITY_SPACE,
    'SPACE': serial.PARITY_SPACE,
}
_CONTROL_PARITY = {
    serial.PARITY_NONE: ControlParity.NONE,
    serial.PARITY_EVEN: ControlParity.EVEN,
    serial.PARITY_ODD: ControlParity.ODD,
    serial.PARITY_MARK: ControlParity.MARK,
    serial.PARITY_SPACE: ControlParity.SPACE,
}


class SerialTransport(asyncio.Transport):
    def __init__(self, loop: asyncio.AbstractEventLoop, protocol: asyncio.Protocol, port: serial.Serial,
                 extra=None):
        super().__init__(extra=extra)
        self._loop = loop
        self._protocol = protocol
        self._port: typing.Optional[serial.Serial] = port
        self._control_socket: str = None

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

    @property
    def control_socket(self) -> str:
        return self._control_socket

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


async def open_serial(limit: int = None,
                      **kwargs) -> typing.Tuple[asyncio.StreamReader, asyncio.StreamWriter, serial.Serial]:
    if have_termios:
        device = kwargs.pop('port')
        bytesize = kwargs.pop('bytesize', None)
        parity = kwargs.pop('parity', None)

        port = serial.Serial(**kwargs)
        port.port = device
        try:
            port.open()
        except termios.error as e:
            raise IOError from e

        # Separate step for pseudoterminals which silently ignore this on linux, but various
        # distributions patch python/glibc to report EINVAL instead.
        if bytesize:
            try:
                port.bytesize = bytesize
            except termios.error:
                _LOGGER.debug("Error changing bytesize (probably a pseudoterminal)", exc_info=True)
        if parity:
            try:
                port.parity = parity
            except termios.error:
                _LOGGER.debug("Error changing parity (probably a pseudoterminal)", exc_info=True)
    else:
        port = serial.Serial(**kwargs)

    if limit is None:
        limit = 64 * 1024

    loop = asyncio.get_event_loop()
    reader = asyncio.StreamReader(limit=limit, loop=loop)
    protocol = asyncio.StreamReaderProtocol(reader, loop=loop)
    transport = SerialTransport(loop, protocol, port)
    reader.serial_transport = transport
    writer = asyncio.StreamWriter(transport, protocol, reader, loop)
    return reader, writer, port


class SerialPortContext(StreamingContext):
    def __init__(self, config: LayeredConfiguration, data: BaseDataOutput, bus: BaseBusInterface,
                 persistent: BasePersistentInterface, serial_config: typing.Union[str, LayeredConfiguration],
                 serial_args: typing.Dict[str, typing.Any] = None,
                 control_socket: typing.Optional[str] = None):
        super().__init__(config, data, bus, persistent)

        self._control_socket = control_socket

        self._serial_args: typing.Dict[str, typing.Any] = dict()
        if serial_args:
            self._serial_args.update(serial_args)

        rs485_config = None
        if isinstance(serial_config, str):
            self._serial_args['port'] = serial_config
        else:
            self._serial_args['port'] = serial_config["PORT"]

            baud = serial_config.get("BAUD")
            if baud:
                self._serial_args['baudrate'] = int(baud)

            data_bits = serial_config.get("DATA_BITS")
            if data_bits:
                self._serial_args['bytesize'] = _DATA_BITS[int(data_bits)]

            stop_bits = serial_config.get("STOP_BITS")
            if stop_bits:
                self._serial_args['stopbits'] = _STOP_BITS[float(stop_bits)]

            parity = serial_config.get("PARITY")
            if parity:
                if isinstance(parity, str):
                    parity = parity.upper()
                self._serial_args['parity'] = _PARITY[parity]

            xonxoff = serial_config.get("XON_XOFF")
            if xonxoff is not None:
                self._serial_args['xonxoff'] = bool(xonxoff)

            rtscts = serial_config.get("RTS_CTS")
            if rtscts is not None:
                self._serial_args['rtscts'] = bool(rtscts)

            dsrdtr = serial_config.get("DSR_DTR")
            if rtscts is not None:
                self._serial_args['dsrdtr'] = bool(dsrdtr)

            rs485_config = serial_config.get("RS485")

        self.read_only = config.get("READ_ONLY", default=False)

        self._serial_args['timeout'] = 0
        self._serial_args['inter_byte_timeout'] = 0
        self._serial_args['write_timeout'] = 0
        self._serial_args['exclusive'] = False

        self._rs485: typing.Optional[typing.Union[RS485Settings, bool]] = self._serial_args.pop('rs485', None)
        if isinstance(rs485_config, bool) and not rs485_config:
            self._rs485 = False
        elif rs485_config is not None:
            if self._rs485 is None:
                self._rs485 = RS485Settings()
            if isinstance(rs485_config, dict):
                rts_level_for_tx = rs485_config.get("RTS_LEVEL_FOR_TX")
                if rts_level_for_tx is not None:
                    self._rs485.rts_level_for_tx = bool(rts_level_for_tx)

                rts_level_for_rx = rs485_config.get("RTS_LEVEL_FOR_RX")
                if rts_level_for_rx is not None:
                    self._rs485.rts_level_for_rx = bool(rts_level_for_rx)

                loopback = rs485_config.get("LOOPBACK")
                if loopback is not None:
                    self._rs485.loopback = bool(loopback)

                delay_before_tx = rs485_config.get("DELAY_BEFORE_TX")
                if delay_before_tx is not None:
                    self._rs485.delay_before_tx = float(delay_before_tx)

                delay_before_rx = rs485_config.get("DELAY_BEFORE_RX")
                if delay_before_rx is not None:
                    self._rs485.delay_before_rx = float(delay_before_rx)

    @property
    def bit_time(self) -> float:
        baud = self._serial_args.get('baudrate')
        if not baud:
            return 0.0
        return 1.0 / baud

    def _apply_open_control(self) -> None:
        if not self._control_socket:
            return

        bytesize = self._serial_args.get('bytesize')
        parity = self._serial_args.get('parity')
        if (not bytesize or bytesize == serial.EIGHTBITS) and \
                (not parity or parity == serial.PARITY_NONE) and \
                self._rs485 is None:
            return

        try:
            s = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        except IOError:
            _LOGGER.debug(f"Error creating control socket", exc_info=True)
            return

        def send_control(op: ControlOperation, data: bytes = None) -> None:
            packet = struct.pack('<B', op.value)
            if data:
                packet = packet + data
            s.sendto(packet, self._control_socket)

        _LOGGER.debug(f"Setting serial parameters on control socket {self._control_socket}")
        try:
            if bytesize:
                send_control(ControlOperation.SET_DATA_BITS, struct.pack('<B', int(bytesize)))

            if parity:
                send_control(ControlOperation.SET_PARITY, struct.pack('<B', _CONTROL_PARITY[parity]))

            if self._rs485 is not None:
                if not self._rs485:
                    send_control(ControlOperation.SET_RS485, struct.pack('<B', 0))
                else:
                    send_control(
                        ControlOperation.SET_RS485, struct.pack(
                            '<BBBBff', 1,
                            self._rs485.rts_level_for_tx and 1 or 0,
                            self._rs485.rts_level_for_rx and 1 or 0,
                            self._rs485.loopback and 1 or 0,
                            self._rs485.delay_before_tx,
                            self._rs485.delay_before_rx
                        ))
        except (IOError, NameError):
            _LOGGER.warning("Failed to send control packet", exc_info=True)
        finally:
            try:
                s.close()
            except:
                pass

    async def open_stream(self) -> typing.Tuple[typing.Optional[asyncio.StreamReader],
                                                typing.Optional[asyncio.StreamWriter]]:
        (reader, writer, port) = await open_serial(**self._serial_args)
        writer.transport._control_socket = self._control_socket

        if self._rs485 is not None:
            rs485 = self._rs485
            if not rs485:
                rs485 = None
            try:
                port.rs485_mode = rs485
            except (ValueError, IOError, NotImplementedError):
                _LOGGER.debug("Failed to set RS-485 mode", exc_info=True)

        self._apply_open_control()

        return reader, writer

    async def close_stream(self, reader: typing.Optional[asyncio.StreamReader],
                           writer: typing.Optional[asyncio.StreamWriter]) -> None:
        if writer:
            writer.transport.abort()
            await writer.transport.wait_for_closed()
        elif reader:
            reader.serial_transport.abort()
            await reader.serial_transport.wait_for_closed()


def _set_control_boolean(control_socket: str, op: ControlOperation, state: bool) -> None:
    if not control_socket:
        return

    try:
        s = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    except IOError:
        _LOGGER.debug(f"Error creating control socket", exc_info=True)
        return

    try:
        s.sendto(struct.pack('<BB', op.value, state and 1 or 0), control_socket)
    except (IOError, NameError):
        _LOGGER.debug("Failed to send control packet", exc_info=True)
    finally:
        try:
            s.close()
        except:
            pass


def set_rts(writer: asyncio.StreamWriter, state: bool) -> None:
    transport = writer.transport
    if not isinstance(transport, SerialTransport):
        return
    port = transport.port
    if port:
        try:
            port.rts = state
        except (ValueError, IOError):
            _LOGGER.debug("Failed to set RTS directly", exc_info=True)

    _set_control_boolean(transport.control_socket, ControlOperation.SET_RTS, state)


def set_dtr(writer: asyncio.StreamWriter, state: bool) -> None:
    transport = writer.transport
    if not isinstance(transport, SerialTransport):
        return
    port = transport.port
    if port:
        try:
            port.dtr = state
        except (ValueError, IOError):
            _LOGGER.debug("Failed to set DTR directly", exc_info=True)

    _set_control_boolean(transport.control_socket, ControlOperation.SET_DTR, state)
