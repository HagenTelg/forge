import typing
import argparse
import logging
import struct
import select
import os
import signal
import time
import termios
import socket
import errno
from enum import IntEnum
from fcntl import ioctl
from serial.serialposix import PlatformSpecific as SerialPlatformSpecific
from serial.serialposix import CMSPAR, TIOCMBIS, TIOCMBIC, TIOCM_RTS_str, TIOCM_DTR_str
from serial.rs485 import RS485Settings
from forge.acquisition.serial.multiplexer.protocol import ToMultiplexer, FromMultiplexer, ControlOperation, Parity
from forge.acquisition.serial.util import standard_termios, TCAttr


_LOGGER = logging.getLogger(__name__)

UMASK = os.umask(0o666) | 0o111
os.umask(UMASK)


class TIOCPKT(IntEnum):
    DATA = 0
    FLUSHREAD = 1
    FLUSHWRITE = 2
    STOP = 4
    START = 8
    NOSTOP = 16
    DOSTOP = 32
    IOCTL = 64


class ApplyPlatformSpecific(SerialPlatformSpecific):
    def __init__(self, fd: int):
        self.fd = fd


class Upstream:
    def __init__(self, device: str, poll):
        self._fd = None
        self.device = device
        self._poll = poll

        self.data_to_downstream: typing.Optional[bytes] = None
        self._write_buffer = bytearray()

        self._fd = os.open(self.device, os.O_RDWR | os.O_NONBLOCK | os.O_NOCTTY | os.O_CLOEXEC)
        self._initialize_device()
        self._poll.register(self._fd, select.POLLIN)
        _LOGGER.debug(f"Serial port {device} opened as {self._fd}")

    def __repr__(self):
        return self.device

    def __del__(self):
        if self._fd is not None:
            try:
                os.close(self._fd)
            except OSError:
                pass
            self._fd = None

    def poll_begin(self) -> None:
        mask = select.POLLIN
        if self._write_buffer:
            mask |= select.POLLOUT

        self._poll.modify(self._fd, mask)

    def poll_result(self, events: typing.List[typing.Tuple[int, int]]) -> None:
        for fd, mask in events:
            if fd == self._fd:
                if (mask & select.POLLIN) and self.data_to_downstream is None:
                    try:
                        self.data_to_downstream = os.read(self._fd, 65536)
                        print(self.data_to_downstream)
                    except OSError as e:
                        if e.errno != errno.EAGAIN and e.errno != errno.ETIMEDOUT:
                            raise

                if (mask & select.POLLOUT) and self._write_buffer:
                    try:
                        n = os.write(self._fd, self._write_buffer)
                        if n >= len(self._write_buffer):
                            self._write_buffer.clear()
                        elif n > 0:
                            del self._write_buffer[0:n]
                    except OSError as e:
                        if e.errno != errno.EAGAIN and e.errno != errno.ETIMEDOUT:
                            raise

    @property
    def can_accept_downstream_data(self) -> bool:
        return len(self._write_buffer) < 256

    def data_from_downstream(self, data: bytes):
        self._write_buffer += data

    def data_from_eavesdropper(self, data: bytes):
        self._write_buffer += data

    def data_from_raw(self, data: bytes):
        self._write_buffer += data

    def termios_from_downstream(self, tio) -> None:
        # Pseudoterminals do not propagate bytesize/parity, so preserve the existing setting
        tio_base = termios.tcgetattr(self._fd)
        tio[TCAttr.c_cflag] &= ~termios.CSIZE
        tio[TCAttr.c_cflag] |= (tio_base[TCAttr.c_cflag] & termios.CSIZE)
        tio[TCAttr.c_cflag] &= ~(termios.PARENB | termios.PARODD)
        tio[TCAttr.c_cflag] |= (tio_base[TCAttr.c_cflag] & (termios.PARENB | termios.PARODD))
        if CMSPAR:
            tio[TCAttr.c_cflag] &= ~CMSPAR
            tio[TCAttr.c_cflag] |= (tio_base[TCAttr.c_cflag] & CMSPAR)

        standard_termios(tio)
        termios.tcsetattr(self._fd, termios.TCSANOW, tio)

    def set_baud(self, baud: int) -> None:
        tio = termios.tcgetattr(self._fd)
        standard_termios(tio)

        baud_constant = SerialPlatformSpecific.BAUDRATE_CONSTANTS.get(baud)
        if baud_constant:
            try:
                from serial.serialposix import BOTHER
                baud_constant = BOTHER
            except (ImportError, NameError):
                baud_constant = termios.B38400
            custom_speed = baud
        else:
            custom_speed = None
        tio[TCAttr.c_ispeed] = baud_constant
        tio[TCAttr.c_ospeed] = baud_constant

        termios.tcsetattr(self._fd, termios.TCSANOW, tio)

        if custom_speed:
            apply = ApplyPlatformSpecific(self._fd)
            try:
                apply._set_special_baudrate(baud)
            except (IOError, ValueError, NotImplementedError):
                _LOGGER.warning(f"Failed to set custom baud {baud}", exc_info=True)

    def set_data_bits(self, bits: int) -> None:
        tio = termios.tcgetattr(self._fd)
        standard_termios(tio)

        tio[TCAttr.c_cflag] &= ~termios.CSIZE
        if bits == 8:
            tio[TCAttr.c_cflag] |= termios.CS8
        elif bits == 7:
            tio[TCAttr.c_cflag] |= termios.CS7
        elif bits == 6:
            tio[TCAttr.c_cflag] |= termios.CS6
        elif bits == 5:
            tio[TCAttr.c_cflag] |= termios.CS5

        termios.tcsetattr(self._fd, termios.TCSANOW, tio)

    def set_parity(self, parity: Parity) -> None:
        tio = termios.tcgetattr(self._fd)
        standard_termios(tio)

        tio[TCAttr.c_cflag] &= ~(termios.PARENB | termios.PARODD)
        if CMSPAR:
            tio[TCAttr.c_cflag] &= ~CMSPAR
        if parity == Parity.NONE:
            pass
        elif parity == Parity.EVEN:
            tio[TCAttr.c_cflag] |= termios.PARENB
        elif parity == Parity.ODD:
            tio[TCAttr.c_cflag] |= (termios.PARENB | termios.PARODD)
        elif CMSPAR and parity == Parity.MARK:
            tio[TCAttr.c_cflag] |= (termios.PARENB | CMSPAR | termios.PARODD)
        elif CMSPAR and parity == Parity.SPACE:
            tio[TCAttr.c_cflag] |= (termios.PARENB | CMSPAR)
        else:
            _LOGGER.warning(f"Unsupported parity {parity}")

        termios.tcsetattr(self._fd, termios.TCSANOW, tio)

    def set_stop_bits(self, bits: int) -> None:
        tio = termios.tcgetattr(self._fd)
        standard_termios(tio)

        if bits == 1:
            tio[TCAttr.c_cflag] &= ~termios.CSTOPB
        else:
            tio[TCAttr.c_cflag] |= termios.CSTOPB

        termios.tcsetattr(self._fd, termios.TCSANOW, tio)

    def set_rs485(self, rs485: typing.Optional[RS485Settings]) -> None:
        apply = ApplyPlatformSpecific(self._fd)
        try:
            apply._set_rs485_mode(rs485)
        except (IOError, ValueError, NotImplementedError):
            _LOGGER.warning("Failed to set RS485 mode", exc_info=True)

    def set_rts(self, state: bool) -> None:
        try:
            if state:
                ioctl(self._fd, TIOCMBIS, TIOCM_RTS_str)
            else:
                ioctl(self._fd, TIOCMBIC, TIOCM_RTS_str)
        except IOError:
            _LOGGER.warning("Failed to set RTS line", exc_info=True)

    def set_dtr(self, state: bool) -> None:
        try:
            if state:
                ioctl(self._fd, TIOCMBIS, TIOCM_DTR_str)
            else:
                ioctl(self._fd, TIOCMBIC, TIOCM_DTR_str)
        except IOError:
            _LOGGER.warning("Failed to set DTR line", exc_info=True)

    def flush_from_downstream(self) -> None:
        self._write_buffer.clear()
        termios.tcflush(self._fd, termios.TCOFLUSH)

    def break_from_downstream(self) -> None:
        self._write_buffer.clear()
        flush_begin = time.monotonic()
        termios.tcsendbreak(self._fd, 0)
        elapsed = time.monotonic() - flush_begin

        total_sleep = 0.1
        if elapsed < 0.5:
            total_sleep += 0.5 - elapsed

        tio = termios.tcgetattr(self._fd)
        speed_constant = tio[TCAttr.c_ispeed]
        port_baud = 0
        for baud, c in SerialPlatformSpecific.BAUDRATE_CONSTANTS.items():
            if c == speed_constant and baud > 0:
                symbols_to_sleep = 10 * 2  # Approx 2 frames
                hz_to_sleep = symbols_to_sleep * baud
                total_sleep += 1.0 / hz_to_sleep
                port_baud = baud
                break

        flush_bytes = 4096
        if port_baud > 0:
            expected_symbols = port_baud * total_sleep
            expected_bytes = expected_symbols / 10
            expected_bytes += 1
            if expected_bytes > flush_bytes:
                flush_bytes = expected_bytes

        sleep_begin = time.monotonic()
        total_bytes_read = 0
        while True:
            remaining_timeout = total_sleep - (time.monotonic() - sleep_begin)
            if remaining_timeout < 0.01:
                remaining_timeout = 0
            ready_read, _, _ = select.select([self._fd], [], [], remaining_timeout)
            if remaining_timeout <= 0.0:
                if not ready_read:
                    break
                if total_bytes_read >= flush_bytes:
                    break

            try:
                data = os.read(self._fd, 4096)
                total_bytes_read += len(data)
            except OSError as e:
                if e.errno != errno.EAGAIN and e.errno != errno.ETIMEDOUT:
                    raise

    def _initialize_device(self) -> None:
        tio = termios.tcgetattr(self._fd)
        standard_termios(tio)

        # Since we do not get bytesize/parity from the downstream, set it to 8/N (what the psuedoterminal always reads)
        tio[TCAttr.c_cflag] &= ~termios.CSIZE
        tio[TCAttr.c_cflag] |= termios.CS8
        tio[TCAttr.c_cflag] &= ~(termios.PARENB | termios.PARODD)
        if CMSPAR:
            tio[TCAttr.c_cflag] &= ~CMSPAR

        termios.tcsetattr(self._fd, termios.TCSANOW, tio)

    def reopen(self) -> None:
        self._write_buffer.clear()
        self.data_to_downstream = None

        self._poll.unregister(self._fd)
        os.close(self._fd)

        time.sleep(0.1)

        self._fd = os.open(self.device, os.O_RDWR | os.O_NONBLOCK | os.O_NOCTTY | os.O_CLOEXEC)
        self._initialize_device()
        self._poll.register(self._fd, select.POLLIN)

        _LOGGER.info(f"Serial port {self} re-opened")


class Downstream:
    def __init__(self, target: str, poll):
        self._fd = None
        self.target = target
        self._poll = poll

        master, slave = os.openpty()
        self.slave_name = os.ttyname(slave)

        ioctl(master, 0x5420, struct.pack('@i', 1))  # TIOCPKT
        tio = termios.tcgetattr(slave)
        standard_termios(tio)
        tio[TCAttr.c_lflag.value] |= 0o0200000  # EXTPROC
        termios.tcsetattr(slave, termios.TCSANOW, tio)

        os.fchmod(slave, 0o666 & ~UMASK)
        try:
            os.fchown(slave, os.geteuid(), os.getegid())
        except PermissionError:
            pass

        os.close(slave)

        os.set_blocking(master, False)
        # Discard the first packet/termios operation
        try:
            os.read(master, 4096)
        except OSError as e:
            if e.errno != errno.EAGAIN and e.errno != errno.ETIMEDOUT:
                raise

        self._fd = master
        self._slave_fd_open: typing.Optional[int] = None

        self.tty_name = self.slave_name

        try:
            os.unlink(self.target)
        except OSError:
            pass
        os.symlink(self.tty_name, self.target)

        self._slave_possibly_open = False
        self._slave_close_event = False
        self.data_to_upstream: typing.Optional[bytes] = None
        self._write_buffer = bytearray()

        self.termios_to_upstream: typing.Optional[typing.List] = None
        self.break_to_upstream = False
        self.flush_to_upstream = False

        self._poll.register(self._fd, select.POLLIN | select.POLLHUP)
        _LOGGER.debug(f"Downstream master {target} linked to {self.tty_name} opened as {self._fd}")

    def __repr__(self):
        return self.target

    def __del__(self):
        if self._fd is not None:
            try:
                os.close(self._fd)
            except OSError:
                pass
            self._fd = None
        try:
            os.unlink(self.target)
        except OSError:
            pass

    def shutdown(self) -> None:
        try:
            os.unlink(self.target)
        except OSError:
            pass

    def _should_read_data(self) -> bool:
        return not self.data_to_upstream

    def data_from_upstream(self, data: bytes):
        if len(self._write_buffer) > 65536:
            return
        self._write_buffer += data

    def data_from_eavesdropper(self, data: bytes):
        self._write_buffer += data

    def poll_begin(self) -> None:
        mask = 0
        if self._should_read_data():
            mask |= select.POLLIN | select.POLLPRI
        if self._write_buffer:
            mask |= select.POLLOUT

        if self._slave_possibly_open:
            mask |= select.POLLHUP

        self._poll.modify(self._fd, mask)

    def _process_packet(self, raw: bytes) -> None:
        flags = int(raw[0])
        if flags == 0:
            self.data_to_upstream = raw[1:]
            return
        # Repurpose tcflow TCOOFF->TCOON (ser.set_output_flow_control) to send a break
        if flags & TIOCPKT.START.value:
            self.break_to_upstream = True
        if flags & TIOCPKT.FLUSHREAD.value:
            self._write_buffer.clear()
        if flags & TIOCPKT.FLUSHWRITE.value:
            self.data_to_upstream = None
            self.flush_to_upstream = True
        if flags & TIOCPKT.IOCTL.value:
            self.termios_to_upstream = termios.tcgetattr(self._fd)

    def close_event_detected(self) -> bool:
        result = self._slave_close_event
        self._slave_close_event = False
        return result

    def poll_result(self, events: typing.List[typing.Tuple[int, int]]) -> None:
        for fd, mask in events:
            if fd == self._fd:
                if (mask & (select.POLLIN | select.POLLPRI)) and self.data_to_upstream is None:
                    try:
                        raw = os.read(self._fd, 4096)
                    except OSError as e:
                        if e.errno != errno.EAGAIN and e.errno != errno.ETIMEDOUT:
                            raise
                        raw = None
                    if raw:
                        self._slave_possibly_open = True
                        if self._slave_fd_open is not None:
                            os.close(self._slave_fd_open)
                            self._slave_fd_open = None

                        self._process_packet(raw)

                if (mask & select.POLLOUT) and self._write_buffer:
                    try:
                        n = os.write(self._fd, self._write_buffer)
                        if n >= len(self._write_buffer):
                            self._write_buffer.clear()
                        elif n > 0:
                            del self._write_buffer[0:n]
                    except OSError as e:
                        if e.errno != errno.EAGAIN and e.errno != errno.ETIMEDOUT:
                            raise

                if mask & select.POLLHUP:
                    if self._slave_possibly_open:
                        _LOGGER.debug(f"Detected close on {self}")
                        self._slave_close_event = True
                    self._slave_possibly_open = False

                    # Open a copy, since we get POLLHUP (even if masked out) until there's something with it open
                    if self._slave_fd_open is None:
                        self._slave_fd_open = os.open(self.slave_name, os.O_RDWR | os.O_NOCTTY | os.O_CLOEXEC)

    def reset(self) -> None:
        self._write_buffer.clear()
        self.data_to_upstream = None
        self.termios_to_upstream = None
        self.break_to_upstream = False
        self.flush_to_upstream = False


class Eavesdropper:
    class _Connection:
        def __init__(self, socket: socket.socket, poll):
            self._socket = socket
            self._poll = poll

            self._socket.setblocking(True)
            self._poll.register(self._socket, select.POLLIN)

            self._read_buffer = bytearray()
            self._write_buffer = bytearray()

            self.data_to_upstream: typing.Optional[bytes] = None
            self.data_to_downstream: typing.Optional[bytes] = None
            self.apply_reset = False

        def shutdown(self):
            if self._socket:
                self._poll.unregister(self._socket)
                try:
                    self._socket.close()
                except OSError:
                    pass
                self._socket = None

        def _queue_data_packets(self, data: bytes, packet_type: FromMultiplexer):
            while data:
                packet_length = min(0xFF, len(data))
                self._write_buffer += struct.pack('<BB', packet_type.value, packet_length)
                self._write_buffer += data[0:packet_length]
                if packet_length >= len(data):
                    break
                data = data[packet_length:]

        def data_from_upstream(self, data: bytes):
            self._queue_data_packets(data, FromMultiplexer.FROM_SERIAL_PORT)

        def data_from_downstream(self, data: bytes):
            self._queue_data_packets(data, FromMultiplexer.TO_SERIAL_PORT)

        def _process_read(self) -> bool:
            if not self._read_buffer:
                return True

            try:
                packet_type = ToMultiplexer(struct.unpack_from('<B', self._read_buffer)[0])
            except ValueError:
                _LOGGER.warning(f"Invalid eavesdropper packet type", exc_info=True)
                return False

            if packet_type == ToMultiplexer.RESET_SERIAL_PORT:
                self.apply_reset = True
                del self._read_buffer[0]
            elif packet_type == ToMultiplexer.WRITE_SERIAL_PORT:
                if len(self._read_buffer) < 2:
                    return True
                packet_length = struct.unpack_from('<B', self._read_buffer, offset=1)[0]
                end_of_data = 2 + packet_length
                if len(self._read_buffer) < end_of_data:
                    return True
                if self.data_to_upstream is None:
                    self.data_to_upstream = self._read_buffer[2:end_of_data]
                else:
                    self.data_to_upstream = self.data_to_upstream + self._read_buffer[2:end_of_data]
                del self._read_buffer[0:end_of_data]
            elif packet_type == ToMultiplexer.WRITE_MULTIPLEXED:
                if len(self._read_buffer) < 2:
                    return True
                packet_length = struct.unpack_from('<B', self._read_buffer, offset=1)[0]
                end_of_data = 2 + packet_length
                if len(self._read_buffer) < end_of_data:
                    return True
                if self.data_to_downstream is None:
                    self.data_to_downstream = self._read_buffer[2:end_of_data]
                else:
                    self.data_to_downstream = self.data_to_downstream + self._read_buffer[2:end_of_data]
                del self._read_buffer[0:end_of_data]
            else:
                _LOGGER.warning(f"Unsupported eavesdropper packet type {packet_type}")
                return False

            return self._process_read()

        def poll_begin(self):
            mask = select.POLLIN
            if self._write_buffer:
                mask |= select.POLLOUT
            self._poll.modify(self._socket, mask)

        def poll_result(self, mask: int) -> bool:
            if mask & select.POLLIN:
                try:
                    data = self._socket.recv(4096)
                    if not data:
                        return False
                    self._read_buffer += data
                except OSError as e:
                    if e.errno != errno.EAGAIN and e.errno != errno.ETIMEDOUT:
                        return False
                if not self._process_read():
                    return False

            if (mask & select.POLLOUT) and self._write_buffer:
                try:
                    n = self._socket.send(self._write_buffer)
                    if n >= len(self._write_buffer):
                        self._write_buffer.clear()
                    elif n > 0:
                        del self._write_buffer[0:n]
                except OSError as e:
                    if e.errno != errno.EAGAIN and e.errno != errno.ETIMEDOUT:
                        return False

            return True

    def __init__(self, socket_name: str, poll):
        self.socket_name = socket_name
        self._poll = poll

        try:
            os.unlink(self.socket_name)
        except OSError:
            pass

        self._server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._server.setblocking(False)
        self._server.bind(self.socket_name)
        self._server.listen(8)
        self._poll.register(self._server, select.POLLIN)

        self._connections: typing.Dict[int, "Eavesdropper._Connection"] = dict()

    def __del__(self):
        try:
            os.unlink(self.socket_name)
        except OSError:
            pass

    def shutdown(self) -> None:
        if self._server is not None:
            try:
                self._server.close()
            except OSError:
                pass
            self._server = None
        for c in self._connections.values():
            c.shutdown()
        try:
            os.unlink(self.socket_name)
        except OSError:
            pass

    def _accept_connection(self, conn: typing.Optional[socket.socket]) -> None:
        if not conn:
            return
        self._connections[conn.fileno()] = self._Connection(conn, self._poll)
        _LOGGER.debug("Accepted eavesdropper connection")

    def get_upstream_data(self) -> typing.Optional[bytes]:
        result = None
        for c in self._connections.values():
            data = c.data_to_upstream
            c.data_to_upstream = None
            if not result:
                result = data
            else:
                result = result + data
        return result

    def get_downstream_data(self) -> typing.Optional[bytes]:
        result = None
        for c in self._connections.values():
            data = c.data_to_downstream
            c.data_to_downstream = None
            if not result:
                result = data
            else:
                result = result + data
        return result

    def get_reset(self) -> bool:
        result = False
        for c in self._connections.values():
            if c.apply_reset:
                result = True
                c.apply_reset = False
        return result

    def data_from_upstream(self, data: bytes) -> None:
        for c in self._connections.values():
            c.data_from_upstream(data)

    def data_from_downstream(self, data: bytes) -> None:
        for c in self._connections.values():
            c.data_from_downstream(data)

    def poll_begin(self) -> None:
        for c in self._connections.values():
            c.poll_begin()

    def poll_result(self, events: typing.List[typing.Tuple[int, int]]) -> None:
        for fd, mask in events:
            check = self._connections.get(fd)
            if check:
                if not check.poll_result(mask):
                    check.shutdown()
                    del self._connections[fd]
                continue

            if fd == self._server.fileno():
                conn, addr = self._server.accept()
                self._accept_connection(conn)


class Raw:
    class _Connection:
        def __init__(self, socket: socket.socket, poll):
            self._socket = socket
            self._poll = poll

            self._socket.setblocking(True)
            self._poll.register(self._socket, select.POLLIN)

            self._write_buffer = bytearray()
            self.data_to_upstream: typing.Optional[bytes] = None

        def shutdown(self):
            if self._socket:
                self._poll.unregister(self._socket)
                try:
                    self._socket.close()
                except OSError:
                    pass
                self._socket = None

        def data_from_upstream(self, data: bytes):
            self._write_buffer += data

        def poll_begin(self):
            mask = select.POLLIN
            if self._write_buffer:
                mask |= select.POLLOUT
            self._poll.modify(self._socket, mask)

        def poll_result(self, mask: int) -> bool:
            if mask & select.POLLIN:
                try:
                    data = self._socket.recv(4096)
                    if not data:
                        return False
                    if not self.data_to_upstream:
                        self.data_to_upstream = data
                    else:
                        self.data_to_upstream = self.data_to_upstream + data
                except OSError as e:
                    if e.errno != errno.EAGAIN and e.errno != errno.ETIMEDOUT:
                        return False

            if (mask & select.POLLOUT) and self._write_buffer:
                try:
                    n = self._socket.send(self._write_buffer)
                    if n >= len(self._write_buffer):
                        self._write_buffer.clear()
                    elif n > 0:
                        del self._write_buffer[0:n]
                except OSError as e:
                    if e.errno != errno.EAGAIN and e.errno != errno.ETIMEDOUT:
                        return False

            return True

    def __init__(self, socket_name: str, poll):
        self.socket_name = socket_name
        self._poll = poll

        try:
            os.unlink(self.socket_name)
        except OSError:
            pass

        self._server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._server.setblocking(False)
        self._server.bind(self.socket_name)
        self._server.listen(8)
        self._poll.register(self._server, select.POLLIN)

        self._connections: typing.Dict[int, "Raw._Connection"] = dict()

    def __del__(self):
        try:
            os.unlink(self.socket_name)
        except OSError:
            pass

    def shutdown(self) -> None:
        if self._server is not None:
            try:
                self._server.close()
            except OSError:
                pass
            self._server = None
        for c in self._connections.values():
            c.shutdown()
        try:
            os.unlink(self.socket_name)
        except OSError:
            pass

    def _accept_connection(self, conn: typing.Optional[socket.socket]) -> None:
        if not conn:
            return
        self._connections[conn.fileno()] = self._Connection(conn, self._poll)
        _LOGGER.debug("Accepted raw connection")

    def get_upstream_data(self) -> typing.Optional[bytes]:
        result = None
        for c in self._connections.values():
            data = c.data_to_upstream
            c.data_to_upstream = None
            if not result:
                result = data
            else:
                result = result + data
        return result

    def data_from_upstream(self, data: bytes) -> None:
        for c in self._connections.values():
            c.data_from_upstream(data)

    def poll_begin(self) -> None:
        for c in self._connections.values():
            c.poll_begin()

    def poll_result(self, events: typing.List[typing.Tuple[int, int]]) -> None:
        for fd, mask in events:
            check = self._connections.get(fd)
            if check:
                if not check.poll_result(mask):
                    check.shutdown()
                    del self._connections[fd]
                continue

            if fd == self._server.fileno():
                conn, addr = self._server.accept()
                self._accept_connection(conn)


class Control:
    def __init__(self, socket_name: str, poll):
        self.socket_name = socket_name
        self._poll = poll

        try:
            os.unlink(self.socket_name)
        except OSError:
            pass

        self._server = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        self._server.setblocking(False)
        self._server.bind(self.socket_name)
        self._poll.register(self._server, select.POLLIN)

        self.upstream_operations: typing.Optional[typing.List[typing.Callable[[Upstream], None]]] = None

    def __del__(self):
        try:
            os.unlink(self.socket_name)
        except OSError:
            pass

    def shutdown(self) -> None:
        if self._server is not None:
            try:
                self._server.close()
            except OSError:
                pass
            self._server = None
        try:
            os.unlink(self.socket_name)
        except OSError:
            pass

    def _add_operation(self, op: typing.Callable[[Upstream], None]) -> None:
        if not self.upstream_operations:
            self.upstream_operations = list()
        self.upstream_operations.append(op)

    def _process_packet(self, packet: bytes) -> None:
        if len(packet) < 1:
            return
        try:
            operation = ControlOperation(struct.unpack_from('<B', packet)[0])
        except ValueError:
            _LOGGER.warning(f"Invalid control packet type", exc_info=True)
            return

        if operation == ControlOperation.SET_BAUD:
            try:
                baud = struct.unpack_from('<I', packet, offset=1)[0]
                if baud <= 1:
                    raise struct.error
            except struct.error:
                _LOGGER.warning(f"Invalid control baud", exc_info=True)
                return
            self._add_operation(lambda upstream: upstream.set_baud(baud))
        elif operation == ControlOperation.SET_DATA_BITS:
            try:
                data_bits = struct.unpack_from('<B', packet, offset=1)[0]
                if not (5 <= data_bits <= 8):
                    raise struct.error
            except struct.error:
                _LOGGER.warning(f"Invalid control data bits", exc_info=True)
                return
            self._add_operation(lambda upstream: upstream.set_data_bits(data_bits))
        elif operation == ControlOperation.SET_PARITY:
            try:
                parity = Parity(struct.unpack_from('<B', packet, offset=1)[0])
            except (struct.error, ValueError):
                _LOGGER.warning(f"Invalid control parity", exc_info=True)
                return
            self._add_operation(lambda upstream: upstream.set_parity(parity))
        elif operation == ControlOperation.SET_STOP_BITS:
            try:
                stop_bits = struct.unpack_from('<B', packet, offset=1)[0]
                if not (1 <= stop_bits <= 2):
                    raise struct.error
            except struct.error:
                _LOGGER.warning(f"Invalid control stop bits", exc_info=True)
                return
            self._add_operation(lambda upstream: upstream.set_stop_bits(stop_bits))
        elif operation == ControlOperation.SET_RS485:
            try:
                enable = struct.unpack_from('<B', packet, offset=1)[0]
            except struct.error:
                _LOGGER.warning(f"Invalid RS485 mode", exc_info=True)
                return
            if enable == 0:
                self._add_operation(lambda upstream: upstream.set_rs485(None))
                return

            try:
                rts_for_tx, rts_for_rx, loopback, before_tx, before_rx = struct.unpack_from('<BBBff', packet, offset=2)
            except struct.error:
                _LOGGER.warning(f"Invalid RS485 settings", exc_info=True)
                return
            settings = RS485Settings(
                rts_for_tx != 0,
                rts_for_rx != 0,
                loopback != 0,
                before_tx,
                before_rx,
            )
            self._add_operation(lambda upstream: upstream.set_rs485(settings))
        elif operation == ControlOperation.SET_RTS:
            try:
                enable = (struct.unpack_from('<B', packet, offset=1)[0]) != 0
            except struct.error:
                _LOGGER.warning(f"Invalid RTS state", exc_info=True)
                return
            self._add_operation(lambda upstream: upstream.set_rts(enable))
        elif operation == ControlOperation.SET_DTR:
            try:
                enable = (struct.unpack_from('<B', packet, offset=1)[0]) != 0
            except struct.error:
                _LOGGER.warning(f"Invalid DTR state", exc_info=True)
                return
            self._add_operation(lambda upstream: upstream.set_dtr(enable))
        elif operation == ControlOperation.FLUSH:
            self._add_operation(lambda upstream: upstream.flush_from_downstream())
        elif operation == ControlOperation.BREAK:
            self._add_operation(lambda upstream: upstream.break_from_downstream())
        elif operation == ControlOperation.REOPEN:
            self._add_operation(lambda upstream: upstream.reopen())
        else:
            raise RuntimeError

    def poll_result(self, events: typing.List[typing.Tuple[int, int]]) -> None:
        for fd, mask in events:
            if fd == self._server.fileno():
                data = self._server.recv(64)
                if not data:
                    continue
                self._process_packet(data)


def main():
    parser = argparse.ArgumentParser(description="Forge serial port multiplexer.")

    parser.add_argument('--debug',
                        dest='debug', action='store_true',
                        help="enable debug output")
    parser.add_argument('--systemd',
                        dest='systemd', action='store_true',
                        help="enable systemd integration")
    parser.add_argument('--multiple-master',
                        dest='multiple_master', action='store_true',
                        help="allow upstream control for downstream connections after the first")

    parser.add_argument('--eavesdropper',
                        dest='eavesdropper',
                        help="specify a socket to listen for eavesdropper protocol connection on")
    parser.add_argument('--raw',
                        dest='raw',
                        help="specify a socket to listen for raw connections on")
    parser.add_argument('--control',
                        dest='control',
                        help="specify a datagram socket to accept control packets from")

    parser.add_argument('upstream',
                        help="backing serial port device",)
    parser.add_argument('downstream',
                        help="downstream device link",
                        nargs='+')

    args = parser.parse_args()
    if args.debug:
        root_logger = logging.getLogger()
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(name)-40s %(message)s')
        handler.setFormatter(formatter)
        root_logger.setLevel(logging.DEBUG)
        root_logger.addHandler(handler)

    def shutdown(sig, frame):
        _LOGGER.info("Shutting down")

        if args.systemd:
            import systemd.daemon
            systemd.daemon.notify("STOPPING=1")

        exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    poll = select.poll()

    upstream = Upstream(args.upstream, poll)
    downstream: typing.List[Downstream] = list()
    for name in args.downstream:
        downstream.append(Downstream(name, poll))
    eavesdropper: typing.Optional[Eavesdropper] = None
    if args.eavesdropper:
        eavesdropper = Eavesdropper(args.eavesdropper, poll)
    raw: typing.Optional[Raw] = None
    if args.raw:
        raw = Raw(args.raw, poll)
    control: typing.Optional[Control] = None
    if args.control:
        control = Control(args.control, poll)

    try:
        _LOGGER.info(f"Multiplexer connected to {args.upstream} on downstream: {' '.join(args.downstream)}")
        if eavesdropper:
            _LOGGER.info(f"Eavesdropper socket listening on {args.eavesdropper}")
        if raw:
            _LOGGER.info(f"Raw socket listening on {args.raw}")
        if control:
            _LOGGER.info(f"Control socket listening on {args.control}")

        service_heartbeat = lambda: None
        if args.systemd:
            import systemd.daemon
            systemd.daemon.notify("READY=1")
            service_heartbeat = lambda: systemd.daemon.notify("WATCHDOG=1")

        next_heartbeat = time.monotonic() + 10.0
        while True:
            remaining_time = next_heartbeat - time.monotonic()
            if remaining_time < 0.01:
                next_heartbeat = time.monotonic() + 10.0
                service_heartbeat()
                remaining_time = 10.0

            if control:
                if control.upstream_operations:
                    ops = control.upstream_operations
                    control.upstream_operations = None
                    for op in ops:
                        op(upstream)

            for d in downstream:
                if d.termios_to_upstream:
                    upstream.termios_from_downstream(d.termios_to_upstream)
                    d.termios_to_upstream = None

                if d.flush_to_upstream:
                    upstream.flush_from_downstream()
                    d.flush_to_upstream = False

                if d.break_to_upstream:
                    upstream.break_from_downstream()
                    d.break_to_upstream = False

                if not args.multiple_master:
                    break

            data = upstream.data_to_downstream
            upstream.data_to_downstream = None
            if data:
                for d in downstream:
                    d.data_from_upstream(data)
                if eavesdropper:
                    eavesdropper.data_from_upstream(data)
                if raw:
                    raw.data_from_upstream(data)

            for d in downstream:
                if not upstream.can_accept_downstream_data:
                    break
                data = d.data_to_upstream
                d.data_to_upstream = None
                if data:
                    upstream.data_from_downstream(data)
                    if eavesdropper:
                        eavesdropper.data_from_downstream(data)

            reset_detected = False
            for d in downstream:
                if d.close_event_detected():
                    reset_detected = True
                if not args.multiple_master:
                    break

            if eavesdropper:
                data = eavesdropper.get_upstream_data()
                if data:
                    upstream.data_from_eavesdropper(data)

                data = eavesdropper.get_downstream_data()
                if data:
                    for d in downstream:
                        d.data_from_eavesdropper(data)

                if eavesdropper.get_reset():
                    reset_detected = True

            if raw:
                data = raw.get_upstream_data()
                if data:
                    upstream.data_from_raw(data)

            if reset_detected:
                upstream.reopen()
                for d in downstream:
                    d.reset()

            upstream.poll_begin()
            for d in downstream:
                d.poll_begin()
            if eavesdropper:
                eavesdropper.poll_begin()
            if raw:
                raw.poll_begin()

            poll_result = poll.poll(max(int(remaining_time * 1000), 10))

            upstream.poll_result(poll_result)
            for d in downstream:
                d.poll_result(poll_result)
            if eavesdropper:
                eavesdropper.poll_result(poll_result)
            if raw:
                raw.poll_result(poll_result)
            if control:
                control.poll_result(poll_result)
    finally:
        for d in downstream:
            d.shutdown()
        if eavesdropper:
            eavesdropper.shutdown()
        if raw:
            raw.shutdown()
        if control:
            control.shutdown()


if __name__ == '__main__':
    main()
