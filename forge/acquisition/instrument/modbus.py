import typing
import asyncio
import logging
import enum
import struct
import crc
from io import BytesIO
from forge.tasks import wait_cancelable
from .base import CommunicationsError
from .streaming import StreamingContext, StreamingInstrument, StreamingSimulator
from .flag import Flag, Notification

_LOGGER = logging.getLogger(__name__)


# crc 1.0 compat
CrcCalculator = getattr(crc, "Calculator", None)
if not CrcCalculator:
    CrcCalculator = crc.CrcCalculator
if not getattr(CrcCalculator, 'checksum', None):
    setattr(CrcCalculator, 'checksum', getattr(CrcCalculator, 'calculate_checksum'))


class ModbusExceptionCode(enum.IntEnum):
    ILLEGAL_FUNCTION = 1
    ILLEGAL_DATA_ADDRESS = 2
    ILLEGAL_DATA_VALUE = 3
    SERVER_DEVICE_FAILURE = 4
    ACKNOWLEDGE = 5
    SERVER_DEVICE_BUSY = 6
    NEGATIVE_ACKNOWLEDGE = 7
    MEMORY_PARITY_ERROR = 8
    GATEWAY_PATH_UNAVAILABLE = 10
    GATEWAY_TARGET_UNRESPONSIVE = 11

    def __str__(self) -> str:
        if self == ModbusExceptionCode.ILLEGAL_FUNCTION:
            return "Illegal Function"
        elif self == ModbusExceptionCode.ILLEGAL_DATA_ADDRESS:
            return "Illegal Data Address"
        elif self == ModbusExceptionCode.ILLEGAL_DATA_VALUE:
            return "Illegal Data Value"
        elif self == ModbusExceptionCode.SERVER_DEVICE_FAILURE:
            return "Server Device Failure"
        elif self == ModbusExceptionCode.ACKNOWLEDGE:
            return "Acknowledge (polling unsupported)"
        elif self == ModbusExceptionCode.SERVER_DEVICE_BUSY:
            return "Server Device Busy"
        elif self == ModbusExceptionCode.NEGATIVE_ACKNOWLEDGE:
            return "Negative Acknowledge"
        elif self == ModbusExceptionCode.MEMORY_PARITY_ERROR:
            return "Memory Parity Error"
        elif self == ModbusExceptionCode.GATEWAY_PATH_UNAVAILABLE:
            return "Gateway Path Unavailable"
        elif self == ModbusExceptionCode.GATEWAY_TARGET_UNRESPONSIVE:
            return "Gateway Target Device Failed to Respond"


class ModbusProtocol(enum.Enum):
    RTU = "RTU"
    ASCII = "ASCII"
    TCP = "TCP"


class ModbusException(CommunicationsError):
    def __init__(self, exception_code: typing.Union[int, ModbusExceptionCode], *args,
                 function_code: typing.Optional[int] = None):
        super().__init__(*args)
        if function_code is not None:
            self.function_code: typing.Optional[int] = function_code & ~0x80
        else:
            self.function_code: typing.Optional[int] = None
        if isinstance(exception_code, ModbusExceptionCode):
            self.exception_code = exception_code
            self.raw_exception_code: int = int(self.exception_code)
        else:
            self.raw_exception_code: int = exception_code
            try:
                self.exception_code: typing.Optional[ModbusExceptionCode] = ModbusExceptionCode(exception_code)
            except ValueError:
                self.exception_code: typing.Optional[ModbusExceptionCode] = None

    def __str__(self):
        if self.exception_code is not None:
            raw = str(self.exception_code)
        else:
            raw = f"exception code {self.raw_exception_code}"
        message = super().__str__()
        if not message:
            return raw
        else:
            return raw + ": " + message

    def __repr__(self):
        return f"ModbusException('{str(self)}')"


def _ascii_lrc(data: typing.Iterable[int]) -> int:
    s = sum(data)
    s = -s
    return s & 0xFF


async def _read_mapped_inner(indices: typing.Iterable[int], span_gaps: bool,
                             read_multiple: typing.Callable[[int, int], typing.Awaitable[typing.List]],
                             maximum_span: int = 2008) -> typing.Dict:
    sorted = list(indices)
    sorted.sort()

    result = dict()
    start_index = 0
    while start_index < len(sorted):
        end_index = start_index + 1
        while end_index < len(sorted):
            if not span_gaps and sorted[end_index] != sorted[end_index-1]:
                break
            if sorted[end_index] - sorted[start_index] >= maximum_span:
                break
            end_index += 1

        read_data = await read_multiple(sorted[start_index], sorted[end_index-1])
        read_index = sorted[start_index]
        output_index = start_index
        for i in range(len(read_data)):
            if read_index == sorted[output_index]:
                result[read_index] = read_data[i]
                output_index += 1
            read_index += 1

        start_index = end_index
    return result


class ModbusInstrument(StreamingInstrument):
    def __init__(self, context: StreamingContext):
        super().__init__(context)

        self.station_address: int = context.config.get("MODBUS.ADDRESS", default=getattr(self, 'STATION_ADDRESS', 255))

        mode = context.config.get("MODBUS.PROTOCOL")
        if mode is not None:
            mode = ModbusProtocol(str(mode).upper())
        else:
            from .tcp import TCPContext
            if isinstance(context, TCPContext):
                mode = ModbusProtocol.TCP
            else:
                mode = getattr(self, 'SERIAL_MODE', ModbusProtocol.ASCII)

        if mode == ModbusProtocol.RTU:
            self._rtu_crc = CrcCalculator(crc.Configuration(
                width=16,
                polynomial=0x8005,
                init_value=0xFFFF,
                reverse_input=True,
                reverse_output=True,
            ), True)
            self._rtu_silence_time = getattr(self.context, 'bit_time', 0.0) * 3.5 * 8

            self.write_frame: typing.Callable[[int, typing.Union[bytes, bytearray]],
                                              typing.Awaitable[None]] = self._write_rtu_frame
            self.read_frame: typing.Callable[[int, int], typing.Awaitable[bytes]] = self._read_rtu_frame
        elif mode == ModbusProtocol.ASCII:
            self.write_frame: typing.Callable[[int, typing.Union[bytes, bytearray]],
                                              typing.Awaitable[None]] = self._write_ascii_frame
            self.read_frame: typing.Callable[[int, int], typing.Awaitable[bytes]] = self._read_ascii_frame
        elif mode == ModbusProtocol.TCP:
            self.write_frame: typing.Callable[[int, typing.Union[bytes, bytearray]],
                                              typing.Awaitable[None]] = self._write_tcp_frame
            self.read_frame: typing.Callable[[int, int], typing.Awaitable[bytes]] = self._read_tcp_frame

            self.transaction_identifier: typing.Optional[int] = context.config.get(
                "MODBUS.TRANSACTION_IDENTIFIER", default=getattr(self, 'TRANSACTION_IDENTIFIER', None))
        else:
            raise ValueError

    @staticmethod
    def _check_packet(function_code: int, expected_function_code: int, expected_length: int,
                      packet: typing.Union[bytes, bytearray]) -> None:
        if function_code & 0x80:
            if len(packet) < 1:
                raise CommunicationsError(f"packet too short")
            code = struct.unpack('>B', packet[:1])[0]
            raise ModbusException(code, function_code=function_code)

        if function_code != expected_function_code:
            raise CommunicationsError(f"invalid function code {function_code} expecting {expected_function_code}")
        if len(packet) != expected_length:
            raise CommunicationsError(f"invalid packet length {len(packet)} expecting {expected_length}")

    async def _write_rtu_frame(self, function_code: int, data: typing.Union[bytes, bytearray]) -> None:
        if self._rtu_silence_time:
            await asyncio.sleep(self._rtu_silence_time)
        packet = struct.pack('>BB', self.station_address, function_code) + data
        self.writer.write(packet)
        calculated_crc = self._rtu_crc.checksum(packet)
        self.writer.write(struct.pack('<H', calculated_crc))  # CRC is little endian

    async def _read_rtu_frame(self, expected_function_code: int, length: int) -> bytes:
        packet = await self.reader.readexactly(2)
        address, function_code = struct.unpack('>BB', packet)
        if address != 255 and self.station_address != 255 and address != self.station_address:
            raise CommunicationsError(f"address {address} expecting {self.station_address}")

        if function_code & 0x80:
            length = 1

        packet = packet + (await self.reader.readexactly(length + 2))
        calculated_crc = self._rtu_crc.checksum(packet[:-2])
        response_crc = struct.unpack('<H', packet[-2:])[0]  # CRC is little endian
        if calculated_crc != response_crc:
            raise CommunicationsError(f"CRC mismatch in {packet}, calculated {calculated_crc:04X} but got {response_crc:04X}")

        data = packet[2:-2]
        self._check_packet(function_code, expected_function_code, length, data)
        return data

    async def _write_ascii_frame(self, function_code: int, data: typing.Union[bytes, bytearray]) -> None:
        packet = bytearray([self.station_address, function_code])
        packet.extend(data)
        lrc = _ascii_lrc(packet)
        packet.append(lrc)
        self.writer.write(b':' + packet.hex().upper().encode('ascii') + b'\r\n')

    async def _read_ascii_frame(self, expected_function_code: int, length: int) -> bytes:
        response = await self.read_line()
        try:
            frame_begin = response.index(b':')
        except ValueError:
            raise CommunicationsError(f"no frame start character in {response}")
        if len(response) < (6+frame_begin+1):
            raise CommunicationsError(f"invalid response {response}")
        response = response[(frame_begin+1):]
        try:
            packet = bytes.fromhex(response.decode('ascii'))
        except ValueError as e:
            raise CommunicationsError(f"error decoding response {response}") from e
        address = packet[0]
        function_code = packet[1]
        response_lrc = packet[-1]

        if address != 255 and self.station_address != 255 and address != self.station_address:
            raise CommunicationsError(f"address {address} expecting {self.station_address}")

        calculated_lrc = _ascii_lrc(packet[:-1])
        if calculated_lrc != response_lrc:
            raise CommunicationsError(f"LRC mismatch in {response}, calculated {calculated_lrc:02X} but got {response_lrc:02X}")

        data = packet[2:-1]
        self._check_packet(function_code, expected_function_code, length, data)
        return data

    def next_transaction_identifier(self) -> int:
        n = self.transaction_identifier + 1
        if n > 0xFFFF:
            n = 0
        return n

    async def _write_tcp_frame(self, function_code: int, data: typing.Union[bytes, bytearray]) -> None:
        txid = self.transaction_identifier or 0
        self.writer.write(struct.pack('>HHHBB', txid, 0, 2 + len(data), self.station_address, function_code) + data)

    async def _read_tcp_frame(self, expected_function_code: int, length: int) -> bytes:
        txid, protocol, packet_length, unit_id, function_code = struct.unpack('>HHHBB', await self.reader.readexactly(8))
        if self.transaction_identifier is not None and txid != self.transaction_identifier:
            raise CommunicationsError(f"invalid transaction identifier {txid} expecting {self.transaction_identifier}")
        if protocol != 0:
            raise CommunicationsError(f"invalid response protocol {protocol}")
        if packet_length < 2:
            raise CommunicationsError(f"invalid packet length {packet_length}")
        if unit_id != 255 and self.station_address != 255 and unit_id != self.station_address:
            raise CommunicationsError(f"invalid unit identifier {unit_id} expecting {self.station_address}")

        packet = await self.reader.readexactly(packet_length - 2)

        self._check_packet(function_code, expected_function_code, length, packet)

        if self.transaction_identifier is not None:
            self.transaction_identifier = self.next_transaction_identifier()

        return packet

    async def _read_inputs_or_coils(self, function_code: int, first: int, last: int) -> typing.List[bool]:
        count = (last - first) + 1
        if count < 1 or count > 2008:
            raise ValueError
        await self.write_frame(function_code, struct.pack('>HH', first, count))
        expected_bytes = (count + 7) // 8
        response = await self.read_frame(function_code, expected_bytes+1)

        response_bytes = struct.unpack('>B', response[:1])[0]
        if response_bytes != expected_bytes:
            raise CommunicationsError(f"invalid response size {response_bytes} expected {expected_bytes}")
        response = response[1:]

        result: typing.List[bool] = list()
        for i in range(count):
            byte_index = i // 8
            bit_index = (i % 8)
            result.append((response[byte_index] & (1 << bit_index)) != 0)
        return result

    async def read_inputs(self, first: int, last: int) -> typing.List[bool]:
        return await self._read_inputs_or_coils(2, first, last)

    async def read_input(self, index: int) -> bool:
        return (await self.read_inputs(index, index))[0]

    async def read_coils(self, first: int, last: int) -> typing.List[bool]:
        return await self._read_inputs_or_coils(1, first, last)

    async def read_coil(self, index: int) -> bool:
        return (await self.read_coils(index, index))[0]

    async def write_coil(self, index: int, value: bool) -> None:
        await self.write_frame(5, struct.pack('>HH', index, value and 0xFF00 or 0))
        response = await self.read_frame(5, 4)
        response_index, response_value = struct.unpack('>HH', response)
        if response_index != index:
            raise CommunicationsError(f"mismatched response coil index {response_index} expected {index}")
        if (response_value == 0 and value) or (response_value != 0 and not value):
            raise CommunicationsError(f"mismatched response coil state")

    async def write_coils(self, first: int, values: typing.List[bool]) -> None:
        count = len(values)
        if count < 1 or count > 2008:
            raise ValueError
        byte_count = (count + 7) // 8
        packet = bytearray(struct.pack('>HHB', first, count, byte_count))
        for i in range(count):
            bit_index = (i % 8)
            if bit_index == 0:
                packet.append(0)
            if values[i]:
                packet[-1] |= (1 << bit_index)

        await self.write_frame(15, packet)
        response = await self.read_frame(15, 4)
        response_first, response_count = struct.unpack('>HH', response)
        if response_first != first:
            raise CommunicationsError(f"mismatched response coil index {response_first} expected {first}")
        if response_count != count:
            raise CommunicationsError(f"mismatched response coil count {response_count} expected {count}")

    async def _read_input_or_holding_registers(self, function_code: int, first: int, last: int) -> bytes:
        count = (last - first) + 1
        if count < 1 or count > 123:
            raise ValueError
        await self.write_frame(function_code, struct.pack('>HH', first, count))
        expected_bytes = count * 2
        response = await self.read_frame(function_code, expected_bytes + 1)

        response_bytes = struct.unpack('>B', response[:1])[0]
        if response_bytes != expected_bytes:
            raise CommunicationsError(f"invalid response size {response_bytes} expected {expected_bytes}")
        return response[1:]

    async def read_input_registers(self, first: int, last: int) -> bytes:
        return await self._read_input_or_holding_registers(4, first, last)

    async def read_input_register(self, index: int) -> bytes:
        return await self.read_input_registers(index, index)

    async def read_input_integer_registers(self, first: int, last: int,
                                           byteorder: str = '>', unsigned: bool = True) -> typing.List[int]:
        raw = await self.read_input_registers(first, last)
        return list(struct.unpack(f'{byteorder}{last-first+1}{"H" if unsigned else "h"}', raw))

    async def read_input_integer_register(self, index: int, byteorder: str = '>', unsigned: bool = True) -> int:
        return (await self.read_input_integer_registers(index, index, byteorder, unsigned))[0]

    async def read_holding_registers(self, first: int, last: int) -> bytes:
        return await self._read_input_or_holding_registers(3, first, last)

    async def read_holding_register(self, index: int) -> bytes:
        return await self.read_holding_registers(index, index)

    async def read_holding_integer_registers(self, first: int, last: int,
                                             byteorder: str = '>', unsigned: bool = True) -> typing.List[int]:
        raw = await self.read_holding_registers(first, last)
        return list(struct.unpack(f'{byteorder}{last-first+1}{"H" if unsigned else "h"}', raw))

    async def read_holding_integer_register(self, index: int, byteorder: str = '>', unsigned: bool = True) -> int:
        return (await self.read_holding_integer_registers(index, index, byteorder, unsigned))[0]

    async def write_register(self, index: int, data: typing.Union[bytes, bytearray]) -> None:
        if len(data) != 2:
            raise ValueError
        await self.write_frame(6, struct.pack('>H', index) + data)
        response = await self.read_frame(6, 4)
        response_index = struct.unpack('>H', response[:2])[0]
        if response_index != index:
            raise CommunicationsError(f"mismatched response register index {response_index} expected {index}")
        if response[2:] != data:
            raise CommunicationsError(f"mismatched response register value {response[2:]} expected {data}")

    async def write_registers(self, first: int, last: int, data: typing.Union[bytes, bytearray]) -> None:
        count = (last - first) + 1
        if count < 1 or count > 123:
            raise ValueError
        byte_count = count * 2
        if len(data) != byte_count:
            raise ValueError

        await self.write_frame(16, struct.pack('>HHB', first, count, byte_count) + data)
        response = await self.read_frame(16, 4)
        response_first, response_count = struct.unpack('>HH', response)
        if response_first != first:
            raise CommunicationsError(f"mismatched response register index {response_first} expected {first}")
        if response_count != count:
            raise CommunicationsError(f"mismatched response register count {response_count} expected {count}")

    async def write_integer_registers(self, index: int, values: typing.List[int],
                                      byteorder: str = '>', unsigned: bool = True) -> None:
        packet = struct.pack(f'{byteorder}{len(values)}{"H" if unsigned else "h"}', *values)
        await self.write_registers(index, index+len(values)-1, packet)

    async def write_integer_register(self, index: int, value: int,
                                     byteorder: str = '>', unsigned: bool = True) -> None:
        packet = struct.pack(f'{byteorder}{"H" if unsigned else "h"}', value)
        await self.write_register(index, packet)

    def flag_input(self, lookup: typing.Dict[int, Notification], index: int, name: str,
                   bit_offset: int = 0, **kwargs) -> Flag:
        n = self.notification(name, **kwargs)
        lookup[index] = n
        f = self.flag(n, preferred_bit=(1 << (index - bit_offset)))
        return f

    async def read_mapped_inputs(self, inputs: typing.Iterable[int],
                                 span_gaps: bool = True) -> typing.Dict[int, bool]:
        return await _read_mapped_inner(inputs, span_gaps, self.read_inputs)

    async def read_mapped_coils(self, coils: typing.Iterable[int],
                                span_gaps: bool = True) -> typing.Dict[int, bool]:
        return await _read_mapped_inner(coils, span_gaps, self.read_coils)

    async def read_mapped_input_integers(self, inputs: typing.Iterable[int], span_gaps: bool = True,
                                         byteorder: str = '>', unsigned: bool = True) -> typing.Dict[int, int]:
        async def _reader(first: int, last: int):
            return await self.read_input_integer_registers(first, last, byteorder=byteorder, unsigned=unsigned)
        return await _read_mapped_inner(inputs, span_gaps, _reader, maximum_span=123)

    async def read_mapped_holding_integers(self, inputs: typing.Iterable[int], span_gaps: bool = True,
                                           byteorder: str = '>', unsigned: bool = True) -> typing.Dict[int, int]:
        async def _reader(first: int, last: int):
            return await self.read_holding_integer_registers(first, last, byteorder=byteorder, unsigned=unsigned)
        return await _read_mapped_inner(inputs, span_gaps, _reader, maximum_span=123)


class ModbusSimulator(StreamingSimulator):
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        super().__init__(reader, writer)
        self.protocol: ModbusProtocol = ModbusProtocol.ASCII
        self.station_address: int = 255

        self._rtu_crc = CrcCalculator(crc.Configuration(
            width=16,
            polynomial=0x8005,
            init_value=0xFFFF,
            reverse_input=True,
            reverse_output=True,
        ), True)
        self._txid: int = 0

    async def get_input_value(self, index: int) -> bool:
        raise ModbusException(ModbusExceptionCode.ILLEGAL_FUNCTION)

    async def get_coil_value(self, index: int) -> bool:
        return await self.get_input_value(index)

    async def set_coil_value(self, index: int, value: bool) -> None:
        raise ModbusException(ModbusExceptionCode.ILLEGAL_FUNCTION)

    async def get_input_register(self, index: int) -> bytes:
        raise ModbusException(ModbusExceptionCode.ILLEGAL_FUNCTION)

    async def get_holding_register(self, index: int) -> bytes:
        return await self.get_input_register(index)

    async def set_holding_register(self, index: int, value: bytes) -> None:
        raise ModbusException(ModbusExceptionCode.ILLEGAL_FUNCTION)

    @staticmethod
    async def _read_inner_data(function_code: int, read_n: typing.Callable[[int], typing.Awaitable[bytes]]) -> bytes:
        if function_code == 1:
            return await read_n(4)
        elif function_code == 2:
            return await read_n(4)
        elif function_code == 5:
            return await read_n(4)
        elif function_code == 15:
            data = await read_n(5)
            _, _, byte_count = struct.unpack('>HHB', data)
            return data + (await read_n(byte_count))
        elif function_code == 4:
            return await read_n(4)
        elif function_code == 3:
            return await read_n(4)
        elif function_code == 6:
            return await read_n(4)
        elif function_code == 16:
            data = await read_n(5)
            _, _, byte_count = struct.unpack('>HHB', data)
            return data + (await read_n(byte_count))
        else:
            raise ValueError(f"invalid function code {function_code}")

    @staticmethod
    def _static_reader(source: typing.BinaryIO) -> typing.Callable[[int], typing.Awaitable[bytes]]:
        async def read_n(n: int) -> bytes:
            result = bytes()
            while n > 0:
                add = source.read(n)
                if not add:
                    raise ValueError(f"end of data with {n} bytes remaining")
                result += add
                n -= len(add)
            return result
        return read_n

    async def _receive_data(self) -> typing.Tuple[int, bytes]:
        if self.protocol == ModbusProtocol.ASCII:
            line = await self.reader.readuntil(b'\r')
            line = line.strip()
            if line[:1] != b':':
                raise ValueError
            line = line[1:]
            raw = bytes.fromhex(line.decode('ascii'))
            if len(raw) < 3:
                raise ValueError("raw data too short")
            calculated_lrc = _ascii_lrc(raw[:-1])
            received_lrc = struct.unpack('>B', raw[-1:])[0]
            if calculated_lrc != received_lrc:
                raise ValueError(f"LRC mismatch {calculated_lrc:02X} vs {received_lrc:02X} for frame {line}")
            address, function_code = struct.unpack('>BB', raw[:2])
            if address != 255 and self.station_address != 255 and address != self.station_address:
                _LOGGER.debug(f"Ignoring frame for {address}")
                return function_code, bytes()

            source = BytesIO(raw[2:-1])
            reader = self._static_reader(source)
            data = await self._read_inner_data(function_code, reader)
            if len(data) != len(raw) - 3:
                raise ModbusException(ModbusExceptionCode.ILLEGAL_DATA_VALUE, function_code=function_code)

            return function_code, data
        elif self.protocol == ModbusProtocol.RTU:
            header = await self.reader.readexactly(2)
            address, function_code = struct.unpack('>BB', header)
            data = await self._read_inner_data(function_code, self.reader.readexactly)
            received_crc = struct.unpack('<H', await self.reader.readexactly(2))[0]  # CRC is little endian
            frame = header + data
            calculated_crc = self._rtu_crc.checksum(frame)
            if received_crc != calculated_crc:
                raise ValueError(f"CRC mismatch {calculated_crc:04X} vs {received_crc:04X} for {frame}")
            if address != 255 and self.station_address != 255 and address != self.station_address:
                _LOGGER.debug(f"Ignoring frame for {address}")
                return function_code, bytes()

            return function_code, data
        elif self.protocol == ModbusProtocol.TCP:
            header = await self.reader.readexactly(8)
            txid, _, length, address, function_code = struct.unpack('>HHHBB', header)
            if length < 2:
                raise ValueError("frame too short")
            raw = await self.reader.readexactly(length-2)
            if address != 255 and self.station_address != 255 and address != self.station_address:
                _LOGGER.debug(f"Ignoring frame for {address}")
                return function_code, bytes()

            self._txid = txid

            source = BytesIO(raw)
            reader = self._static_reader(source)
            data = await self._read_inner_data(function_code, reader)
            if len(data) != len(raw):
                raise ModbusException(ModbusExceptionCode.ILLEGAL_DATA_VALUE, function_code=function_code)

            return function_code, data
        else:
            raise RuntimeError

    async def _send_data(self, function_code: int, data: typing.Union[bytes, bytearray]) -> None:
        if self.protocol == ModbusProtocol.ASCII:
            frame = struct.pack('>BB', self.station_address, function_code) + data
            lrc = _ascii_lrc(frame)
            frame = frame + struct.pack('>B', lrc)
            self.writer.write(b':' + frame.hex().encode('ascii') + b'\r\n')
        elif self.protocol == ModbusProtocol.RTU:
            frame = struct.pack('>BB', self.station_address, function_code) + data
            calculated_crc = self._rtu_crc.checksum(frame)
            frame = frame + struct.pack('<H', calculated_crc)  # CRC is little endian
            self.writer.write(frame)
        elif self.protocol == ModbusProtocol.TCP:
            self.writer.write(struct.pack('>HHHBB', self._txid, 0, 2 + len(data),
                                          self.station_address, function_code))
            self.writer.write(data)
        else:
            raise RuntimeError
        await self.writer.drain()

    async def _send_exception(self, function_code, exc: typing.Union[int, ModbusExceptionCode]) -> None:
        if isinstance(exc, ModbusExceptionCode):
            exc = exc.value
        await self._send_data(function_code | 0x80, bytes([exc]))

    async def run(self) -> typing.NoReturn:
        while True:
            try:
                function_code, data = await self._receive_data()
            except ValueError:
                _LOGGER.debug("Invalid Modbus frame", exc_info=True)
                continue
            except ModbusException as e:
                _LOGGER.debug("Modbus error receiving data", exc_info=True)
                await self._send_exception(e.function_code, e.raw_exception_code)
                continue
            if not data:
                continue

            try:
                if function_code == 1:
                    first, count = struct.unpack('>HH', data)
                    byte_count = (count + 7) // 8
                    if byte_count > 255:
                        raise ModbusException(ModbusExceptionCode.ILLEGAL_DATA_VALUE, function_code=function_code)
                    response = bytearray()
                    response += struct.pack('>B', byte_count)
                    for i in range(count):
                        bit_index = i % 8
                        if bit_index == 0:
                            response.append(0)
                        if await self.get_coil_value(first + i):
                            response[-1] |= (1 << bit_index)
                    await self._send_data(function_code, response)
                elif function_code == 2:
                    first, count = struct.unpack('>HH', data)
                    byte_count = (count + 7) // 8
                    if byte_count > 255:
                        raise ModbusException(ModbusExceptionCode.ILLEGAL_DATA_VALUE, function_code=function_code)
                    response = bytearray()
                    response += struct.pack('>B', byte_count)
                    for i in range(count):
                        bit_index = i % 8
                        if bit_index == 0:
                            response.append(0)
                        if await self.get_input_value(first + i):
                            response[-1] |= (1 << bit_index)
                    await self._send_data(function_code, response)
                elif function_code == 5:
                    index, value = struct.unpack('>HH', data)
                    await self.set_coil_value(index, value != 0)
                    await self._send_data(function_code, data)
                elif function_code == 15:
                    first, count, byte_count = struct.unpack('>HHB', data[:5])
                    if byte_count != (count + 7) // 8:
                        raise ModbusException(ModbusExceptionCode.ILLEGAL_DATA_VALUE, function_code=function_code)
                    data = data[5:]
                    for i in range(count):
                        byte_index = i // 8
                        bit_index = i % 8
                        value = (data[byte_index] & (1 << bit_index)) != 0
                        await self.set_coil_value(first + i, value)
                    await self._send_data(function_code, struct.pack('>HH', first, count))
                elif function_code == 4:
                    first, count = struct.unpack('>HH', data)
                    byte_count = count * 2
                    if byte_count > 255:
                        raise ModbusException(ModbusExceptionCode.ILLEGAL_DATA_VALUE, function_code=function_code)
                    response = bytearray()
                    response += struct.pack('>B', byte_count)
                    for i in range(count):
                        contents = await self.get_input_register(first + i)
                        if len(contents) != 2:
                            raise RuntimeError
                        response += contents
                    await self._send_data(function_code, response)
                elif function_code == 3:
                    first, count = struct.unpack('>HH', data)
                    byte_count = count * 2
                    if byte_count > 255:
                        raise ModbusException(ModbusExceptionCode.ILLEGAL_DATA_VALUE, function_code=function_code)
                    response = bytearray()
                    response += struct.pack('>B', byte_count)
                    for i in range(count):
                        contents = await self.get_holding_register(first + i)
                        if len(contents) != 2:
                            raise RuntimeError
                        response += contents
                    await self._send_data(function_code, response)
                elif function_code == 6:
                    index = struct.unpack('>H', data[:2])[0]
                    value = data[2:]
                    await self.set_holding_register(index, value)
                    await self._send_data(function_code, data)
                elif function_code == 16:
                    first, count, byte_count = struct.unpack('>HHB', data[:5])
                    if byte_count != count * 2:
                        raise ModbusException(ModbusExceptionCode.ILLEGAL_DATA_VALUE, function_code=function_code)
                    data = data[5:]
                    for i in range(count):
                        start_index = i * 2
                        await self.set_holding_register(first + i, data[start_index:(start_index+2)])
                    await self._send_data(function_code, struct.pack('>HH', first, count))
                else:
                    await self._send_exception(function_code, ModbusExceptionCode.ILLEGAL_FUNCTION)
            except ModbusException as e:
                _LOGGER.debug("Sending exception response", exc_info=True)
                await self._send_exception(function_code, e.raw_exception_code)


def launch(instrument: typing.Type[ModbusInstrument]) -> None:
    from .streaming import launch as launch_streaming
    return launch_streaming(instrument)
