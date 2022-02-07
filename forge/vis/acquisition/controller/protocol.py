import asyncio
import typing
import struct
import logging
from enum import IntEnum
from math import nan, isfinite

_LOGGER = logging.getLogger(__name__)


class ConnectionType(IntEnum):
    ACQUISITION = 0
    DISPLAY = 1


class PacketType(IntEnum):
    DATA = 0
    INSTRUMENT_ADD = 1
    INSTRUMENT_UPDATE = 2
    INSTRUMENT_REMOVE = 3
    INSTRUMENT_STATE = 4
    EVENT_LOG = 5
    COMMAND = 6
    BYPASS = 7
    WRITE_MESSAGE_LOG = 8
    RESTART_SYSTEM = 9
    CHAT = 10


class ValueType(IntEnum):
    NONE = 0
    FLOAT = 1
    ARRAY_OF_FLOAT = 2
    STRING = 3
    INTEGER = 4
    ARRAY = 5
    DICT = 6


_MVC_FLOAT = struct.pack('<f', nan)


def serialize_value(writer: asyncio.StreamWriter, value: typing.Any) -> None:
    def write_float(f: float) -> None:
        if f is None:
            writer.write(_MVC_FLOAT)
            return
        f = float(f)
        if not isfinite(f):
            writer.write(_MVC_FLOAT)
            return
        try:
            writer.write(struct.pack('<f', f))
        except OverflowError:
            writer.write(_MVC_FLOAT)

    if value is None:
        writer.write(struct.pack('<B', ValueType.NONE.value))
        return
    elif isinstance(value, float):
        writer.write(struct.pack('<B', ValueType.FLOAT.value))
        write_float(value)
        return
    elif isinstance(value, str):
        raw = value.encode('utf-8')
        writer.write(struct.pack('<BI', ValueType.STRING.value, len(raw)))
        writer.write(raw)
        return
    elif isinstance(value, int):
        try:
            writer.write(struct.pack('<Bq', ValueType.INTEGER.value, int(value)))
        except OverflowError:
            writer.write(struct.pack('<Bq', ValueType.INTEGER.value, int(0x7FFF_FFFF_FFFF_FFFF)))
        return

    def is_dict() -> bool:
        try:
            iter(value.items())
            return True
        except (AttributeError, TypeError):
            pass
        return False

    if is_dict():
        writer.write(struct.pack('<BI', ValueType.DICT.value, len(value)))
        for k, v in value.items():
            raw = str(k).encode('utf-8')
            writer.write(struct.pack('<I', len(raw)))
            writer.write(raw)
            serialize_value(writer, v)
        return

    def is_array() -> bool:
        try:
            iter(value)
            return True
        except TypeError:
            pass
        return False

    def is_array_of_float() -> bool:
        if len(value) == 0:
            return False
        for v in value:
            if isinstance(v, float):
                continue
            if v is None:
                continue
            return False
        return True

    if is_array():
        if is_array_of_float():
            writer.write(struct.pack('<BI', ValueType.ARRAY_OF_FLOAT.value, len(value)))
            for v in value:
                write_float(v)
            return
        writer.write(struct.pack('<BI', ValueType.ARRAY.value, len(value)))
        for v in value:
            serialize_value(writer, v)
        return

    _LOGGER.warning(f"Unsupported value in serialization {value}")
    writer.write(struct.pack('<B', ValueType.NONE.value))


async def deserialize_value(reader: asyncio.StreamReader) -> typing.Any:
    value_type = ValueType(struct.unpack('<B', await reader.readexactly(1))[0])

    if value_type == ValueType.NONE:
        return None
    elif value_type == ValueType.FLOAT:
        return struct.unpack('<f', await reader.readexactly(4))[0]
    elif value_type == ValueType.ARRAY_OF_FLOAT:
        count = struct.unpack('<I', await reader.readexactly(4))[0]
        raw = await reader.readexactly(4 * count)
        result: typing.List[float] = list()
        for i in range(count):
            result.append(struct.unpack('<f', raw[(i*4):(i*4+4)])[0])
        return result
    elif value_type == ValueType.STRING:
        count = struct.unpack('<I', await reader.readexactly(4))[0]
        raw = await reader.readexactly(count)
        return raw.decode('utf-8')
    elif value_type == ValueType.INTEGER:
        return struct.unpack('<q', await reader.readexactly(8))[0]
    elif value_type == ValueType.ARRAY:
        count = struct.unpack('<I', await reader.readexactly(4))[0]
        result: typing.List[typing.Any] = list()
        for i in range(count):
            result.append(await deserialize_value(reader))
        return result
    elif value_type == ValueType.DICT:
        count = struct.unpack('<I', await reader.readexactly(4))[0]
        result: typing.Dict[str, typing.Any] = dict()
        for i in range(count):
            c = struct.unpack('<I', await reader.readexactly(4))[0]
            raw = await reader.readexactly(c)
            k = raw.decode('utf-8')
            v = await deserialize_value(reader)
            result[k] = v
        return result
    else:
        raise ValueError("Invalid value type")
