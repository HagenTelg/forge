import asyncio
import typing
import struct
import logging
from math import nan, isfinite
from enum import IntEnum
from forge.const import MAX_I64

_LOGGER = logging.getLogger(__name__)


class AcquisitionBusSerializer:
    class ValueType(IntEnum):
        NONE = 0
        FLOAT = 1
        ARRAY_OF_FLOAT = 2
        STRING = 3
        INTEGER = 4
        ARRAY = 5
        DICT = 6

    def __init__(self, double_float: bool = True):
        if double_float:
            self._float_format = '<d'
            self._float_width = 8
        else:
            self._float_format = '<f'
            self._float_width = 4
        self._mvc_float = struct.pack(self._float_format, nan)

    @staticmethod
    def get_read_n(source: typing.Union[asyncio.StreamReader, typing.BinaryIO]) -> typing.Callable[[int], typing.Awaitable[bytes]]:
        try:
            return source.readexactly
        except AttributeError:
            async def read(n: int) -> bytes:
                result = bytes()
                while n > 0:
                    add = source.read(n)
                    if not add:
                        raise EOFError
                    result += add
                    n -= len(add)
                return result

            return read

    @staticmethod
    def serialize_length(writer: typing.BinaryIO, n: int) -> None:
        writer.write(struct.pack('<I', n))

    async def deserialize_length(self, reader: typing.Union[asyncio.StreamReader, typing.BinaryIO],
                                 read_n: typing.Callable[[int], typing.Awaitable[bytes]] = None) -> int:
        read_n = read_n or self.get_read_n(reader)
        return struct.unpack('<I', await read_n(4))[0]

    def serialize_string(self, writer: typing.BinaryIO, s: str) -> None:
        raw = s.encode('utf-8')
        self.serialize_length(writer, len(raw))
        writer.write(raw)

    async def deserialize_string(self, reader: typing.Union[asyncio.StreamReader, typing.BinaryIO],
                                 read_n: typing.Callable[[int], typing.Awaitable[bytes]] = None) -> str:
        read_n = read_n or self.get_read_n(reader)
        count = await self.deserialize_length(reader, read_n=read_n)
        raw = await read_n(count)
        return raw.decode('utf-8')

    def serialize_integer(self, writer: typing.BinaryIO, n: int) -> None:
        try:
            writer.write(struct.pack('<q', n))
        except (OverflowError, struct.error):
            writer.write(struct.pack('<q', MAX_I64))

    async def deserialize_integer(self, reader: typing.Union[asyncio.StreamReader, typing.BinaryIO],
                                  read_n: typing.Callable[[int], typing.Awaitable[bytes]] = None) -> int:
        read_n = read_n or self.get_read_n(reader)
        return struct.unpack('<q', await read_n(8))[0]

    def serialize_value(self, writer: typing.BinaryIO, value: typing.Any) -> None:
        def write_float(f: float) -> None:
            if f is None:
                writer.write(self._mvc_float)
                return
            try:
                f = float(f)
                if not isfinite(f):
                    writer.write(self._mvc_float)
                    return
                writer.write(struct.pack(self._float_format, f))
            except OverflowError:
                writer.write(self._mvc_float)

        if value is None:
            writer.write(struct.pack('<B', self.ValueType.NONE.value))
            return
        elif isinstance(value, float):
            writer.write(struct.pack('<B', self.ValueType.FLOAT.value))
            write_float(value)
            return
        elif isinstance(value, str):
            writer.write(struct.pack('<B', self.ValueType.STRING.value))
            self.serialize_string(writer, value)
            return
        elif isinstance(value, int):
            writer.write(struct.pack('<B', self.ValueType.INTEGER.value))
            self.serialize_integer(writer, int(value))
            return

        def is_dict() -> bool:
            try:
                iter(value.items())
                len(value)
                return True
            except (AttributeError, TypeError):
                pass
            return False

        if is_dict():
            writer.write(struct.pack('<B', self.ValueType.DICT.value))
            self.serialize_length(writer, len(value))
            for k, v in value.items():
                self.serialize_string(writer, str(k))
                self.serialize_value(writer, v)
            return

        def is_array() -> bool:
            try:
                iter(value)
                len(value)
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
                writer.write(struct.pack('<B', self.ValueType.ARRAY_OF_FLOAT.value))
                self.serialize_length(writer, len(value))
                for v in value:
                    write_float(v)
                return
            writer.write(struct.pack('<B', self.ValueType.ARRAY.value))
            self.serialize_length(writer, len(value))
            for v in value:
                self.serialize_value(writer, v)
            return

        _LOGGER.warning(f"Unsupported value in serialization {value}")
        writer.write(struct.pack('<B', self.ValueType.NONE.value))

    async def deserialize_type(self, reader: typing.Union[asyncio.StreamReader, typing.BinaryIO],
                               value_type: "AcquisitionBusSerializer.ValueType",
                               read_n: typing.Callable[[int], typing.Awaitable[bytes]] = None) -> typing.Any:
        read_n = read_n or self.get_read_n(reader)
        if value_type == self.ValueType.NONE:
            return None
        elif value_type == self.ValueType.FLOAT:
            return struct.unpack(self._float_format, await read_n(self._float_width))[0]
        elif value_type == self.ValueType.ARRAY_OF_FLOAT:
            count = await self.deserialize_length(reader, read_n=read_n)
            raw = await read_n(self._float_width * count)
            result: typing.List[float] = list()
            for i in range(count):
                origin = i * self._float_width
                result.append(struct.unpack(self._float_format, raw[origin:(origin+self._float_width)])[0])
            return result
        elif value_type == self.ValueType.STRING:
            return await self.deserialize_string(reader, read_n=read_n)
        elif value_type == self.ValueType.INTEGER:
            return await self.deserialize_integer(reader, read_n=read_n)
        elif value_type == self.ValueType.ARRAY:
            count = await self.deserialize_length(reader, read_n=read_n)
            result: typing.List[typing.Any] = list()
            for i in range(count):
                result.append(await self.deserialize_value(reader, read_n=read_n))
            return result
        elif value_type == self.ValueType.DICT:
            count = await self.deserialize_length(reader, read_n=read_n)
            result: typing.Dict[str, typing.Any] = dict()
            for i in range(count):
                k = await self.deserialize_string(reader, read_n=read_n)
                v = await self.deserialize_value(reader, read_n=read_n)
                result[k] = v
            return result
        else:
            raise ValueError("Invalid value type")

    async def deserialize_value(self, reader: typing.Union[asyncio.StreamReader, typing.BinaryIO],
                                read_n: typing.Callable[[int], typing.Awaitable[bytes]] = None) -> typing.Any:
        read_n = read_n or self.get_read_n(reader)
        value_type = self.ValueType(struct.unpack('<B', await read_n(1))[0])
        return await self.deserialize_type(reader, value_type)
