import asyncio
import typing
import struct
import logging
from math import nan, isfinite
from enum import IntEnum

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
    def serialize_string(writer: typing.BinaryIO, s: str) -> None:
        raw = s.encode('utf-8')
        writer.write(struct.pack('<I', len(raw)))
        writer.write(raw)

    def serialize_value(self, writer: typing.BinaryIO, value: typing.Any) -> None:
        def write_float(f: float) -> None:
            if f is None:
                writer.write(self._mvc_float)
                return
            f = float(f)
            if not isfinite(f):
                writer.write(self._mvc_float)
                return
            try:
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
            raw = value.encode('utf-8')
            writer.write(struct.pack('<BI', self.ValueType.STRING.value, len(raw)))
            writer.write(raw)
            return
        elif isinstance(value, int):
            try:
                writer.write(struct.pack('<Bq', self.ValueType.INTEGER.value, int(value)))
            except OverflowError:
                writer.write(struct.pack('<Bq', self.ValueType.INTEGER.value, int(0x7FFF_FFFF_FFFF_FFFF)))
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
            writer.write(struct.pack('<BI', self.ValueType.DICT.value, len(value)))
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
                writer.write(struct.pack('<BI', self.ValueType.ARRAY_OF_FLOAT.value, len(value)))
                for v in value:
                    write_float(v)
                return
            writer.write(struct.pack('<BI', self.ValueType.ARRAY.value, len(value)))
            for v in value:
                self.serialize_value(writer, v)
            return

        _LOGGER.warning(f"Unsupported value in serialization {value}")
        writer.write(struct.pack('<B', self.ValueType.NONE.value))

    @staticmethod
    async def deserialize_string(reader: asyncio.StreamReader) -> str:
        count = struct.unpack('<I', await reader.readexactly(4))[0]
        raw = await reader.readexactly(count)
        return raw.decode('utf-8')

    async def deserialize_value(self, reader: asyncio.StreamReader) -> typing.Any:
        value_type = self.ValueType(struct.unpack('<B', await reader.readexactly(1))[0])

        if value_type == self.ValueType.NONE:
            return None
        elif value_type == self.ValueType.FLOAT:
            return struct.unpack(self._float_format, await reader.readexactly(self._float_width))[0]
        elif value_type == self.ValueType.ARRAY_OF_FLOAT:
            count = struct.unpack('<I', await reader.readexactly(4))[0]
            raw = await reader.readexactly(self._float_width * count)
            result: typing.List[float] = list()
            for i in range(count):
                origin = i * self._float_width
                result.append(struct.unpack(self._float_format, raw[origin:(origin+self._float_width)])[0])
            return result
        elif value_type == self.ValueType.STRING:
            return await self.deserialize_string(reader)
        elif value_type == self.ValueType.INTEGER:
            return struct.unpack('<q', await reader.readexactly(8))[0]
        elif value_type == self.ValueType.ARRAY:
            count = struct.unpack('<I', await reader.readexactly(4))[0]
            result: typing.List[typing.Any] = list()
            for i in range(count):
                result.append(await self.deserialize_value(reader))
            return result
        elif value_type == self.ValueType.DICT:
            count = struct.unpack('<I', await reader.readexactly(4))[0]
            result: typing.Dict[str, typing.Any] = dict()
            for i in range(count):
                k = await self.deserialize_string(reader)
                v = await self.deserialize_value(reader)
                result[k] = v
            return result
        else:
            raise ValueError("Invalid value type")