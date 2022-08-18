import typing
import asyncio
import struct
from forge.acquisition.bus.serializer import AcquisitionBusSerializer


PROTOCOL_VERSION = 1


class UplinkSerializer(AcquisitionBusSerializer):
    def __init__(self):
        super().__init__(double_float=False)

        self._serialize_string_lookup: typing.Dict[str, int] = {
            "": 1,
        }
        self._serialize_string_index: typing.List[str] = ["", ""]
        self._serialize_string_next = 2

        self._deserialize_string_index: typing.List[str] = ["", ""]
        self._deserialize_string_next = 2

    @staticmethod
    def serialize_length(writer: typing.BinaryIO, n: int) -> None:
        if n < 0xFF:
            writer.write(struct.pack('<B', n))
        else:
            writer.write(struct.pack('<BI', 0xFF, n))

    async def deserialize_length(self, reader: typing.Union[asyncio.StreamReader, typing.BinaryIO],
                                 read_n: typing.Callable[[int], typing.Awaitable[bytes]] = None) -> int:
        read_n = read_n or self.get_read_n(reader)
        n = struct.unpack('<B', await read_n(1))[0]
        if n != 0xFF:
            return n
        return struct.unpack('<I', await read_n(4))[0]

    def serialize_string_lookup(self, writer: typing.BinaryIO, s: str) -> None:
        index = self._serialize_string_lookup.get(s)
        if index is not None:
            writer.write(struct.pack('<H', index))
            return

        index = self._serialize_string_next
        if index >= len(self._serialize_string_index):
            self._serialize_string_index.append(s)
            self._serialize_string_lookup[s] = index
        else:
            existing = self._serialize_string_index[index]
            del self._serialize_string_lookup[existing]
            self._serialize_string_index[index] = s
            self._serialize_string_lookup[s] = index

        self._serialize_string_next = (self._serialize_string_next + 1) & 0xFFFF
        if self._serialize_string_next < 2:
            self._serialize_string_next = 2

        writer.write(struct.pack('<H', 0))
        self.serialize_string(writer, s)

    async def deserialize_string_lookup(self, reader: typing.Union[asyncio.StreamReader, typing.BinaryIO],
                                        read_n: typing.Callable[[int], typing.Awaitable[bytes]] = None) -> str:
        read_n = read_n or self.get_read_n(reader)
        index = struct.unpack('<H', await read_n(2))[0]
        if index == 0:
            s = await self.deserialize_string(reader)

            index = self._deserialize_string_next
            if index >= len(self._deserialize_string_index):
                self._deserialize_string_index.append(s)
            else:
                self._deserialize_string_index[index] = s

            self._deserialize_string_next = (self._deserialize_string_next + 1) & 0xFFFF
            if self._deserialize_string_next < 2:
                self._deserialize_string_next = 2

            return s

        if index >= len(self._deserialize_string_index):
            raise ValueError("deserialization string index out of range")

        return self._deserialize_string_index[index]

    def serialize_integer(self, writer: typing.BinaryIO, n: int) -> None:
        if n != 0xFFFF:
            try:
                writer.write(struct.pack('<H', n))
                return
            except (OverflowError, struct.error):
                pass
        try:
            writer.write(struct.pack('<Hq', 0xFFFF, n))
        except (OverflowError, struct.error):
            writer.write(struct.pack('<Hq', 0xFFFF, int(0x7FFF_FFFF_FFFF_FFFF)))

    async def deserialize_integer(self, reader: typing.Union[asyncio.StreamReader, typing.BinaryIO],
                                  read_n: typing.Callable[[int], typing.Awaitable[bytes]] = None) -> int:
        read_n = read_n or self.get_read_n(reader)
        n = struct.unpack('<H', await read_n(2))[0]
        if n != 0xFFFF:
            return n
        return struct.unpack('<q', await read_n(8))[0]

    def serialize_message(self, writer: typing.BinaryIO, value: typing.Any) -> None:
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
                self.serialize_string_lookup(writer, str(k))
                self.serialize_value(writer, v)
            return

        self.serialize_value(writer, value)

    async def deserialize_message(self, reader: typing.Union[asyncio.StreamReader, typing.BinaryIO]) -> typing.Any:
        read_n = self.get_read_n(reader)
        value_type = self.ValueType(struct.unpack('<B', await read_n(1))[0])

        if value_type == self.ValueType.DICT:
            count = await self.deserialize_length(reader, read_n=read_n)
            result: typing.Dict[str, typing.Any] = dict()
            for i in range(count):
                k = await self.deserialize_string_lookup(reader, read_n=read_n)
                v = await self.deserialize_value(reader, read_n=read_n)
                result[k] = v
            return result

        return await self.deserialize_type(reader, value_type, read_n=read_n)
