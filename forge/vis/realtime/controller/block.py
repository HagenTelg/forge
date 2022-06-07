import asyncio
import typing
import struct
from enum import IntEnum
from forge.vis import CONFIGURATION


class ValueType(IntEnum):
    MISSING = 0
    FLOAT = 1
    ARRAY_OF_FLOAT = 2


def _file_read_n(source: typing.BinaryIO, n: int) -> bytes:
    result = bytes()
    while n > 0:
        add = source.read(n)
        if not add:
            raise EOFError
        result += add
        n -= len(add)
    return result


class DataBlock:
    class Record:
        def __init__(self, epoch_ms: int, fields: typing.Dict[str, typing.Union[float, typing.List[float]]]):
            self.epoch_ms = epoch_ms
            self.fields = fields

    def __init__(self):
        self.records: typing.List["DataBlock.Record"] = list()

    def add_record(self, epoch_ms: int,
                   record: typing.Dict[str, typing.Union[float, typing.List[float]]],
                   rounding_ms: int = None) -> None:
        if not rounding_ms:
            rounding_ms = int(CONFIGURATION.get('REALTIME.TIME_ROUNDING', 10)) * 1000
        if rounding_ms <= 0:
            rounding_ms = 1
        epoch_ms = int(int(epoch_ms * rounding_ms) / rounding_ms)

        if len(self.records) > 0 and (epoch_ms - self.records[-1].epoch_ms) < rounding_ms:
            self.records[-1].fields.update(record)
            return

        self.records.append(self.Record(epoch_ms, record))

    @staticmethod
    async def _load_times(read_n: typing.Callable[[int], typing.Awaitable]) -> typing.List[int]:
        n_records = struct.unpack('<I', await read_n(4))[0]
        times: typing.List[int] = list()
        raw = await read_n(8 * n_records)
        for time_index in range(n_records):
            add_time = struct.unpack_from('<Q', raw, time_index*8)[0]
            if len(times) > 0 and add_time < times[-1]:
                add_time = times[-1]
            times.append(add_time)
        return times

    @staticmethod
    async def _load_fields(read_n: typing.Callable[[int], typing.Awaitable],
                           n_records: int) -> typing.Dict[str, typing.List[typing.Optional[typing.Union[float, typing.List[float]]]]]:
        n_fields = struct.unpack('<I', await read_n(4))[0]
        field_values: typing.Dict[str, typing.List[typing.Union[float, typing.List[float]]]] = dict()
        for field_index in range(n_fields):
            field_name_len = struct.unpack('<I', await read_n(4))[0]
            field_name = (await read_n(field_name_len)).decode('utf-8')

            values: typing.List[typing.Optional[typing.Union[float, typing.List[float]]]] = [None] * n_records
            field_values[field_name] = values
            for time_index in range(n_records):
                field_type = ValueType(struct.unpack('<B', await read_n(1))[0])

                if field_type == ValueType.FLOAT:
                    values[time_index] = struct.unpack('<f', await read_n(4))[0]
                elif field_type == ValueType.MISSING:
                    continue
                elif field_type == ValueType.ARRAY_OF_FLOAT:
                    value: typing.List[float] = list()
                    n_entries = struct.unpack('<I', await read_n(4))[0]
                    raw = await read_n(4 * n_entries)
                    for entry_index in range(n_entries):
                        value.append(struct.unpack_from('<f', raw, entry_index*4)[0])
                    values[time_index] = value
                else:
                    raise ValueError(f"Unsupported field type {field_type}")
        return field_values

    async def load(self, source: typing.Union[asyncio.StreamReader, typing.BinaryIO]) -> None:
        if isinstance(source, asyncio.StreamReader):
            async def read_n(n: int) -> bytes:
                return await source.readexactly(n)
        else:
            async def read_n(n: int) -> bytes:
                return _file_read_n(source, n)

        times = await self._load_times(read_n)
        n_records = len(times)
        field_values = await self._load_fields(read_n, n_records)

        for time_index in range(n_records):
            record_contents: typing.Dict[str, typing.Union[float, typing.List[float]]] = dict()
            for field_name, values in field_values.items():
                v = values[time_index]
                if v is None:
                    continue
                record_contents[field_name] = v
            self.records.append(self.Record(times[time_index], record_contents))

    @staticmethod
    def _save_fields(target: typing.BinaryIO,
                     field_values: typing.Dict[str, typing.List[typing.Optional[typing.Union[float, typing.List[float]]]]]) -> None:
        target.write(struct.pack('<I', len(field_values)))
        for field_name, values in field_values.items():
            field_name_raw = field_name.encode('utf-8')
            target.write(struct.pack('<I', len(field_name_raw)))
            target.write(field_name_raw)

            for value in values:
                if value is None:
                    target.write(struct.pack('<B', ValueType.MISSING.value))
                elif isinstance(value, list):
                    target.write(struct.pack('<BI', ValueType.ARRAY_OF_FLOAT.value, len(value)))
                    for v in value:
                        target.write(struct.pack('<f', float(v)))
                else:
                    target.write(struct.pack('<Bf', ValueType.FLOAT.value, float(value)))

    async def save(self, target: typing.BinaryIO) -> None:
        field_values: typing.Dict[str, typing.List[typing.Optional[typing.Union[float, typing.List[float]]]]] = dict()

        n_records = len(self.records)
        target.write(struct.pack('<I', n_records))
        for time_index in range(n_records):
            record = self.records[time_index]
            target.write(struct.pack('<Q', record.epoch_ms))

            for field_name, value in record.fields.items():
                output_values = field_values.get(field_name)
                if output_values is None:
                    output_values = [None] * n_records
                    field_values[field_name] = output_values
                output_values[time_index] = value

        self._save_fields(target, field_values)

    @classmethod
    async def trim(cls, target: typing.BinaryIO, discard: typing.Callable[[typing.List[int]], int]) -> bool:
        async def read_n(n: int) -> bytes:
            return _file_read_n(target, n)

        times = await cls._load_times(read_n)
        n_records = len(times)

        if n_records <= 0:
            return True

        n_discard = discard(times)
        if n_discard <= 0:
            return False
        if n_discard >= n_records:
            return True

        field_values = await cls._load_fields(read_n, n_records)

        target.seek(0)
        target.truncate(0)

        remaining_records = n_records - n_discard
        target.write(struct.pack('<I', remaining_records))
        for time_index in range(n_discard, n_records):
            target.write(struct.pack('<Q', times[time_index]))

        def can_discard(values: typing.List[typing.Optional[typing.Union[float, typing.List[float]]]]) -> bool:
            for v in values:
                if v is not None:
                    return False
            return True

        for field_name in list(field_values.keys()):
            values = field_values[field_name]
            del values[:n_discard]
            if can_discard(values):
                del field_values[field_name]

        if len(field_values) <= 0:
            return True

        cls._save_fields(target, field_values)
        return False


def serialize_single_record(epoch_ms: int,
                            record: typing.Dict[str, typing.Union[float, typing.List[float]]]) -> bytes:
    result = bytearray()

    result += struct.pack('<IQI', 1, epoch_ms, len(record))
    for name, value in record.items():
        raw_name = name.encode('utf-8')
        result += struct.pack('<I', len(raw_name))
        result += raw_name

        if value is None:
            result += struct.pack('<B', ValueType.MISSING.value)
        elif isinstance(value, list):
            result += struct.pack('<BI', ValueType.ARRAY_OF_FLOAT.value, len(value))
            for v in value:
                result += struct.pack('<f', float(v))
        else:
            result += struct.pack('<Bf', ValueType.FLOAT.value, float(value))

    return bytes(result)


if __name__ == '__main__':
    import argparse
    import time
    from json import dumps as to_json
    from forge.formattime import format_export_time

    parser = argparse.ArgumentParser(description="Realtime test block reader.")
    parser.add_argument('block',
                        help="block file to read")
    args = parser.parse_args()

    async def run():
        def format_time(ts: int) -> str:
            if not ts:
                return ""
            return format_export_time(ts / 1000.0)

        with open(args.block, mode='rb') as f:
            block = DataBlock()
            await block.load(f)
            for record in block.records:
                print(format_time(record.epoch_ms) + " " + to_json(record.fields), flush=True)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(run())
    loop.close()
