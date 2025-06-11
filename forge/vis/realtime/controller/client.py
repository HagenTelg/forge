import typing
import asyncio
import struct
from abc import ABC, abstractmethod
from math import isfinite, nan
from forge.vis.realtime.controller.protocol import ConnectionType
from forge.vis.realtime.controller.block import ValueType, DataBlock


class WriteData:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self.reader = reader
        self.writer = writer

    def _send_string(self, s: str):
        raw = s.encode('utf-8')
        self.writer.write(struct.pack('<I', len(raw)))
        self.writer.write(raw)

    async def connect(self) -> None:
        self.writer.write(struct.pack('<B', ConnectionType.WRITE.value))
        await self.writer.drain()

    async def run(self) -> None:
        await self.reader.readexactly(1)

    async def _send_contents(self, record: typing.Dict[str, typing.Union[float, typing.List[float]]]) -> None:
        self.writer.write(struct.pack('<I', len(record)))
        for field_name, value in record.items():
            self._send_string(field_name)
            if value is None:
                self.writer.write(struct.pack('<B', ValueType.MISSING.value))
            elif isinstance(value, list):
                self.writer.write(struct.pack('<BI', ValueType.ARRAY_OF_FLOAT.value, len(value)))
                for v in value:
                    try:
                        v = float(v)
                        if not isfinite(v):
                            v = nan
                        self.writer.write(struct.pack('<f', v))
                    except (ValueError, TypeError, OverflowError):
                        self.writer.write(struct.pack('<f', nan))
            else:
                try:
                    value = float(value)
                    if not isfinite(value):
                        value = nan
                    self.writer.write(struct.pack('<Bf', ValueType.FLOAT.value, float(value)))
                except (ValueError, TypeError, OverflowError):
                    self.writer.write(struct.pack('<B', ValueType.MISSING.value))
        await self.writer.drain()

    async def send_data(self, station: str, data_name: str,
                        record: typing.Dict[str, typing.Union[float, typing.List[float]]]) -> None:
        self._send_string(station)
        self._send_string(data_name)
        await self._send_contents(record)


class ReadData(ABC):
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
                 station: str, data_name: str, stream_incoming: bool = True):
        self.reader = reader
        self.writer = writer
        self.station = station
        self.data_name = data_name
        self.stream_incoming = stream_incoming

    async def run(self) -> None:
        if self.stream_incoming:
            connection_type = ConnectionType.STREAM
        else:
            connection_type = ConnectionType.READ
        self.writer.write(struct.pack('<B', connection_type.value))

        def send_string(s: str):
            raw = s.encode('utf-8')
            self.writer.write(struct.pack('<I', len(raw)))
            self.writer.write(raw)
        send_string(self.station)
        send_string(self.data_name)
        await self.writer.drain()

        while True:
            block = DataBlock()
            try:
                await block.load(self.reader)
            except EOFError:
                break
            await self.block_ready(block)

    @abstractmethod
    async def block_ready(self, block: DataBlock) -> None:
        pass


if __name__ == '__main__':
    import argparse
    import time
    from json import dumps as to_json
    from forge.formattime import format_export_time
    from forge.vis import CONFIGURATION

    parser = argparse.ArgumentParser(description="Realtime test client.")
    parser.add_argument('--socket',
                        dest='socket', type=str,
                        default=CONFIGURATION.get('REALTIME.SOCKET', '/run/forge-vis-realtime.socket'),
                        help="server socket")
    parser.add_argument('--stream',
                        dest='stream', action='store_true',
                        default=False,
                        help="continuously stream data")
    parser.add_argument('station',
                        help="station to read from")
    parser.add_argument('record',
                        help="record to read")
    args = parser.parse_args()

    async def run():
        reader, writer = await asyncio.open_unix_connection(args.socket)

        def format_time(ts: int) -> str:
            if not ts:
                return ""
            return format_export_time(ts / 1000.0)

        class DebugReader(ReadData):
            async def block_ready(self, block: DataBlock):
                for record in block.records:
                    print(format_time(record.epoch_ms) + " " + to_json(record.fields), flush=True)

        realtime_reader = DebugReader(reader, writer, args.station, args.record, stream_incoming=args.stream)
        await realtime_reader.run()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(run())
    loop.close()
