import typing
import asyncio
import os
from forge.acquisition import LayeredConfiguration
from .base import BaseDataOutput, BaseBusInterface, BasePersistentInterface, CutSize
from .streaming import StreamingContext, StreamingInstrument, StreamingSimulator


class DataOutput(BaseDataOutput):
    pass


class BusInterface(BaseBusInterface):
    def __init__(self):
        super().__init__('__TEST')

        self.instrument_info: typing.Optional[typing.Dict[str, typing.Any]] = None
        self.instrument_info_updated = asyncio.Event()

        self.instrument_state: typing.Optional[typing.Dict[str, typing.Any]] = None
        self.instrument_state_updated = asyncio.Event()

        self.data_values: typing.Dict[str, float] = dict()
        self.data_value_updated = asyncio.Event()

        self.state_records: typing.Dict[str, typing.Any] = dict()
        self.state_record_updated = asyncio.Event()

    async def emit_instrument_info(self, contents: typing.Dict[str, typing.Any]) -> None:
        self.instrument_info = contents
        self.instrument_info_updated.set()

    async def emit_instrument_state(self, contents: typing.Dict[str, typing.Any]) -> None:
        self.instrument_state = contents
        self.instrument_state_updated.set()

    async def emit_data_record(self, contents: typing.Dict[str, float]) -> None:
        self.data_values.update(contents)
        self.data_value_updated.set()

    async def emit_state_value(self, name: str, contents: typing.Any) -> None:
        self.state_records[name] = contents
        self.state_record_updated.set()

    async def value(self, name: str) -> float:
        while True:
            v = self.data_values.get(name)
            if v is not None:
                return v
            await self.data_value_updated.wait()
            self.data_value_updated.clear()

    async def wait_for_communicating(self) -> None:
        while True:
            if self.instrument_state and self.instrument_state.get('communicating'):
                break
            await self.instrument_state_updated.wait()
            self.instrument_state_updated.clear()

    async def emit_average_record(self, contents: typing.Dict[str, float],
                                  cutsize: CutSize.Size = CutSize.Size.WHOLE) -> None:
        pass


class PersistentInterface(BasePersistentInterface):
    class Value:
        def __init__(self, data: typing.Any, time: typing.Optional[float] = None):
            self.data: typing.Any = data
            self.time: typing.Optional[float] = time

    def __init__(self):
        super().__init__()
        self.values: typing.Dict[str, PersistentInterface.Value] = dict()

    def load(self, name: str) -> typing.Tuple[typing.Any, typing.Optional[float]]:
        existing = self.values.get(name)
        if not existing:
            return None, None
        return existing.data, existing.time

    async def save(self, name: str, value: typing.Any, effective_time: typing.Optional[float]) -> None:
        if value is None:
            self.values.pop(name, None)
        else:
            self.values[name] = self.Value(value, effective_time)


async def _aio_pipe() -> typing.Tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    read, write = os.pipe()
    read = os.fdopen(read, mode='rb')
    write = os.fdopen(write, mode='wb')

    loop = asyncio.get_event_loop()
    reader = asyncio.StreamReader()
    await loop.connect_read_pipe(lambda: asyncio.StreamReaderProtocol(reader), read)

    transport, protocol = await loop.connect_write_pipe(asyncio.streams.FlowControlMixin, write)
    writer = asyncio.StreamWriter(transport, protocol, reader, loop)

    return reader, writer


class PipeStreamingContext(StreamingContext):
    def __init__(self, config: LayeredConfiguration, data: DataOutput,
                 bus: BusInterface,
                 persistent: PersistentInterface,
                 reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        super().__init__(config, data, bus, persistent)
        self.reader = reader
        self.writer = writer

    async def open_stream(self) -> typing.Tuple[typing.Optional[asyncio.StreamReader],
                                                typing.Optional[asyncio.StreamWriter]]:
        return self.reader, self.writer

    async def close_stream(self, reader: typing.Optional[asyncio.StreamReader],
                           writer: typing.Optional[asyncio.StreamWriter]) -> None:
        pass


async def create_streaming_simulator(simulator: typing.Type[StreamingSimulator],
                                     *args, **kwargs) -> typing.Tuple[StreamingSimulator,
                                                                      typing.Optional[asyncio.StreamReader],
                                                                      typing.Optional[asyncio.StreamWriter]]:
    simulator_read, simulator_write = await _aio_pipe()
    instrument_read, instrument_write = await _aio_pipe()
    return simulator(simulator_read, instrument_write, *args, **kwargs), instrument_read, simulator_write


async def create_streaming_instrument(instrument: typing.Type[StreamingInstrument],
                                      simulator: typing.Type[StreamingSimulator],
                                      config: typing.Optional[dict] = None) -> typing.Tuple[StreamingSimulator,
                                                                                            StreamingInstrument]:
    s, reader, writer = await create_streaming_simulator(simulator)
    data = DataOutput()
    bus = BusInterface()
    persistent = PersistentInterface()
    context = PipeStreamingContext(LayeredConfiguration(config or dict()), data, bus, persistent, reader, writer)
    i = instrument(context)
    return s, i
