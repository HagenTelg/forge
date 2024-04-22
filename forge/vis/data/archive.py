import typing
import logging
import asyncio
import time
import numpy as np
from abc import ABC, abstractmethod
from math import nan, ceil
from netCDF4 import Dataset
from forge.const import MAX_I64
from forge.vis import CONFIGURATION
from forge.vis.realtime.controller.client import ReadData as RealtimeRead
from forge.vis.realtime.controller.block import DataBlock as RealtimeDataBlock
from .selection import Selection, RealtimeSelection, InstrumentSelection, FileSequence, FileSource, ArchiveIndex, VariableRootContext, VariableContext
from .stream import DataStream, ArchiveReadStream, ArchiveRecordStream

_LOGGER = logging.getLogger(__name__)
_NEVER_MATCH_VARIABLES = frozenset({
    "time",
    "cut_size",
    "averaged_time",
    "averaged_count",
})
_NEVER_MATCH_ROOT_VARIABLES = frozenset({
    "station_name",
    "lat",
    "lon",
    "alt",
    "station_inlet_height"
}.union(_NEVER_MATCH_VARIABLES))


class Record(ABC):
    @abstractmethod
    def __call__(self, station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
                 send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
        pass


def walk_selectable(root: Dataset) -> typing.Iterator[VariableContext]:
    root_ctx = VariableRootContext(root)
    for var in root.variables.values():
        if var.name in _NEVER_MATCH_ROOT_VARIABLES:
            continue
        yield VariableContext(root_ctx, var)

    def walk(group: Dataset):
        for var in group.variables.values():
            if var.name in _NEVER_MATCH_VARIABLES:
                continue
            yield VariableContext(root_ctx, var)

        for g in group.groups.values():
            yield from walk(g)

    for g in root.groups.values():
        yield from walk(g)


class FieldStream:
    class Source(ABC):
        def __init__(self, stream: "FieldStream", priority: int, times: np.ndarray, values: np.ndarray,
                     convert: typing.Callable[[np.ndarray], typing.Any] = None):
            self.stream = stream
            self.priority = priority
            self.times = times
            self.values = values
            if convert is not None:
                self.convert = convert
            else:
                self.convert = lambda x: x.tolist()

        @abstractmethod
        def advance(self, completed: int) -> bool:
            pass

        @property
        def next(self) -> typing.Optional[typing.Tuple[int, typing.Any]]:
            raise NotImplementedError

        @property
        def expected_interval_ms(self) -> typing.Optional[int]:
            return None

        @property
        def round_interval_ms(self) -> typing.Optional[int]:
            return None

    class ConstantSource(Source):
        def __init__(self, stream: "FieldStream", priority: int, times: np.ndarray, values: np.ndarray,
                     convert: typing.Callable[[np.ndarray], typing.Any] = None):
            super().__init__(stream, priority, times, values, convert)
            self._begin_time = int(times)
            if self._begin_time <= stream._latest:
                self._begin_time = stream._latest

        def advance(self, completed: int) -> bool:
            return completed <= self._begin_time

        @property
        def next(self) -> typing.Optional[typing.Tuple[int, typing.Any]]:
            return self._begin_time, self.convert(self.values)

    class DataSource(Source):
        def __init__(self, stream: "FieldStream", priority: int, times: np.ndarray, values: np.ndarray,
                     interval: typing.Optional[float] = None,
                     convert: typing.Callable[[np.ndarray], typing.Any] = None):
            super().__init__(stream, priority, times, values, convert)
            self.interval_ms: typing.Optional[int] = int(ceil(interval * 1000)) if interval else None
            self._insert_break: typing.Optional[int] = None
            self._current_index = np.searchsorted(times, stream._latest)
            if self._current_index < times.shape[0]:
                if int(times[self._current_index]) < stream._latest:
                    self._current_index += 1

        def _add_break_if_needed(self, completed: int, next_time: int, current_time: int) -> None:
            time_change = next_time - current_time
            if self.interval_ms:
                if time_change >= self.interval_ms * 2:
                    self._insert_break = completed + self.interval_ms
            # else:
            #     if self._current_index >= 2:
            #         prior_interval = int(self.times[self._current_index-1]) - int(self.times[self._current_index-2])
            #         if time_change >= prior_interval * 5:
            #             self._insert_break = completed + 1

        def advance(self, completed: int) -> bool:
            if self._insert_break and completed >= self._insert_break:
                self._insert_break = None

            current_index = self._current_index
            available_times = self.times.shape[0]
            if current_index >= available_times:
                return False
            current_time = int(self.times[current_index])
            if current_time > completed:
                return True
            if current_index+1 < available_times:
                if int(self.times[current_index+1]) > completed:
                    self._current_index += 1
                    next_time = int(self.times[self._current_index])
                    self._add_break_if_needed(completed, next_time, current_time)
                    return next_time < self.stream.end_epoch_ms
            self._current_index += np.searchsorted(self.times[self._current_index:], completed, side='right')
            if self._current_index >= self.times.shape[0]:
                return False
            next_time = int(self.times[self._current_index])
            self._add_break_if_needed(completed, next_time, current_time)
            return next_time < self.stream.end_epoch_ms

        @property
        def break_value(self) -> np.ndarray:
            if np.issubdtype(self.values.dtype, np.floating):
                return np.array(nan, dtype=self.values.dtype)
            else:
                return np.empty((0,), dtype=self.values.dtype)

        @property
        def next(self) -> typing.Optional[typing.Tuple[int, typing.Any]]:
            if self._insert_break:
                return self._insert_break, self.convert(self.break_value)
            if self._current_index >= self.times.shape[0]:
                return None
            current_time = int(self.times[self._current_index])
            if current_time >= self.stream.end_epoch_ms:
                return None
            return current_time, self.convert(self.values[self._current_index])

        @property
        def expected_interval_ms(self) -> typing.Optional[int]:
            if self.interval_ms:
                return self.interval_ms
            if self.times.shape[0] < 2:
                return None
            if self._current_index >= self.times.shape[0]:
                return int(self.times[-1]) - int(self.times[-2])
            if self._current_index > 1:
                return int(self.times[self._current_index]) - int(self.times[self._current_index-1])
            return int(self.times[1]) - int(self.times[0])

        @property
        def round_interval_ms(self) -> typing.Optional[int]:
            return self.interval_ms

    class StateSource(Source):
        def __init__(self, stream: "FieldStream", priority: int, times: np.ndarray, values: np.ndarray,
                     convert: typing.Callable[[np.ndarray], typing.Any] = None):
            super().__init__(stream, priority, times, values, convert)
            self._current_index = np.searchsorted(times, stream._latest)
            if times.shape[0] > 0 and stream._first:
                if self._current_index >= times.shape[0]:
                    if int(times[-1]) <= stream._latest:
                        self._current_index = times.shape[0] - 1
                elif self._current_index > 0:
                    if int(times[self._current_index]) > stream._latest:
                        self._current_index -= 1

        def advance(self, completed: int) -> bool:
            current_index = self._current_index
            available_times = self.times.shape[0]
            if current_index >= available_times:
                return False
            current_time = int(self.times[current_index])
            if current_time > completed:
                return True

            if current_index+1 < available_times:
                next_time = int(self.times[self._current_index+1])
                if next_time > completed:
                    self._current_index += 1
                    return next_time < self.stream.end_epoch_ms

            self._current_index += np.searchsorted(self.times[self._current_index:], completed, side='right')
            if self._current_index >= self.times.shape[0]:
                return False
            next_time = int(self.times[self._current_index])
            return next_time < self.stream.end_epoch_ms

        @property
        def next(self) -> typing.Optional[typing.Tuple[int, typing.Any]]:
            if self._current_index >= self.times.shape[0]:
                return None
            current_time = int(self.times[self._current_index])
            if self.stream._first and current_time < self.stream._latest:
                current_time = self.stream._latest
            return current_time, self.convert(self.values[self._current_index])

        @property
        def expected_interval_ms(self) -> typing.Optional[int]:
            if self.times.shape[0] < 2:
                return None
            if self._current_index >= self.times.shape[0]:
                return int(self.times[-1]) - int(self.times[-2])
            if self._current_index > 1:
                return int(self.times[self._current_index]) - int(self.times[self._current_index - 1])
            return int(self.times[1]) - int(self.times[0])

    def __init__(self, selections: typing.List[InstrumentSelection], start_epoch_ms: int, end_epoch_ms: int):
        self.selections = selections
        self.start_epoch_ms = start_epoch_ms
        self.end_epoch_ms = end_epoch_ms
        self._latest: int = start_epoch_ms
        self._advanced_interval: typing.Optional[int] = None
        self.streams: typing.List["FieldStream.Source"] = list()
        self._first: bool = True

    def add_data(self, matched: InstrumentSelection, times: np.ndarray, values: np.ndarray,
                 interval: typing.Optional[float] = None,
                 convert: typing.Callable[[np.ndarray], typing.Any] = None) -> None:
        selection_index = self.selections.index(matched)
        if not times.shape:
            self.streams.append(self.ConstantSource(self, selection_index, times, values, convert))
        else:
            self.streams.append(self.DataSource(self, selection_index, times, values, interval, convert))
        self.streams.sort(key=lambda x: x.priority)

    def add_state(self, matched: InstrumentSelection, times: np.ndarray, values: np.ndarray,
                  convert: typing.Callable[[np.ndarray], typing.Any] = None) -> None:
        selection_index = self.selections.index(matched)
        if not times.shape:
            self.streams.append(self.ConstantSource(self, selection_index, times, values, convert))
        else:
            self.streams.append(self.StateSource(self, selection_index, times, values, convert))
        self.streams.sort(key=lambda x: x.priority)

    @property
    def next(self) -> typing.Optional[typing.Tuple[int, typing.Any]]:
        selected_time: typing.Optional[int] = None
        selected_value: typing.Any = None
        for source in self.streams:
            hit = source.next
            if not hit:
                continue
            hit_time, hit_value = hit
            if selected_time is None or hit_time < selected_time:
                selected_time = hit_time
                selected_value = hit_value
            elif hit_time == selected_time and selected_value is None:
                selected_value = hit_value
        if selected_time is None:
            return None
        return selected_time, selected_value

    def advance(self, completed_epoch_ms: int) -> None:
        idx: int = len(self.streams) - 1
        while idx >= 0:
            if not self.streams[idx].advance(completed_epoch_ms):
                interval = self.streams[idx].expected_interval_ms
                if interval:
                    self._advanced_interval = interval
                del self.streams[idx]
            idx -= 1
        self._first = False

    @property
    def expected_interval_ms(self) -> typing.Optional[int]:
        for source in self.streams:
            interval = source.expected_interval_ms
            if interval:
                return interval
        return self._advanced_interval

    @property
    def round_interval_ms(self) -> typing.Optional[int]:
        for source in self.streams:
            interval = source.round_interval_ms
            if interval:
                return interval
        return None


class ContaminationRecord(Record):
    class _ContaminationFiles(FileSequence):
        def selection_index_to_instruments(self, index: ArchiveIndex,
                                           selection: InstrumentSelection) -> typing.Optional[typing.Set[str]]:
            instruments_with_flags = index.variable_names.get('system_flags')
            if not instruments_with_flags:
                return None
            candidate_instruments = selection.index_to_instruments(index)
            if not candidate_instruments:
                return None
            return candidate_instruments.intersection(instruments_with_flags)

    class _Stream(ArchiveReadStream):
        def __init__(
                self,
                send: typing.Callable[[typing.Dict], typing.Awaitable[None]],
                record: "ContaminationRecord",
                files: FileSequence,
        ):
            super().__init__(send)
            self.files = files
            self._stream = FieldStream(record.sources, files.start_epoch_ms, files.end_epoch_ms)
            self._contamination_begin: typing.Optional[int] = None
            self._contamination_end: typing.Optional[int] = None

        @property
        def connection_name(self) -> str:
            return "read data"

        def _attach(self, selection: InstrumentSelection, var: VariableContext, times: np.ndarray, values: np.ndarray) -> None:
            if not np.issubdtype(var.variable.dtype, np.integer):
                return

            contamination_mask: int = 0
            for bit, flag in var.flags.items():
                if not flag.startswith("data_contamination_"):
                    continue
                contamination_mask |= bit
            if contamination_mask == 0:
                return

            def convert(value: np.ndarray):
                if value.shape:
                    bits = 0
                    for v in value:
                        try:
                            bits |= int(v)
                        except (TypeError, ValueError, OverflowError):
                            continue
                else:
                    try:
                        bits = int(value)
                    except (TypeError, ValueError, OverflowError):
                        return None
                if (bits & contamination_mask) == 0:
                    return None
                return True

            if var.is_state:
                self._stream.add_state(selection, times, values, convert=convert)
            else:
                self._stream.add_data(selection, times, values, convert=convert, interval=var.interval)

        async def _complete_contamination(self) -> None:
            await self.send({
                'start_epoch_ms': self._contamination_begin,
                'end_epoch_ms': self._contamination_end,
            })
            self._contamination_begin = None
            self._contamination_end = None

        async def _drain_ready(self, before_ms: int) -> None:
            if before_ms < self.files.start_epoch_ms:
                return
            while True:
                stream_next = self._stream.next
                if not stream_next:
                    break
                epoch_ms, is_contaminated = stream_next
                if epoch_ms >= before_ms:
                    break
                if is_contaminated:
                    self._contamination_end = epoch_ms
                    if not self._contamination_begin:
                        self._contamination_begin = epoch_ms
                else:
                    if self._contamination_begin:
                        self._contamination_end = epoch_ms
                        await self._complete_contamination()
                self._stream.advance(epoch_ms)
            if before_ms >= MAX_I64 and self._contamination_begin:
                await self._complete_contamination()

        async def acquire_locks(self) -> None:
            await self.files.acquire_locks(self.connection)

        async def with_locks_held(self) -> None:
            async for chunk_begin, chunk_files in self.files.run(self.connection):
                await self._drain_ready(chunk_begin)
                for src, file_selections in chunk_files.items():
                    for file, selections in file_selections:
                        for var in walk_selectable(file):
                            if var.variable_name != 'system_flags':
                                continue
                            if len(var.variable.dimensions) < 1 or var.variable.dimensions[0] != 'time':
                                continue
                            if var.times is None:
                                continue
                            for sel in selections:
                                self._attach(sel, var, var.times, var.values)
            await self._drain_ready(MAX_I64)

    def __init__(self, sources: typing.List[InstrumentSelection]):
        self.sources = sources

    def __call__(self, station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
                 send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
        components = data_name.split('-', 2)
        archive = "raw"
        if len(components) >= 2:
            archive = components[1]
            if archive == "editing":
                archive = "edited"

        files = self._ContaminationFiles(self.sources, station, archive, start_epoch_ms, end_epoch_ms)
        return self._Stream(send, self, files)


class DataRecord(Record):
    class _Stream(ArchiveRecordStream):
        def __init__(
                self,
                send: typing.Callable[[typing.Dict], typing.Awaitable[None]],
                record: "DataRecord",
                files: FileSequence,
        ):
            super().__init__(send, list(record.fields.keys()))
            self.files = files
            self.streams: typing.Dict[str, FieldStream] = {
                field: FieldStream(selections, files.start_epoch_ms, files.end_epoch_ms)
                for field, selections in record.fields.items()
            }
            self._field_sources: typing.Dict[FileSource, typing.Dict[Selection, typing.List[FieldStream]]] = dict()
            self.hold_fields: typing.Dict[str, typing.Any] = {field: None for field in record.hold_fields}
            self.latest_record: int = self.files.start_epoch_ms

        @property
        def connection_name(self) -> str:
            return "read data"

        def _selection_to_streams(self, selection: Selection) -> typing.List[FieldStream]:
            result: typing.List[FieldStream] = list()
            for stream in self.streams.values():
                if selection not in stream.selections:
                    continue
                result.append(stream)
            return result

        def _attach_to_fields(self, src: FileSource, selection: Selection, var: VariableContext,
                              times: np.ndarray, values: np.ndarray) -> None:
            selection_stream = self._field_sources.get(src)
            if selection_stream is None:
                selection_stream = dict()
                self._field_sources[src] = selection_stream
            target_streams = selection_stream.get(selection)
            if target_streams is None:
                target_streams = self._selection_to_streams(selection)
                selection_stream[selection] = target_streams
            for stream in target_streams:
                if var.is_state:
                    stream.add_state(selection, times, values)
                else:
                    stream.add_data(selection, times, values, interval=var.interval)

        async def _drain_ready(self, before_ms: int):
            if before_ms < self.files.start_epoch_ms:
                return
            while True:
                record_time: int = MAX_I64
                record_data: typing.Dict[str, typing.Any] = dict()
                for field, stream in self.streams.items():
                    stream_next = stream.next
                    if not stream_next:
                        continue
                    epoch_ms, value = stream_next
                    if epoch_ms > record_time:
                        continue
                    elif epoch_ms != record_time:
                        record_time = epoch_ms
                        record_data.clear()
                        record_data.update(self.hold_fields)
                    record_data[field] = value
                    if field in self.hold_fields:
                        self.hold_fields[field] = value
                if record_time >= before_ms:
                    break
                await self.send_record(record_time, record_data)
                for field, stream in self.streams.items():
                    stream.advance(record_time)
                self.latest_record = record_time

        async def acquire_locks(self) -> None:
            await self.files.acquire_locks(self.connection)

        async def with_locks_held(self) -> None:
            async for chunk_begin, chunk_files in self.files.run(self.connection):
                await self._drain_ready(chunk_begin)
                for src, file_selections in chunk_files.items():
                    for file, selections in file_selections:
                        for var in walk_selectable(file):
                            for sel in selections:  # type: Selection
                                hit = sel.variable_values(var)
                                if hit is None:
                                    continue
                                times, values = hit
                                if times is None:
                                    times = np.ndarray(chunk_begin)
                                self._attach_to_fields(src, sel, var, times, values)
            await self._drain_ready(MAX_I64)

    def __init__(self, fields: typing.Dict[str, typing.List[Selection]], hold_fields: typing.Set[str] = None):
        self.fields = fields
        self.hold_fields = hold_fields if hold_fields else set()
        self._all_selections: typing.List[Selection] = list()
        for add in fields.values():
            self._all_selections.extend(add)

    def __call__(self, station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
                 send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
        components = data_name.split('-', 2)
        archive = "raw"
        if len(components) >= 2:
            archive = components[1]
            if archive == "editing":
                archive = "edited"

        files = FileSequence(self._all_selections, station, archive, start_epoch_ms, end_epoch_ms)
        return self._Stream(send, self, files)


class RealtimeRecord(DataRecord):
    class _Stream(DataRecord._Stream):
        class RealtimeStream(RealtimeRead):
            def __init__(self, stream: "RealtimeRecord._Stream",
                         reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
                super().__init__(reader, writer, stream.station, stream.data_name, stream_incoming=True)
                self._archive_stream = stream
                self.discard_epoch_ms: int = stream.latest_record

                record_interval = stream.realtime_interval_ms

                # Realtime records are recorded at the END of the average, so offset them back to the start
                self.record_time_offset: int = record_interval
                # A bit of slack for network delays
                self.discard_epoch_ms += max(ceil(record_interval / 4), 100)

                # Since we only get the current time, add the expected interval to the break threshold
                self.data_break_threshold = ceil(record_interval * 2 + 1200)
                self.data_break_epoch_ms = self.discard_epoch_ms + self.data_break_threshold

                self._hold_fields = stream.hold_fields

            async def block_ready(self, block: RealtimeDataBlock) -> None:
                for record in block.records:
                    adjusted_time = record.epoch_ms - self.record_time_offset
                    if adjusted_time <= self.discard_epoch_ms:
                        continue
                    if adjusted_time > self.data_break_epoch_ms:
                        await self._archive_stream.send_record(self.data_break_epoch_ms - 1, {})
                    if self._hold_fields:
                        output_record = dict(self._hold_fields)
                        output_record.update(record.fields)
                        for field in self._hold_fields.keys():
                            self._hold_fields[field] = output_record[field]
                        await self._archive_stream.send_record(adjusted_time, output_record)
                    else:
                        await self._archive_stream.send_record(adjusted_time, record.fields)
                    self.data_break_epoch_ms = adjusted_time + self.data_break_threshold
                await self._archive_stream.flush()

        def __init__(
                self,
                send: typing.Callable[[typing.Dict], typing.Awaitable[None]],
                record: "RealtimeRecord",
                files: FileSequence,
                station: str,
                data_name: str,
        ):
            super().__init__(send, record, files)
            self._expected_interval = record.expected_interval
            self.station = station
            self.data_name = data_name

        @property
        def realtime_interval_ms(self) -> int:
            if self._expected_interval is not None:
                return int(ceil(self._expected_interval * 1000))
            for stream in self.streams.values():
                interval = stream.expected_interval_ms
                if interval:
                    return interval
            return 0

        async def _stream_realtime(self) -> None:
            socket_name = CONFIGURATION.get('REALTIME.SOCKET', '/run/forge-vis-realtime.socket')
            try:
                reader, writer = await asyncio.open_unix_connection(socket_name)
            except FileNotFoundError:
                _LOGGER.debug(f"Unable to open realtime connection for {self.station} {self.data_name} on {socket_name}")
                return
            _LOGGER.debug(f"Realtime data connection open for {self.station} {self.data_name} on {socket_name}")
            try:
                stream = self.RealtimeStream(self, reader, writer)
                await stream.run()
                _LOGGER.debug(f"Realtime data connection ended for {self.station} {self.data_name}")
            finally:
                try:
                    writer.close()
                except OSError:
                    pass

        async def run(self) -> None:
            await super().run()
            await self._stream_realtime()

    def __init__(self, fields: typing.Dict[str, typing.List[RealtimeSelection]],
                 hold_fields: typing.Set[str] = None,
                 expected_interval: typing.Optional[float] = None):
        super().__init__(fields, hold_fields=hold_fields)
        self.expected_interval = expected_interval

    def __call__(self, station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
                 send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
        now_ms = round(time.time() * 1000)
        if end_epoch_ms < now_ms - 60 * 60 * 1000:
            end_epoch_ms = now_ms - 60 * 60 * 1000
        if end_epoch_ms <= start_epoch_ms:
            start_epoch_ms = end_epoch_ms - 1
        files = FileSequence(self._all_selections, station, "raw", start_epoch_ms, end_epoch_ms)
        return self._Stream(send, self, files, station, data_name)
