import typing
import logging
import asyncio
import time
import os
from abc import ABC, abstractmethod
from pathlib import Path
from math import floor, ceil
from tempfile import TemporaryDirectory
from json import load as from_json, dump as to_json
from forge.range import Merge as RangeMerge
from netCDF4 import Dataset
from forge.logicaltime import containing_year_range, year_bounds_ms, start_of_year_ms
from forge.timeparse import parse_iso8601_time, parse_iso8601_duration
from forge.product.selection import InstrumentSelection
from forge.data.state import is_state_group
from forge.data.dimensions import find_dimension_values
from forge.archive.client import data_lock_key, index_lock_key
from forge.archive.client.connection import Connection, LockDenied, LockBackoff

_LOGGER = logging.getLogger(__name__)


class CommitFailure(Exception):
    pass


class Tracker(ABC):
    STATE_VERSION = 1

    class _Candidate:
        def __init__(self, start_epoch_ms: int, end_epoch_ms: int):
            self.start_epoch_ms = start_epoch_ms
            self.end_epoch_ms = end_epoch_ms

    class Output(ABC):
        class Update:
            def __init__(self, output: "Tracker.Output", start_epoch_ms: int, end_epoch_ms: int):
                self.output = output
                self.start_epoch_ms = start_epoch_ms
                self.end_epoch_ms = end_epoch_ms

            def to_state(self) -> typing.Any:
                return [int(self.start_epoch_ms), int(self.end_epoch_ms)]

            @classmethod
            def from_state(cls, output: "Tracker.Output", state: typing.Any):
                return cls(output, int(state[0]), int(state[1]))

        def __init__(self, tracker: "Tracker", start_epoch_ms: int, end_epoch_ms: int):
            self.tracker = tracker
            self.start_epoch_ms = start_epoch_ms
            self.end_epoch_ms = end_epoch_ms
            self.updated: typing.List[Tracker.Output.Update] = list()
            self.have_committed: bool = False

        def to_state(self) -> typing.Dict[str, typing.Any]:
            return {
                'start': self.start_epoch_ms,
                'end': self.end_epoch_ms,
                'committed': self.have_committed,
                'updated': [u.to_state() for u in self.updated],
            }

        @classmethod
        def from_state(cls, tracker: "Tracker", state: typing.Dict[str, typing.Any]) -> "Tracker.Output":
            out = cls(tracker, int(state['start']), int(state['end']))
            out.updated = [cls.Update.from_state(out, u) for u in state['updated']]
            out.have_committed = bool(state['committed'])
            return out

        @property
        def retain_after_commit(self) -> bool:
            return False

        @property
        def merge_contiguous(self) -> bool:
            return False

        def is_ready(self, output_index: int, outputs: typing.List["Tracker.Output"]) -> bool:
            return len(self.updated) != 0

        def apply_updated(self, update_start_epoch_ms: int, update_end_epoch_ms: int) -> None:
            update_start_epoch_ms = max(self.start_epoch_ms, update_start_epoch_ms)
            update_end_epoch_ms = min(self.end_epoch_ms, update_end_epoch_ms)

            class Merge(RangeMerge):
                def __init__(self, output: "Tracker.Output"):
                    self.output = output

                @property
                def canonical(self) -> bool:
                    return True

                def __len__(self) -> int:
                    return len(self.output.updated)

                def __delitem__(self, key: typing.Union[slice, int]) -> None:
                    del self.output.updated[key]

                def insert(self, index: int, start: int, end: int) -> typing.Any:
                    self.output.updated.insert(index, self.output.Update(self.output, start, end))

                def get_start(self, index: int) -> typing.Union[int, float]:
                    return self.output.updated[index].start_epoch_ms

                def get_end(self, index: int) -> typing.Union[int, float]:
                    return self.output.updated[index].end_epoch_ms
            
            Merge(self)(update_start_epoch_ms, update_end_epoch_ms)

        def merge_replaced(self, replaced: "Tracker.Output") -> None:
            for u in replaced.updated:
                self.apply_updated(u.start_epoch_ms, u.end_epoch_ms)
            self.have_committed = self.have_committed or replaced.have_committed

        @abstractmethod
        async def commit(self) -> None:
            pass

    def __init__(self):
        self._candidates: typing.List[Tracker._Candidate] = list()
        self._candidate_scan_epoch_ms: int = int(floor(time.time() * 1000))

        self._outputs: typing.List[Tracker.Output] = list()

        self.latest_commit: int = 0

    @abstractmethod
    def round_candidate(self, start_epoch_ms: int, end_epoch_ms: int) -> typing.Tuple[int, int]:
        pass

    @abstractmethod
    async def candidate_to_updated(self, start_epoch_ms: int, end_epoch_ms: int,
                                   modified_after_epoch_ms: int) -> "typing.AsyncIterable[typing.Tuple[int, int]]":
        pass

    @abstractmethod
    def updated_to_outputs(self, start_epoch_ms: int, end_epoch_ms: int) -> typing.Iterable[typing.Tuple[int, int]]:
        pass

    @property
    def state_file(self) -> Path:
        raise NotImplementedError

    def save_state(self, sync: bool = False) -> None:
        state_contents: typing.Dict[str, typing.Any] = {
            'version': self.STATE_VERSION,
            'candidate_scan': self._candidate_scan_epoch_ms,
            'candidates': [[p.start_epoch_ms, p.end_epoch_ms] for p in self._candidates],
            'outputs': [o.to_state() for o in self._outputs],
            'latest_commit': self.latest_commit,
        }

        with open(self.state_file, "w") as f:
            to_json(state_contents, f)

            if sync:
                try:
                    f.flush()
                    os.fdatasync(f.fileno())
                except AttributeError:
                    os.fsync(f.fileno())

    def load_state(self) -> None:
        try:
            with open(self.state_file, "r") as f:
                if os.fstat(f.fileno()).st_size == 0:
                    raise FileNotFoundError
                state = from_json(f)
        except FileNotFoundError:
            return

        state_version = state.get('version')
        if state.get('version') != self.STATE_VERSION:
            raise RuntimeError(f"Unsupported state version {state_version} vs {self.STATE_VERSION}")
        self._candidate_scan_epoch_ms = int(state['candidate_scan'])
        for c in state['candidates']:
            self.notify_candidate(int(c[0]), int(c[1]), save_state=False)
        for o in state['outputs']:
            self._outputs.append(self.Output.from_state(self, o))
        _LOGGER.debug("Loaded state with %d(%d) candidates and %d outputs",
                      len(self._candidates), len(state['candidates']), len(state['outputs']),)

    def notify_candidate(self, start_epoch_ms: int, end_epoch_ms: int, save_state: bool = True) -> None:
        start_epoch_ms, end_epoch_ms = self.round_candidate(start_epoch_ms, end_epoch_ms)

        class Merge(RangeMerge):
            def __init__(self, tracker: "Tracker"):
                self.tracker = tracker

            @property
            def canonical(self) -> bool:
                return True

            def __len__(self) -> int:
                return len(self.tracker._candidates)

            def __delitem__(self, key: typing.Union[slice, int]) -> None:
                del self.tracker._candidates[key]

            def insert(self, index: int, start: int, end: int) -> typing.Any:
                self.tracker._candidates.insert(index, self.tracker._Candidate(start, end))
                return True

            def merge_contained(self, index: int) -> typing.Any:
                return False

            def get_start(self, index: int) -> typing.Union[int, float]:
                return self.tracker._candidates[index].start_epoch_ms

            def get_end(self, index: int) -> typing.Union[int, float]:
                return self.tracker._candidates[index].end_epoch_ms

        if not Merge(self)(start_epoch_ms, end_epoch_ms):
            _LOGGER.debug("Already have candidate containing %d,%d", start_epoch_ms, end_epoch_ms)
            return

        if save_state:
            self.save_state(sync=True)

    async def process_candidates(self) -> bool:
        if not self._candidates:
            return False
        remaining_candidates: typing.List[Tracker._Candidate] = list()
        any_updates = False
        try:
            scan_begin = int(floor(time.time() * 1000))
            remaining_candidates = list(self._candidates)
            self._candidates.clear()

            for idx in reversed(range(len(remaining_candidates))):
                c = remaining_candidates[idx]
                _LOGGER.debug("Processing candidate %d,%d", c.start_epoch_ms, c.end_epoch_ms)
                async for start, end in self.candidate_to_updated(c.start_epoch_ms, c.end_epoch_ms, self._candidate_scan_epoch_ms):
                    _LOGGER.debug("Candidate %d,%d resulted in update to %d,%d",
                                  c.start_epoch_ms, c.end_epoch_ms, start, end)
                    self._apply_update(start, end)
                    any_updates = True
                del remaining_candidates[idx]

            self._candidate_scan_epoch_ms = scan_begin
        finally:
            for c in remaining_candidates:
                _LOGGER.debug("Restoring unprocessed candidate %d,%d", c.start_epoch_ms, c.end_epoch_ms)
                self.notify_candidate(c.start_epoch_ms, c.end_epoch_ms, save_state=False)
        self.save_state(sync=True)
        return any_updates

    async def notify_update(self, start_epoch_ms: int, end_epoch_ms: int, save_state: bool = True) -> None:
        self._apply_update(start_epoch_ms, end_epoch_ms)
        if save_state:
            self.save_state(sync=True)

    def _apply_update(self, update_start: int, update_end: int) -> None:
        class Merge(RangeMerge):
            def __init__(self, tracker: "Tracker"):
                self.tracker = tracker
                self.to_merge: typing.List[Tracker.Output] = list()

            @property
            def canonical(self) -> bool:
                return True

            def combine_contiguous(self, index: int) -> bool:
                return self.tracker._outputs[index].merge_contiguous

            def __len__(self) -> int:
                return len(self.tracker._outputs)

            def __delitem__(self, key: typing.Union[slice, int]) -> None:
                if isinstance(key, slice):
                    self.to_merge.extend(self.tracker._outputs[key])
                else:
                    self.to_merge.append(self.tracker._outputs[key])
                del self.tracker._outputs[key]

            def insert(self, index: int, start: int, end: int) -> typing.Any:
                new_output = self.tracker.Output(self.tracker, start, end)
                new_output.apply_updated(update_start, update_end)
                self.tracker._outputs.insert(index, new_output)
                return new_output

            def merge_contained(self, index: int) -> typing.Any:
                self.tracker._outputs[index].apply_updated(update_start, update_end)
                return None

            def get_start(self, index: int) -> typing.Union[int, float]:
                return self.tracker._outputs[index].start_epoch_ms

            def get_end(self, index: int) -> typing.Union[int, float]:
                return self.tracker._outputs[index].end_epoch_ms

        for out_start, out_end in self.updated_to_outputs(update_start, update_end):
            _LOGGER.debug("Update %d,%d resulted in output %d,%d",
                          update_start, update_end, out_start, out_end)
            merge = Merge(self)
            new_output = merge(out_start, out_end)
            if not new_output:
                continue
            for m in merge.to_merge:
                new_output.merge_replaced(m)

    def notify_external_commit(self, start_epoch_ms: int, end_epoch_ms: int, save_state: bool = True) -> None:
        class Merge(RangeMerge):
            def __init__(self, tracker: "Tracker"):
                self.tracker = tracker
                self.to_merge: typing.List[Tracker.Output] = list()

            @property
            def canonical(self) -> bool:
                return True

            def combine_contiguous(self, index: int) -> bool:
                return self.tracker._outputs[index].merge_contiguous

            def __len__(self) -> int:
                return len(self.tracker._outputs)

            def __delitem__(self, key: typing.Union[slice, int]) -> None:
                if isinstance(key, slice):
                    self.to_merge.extend(self.tracker._outputs[key])
                else:
                    self.to_merge.append(self.tracker._outputs[key])
                del self.tracker._outputs[key]

            def insert(self, index: int, start: int, end: int) -> typing.Any:
                new_output = self.tracker.Output(self.tracker, start, end)
                new_output.have_committed = True
                self.tracker._outputs.insert(index, new_output)
                return new_output

            def merge_contained(self, index: int) -> typing.Any:
                self.tracker._outputs[index].have_committed = True
                return None

            def get_start(self, index: int) -> typing.Union[int, float]:
                return self.tracker._outputs[index].start_epoch_ms

            def get_end(self, index: int) -> typing.Union[int, float]:
                return self.tracker._outputs[index].end_epoch_ms

        for out_start, out_end in self.updated_to_outputs(start_epoch_ms, end_epoch_ms):
            _LOGGER.debug("External commit %d,%d resulted in output %d,%d",
                          start_epoch_ms, end_epoch_ms, out_start, out_end)
            merge = Merge(self)
            new_output = merge(out_start, out_end)
            if not new_output:
                continue
            for m in merge.to_merge:
                new_output.merge_replaced(m)

        if save_state:
            self.save_state()

    async def commit(self) -> None:
        any_commited = False

        async def process_output(index: int) -> bool:
            o = self._outputs[index]
            if not o.is_ready(index, self._outputs):
                return True

            _LOGGER.debug("Committing output %d,%d", o.start_epoch_ms, o.end_epoch_ms)
            try:
                await o.commit()
            except CommitFailure:
                _LOGGER.debug("Retaining failed output commit", exc_info=True)
                return True

            nonlocal any_commited
            any_commited = True

            self.latest_commit = max(self.latest_commit, o.end_epoch_ms)

            if not o.retain_after_commit:
                return False

            o.have_committed = True
            o.updated.clear()
            return True

        process_idx = 0
        while process_idx < len(self._outputs):
            if not await process_output(process_idx):
                del self._outputs[process_idx]
            else:
                process_idx += 1

        if any_commited:
            self.save_state(sync=True)


class FileModifiedTracker(Tracker):
    def __init__(self, connection: Connection, station: str, archive: str,
                 selections: typing.List[InstrumentSelection]):
        super().__init__()
        self.connection = connection
        self.station = station
        self.archive = archive
        self.selections = selections

    @property
    def update_key(self) -> typing.Optional[str]:
        return None

    def __str__(self) -> str:
        key = self.update_key
        if key:
            return f"{self.station.upper()}/{self.archive.upper()}/{self.update_key}"
        else:
            return f"{self.station.upper()}/{self.archive.upper()}"

    def round_candidate(self, start_epoch_ms: int, end_epoch_ms: int) -> typing.Tuple[int, int]:
        if self.archive in ("avgd", "avgm"):
            year_start, year_end = containing_year_range(start_epoch_ms / 1000.0, end_epoch_ms / 1000.0)
            return start_of_year_ms(year_start), start_of_year_ms(year_end)
        else:
            day_start = int(floor(start_epoch_ms / (24 * 60 * 60 * 1000)))
            day_end = int(ceil(end_epoch_ms / (24 * 60 * 60 * 1000)))
            day_end = max(day_end, day_start+1)
            return day_start * 24 * 60 * 60 * 1000, day_end * 24 * 60 * 60 * 1000

    async def _fetch_files(self, output_directory: Path, start_epoch_ms: int, end_epoch_ms: int) -> None:
        for sel in self.selections:
            await sel.fetch_files(self.connection, self.station, self.archive,
                                  start_epoch_ms, end_epoch_ms, output_directory)

    def _inspect_file(self, modified_after_epoch_ms: int, root: Dataset) -> typing.Optional[typing.Tuple[int, int]]:
        file_creation_time = getattr(root, 'date_created', None)
        if file_creation_time is not None:
            file_creation_time = int(ceil(parse_iso8601_time(str(file_creation_time)).timestamp() * 1000.0))
        if file_creation_time < modified_after_epoch_ms:
            return None

        time_coverage_start = getattr(root, 'time_coverage_start', None)
        if time_coverage_start is not None:
            time_coverage_start = int(floor(parse_iso8601_time(str(time_coverage_start)).timestamp() * 1000.0))

        time_coverage_end = getattr(root, 'time_coverage_end', None)
        if time_coverage_end is not None:
            time_coverage_end = int(ceil(parse_iso8601_time(str(time_coverage_end)).timestamp() * 1000.0))

        time_coverage_resolution = getattr(root, "time_coverage_resolution", None)
        if time_coverage_resolution is not None:
            time_coverage_resolution = int(round(parse_iso8601_duration(str(time_coverage_resolution)) * 1000))

        first_seen_data: typing.Optional[int] = None
        last_seen_data: typing.Optional[int] = None

        def walk_group(g: Dataset):
            nonlocal first_seen_data
            nonlocal last_seen_data

            for var in g.variables.values():
                if len(var.dimensions) == 0 or var.dimensions[0] != 'time':
                    continue

                _, time_values = find_dimension_values(g, 'time')
                if time_values.shape[0] == 0:
                    break

                data_start = int(time_values[:].data[0])
                data_end = int(time_values[:].data[-1])

                if time_coverage_resolution:
                    data_end += time_coverage_resolution
                elif time_values.shape[0] > 1:
                    data_end += data_end - int(time_values[:].data[-2])

                if time_coverage_start:
                    data_start = max(data_start, time_coverage_start)
                if time_coverage_end:
                    data_end = min(data_end, time_coverage_end)

                if first_seen_data is None or data_start < first_seen_data:
                    first_seen_data = data_start
                if last_seen_data is None or data_end > last_seen_data:
                    last_seen_data = data_end
                break

            for name, sub in g.groups.items():
                if name == 'statistics' or name == 'instrument':
                    continue
                if is_state_group(sub):
                    continue
                walk_group(sub)

        walk_group(root)

        if first_seen_data is not None and last_seen_data is not None and first_seen_data < last_seen_data:
            return first_seen_data, last_seen_data
        return None

    async def candidate_to_updated(self, start_epoch_ms: int, end_epoch_ms: int,
                                   modified_after_epoch_ms: int) -> "typing.AsyncIterable[typing.Tuple[int, int]]":
        with TemporaryDirectory() as working_directory:
            working_directory = Path(working_directory)

            backoff = LockBackoff()
            while True:
                try:
                    async with self.connection.transaction():
                        await self.connection.lock_read(index_lock_key(self.station, self.archive),
                                                        start_epoch_ms, end_epoch_ms)
                        await self.connection.lock_read(data_lock_key(self.station, self.archive),
                                                        start_epoch_ms, end_epoch_ms)
                        await self._fetch_files(working_directory, start_epoch_ms, end_epoch_ms)
                        break
                except LockDenied as ld:
                    _LOGGER.debug("Archive busy: %s", ld.status)
                    await backoff()
                    continue

            for file_path in working_directory.iterdir():
                await asyncio.sleep(0)
                root = Dataset(str(file_path), 'r')
                try:
                    file_contents = self._inspect_file(modified_after_epoch_ms, root)
                    if file_contents is None:
                        continue
                    file_start, file_end = file_contents
                    yield file_start, file_end
                finally:
                    root.close()


class YearModifiedTracker(FileModifiedTracker):
    class Output(FileModifiedTracker.Output):
        @property
        def retain_after_commit(self) -> bool:
            return True

        def is_ready(self, output_index: int, outputs: typing.List["YearModifiedTracker.Output"]) -> bool:
            if not self.updated:
                # Nothing changed, so not ready
                return False
            if self.have_committed:
                # Once submitted, any change triggers a resubmission
                return True
            if (self.end_epoch_ms - self.updated[-1].end_epoch_ms) < 24 * 60 * 60 * 1000:
                # Once updated in the final day of the year, consider the year done
                return True

            if output_index+1 < len(outputs):
                # If there's an output after ours, and it has an update in the first day, consider
                # this year done
                next_output = outputs[output_index+1]
                if next_output.updated and (next_output.updated[0].start_epoch_ms - self.end_epoch_ms) < 24 * 60 * 60 * 1000:
                    return True

            return False

    def updated_to_outputs(self, start_epoch_ms: int, end_epoch_ms: int) -> typing.Iterable[typing.Tuple[int, int]]:
        for year in range(*containing_year_range(start_epoch_ms / 1000.0, end_epoch_ms / 1000.0)):
            yield year_bounds_ms(year)


class NRTTracker(FileModifiedTracker):
    MAXIMUM_AGE: typing.Optional[int] = 48 * 60 * 60 * 1000

    class Output(FileModifiedTracker.Output):
        @property
        def retain_after_commit(self) -> bool:
            return False

        def is_ready(self, output_index: int, outputs: typing.List["YearModifiedTracker.Output"]) -> bool:
            if not self.updated:
                # Nothing changed, so not ready
                return False
            if self.have_committed:
                # Only submit NRT once
                return False
            if (self.end_epoch_ms - self.updated[-1].end_epoch_ms) < 60 * 1000:
                # Once the final minute is ready, consider the hour done
                return True

            if output_index+1 < len(outputs):
                # If there's an output after ours, then we're also done
                next_output = outputs[output_index+1]
                if next_output.updated:
                    return True

            return False

    async def candidate_to_updated(self, start_epoch_ms: int, end_epoch_ms: int,
                                   modified_after_epoch_ms: int) -> "typing.Iterable[typing.Tuple[int, int]]":
        if self.MAXIMUM_AGE:
            cutoff_time = int(floor(time.time() * 1000)) - self.MAXIMUM_AGE
            if end_epoch_ms <= cutoff_time:
                return
            start_epoch_ms = max(cutoff_time, start_epoch_ms)
            end_epoch_ms = max(cutoff_time, end_epoch_ms)
            start_epoch_ms, end_epoch_ms = self.round_candidate(start_epoch_ms, end_epoch_ms)
        if end_epoch_ms <= self.latest_commit:
            return
        start_epoch_ms = max(self.latest_commit, start_epoch_ms)
        end_epoch_ms = max(self.latest_commit, end_epoch_ms)
        start_epoch_ms, end_epoch_ms = self.round_candidate(start_epoch_ms, end_epoch_ms)
        async for c in super().candidate_to_updated(start_epoch_ms, end_epoch_ms, modified_after_epoch_ms):
            yield c

    def updated_to_outputs(self, start_epoch_ms: int, end_epoch_ms: int) -> typing.Iterable[typing.Tuple[int, int]]:
        if self.MAXIMUM_AGE:
            cutoff_time = int(floor(time.time() * 1000)) - self.MAXIMUM_AGE
            if end_epoch_ms <= cutoff_time:
                return
            start_epoch_ms = max(cutoff_time, start_epoch_ms)
            end_epoch_ms = max(cutoff_time, end_epoch_ms)
        start_hour = int(floor(start_epoch_ms / (60 * 60 * 1000)))
        end_hour = int(ceil(end_epoch_ms / (60 * 60 * 1000)))
        end_hour = max(end_hour, start_hour+1)
        for hour in range(start_hour, end_hour):
            hour_start = hour * 60 * 60 * 1000
            hour_end = hour_start + 60 * 60 * 1000
            if hour_end <= self.latest_commit:
                continue
            yield hour_start, hour_end
