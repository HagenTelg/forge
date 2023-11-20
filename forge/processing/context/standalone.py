import typing
import datetime
import logging
import time
from pathlib import Path
from math import floor, ceil
from netCDF4 import Dataset
from forge.range import intersects, intersecting_tuple
from forge.timeparse import parse_iso8601_time, parse_iso8601_duration
from forge.const import MAX_I64
from forge.data.structure import instrument_timeseries
from forge.data.structure.timeseries import time_coordinate
from .available import AvailableData
from .data import SelectedData
from .selection import InstrumentSelection

_LOGGER = logging.getLogger(__name__)


class RunSelectedData(SelectedData):
    pass


class RunAvailable(AvailableData):
    def __init__(self, files: typing.List[Path], output_directory: Path):
        self._files = files
        self._output_directory = output_directory

    @staticmethod
    def _get_file_bounds(file: Dataset) -> typing.Tuple[int, int]:
        time_coverage_start = getattr(file, 'time_coverage_start', None)
        if time_coverage_start is not None:
            time_coverage_start = int(floor(parse_iso8601_time(str(time_coverage_start)).timestamp() * 1000.0))
        else:
            time_coverage_start = -MAX_I64
        time_coverage_end = getattr(file, 'time_coverage_end', None)
        if time_coverage_end is not None:
            time_coverage_end = int(ceil(parse_iso8601_time(str(time_coverage_end)).timestamp() * 1000.0))
        else:
            time_coverage_end = MAX_I64
        return time_coverage_start, time_coverage_end

    def select_instrument(
            self,
            instrument: typing.Union[typing.Dict[str, typing.Any], InstrumentSelection, typing.Iterable],
            *auxiliary: typing.Union[typing.Dict[str, typing.Any], InstrumentSelection, typing.Iterable],
            start: typing.Optional[typing.Union[str, float, int, datetime.datetime]] = None,
            end: typing.Optional[typing.Union[str, float, int, datetime.datetime]] = None,
            always_tuple: bool = False,
    ) -> typing.Iterator[typing.Union[SelectedData, typing.Tuple[SelectedData, ...]]]:
        start, end = self._to_bounds_ms(start, end)
        match_instrument = InstrumentSelection.matcher(instrument)
        match_auxiliary = [InstrumentSelection.matcher(aux) for aux in auxiliary]
        for instrument_file in self._files:
            instrument_data = Dataset(instrument_file, 'r+')
            open_files = [instrument_data]
            try:
                if not match_instrument(instrument_data):
                    continue
                instrument_start, instrument_end = self._get_file_bounds(instrument_data)
                if not intersects(start, end, instrument_start, instrument_end):
                    continue

                matched_instrument = RunSelectedData.from_file(instrument_data)
                matched_instrument.restrict_times(start, end)

                aux_selected: typing.List[typing.Optional[RunSelectedData]] = [None] * len(match_auxiliary)
                for aux_file in self._files:
                    if None not in aux_selected:
                        break
                    matched_aux: typing.Optional[RunSelectedData] = None
                    if aux_file == instrument_file:
                        aux_data = instrument_data
                        matched_aux = matched_instrument
                    else:
                        aux_data = Dataset(aux_file, 'r')
                    try:
                        aux_start, aux_end = self._get_file_bounds(aux_data)
                        if not intersects(instrument_start, instrument_end, aux_start, aux_end):
                            continue
                        if not intersects(start, end, aux_start, aux_end):
                            continue
                        for aux_idx in range(len(match_auxiliary)):
                            if aux_selected[aux_idx] is not None:
                                continue
                            if not match_auxiliary[aux_idx](aux_data):
                                continue
                            if matched_aux is None:
                                matched_aux = RunSelectedData.from_file(aux_data)
                                matched_aux.restrict_times(start, end)
                            aux_selected[aux_idx] = matched_aux
                    finally:
                        if aux_file != instrument_file:
                            if matched_aux is None:
                                aux_data.close()
                            else:
                                open_files.append(aux_data)

                if len(aux_selected) == 0 and not always_tuple:
                    yield matched_instrument
                else:
                    for aux_idx in range(len(aux_selected)):
                        if aux_selected[aux_idx] is not None:
                            continue
                        placeholder = RunSelectedData.empty_placeholder()
                        placeholder.restrict_times(start, end)
                        aux_selected[aux_idx] = placeholder
                    yield matched_instrument, *aux_selected
            finally:
                for f in open_files:
                    f.close()

    def _combined_matches(
            self,
            inputs: typing.Tuple[typing.Union[typing.Dict[str, typing.Any], InstrumentSelection, typing.Iterable], ...],
            start: int, end: int,
            open_mode: str,
    ) -> typing.Iterator[typing.Tuple[SelectedData, ...]]:
        match_inputs = [InstrumentSelection.matcher(inp) for inp in inputs]
        covered_ranges: typing.List[typing.Tuple[int, int]] = list()
        for first_file in self._files:
            first_data = Dataset(first_file, open_mode)
            open_files = [first_data]
            try:
                first_start, first_end = self._get_file_bounds(first_data)
                if not intersects(start, end, first_start, first_end):
                    continue
                if intersecting_tuple(covered_ranges, first_start, first_end, canonical=False):
                    continue

                input_selected: typing.List[typing.Optional[RunSelectedData]] = [None] * len(inputs)
                first_selected: typing.Optional[RunSelectedData] = None
                for inp_idx in range(len(match_inputs)):
                    if not match_inputs[inp_idx](first_data):
                        continue
                    if first_selected is None:
                        first_selected = RunSelectedData.from_file(first_data)
                        first_selected.restrict_times(start, end)
                    input_selected[inp_idx] = first_selected
                if first_selected is None:
                    continue

                for other_file in self._files:
                    if None not in input_selected:
                        break
                    if other_file == first_file:
                        continue

                    other_data = Dataset(other_file, open_mode)
                    other_selected: typing.Optional[RunSelectedData] = None
                    try:
                        other_start, other_end = self._get_file_bounds(other_data)
                        if not intersects(start, end, other_start, other_end):
                            continue
                        if not intersects(first_start, first_end, other_start, other_end):
                            continue
                        for other_idx in range(len(match_inputs)):
                            if input_selected[other_idx] is not None:
                                continue
                            if not match_inputs[other_idx](other_data):
                                continue
                            if other_selected is None:
                                other_selected = RunSelectedData.from_file(other_data)
                                other_selected.restrict_times(start, end)
                            input_selected[other_idx] = other_selected
                    finally:
                        if other_selected is None:
                            other_data.close()
                        else:
                            open_files.append(other_data)

                for inp_idx in range(len(input_selected)):
                    if input_selected[inp_idx] is not None:
                        continue
                    placeholder = RunSelectedData.empty_placeholder()
                    placeholder.restrict_times(start, end)
                    input_selected[inp_idx] = placeholder

                yield tuple(input_selected)
                covered_ranges.append((first_start, first_end))

            finally:
                for f in open_files:
                    f.close()

    def select_multiple(
            self,
            *selected: typing.Union[typing.Dict[str, typing.Any], InstrumentSelection, typing.Iterable],
            start: typing.Optional[typing.Union[str, float, int, datetime.datetime]] = None,
            end: typing.Optional[typing.Union[str, float, int, datetime.datetime]] = None,
            always_tuple: bool = False,
    ) -> typing.Iterator[typing.Union[SelectedData, typing.Tuple[SelectedData, ...]]]:
        assert len(selected) > 0
        start, end = self._to_bounds_ms(start, end)

        for selected_data in self._combined_matches(selected, start, end, 'r+'):
            if len(selected_data) == 1 and not always_tuple:
                yield selected_data[0]
            else:
                yield selected_data

    def derive_output(
            self,
            instrument_id: str,
            *inputs: typing.Union[typing.Dict[str, typing.Any], InstrumentSelection, typing.Iterable],
            tags: typing.Optional[typing.Union[str, typing.Iterable[str]]] = None,
            start: typing.Optional[typing.Union[str, float, int, datetime.datetime]] = None,
            end: typing.Optional[typing.Union[str, float, int, datetime.datetime]] = None,
            peer_times: bool = False,
    ) -> typing.Iterator[typing.Tuple[SelectedData, ...]]:
        assert instrument_id
        assert len(inputs) > 0
        
        start, end = self._to_bounds_ms(start, end)
        instrument_id = instrument_id.upper()
        if isinstance(tags, str):
            tags = tags.split()

        def _valid_output(candidate_data: typing.Tuple[SelectedData, ...]) -> bool:
            for i in candidate_data:
                if i.placeholder:
                    continue
                check = getattr(i.root, 'instrument_id', None)
                if check is None:
                    continue
                if check.upper() == instrument_id:
                    _LOGGER.warning(f"Output instrument code {instrument_id} already exists in the inputs")
                    return False
            return True

        for selected_data in self._combined_matches(inputs, start, end, 'r'):
            if not _valid_output(selected_data):
                continue

            average_interval = None
            for i in selected_data:
                if i.placeholder:
                    continue
                avg = getattr(i.root, 'time_coverage_resolution', None)
                if not avg:
                    continue
                average_interval = parse_iso8601_duration(avg)
                break

            time_coverage_start = None
            for i in selected_data:
                if i.placeholder:
                    continue
                time_coverage_start = getattr(i.root, 'time_coverage_start', None)
                if time_coverage_start is None:
                    continue
                time_coverage_start = parse_iso8601_time(str(time_coverage_start)).timestamp()
                break

            time_coverage_end = None
            for i in selected_data:
                if i.placeholder:
                    continue
                time_coverage_end = getattr(i.root, 'time_coverage_end', None)
                if time_coverage_end is None:
                    continue
                time_coverage_end = parse_iso8601_time(str(time_coverage_end)).timestamp()
                break

            station = None
            for i in selected_data:
                if i.placeholder:
                    continue
                station_name = i.root.variables.get("station_name")
                if station_name is None:
                    continue
                station = str(station_name[0])

            if time_coverage_start is None or time_coverage_end is None or station is None:
                continue

            output_times = self._derive_output_times(
                selected_data, average_interval,
                int(floor(time_coverage_start * 1000)), int(ceil(time_coverage_end) * 1000),
                peer_times=peer_times,
            )
            if output_times is None:
                continue

            ts = time.gmtime(time_coverage_start)
            output_file = self._output_directory / f"{station}-{instrument_id}_s{ts.tm_year:04}{ts.tm_mon:02}{ts.tm_mday:02}.nc"
            root = Dataset(str(output_file), 'w', format='NETCDF4')
            try:
                instrument_timeseries(
                    root, station, instrument_id,
                    time_coverage_start, time_coverage_end,
                    average_interval,
                    set(tags) if tags is not None else None,
                )

                data_group = root.createGroup("data")
                time_var = time_coordinate(data_group)
                time_var[:] = output_times

                output_selected = RunSelectedData.from_file(root)
                output_selected.restrict_times(start, end)

                yield output_selected, *selected_data
            finally:
                root.close()


def processing_main(apply: typing.Callable[[AvailableData], None]) -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Data processing script")

    parser.add_argument('--debug',
                        dest='debug', action='store_true',
                        help="enable debug output")

    parser.add_argument('--new-files',
                        dest='output_directory',
                        help="directory to place new output files")

    parser.add_argument('data',
                        help="data file or directory to process",
                        nargs='+')

    args = parser.parse_args()

    if args.debug:
        from forge.log import set_debug_logger
        set_debug_logger()

    output_directory: typing.Optional[Path] = None
    data_files: typing.List[Path] = list()
    for v in args.data:
        v = Path(v)
        if not v.exists():
            parser.error(f"Data source '{v}' does not exist")
        if v.is_dir():
            output_directory = v
            for file in v.iterdir():
                if not file.name.endswith('.nc'):
                    continue
                if not file.is_file():
                    continue
                data_files.append(file)
            continue
        data_files.append(v)
        if output_directory is None:
            output_directory = v.parent

    if args.output_directory:
        output_directory = Path(args.output_directory)

    if not data_files:
        parser.error(f"No data files found")

    _LOGGER.debug("Starting processing on %d data files with output to %s", len(data_files), output_directory)

    ctx = RunAvailable(data_files, output_directory)

    begin_time = time.monotonic()
    apply(ctx)
    _LOGGER.debug("Processing completed in %.3f seconds", time.monotonic() - begin_time)
