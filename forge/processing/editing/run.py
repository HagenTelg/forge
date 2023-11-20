import typing
import logging
import time
import datetime
from pathlib import Path
from netCDF4 import Dataset
from forge.range import intersects
from forge.timeparse import parse_iso8601_duration
from forge.data.structure import instrument_timeseries
from forge.data.structure.timeseries import time_coordinate
from ..station.lookup import station_data
from ..context.available import AvailableData
from ..context.data import SelectedData
from ..context.selection import InstrumentSelection
from .directives import apply_edit_directives

_LOGGER = logging.getLogger(__name__)


class EditingSelectedData(SelectedData):
    pass


class EditingAvailableDay(AvailableData):
    def __init__(self, station: str, output_directory: Path, day_start: int, data_files: typing.List[Dataset]):
        self._station = station
        self._output_directory = output_directory
        self._data_files = data_files
        self._day_start_ms = day_start * 1000
        self._day_end_ms = self._day_start_ms + 24 * 60 * 60 * 1000

    def close(self) -> None:
        for f in self._data_files:
            f.close()
        self._data_files.clear()

    def select_instrument(
            self,
            instrument: typing.Union[typing.Dict[str, typing.Any], InstrumentSelection, typing.Iterable],
            *auxiliary: typing.Union[typing.Dict[str, typing.Any], InstrumentSelection, typing.Iterable],
            start: typing.Optional[typing.Union[str, float, int, datetime.datetime]] = None,
            end: typing.Optional[typing.Union[str, float, int, datetime.datetime]] = None,
            always_tuple: bool = False,
    ) -> typing.Iterator[typing.Union[SelectedData, typing.Tuple[SelectedData, ...]]]:
        start, end = self._to_bounds_ms(start, end)
        if not intersects(self._day_start_ms, self._day_end_ms, start, end):
            return

        aux_matched: typing.List[EditingSelectedData] = list()

        def ready_aux():
            if len(aux_matched) == len(auxiliary):
                return
            for aux in auxiliary:
                matcher = InstrumentSelection.matcher(aux)
                for check_aux in self._data_files:
                    if not matcher(check_aux):
                        continue
                    aux_matched.append(EditingSelectedData.from_file(check_aux))
                    break
                else:
                    aux_matched.append(EditingSelectedData.empty_placeholder())
                aux_matched[-1].restrict_times(start, end)

        match_instrument = InstrumentSelection.matcher(instrument)
        for check_instrument in self._data_files:
            if not match_instrument(check_instrument):
                continue
            matched_instrument = EditingSelectedData.from_file(check_instrument)
            matched_instrument.restrict_times(start, end)

            ready_aux()

            if not aux_matched and not always_tuple:
                yield matched_instrument
            else:
                yield matched_instrument, *aux_matched

    def select_multiple(
            self,
            *selected: typing.Union[typing.Dict[str, typing.Any], InstrumentSelection, typing.Iterable],
            start: typing.Optional[typing.Union[str, float, int, datetime.datetime]] = None,
            end: typing.Optional[typing.Union[str, float, int, datetime.datetime]] = None,
            always_tuple: bool = False,
    ) -> typing.Iterator[typing.Union[SelectedData, typing.Tuple[SelectedData, ...]]]:
        assert len(selected) > 0
        start, end = self._to_bounds_ms(start, end)
        if not intersects(self._day_start_ms, self._day_end_ms, start, end):
            return

        result: typing.List[EditingSelectedData] = list()
        for sel in selected:
            matcher = InstrumentSelection.matcher(sel)
            for check_instrument in self._data_files:
                if not matcher(check_instrument):
                    continue
                matched_instrument = EditingSelectedData.from_file(check_instrument)
                matched_instrument.restrict_times(start, end)
                result.append(matched_instrument)
                break
            else:
                unmatched_instrument = EditingSelectedData.empty_placeholder()
                unmatched_instrument.restrict_times(start, end)
                result.append(unmatched_instrument)

        if len(result) == 1 and not always_tuple:
            yield result[0]
        else:
            yield tuple(result)

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
        if not intersects(self._day_start_ms, self._day_end_ms, start, end):
            return

        instrument_id = instrument_id.upper()
        for check in self._data_files:
            check_instrument_id = getattr(check, 'instrument_id', None)
            if check_instrument_id == instrument_id:
                raise FileExistsError(f"Files for instrument {instrument_id} already exist")

        matched_inputs: typing.List[EditingSelectedData] = list()
        hit_inputs: typing.List[EditingSelectedData] = list()
        for sel in inputs:
            matcher = InstrumentSelection.matcher(sel)
            for check_instrument in self._data_files:
                if not matcher(check_instrument):
                    continue
                matched_instrument = EditingSelectedData.from_file(check_instrument)
                matched_instrument.restrict_times(start, end)
                matched_inputs.append(matched_instrument)
                hit_inputs.append(matched_instrument)
                break
            else:
                unmatched_instrument = EditingSelectedData.empty_placeholder()
                unmatched_instrument.restrict_times(start, end)
                matched_inputs.append(unmatched_instrument)

        if not hit_inputs:
            return

        average_interval = None
        for i in hit_inputs:
            avg = getattr(i.root, 'time_coverage_resolution', None)
            if not avg:
                continue
            average_interval = parse_iso8601_duration(avg)
            break

        output_times = self._derive_output_times(
            hit_inputs, average_interval,
            self._day_start_ms, self._day_end_ms,
            peer_times=peer_times,
        )
        if output_times is None:
            return

        if isinstance(tags, str):
            tags = tags.split()

        ts = time.gmtime(int(self._day_start_ms / 1000))
        output_file = self._output_directory / f"{self._station.upper()}-{instrument_id}_s{ts.tm_year:04}{ts.tm_mon:02}{ts.tm_mday:02}.nc"
        root = Dataset(str(output_file), 'w', format='NETCDF4')
        instrument_timeseries(
            root, self._station, instrument_id,
            self._day_start_ms / 1000.0, self._day_end_ms / 1000.0,
            average_interval,
            set(tags) if tags is not None else None,
        )
        self._data_files.append(root)

        data_group = root.createGroup("data")
        time_var = time_coordinate(data_group)
        time_var[:] = output_times

        output_selected = EditingSelectedData.from_file(root)
        output_selected.restrict_times(start, end)

        yield output_selected, *matched_inputs


def process_day(
        station: str, output_directory: str,
        day_start: int, edit_files: typing.List[str], data_files: typing.List[str],
) -> None:
    open_data_files: typing.List[Dataset] = list()
    try:
        for name in data_files:
            file = Dataset(name, 'r+')
            open_data_files.append(file)

        _LOGGER.debug("Started %s editing day starting at %d, with %d data files",
                      station.upper(), day_start, len(data_files))

        edit_begin = time.monotonic()
        for directives_file in edit_files:
            directives_file = Dataset(directives_file, 'r')
            try:
                apply_edit_directives(directives_file, open_data_files)
            finally:
                directives_file.close()

        _LOGGER.debug("Applied edit directives for %s day %d from %d sources in %.3f seconds",
                      station.upper(), day_start, len(edit_files), time.monotonic() - edit_begin)

        available = EditingAvailableDay(station, Path(output_directory), day_start, list(open_data_files))
        open_data_files.clear()
    finally:
        for f in open_data_files:
            f.close()
    try:
        runner = station_data(station, 'editing', 'run')
        run_begin = time.monotonic()
        runner(available)
        _LOGGER.debug("Day correction complete for %s day %d in %.3f seconds",
                      station.upper(), day_start, time.monotonic() - run_begin)
    finally:
        available.close()
