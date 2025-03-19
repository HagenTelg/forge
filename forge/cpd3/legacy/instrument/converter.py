import typing
import numpy as np
import forge.cpd3.variant as variant
from netCDF4 import Dataset, Group, Variable, VLType
from abc import ABC
from math import isfinite, nan, floor
from forge.const import __short_version__
from forge.formattime import format_iso8601_time
from forge.data.merge.timealign import incoming_before
from forge.data.structure import instrument_timeseries
from forge.data.structure.history import append_history
from forge.data.structure.timeseries import time_coordinate, state_change_coordinate, averaged_time_variable, cutsize_variable, variable_coordinates
from forge.data.structure.variable import variable_wavelength, variable_flags
from forge.cpd3.convert.instrument.lookup import instrument_data
from ..readarchive import read_archive, Selection, Identity


class InstrumentConverter(ABC):
    def __init__(self, station: str, instrument_id: str, file_start: float, file_end: float, root: Dataset):
        self.station = station
        self.instrument_id = instrument_id
        self.file_start = file_start
        self.file_end = file_end
        assert self.file_start < self.file_end
        self.root = root

    @classmethod
    def with_instrument_override(
            cls,
            **override,
    ) -> typing.Type['InstrumentConverter']:
        class Result(cls):
            def apply_instrument_info(
                    self,
                    **kwargs
            ) -> Group:
                kwargs.update(override)
                return super().apply_instrument_info(**kwargs)
        return Result

    @classmethod
    def with_added_tag(
            cls,
            *t: str,
    ) -> typing.Type['InstrumentConverter']:
        class Result(cls):
            @property
            def tags(self) -> typing.Optional[typing.Set[str]]:
                result = set(super().tags)
                result.update(t)
                return result
        return Result

    @property
    def archive(self) -> str:
        return "raw"

    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        raise NotImplementedError

    @property
    def instrument_type(self) -> typing.Optional[str]:
        return None

    @staticmethod
    def calculate_average_interval(times: np.ndarray, minimum_step: float = 2.0) -> typing.Optional[float]:
        if times.shape[0] < 2:
            return None
        seconds = np.round(times / 500.0) * 0.5
        time_difference = seconds[1:] - seconds[:-1]
        valid = time_difference > 0.0
        if not np.any(valid):
            return None
        time_difference = time_difference[valid]
        time_step_values, time_step_count = np.unique(time_difference, return_counts=True)
        time_step = float(time_step_values[np.argmax(time_step_count)])
        if time_step < minimum_step:
            return None
        return time_step

    @property
    def average_interval(self) -> typing.Optional[float]:
        return 60.0

    def calculate_split_monitor(self, candidate_times: np.ndarray) -> bool:
        if candidate_times.shape[0] < 2:
            return False
        seconds = np.round(candidate_times / (60 * 1000.0)) * 60.0
        time_difference = seconds[1:] - seconds[:-1]
        valid = time_difference > 0.0
        if not np.any(valid):
            return False
        time_difference = seconds[1:] - seconds[:-1]
        valid = time_difference > 0.0
        if not np.any(valid):
            return False
        time_difference = time_difference[valid]
        time_step_values, time_step_count = np.unique(time_difference, return_counts=True)
        time_step = float(time_step_values[np.argmax(time_step_count)])
        data_interval = self.average_interval
        if not data_interval:
            return False
        if time_step <= data_interval * 2:
            return False
        return True

    def convert_loaded(
            self,
            values: typing.List[typing.Tuple[Identity, typing.Any, float]],
            convert: typing.Callable[[typing.Any], typing.Any] = None,
            is_state: bool = False,
            dtype: typing.Type = np.float64,
            return_cut_size: bool = False,
    ):
        if convert is None:
            if np.issubdtype(dtype, np.floating):
                def convert(x):
                    if x is None:
                        return nan
                    try:
                        x = dtype(float(x))
                    except (ValueError, TypeError, OverflowError):
                        return nan
                    if not isfinite(x):
                        return nan
                    return x
            elif np.issubdtype(dtype, np.integer):
                def convert(x):
                    if x is None:
                        return 0
                    try:
                        return dtype(int(x))
                    except (ValueError, TypeError, OverflowError):
                        return 0
            else:
                convert = lambda x: dtype(x) if x is not None else dtype()

        convert_values = list()
        convert_times: typing.List[int] = list()
        convert_cut_size: typing.List[float] = list()
        for idx in range(len(values)):
            identity = values[idx][0]
            if not identity.start:
                continue
            if identity.start >= self.file_end:
                continue
            if identity.start < self.file_start:
                if not is_state:
                    continue
                elif idx+1 >= len(values) or values[idx+1][0].start > self.file_start:
                    pass
                else:
                    continue

            converted = convert(values[idx][1])
            if converted is None:
                continue

            convert_times.append(int(round(identity.start * 1000)))
            convert_values.append(converted)
            if return_cut_size:
                if "pm1" in identity.flavors:
                    convert_cut_size.append(1.0)
                elif "pm10" in identity.flavors:
                    convert_cut_size.append(10.0)
                elif "pm25" in identity.flavors:
                    convert_cut_size.append(2.5)
                else:
                    convert_cut_size.append(nan)

        if len(convert_times) == 0:
            if not return_cut_size:
                return np.empty((0,), dtype=np.int64), np.empty((0,), dtype=dtype)
            else:
                return np.empty((0,), dtype=np.int64), np.empty((0,), dtype=dtype), np.empty((0,), dtype=np.float64)

        if not return_cut_size:
            return np.array(convert_times, dtype=np.int64), np.array(convert_values, dtype=dtype)
        else:
            return np.array(convert_times, dtype=np.int64), np.array(convert_values, dtype=dtype), np.array(convert_cut_size, dtype=np.float64)

    class Data:
        def __init__(self, time: np.ndarray, value: np.ndarray, cut_size: typing.Optional[np.ndarray] = None):
            self.time = time
            self.value = value
            self.cut_size = cut_size

    def load_variable(
            self,
            variable: str,
            convert: typing.Callable[[typing.Any], typing.Any] = None,
            dtype: typing.Type = np.float64,
    ) -> "InstrumentConverter.Data":
        return self.Data(*self.convert_loaded(read_archive([Selection(
            start=self.file_start,
            end=self.file_end,
            stations=[self.station],
            archives=[self.archive],
            variables=[variable],
            include_meta_archive=False,
            include_default_station=False,
            lacks_flavors=["cover", "stats"],
        )]), convert=convert, is_state=False, dtype=dtype, return_cut_size=True))

    def _array_data_convert(
            self,
            variable: str
    ) -> typing.Tuple[typing.List[typing.Tuple[Identity, typing.Any, float]], typing.Callable[[typing.List], np.ndarray]]:
        data = read_archive([Selection(
            start=self.file_start,
            end=self.file_end,
            stations=[self.station],
            archives=[self.archive],
            variables=[variable],
            include_meta_archive=False,
            include_default_station=False,
            lacks_flavors=["cover", "stats"],
        )])
        array_size: int = 0
        for _, value, _ in data:
            if not isinstance(value, list):
                continue
            array_size = max(array_size, len(value))

        def convert(v: typing.List) -> np.ndarray:
            if not isinstance(v, list):
                return np.full(array_size, nan, dtype=np.float64)
            return np.array(v + [nan] * (array_size - len(v)), dtype=np.float64)

        return data, convert

    def load_array_variable(self,variable: str) -> "InstrumentConverter.Data":
        data, convert = self._array_data_convert(variable)
        return self.Data(*self.convert_loaded(data, convert=convert, is_state=False, dtype=np.float64, return_cut_size=True))

    def load_state(
            self,
            variable: str,
            convert: typing.Callable[[typing.Any], typing.Any] = None,
            dtype: typing.Type = np.float64,
    ) -> "InstrumentConverter.Data":
        return self.Data(*self.convert_loaded(read_archive([Selection(
            start=self.file_start,
            end=self.file_end,
            stations=[self.station],
            archives=[self.archive],
            variables=[variable],
            include_meta_archive=False,
            include_default_station=False,
            lacks_flavors=["cover", "stats"],
        )]), convert=convert, is_state=True, dtype=dtype))

    def load_array_state(self, variable: str) -> "InstrumentConverter.Data":
        data, convert = self._array_data_convert(variable)
        return self.Data(*self.convert_loaded(data, convert=convert, is_state=True, dtype=np.float64, return_cut_size=True))

    def apply_instrument_info(
            self,
            manufacturer: typing.Optional[str] = None,
            model: typing.Optional[str] = None,
            serial_number: typing.Optional[str] = None,
            firmware_version: typing.Optional[str] = None,
            calibration: typing.Optional[str] = None,
    ) -> Group:
        g = self.root.createGroup("instrument")

        for value, name, long_name in (
                (manufacturer, "manufacturer", "instrument manufacturer name"),
                (model, "model", "instrument model"),
                (serial_number, "serial_number", "instrument serial number"),
                (firmware_version, "firmware_version", "instrument firmware version information"),
                (calibration, "calibration", "instrument calibration information"),
        ):
            if not value:
                continue
            var = g.createVariable(name, str, fill_value=False)
            var.coverage_content_type = "referenceInformation"
            var.long_name = long_name
            var[0] = str(value)

        return g

    def apply_instrument_metadata(
            self,
            variable: typing.Union[str, typing.List[str]],
            manufacturer: typing.Optional[str] = None,
            model: typing.Optional[str] = None,
            serial_number: typing.Optional[str] = None,
            firmware_version: typing.Optional[str] = None,
            calibration: typing.Optional[str] = None,
            extra: typing.Callable[[variant.Metadata], typing.Dict[str, typing.Tuple[str, str]]] = None,
            generic_model: typing.Optional[str] = None,
    ) -> Group:
        if isinstance(variable, str):
            variable = [variable]
        to_set: typing.Dict[str, typing.Tuple[str, str]] = dict()
        acquisition_start: typing.Optional[float] = None
        for _, value, _ in read_archive([Selection(
            start=self.file_start,
            end=self.file_end,
            stations=[self.station],
            archives=[self.archive + "_meta"],
            variables=variable,
            include_meta_archive=False,
            include_default_station=False,
        )]):
            if not isinstance(value, variant.Metadata):
                continue
            if extra:
                to_set.update(extra(value))

            source = value.get("Source")
            if not source:
                continue
            if not manufacturer:
                manufacturer = source.get("Manufacturer")
            if not model:
                model = source.get("Model")
            if not serial_number:
                serial_number = source.get("SerialNumber")
                if len(str(serial_number)) > 32:
                    # Bad queries sometimes result in a garbled response
                    serial_number = None
            if not firmware_version:
                firmware_version = source.get("FirmwareVersion")
            if not calibration:
                calibration = source.get("CalibrationLabel")
            if not calibration:
                calibration = source.get("CalibrationDate")

            processing = value.get("Processing")
            if isinstance(processing, list) and len(processing) > 0:
                first_processing = processing[0]
                if isinstance(first_processing, dict):
                    check_time = first_processing.get("At")
                    if check_time:
                        check_time = float(check_time)
                        if not acquisition_start or check_time > acquisition_start:
                            acquisition_start = check_time

        if generic_model and (not model or len(model) > 32):
            model = generic_model
        g = self.apply_instrument_info(
            manufacturer=manufacturer,
            model=model,
            serial_number=serial_number,
            firmware_version=firmware_version,
            calibration=calibration,
        )
        for name, (value, long_name) in to_set.items():
            if not value:
                continue
            var = g.variables.get(name)
            if var is None:
                var = g.createVariable(name, str, fill_value=False)
                var.coverage_content_type = "referenceInformation"
            var.long_name = long_name
            var[0] = str(value)

        if acquisition_start:
            self.root.setncattr("acquisition_start_time", format_iso8601_time(acquisition_start))

        return g

    def data_group(
            self,
            variable_times: typing.List[typing.Union[np.ndarray, "InstrumentConverter.Data"]],
            name: str = "data",
            fill_gaps: typing.Union[bool, float] = True,
    ) -> typing.Tuple[Group, np.ndarray]:
        g = self.root.createGroup(name)

        time_var = time_coordinate(g)

        combined_time = np.concatenate([
            var.time if isinstance(var, InstrumentConverter.Data) else var
            for var in variable_times
        ])
        if combined_time.shape[0] == 0:
            return g, np.empty((0,), dtype=np.int64)

        round_interval = self.average_interval
        if round_interval:
            round_interval = int(round(round_interval * 1000))
            if round_interval > 1:
                combined_time = np.multiply(np.round(combined_time / round_interval), round_interval,
                                            casting='unsafe', dtype=np.int64)
        combined_time = np.unique(combined_time)
        if isinstance(fill_gaps, float) or isinstance(fill_gaps, int):
            fill_gaps = int(round(fill_gaps * 1000))
        elif fill_gaps and round_interval:
            fill_gaps = round_interval * 10
        else:
            fill_gaps = 0
        if round_interval and fill_gaps > 1 and 1 < round_interval < fill_gaps:
            time_delta = combined_time[1:] - combined_time[:-1]

            recombine: typing.List[np.ndarray] = list()
            begin_index: int = 0
            for gap_start_index in np.where(np.all((
                time_delta < fill_gaps,
                time_delta > round_interval
            ), axis=0))[0]:
                gap_start_time = int(combined_time[gap_start_index]) + round_interval
                gap_end_index = int(gap_start_index) + 1
                gap_end_time = int(combined_time[gap_end_index])
                recombine.append(combined_time[begin_index:gap_end_index])
                begin_index = gap_end_index
                recombine.append(np.arange(gap_start_time, gap_end_time, round_interval, dtype=np.int64))
            recombine.append(combined_time[begin_index:])

            file_start_time = int(round(self.file_start * 1000))
            before_start = combined_time[0] - file_start_time
            if round_interval <= before_start < fill_gaps:
                recombine.insert(0, np.arange(file_start_time, combined_time[0], round_interval, dtype=np.int64))

            combined_time = np.concatenate(recombine)

        time_var[:] = combined_time
        return g, combined_time

    def apply_coverage(
            self,
            g: Group,
            group_times: np.ndarray,
            variable: str,
            snap_start_times: typing.Union[bool, float] = True,
    ) -> None:
        if len(group_times.shape) == 0 or group_times.shape[0] == 0:
            return
        interval = self.average_interval
        if not interval:
            return
        interval = int(round(interval * 1000))
        if not interval:
            return

        def convert(x):
            if isinstance(x, list):
                x = x[0]
            if x is None:
                return None
            x = float(x)
            if not isfinite(x):
                return None
            if x < 0.0 or x >= 1.0:
                return None
            return x

        coverage_times, coverage_fraction = self.convert_loaded(read_archive([Selection(
                start=self.file_start,
                end=self.file_end,
                stations=[self.station],
                archives=[self.archive],
                variables=[variable],
                include_meta_archive=False,
                include_default_station=False,
                has_flavors=["cover"],
        )]), convert=convert)

        var = averaged_time_variable(g)

        covered_ms = np.full(group_times.shape, interval, dtype=np.uint64)

        if isinstance(snap_start_times, float) or isinstance(snap_start_times, int):
            snap_threshold = float(snap_start_times)
        elif snap_start_times:
            snap_threshold = self.average_interval
            if snap_threshold:
                snap_threshold *= 0.1
        else:
            snap_threshold = None
        if snap_threshold:
            snap_boundary = int(round((self.average_interval or 0) * 1000))
            snap_threshold = int(floor(snap_start_times * 1000))
            if snap_boundary > 1 and 1 < snap_threshold < snap_boundary:
                rounded_times = np.multiply(np.round(coverage_times / snap_boundary), snap_boundary,
                                            casting='unsafe', dtype=np.int64)
                snap_down = (coverage_times - rounded_times) < snap_boundary
                coverage_times[snap_down] = rounded_times[snap_down]

                var_times, _ = self.convert_loaded(read_archive([Selection(
                    start=self.file_start,
                    end=self.file_end,
                    stations=[self.station],
                    archives=[self.archive],
                    variables=[variable],
                    include_meta_archive=False,
                    include_default_station=False,
                    lacks_flavors=["cover", "stats"],
                )]), convert=bool, dtype=np.bool_)
                rounded_times = np.multiply(np.round(var_times / snap_boundary), snap_boundary,
                                            casting='unsafe', dtype=np.int64)
                snap_down = (var_times - rounded_times) < snap_boundary
                target_times = rounded_times[snap_down]
                removed_time = var_times[snap_down] - rounded_times[snap_down]

                if target_times.shape[0] != 0:
                    target_idx = np.searchsorted(group_times, target_times, side='right') - 1
                    target_idx[target_idx < 0] = 0
                    covered_ms[target_idx] = covered_ms[target_idx] - removed_time

        if coverage_times.shape[0] != 0:
            target_idx = np.searchsorted(group_times, coverage_times, side='right') - 1
            target_idx[target_idx < 0] = 0
            covered_ms[target_idx] = np.round(coverage_fraction * covered_ms[target_idx])

        var[:] = covered_ms

    def state_group(
            self,
            variable_times: typing.List[typing.Union[np.ndarray, "InstrumentConverter.Data"]],
            name: str = "state",
    ) -> typing.Tuple[Group, np.ndarray]:
        g = self.root.createGroup(name)
        time_var = state_change_coordinate(g)

        combined_time = np.concatenate([
            var.time if isinstance(var, InstrumentConverter.Data) else var
            for var in variable_times
        ])
        combined_time = np.unique(combined_time)
        if combined_time.shape[0] == 0:
            return g, np.empty((0,), dtype=np.int64)

        time_var[:] = combined_time
        return g, combined_time

    @staticmethod
    def _set_values(
            indices: np.ndarray,
            var: Variable,
            var_values: typing.Optional[np.ndarray],
            apply_index: typing.Tuple,
            mask: typing.Optional[np.ndarray] = None,
    ) -> None:
        source = var_values[indices]
        if mask is not None:
            source = np.ma.array(source, mask=mask)
        if isinstance(var.datatype, VLType):
            for vidx in np.ndindex(source.shape):
                var[tuple([*vidx, *apply_index])] = source[vidx]
        else:
            var[tuple([slice(None), *apply_index])] = source

    def apply_data(
            self,
            group_times: np.ndarray,
            var: Variable,
            var_times_or_data: typing.Union[np.ndarray, "InstrumentConverter.Data"],
            var_values: typing.Optional[np.ndarray] = None,
            apply_index: typing.Tuple = (),
            skip_gaps: typing.Union[bool, float] = True,
            snap_start_times: typing.Union[bool, float] = True,
    ) -> None:
        if group_times.shape[0] == 0:
            return

        if isinstance(var_times_or_data, InstrumentConverter.Data):
            var_times = var_times_or_data.time
            if var_values is None:
                var_values = var_times_or_data.value
        else:
            var_times = var_times_or_data
        assert var_times.shape[0] == var_values.shape[0]
        if var_times.shape[0] == 0:
            return

        if isinstance(snap_start_times, float) or isinstance(snap_start_times, int):
            snap_threshold = float(snap_start_times)
        elif snap_start_times:
            snap_threshold = self.average_interval
            if snap_threshold:
                snap_threshold *= 0.1
        else:
            snap_threshold = None
        if snap_threshold:
            snap_boundary = int(round((self.average_interval or 0) * 1000))
            snap_threshold = int(floor(snap_start_times * 1000))
            if snap_boundary > 1 and 1 < snap_threshold < snap_boundary:
                rounded_times = np.multiply(np.round(var_times / snap_boundary), snap_boundary,
                                            casting='unsafe', dtype=np.int64)
                snap_down = (var_times - rounded_times) < snap_boundary
                var_times[snap_down] = rounded_times[snap_down]

        indices = incoming_before(group_times, var_times)
        mask = None
        if isinstance(skip_gaps, float) or isinstance(skip_gaps, int):
            gap_interval = skip_gaps
        elif skip_gaps:
            gap_interval = self.average_interval
        else:
            gap_interval = None
        if gap_interval:
            gap_interval = int(round(gap_interval * 1000))
            if gap_interval > 1:
                mask = (var_times[indices] - group_times) <= -gap_interval
                if len(var_values.shape) > 1:
                    reps = 1
                    for v in var_values.shape[1:]:
                        reps *= v
                    mask = np.reshape(np.repeat(mask, reps), (mask.shape[0], *var_values.shape[1:]))

        self._set_values(indices, var, var_values, apply_index, mask=mask)

    def apply_state(
            self,
            group_times: np.ndarray,
            var: Variable,
            var_times_or_data: typing.Union[np.ndarray, "InstrumentConverter.Data"],
            var_values: typing.Optional[np.ndarray] = None,
            apply_index: typing.Tuple = (),
    ) -> None:
        if group_times.shape[0] == 0:
            return

        if isinstance(var_times_or_data, InstrumentConverter.Data):
            var_times = var_times_or_data.time
            if var_values is None:
                var_values = var_times_or_data.value
        else:
            var_times = var_times_or_data
        assert var_times.shape[0] == var_values.shape[0]
        if var_times.shape[0] == 0:
            return

        self._set_values(incoming_before(group_times, var_times), var, var_values, apply_index)

    @staticmethod
    def apply_cut_size(
            g: Group,
            group_times: np.ndarray,
            variables: "typing.List[typing.Tuple[typing.Optional, ...]]",
            extra_sources: "typing.List[typing.Union[typing.Tuple[np.ndarray, np.ndarray], InstrumentConverter.Data]]" = None,
    ) -> None:
        if len(group_times.shape) == 0 or group_times.shape[0] == 0:
            return
        # InstrumentConverter.Data | times, cut_size
        if extra_sources is None:
            extra_sources = list()

        all_cut_sizes = np.concatenate([
            (var[1].cut_size if isinstance(var[1], InstrumentConverter.Data) else var[2])
            for var in variables if var[0] is not None
        ] + [
            (e.cut_size if isinstance(e, InstrumentConverter.Data) else e[1])
            for e in extra_sources
        ])
        all_cut_sizes = np.unique(all_cut_sizes[np.isfinite(all_cut_sizes)])
        all_cut_sizes = list(all_cut_sizes)
        if len(all_cut_sizes) == 0:
            # Everything is whole air, so no cut size variable
            return
        all_cut_sizes.sort()

        cut_data = np.full(group_times.shape, nan, dtype=np.float64)
        for select_size in all_cut_sizes:
            effective_times = np.concatenate([
                var[1].time[var[1].cut_size == select_size] if isinstance(var[1], InstrumentConverter.Data)
                else var[1][var[2] == select_size]
                for var in variables if var[0] is not None
            ] + [
                e.time[e.cut_size == select_size] if isinstance(e, InstrumentConverter.Data)
                else e[0][e[1] == select_size]
                for e in extra_sources
            ])
            if len(effective_times.shape) == 0 or effective_times.shape[0] == 0:
                continue
            set_indices = np.searchsorted(group_times, effective_times, side='right') - 1
            set_indices[set_indices < 0] = 0
            cut_data[set_indices] = select_size

        var = cutsize_variable(g)
        variable_coordinates(g, var)
        var[:] = cut_data

        for var in variables:
            cut_var = var[0]
            if cut_var is None:
                continue
            ancillary_variables = set(getattr(cut_var, 'ancillary_variables', "").split())
            ancillary_variables.add('cut_size')
            cut_var.ancillary_variables = " ".join(sorted(ancillary_variables))

    def default_flags_map(self, bit_shift: int = 16) -> typing.Dict[str, typing.Union[str, typing.Tuple[str, int]]]:
        flags_map = dict()
        for forge_flag, cpd3_flag in instrument_data(self.instrument_type, 'flags', 'lookup').items():
            bit = (cpd3_flag.bit or 0) >> bit_shift
            if bit:
                flags_map[cpd3_flag.code] = (forge_flag, bit)
            else:
                flags_map[cpd3_flag.code] = forge_flag
        flags_map["ContaminationAutomatic"] = "data_contamination_legacy_automatic"
        flags_map["ContaminationManual"] = "data_contamination_legacy_manual"
        flags_map["ContaminationWindRuntime"] = "data_contamination_legacy_wind"
        flags_map["ContaminationWindPost"] = "data_contamination_legacy_wind"
        return flags_map

    @staticmethod
    def _assign_system_flags(
            flags_map: typing.Dict[str, typing.Union[str, typing.Tuple[str, int]]]
    ) -> typing.Tuple[typing.Dict[int, str], typing.Dict[str, int], typing.Dict[str, int], typing.Set[str]]:
        bit_to_flag: typing.Dict[int, str] = dict()
        flag_to_bit: typing.Dict[str, int] = dict()
        cpd3_to_bit: typing.Dict[str, int] = dict()
        unassigned_flags: typing.Set[str] = set(flags_map.keys())
        bits_allocated: typing.Set[str] = set()
        for cpd3_flag, flag in flags_map.items():
            if isinstance(flag, str):
                continue
            bit = flag[1]
            flag = flag[0]
            if bit_to_flag.get(bit):
                continue
            bit_to_flag[bit] = flag
            flag_to_bit[flag] = bit
            cpd3_to_bit[cpd3_flag] = bit
            unassigned_flags.remove(cpd3_flag)
        for cpd3_flag in unassigned_flags:
            flag = flags_map.get(cpd3_flag)
            if not isinstance(flag, str):
                flag = flag[0]
            bit = flag_to_bit.get(flag)
            if bit:
                cpd3_to_bit[cpd3_flag] = bit
                continue
            bits_allocated.add(cpd3_flag)
            for i in range(64):
                check_bit = 1 << i
                if not bit_to_flag.get(check_bit):
                    bit = check_bit
                    break
            else:
                raise ValueError("No available bit in variable for flag %s to %s", cpd3_flag, flag)
            bit_to_flag[bit] = flag
            flag_to_bit[flag] = bit
            cpd3_to_bit[cpd3_flag] = bit

        return bit_to_flag, flag_to_bit, cpd3_to_bit, bits_allocated

    def _convert_system_flags(
            self,
            bit_to_flag: typing.Dict[int, str],
            flag_to_bit: typing.Dict[str, int],
            cpd3_to_bit: typing.Dict[str, int],
            variable: str = None,
            check_allocated: typing.Set[str] = None,
    ) -> typing.Tuple["InstrumentConverter.Data", bool]:
        if check_allocated is None:
            check_allocated = set()

        # Remove contamination flags when they're not actually used
        remove_bits = flag_to_bit.get("data_contamination_legacy_automatic", 0) | \
                      flag_to_bit.get("data_contamination_legacy_wind", 0) | \
                      flag_to_bit.get("data_contamination_legacy_manual", 0)
        allocated_present = False
        def convert(val: typing.Any) -> int:
            nonlocal remove_bits
            nonlocal allocated_present
            if not isinstance(val, set):
                return 0
            bits = 0
            for flag in val:
                add_bit = cpd3_to_bit.get(flag, 0)
                bits |= add_bit
                remove_bits &= ~add_bit
            if not check_allocated.isdisjoint(val):
                allocated_present = True
            return bits

        flags_data = self.load_variable(variable or f"F1?_{self.instrument_id}", convert=convert, dtype=np.uint64)

        for check_bit in list(bit_to_flag.keys()):
            if (remove_bits & check_bit) != 0:
                del bit_to_flag[check_bit]

        return flags_data, allocated_present

    def declare_system_flags(
            self,
            g: Group,
            group_times: np.ndarray,
            variable: str = None,
            flags_map: typing.Dict[str, typing.Union[str, typing.Tuple[str, int]]] = None,
            bit_shift: int = 16,
    ) -> typing.Tuple["InstrumentConverter.Data", typing.Dict[str, int]]:
        if flags_map is None:
            flags_map = self.default_flags_map(bit_shift)
        bit_to_flag, flag_to_bit, cpd3_to_bit, _ = self._assign_system_flags(flags_map)

        flags_data, _ = self._convert_system_flags(
            bit_to_flag,
            flag_to_bit,
            cpd3_to_bit,
            variable=variable,
        )

        system_flags = g.createVariable("system_flags", "u8", ("time",), fill_value=False)
        variable_coordinates(g, system_flags)
        system_flags.coverage_content_type = "physicalMeasurement"
        system_flags.variable_id = "F1"
        if flags_data.time.shape[0] == 0:
            system_flags[:] = 0
        else:
            self.apply_data(group_times, system_flags, flags_data, skip_gaps=False)
        variable_flags(system_flags, bit_to_flag)

        return flags_data, flag_to_bit

    def analyze_flags_mapping_bug(
            self,
            variable: str = None,
            flags_map: typing.Dict[str, typing.Union[str, typing.Tuple[str, int]]] = None,
            bit_shift: int = 16,
    ) -> typing.Optional[typing.Tuple["InstrumentConverter.Data", typing.Dict[int, str]]]:
        if flags_map is None:
            flags_map = self.default_flags_map(bit_shift)
        bit_to_flag, flag_to_bit, cpd3_to_bit, bits_allocated = self._assign_system_flags(flags_map)

        # No unassigned flags means we're always ok
        if len(bits_allocated) == 0:
            return None
        # Exactly one unassigned flag and the first bit free (it would always go there), also always ok
        if len(bits_allocated) == 1 and not bit_to_flag.get(1 << 0):
            return None

        flags_data, allocated_present = self._convert_system_flags(
            bit_to_flag,
            flag_to_bit,
            cpd3_to_bit,
            variable=variable,
            check_allocated=bits_allocated,
        )
        # Affected flags never came up, so ignore
        if not allocated_present:
            return None

        return flags_data, bit_to_flag

    def reapply_system_flags(
            self,
            flags_data: "InstrumentConverter.Data",
            bit_to_flag: typing.Dict[int, str],
            g: typing.Optional[Group] = None,
    ) -> None:
        if g is None:
            g = self.root.groups.get("data")
            if g is None:
                return

        group_times = g.variables.get("time")
        if group_times is None:
            return
        group_times = group_times[...].data

        system_flags = g.variables.get("system_flags")
        if system_flags is None:
            return

        if flags_data.time.shape[0] == 0:
            system_flags[:] = 0
        else:
            self.apply_data(group_times, system_flags, flags_data, skip_gaps=False)
        variable_flags(system_flags, bit_to_flag)

    def run(self) -> bool:
        instrument_timeseries(self.root, self.station, self.instrument_id,
                              self.file_start, self.file_end, self.average_interval,
                              tags=self.tags)

        if self.instrument_type:
            self.root.setncattr("instrument_vocabulary", f"Forge Acquisition {__short_version__}")
            self.root.setncattr("instrument", self.instrument_type)
            append_history(self.root, "forge.cpd3.convert." + self.instrument_type)
        else:
            append_history(self.root, "forge.cpd3.convert")

        return True


class WavelengthConverter(InstrumentConverter):
    WAVELENGTHS: typing.List[typing.Tuple[float, str]] = list()

    def load_wavelength_variable(
            self, prefix: str, suffix: str = "",
            convert: typing.Callable[[typing.Any], typing.Any] = None,
            dtype: typing.Type = np.float64
    ) -> typing.List["WavelengthConverter.Data"]:
        result: typing.List[WavelengthConverter.Data] = list()
        for _, code in self.WAVELENGTHS:
            result.append(self.load_variable(
                f"{prefix}{code}{suffix}_{self.instrument_id}",
                convert=convert,
                dtype=dtype,
            ))
        return result

    def load_wavelength_state(
            self, prefix: str, suffix: str = "",
            convert: typing.Callable[[typing.Any], typing.Any] = None,
            dtype: typing.Type = np.float64
    ) -> typing.List["WavelengthConverter.Data"]:
        result: typing.List[WavelengthConverter.Data] = list()
        for _, code in self.WAVELENGTHS:
            result.append(self.load_state(
                f"{prefix}{code}{suffix}_{self.instrument_id}",
                convert=convert,
                dtype=dtype,
            ))
        return result

    def declare_wavelength(self, g: Group) -> None:
        g.createDimension('wavelength', len(self.WAVELENGTHS))
        wl = g.createVariable('wavelength', 'f8', ('wavelength',), fill_value=nan)
        wl.coverage_content_type = "coordinate"
        variable_wavelength(wl)
        wl[:] = [wavelength for wavelength, _ in self.WAVELENGTHS]

    def data_group(
            self,
            variable_times: typing.List[typing.Union[np.ndarray, "InstrumentConverter.Data"]],
            name: str = "data",
            fill_gaps: typing.Union[bool, float] = True,
            wavelength: bool = True,
    ) -> typing.Tuple[Group, np.ndarray]:
        g, times = super().data_group(variable_times, name=name, fill_gaps=fill_gaps)
        if wavelength:
            self.declare_wavelength(g)
        return g, times

    def state_group(
            self,
            variable_times: typing.List[typing.Union[np.ndarray, "InstrumentConverter.Data"]],
            name: str = "state",
            wavelength: bool = True,
    ) -> typing.Tuple[Group, np.ndarray]:
        g, times = super().state_group(variable_times, name=name)
        if wavelength:
            self.declare_wavelength(g)
        return g, times

    def apply_wavelength_data(
            self,
            group_times: np.ndarray,
            var: Variable,
            data: typing.List["WavelengthConverter.Data"],
            skip_gaps: typing.Union[bool, float] = True,
            snap_start_times: typing.Union[bool, float] = True,
    ) -> None:
        for wlidx in range(len(self.WAVELENGTHS)):
            self.apply_data(
                group_times, var, data[wlidx].time, data[wlidx].value, (wlidx,),
                skip_gaps=skip_gaps, snap_start_times=snap_start_times,
            )

    def apply_wavelength_state(
            self,
            group_times: np.ndarray,
            var: Variable,
            data: typing.List["WavelengthConverter.Data"]
    ) -> None:
        for wlidx in range(len(self.WAVELENGTHS)):
            self.apply_state(group_times, var, data[wlidx].time, data[wlidx].value, (wlidx,))

    def apply_cut_size(
            self,
            g: Group,
            group_times: np.ndarray,
            variables: "typing.List[typing.Tuple[typing.Optional, ...]]",
            wavelength_variables: typing.List[typing.Tuple[typing.Optional[Variable], typing.List["WavelengthConverter.Data"]]] = None,
            extra_sources: "typing.List[typing.Union[typing.Tuple[np.ndarray, np.ndarray], InstrumentConverter.Data]]" = None,
    ) -> None:
        if wavelength_variables:
            for var, data in wavelength_variables:
                if var is None:
                    continue
                selected_data = data[0]
                for check_data in data:
                    if check_data.time.shape[0] > selected_data.time.shape[0]:
                        selected_data = check_data
                variables.append((var, selected_data))
        super().apply_cut_size(g, group_times, variables, extra_sources=extra_sources)
