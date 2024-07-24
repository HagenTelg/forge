import typing
import logging
import enum
import numpy as np
from math import isfinite, floor, ceil, nan
from bisect import bisect_left
from tempfile import NamedTemporaryFile, TemporaryDirectory
from netCDF4 import Dataset, Variable
from forge.const import STATIONS, MAX_I64
from forge.logicaltime import containing_year_range, start_of_year
from forge.timeparse import parse_iso8601_duration, parse_iso8601_time
from forge.archive.client import index_lock_key, index_file_name, data_lock_key, data_file_name
from forge.archive.client.archiveindex import ArchiveIndex
from forge.archive.client.connection import Connection
from forge.data.state import is_state_group
from forge.data.flags import parse_flags
from forge.data.dimensions import find_dimension_values


_LOGGER = logging.getLogger(__name__)
_VALID_ARCHIVES = frozenset({
    "raw",
    "edited",
    "clean",
    "avgh",
    "avgd",
    "avgm",
})
_EMPTY_SET = frozenset({})


class FileSource:
    def __init__(self, station: str, archive: str):
        self.station = station
        self.archive = archive

    def __eq__(self, other):
        if not isinstance(other, FileSource):
            return NotImplemented
        return self.station == other.station and self.archive == other.archive

    def __hash__(self):
        return hash((self.station, self.archive))

    def __repr__(self):
        return f"ReadIndex({self.station}, {self.archive})"


class FileContext:
    def __init__(self, instrument_id: str, root: Dataset):
        self.instrument_id = instrument_id
        self._root = root
        self._tags: typing.Optional[typing.Set[str]] = None
        self._instrument_code: typing.Optional[str] = None

    @property
    def tags(self) -> typing.Set[str]:
        if self._tags is None:
            self._tags = set(str(getattr(self._root, 'forge_tags', "")).split())
        return self._tags

    @property
    def instrument_code(self) -> str:
        if self._instrument_code is None:
            self._instrument_code = str(getattr(self._root, 'instrument', ""))
        return self._instrument_code


class VariableRootContext:
    def __init__(self, root: Dataset):
        self._root = root
        self._group_wavelength: typing.Dict[Dataset, np.ndarray] = dict()
        self._group_cut_size_dimension: typing.Dict[Dataset, np.ndarray] = dict()
        self._group_cut_size_time: typing.Dict[Dataset, typing.Optional[np.ndarray]] = dict()
        self._group_quantile: typing.Dict[Dataset, typing.Optional[np.ndarray]] = dict()
        self._group_time: typing.Dict[Dataset, typing.Optional[np.ndarray]] = dict()
        self._is_state: typing.Dict[Dataset, bool] = dict()
        self._time_coverage_resolution: typing.Union[bool, typing.Optional[float]] = False
        self._time_bounds_ms: typing.Optional[typing.Tuple[int, int]] = None

    def group_wavelength(self, group: Dataset) -> np.ndarray:
        hit = self._group_wavelength.get(group)
        if hit is not None:
            return hit
        _, wl_values = find_dimension_values(group, 'wavelength')
        hit = wl_values[:].data
        self._group_wavelength[group] = hit
        return hit

    def group_cut_size_dimension(self, group: Dataset) -> np.ndarray:
        hit = self._group_cut_size_dimension.get(group)
        if hit:
            return hit
        _, cut_values = find_dimension_values(group, 'cut_size')
        hit = cut_values[:].data
        self._group_cut_size_dimension[group] = hit
        return hit

    def group_cut_size_time(self, group: Dataset) -> typing.Optional[np.ndarray]:
        hit = self._group_cut_size_time.get(group, False)
        if hit is not False:
            return hit
        values = group.variables.get('cut_size')
        if values is not None:
            values = values[:].data
        self._group_cut_size_time[group] = values
        return values
    
    def group_quantile(self, group: Dataset) -> typing.List[float]:
        hit = self._group_quantile.get(group)
        if hit is not None:
            return hit
        _, wl_values = find_dimension_values(group, 'quantile')
        hit = list(wl_values[:].data)
        self._group_quantile[group] = hit
        return hit

    def group_time(self, group: Dataset) -> typing.Optional[np.ndarray]:
        hit = self._group_time.get(group, False)
        if hit is not False:
            return hit
        try:
            _, time_values = find_dimension_values(group, 'time')
            time_values = time_values[:].data
        except KeyError:
            time_values = None
        self._group_time[group] = time_values
        return time_values

    def is_state(self, group: Dataset) -> bool:
        hit = self._is_state.get(group)
        if hit is not None:
            return hit

        is_state = is_state_group(group)
        while is_state is None:
            group = group.parent
            if group is None:
                break
            is_state = is_state_group(group)
        is_state = bool(is_state)

        self._is_state[group] = is_state
        return is_state

    @property
    def time_coverage_resolution(self) -> typing.Optional[float]:
        if self._time_coverage_resolution is False:
            time_coverage_resolution = getattr(self._root, "time_coverage_resolution", None)
            if time_coverage_resolution is not None:
                try:
                    self._time_coverage_resolution = parse_iso8601_duration(str(time_coverage_resolution))
                except ValueError:
                    self._time_coverage_resolution = None
            else:
                self._time_coverage_resolution = None
        return self._time_coverage_resolution

    @property
    def time_bounds_ms(self) -> typing.Tuple[int, int]:
        if self._time_bounds_ms is None:
            time_coverage_start = getattr(self._root, 'time_coverage_start', None)
            if time_coverage_start is not None:
                time_coverage_start = int(floor(parse_iso8601_time(str(time_coverage_start)).timestamp() * 1000.0))
            else:
                time_coverage_start = -MAX_I64
            time_coverage_end = getattr(self._root, 'time_coverage_end', None)
            if time_coverage_end is not None:
                time_coverage_end = int(ceil(parse_iso8601_time(str(time_coverage_end)).timestamp() * 1000.0))
            else:
                time_coverage_end = MAX_I64
            self._time_bounds_ms = (time_coverage_start, time_coverage_end)
        return self._time_bounds_ms


class VariableContext:
    def __init__(self, root: VariableRootContext, variable: Variable):
        self._root = root
        self.variable = variable
        self._ancillary_variables: typing.Optional[typing.Set[str]] = None
        self._flags: typing.Optional[typing.Dict[int, str]] = None
        self._wavelengths: typing.Optional[typing.Tuple[typing.Optional[int], typing.List[float]]] = None
        self._cut_size_dimension: typing.Optional[typing.Tuple[typing.Optional[int], typing.List[float]]] = None
        self._cut_size_time: typing.Optional[typing.Union[bool, np.ndarray]] = None
        self._statistics_type: typing.Optional[str] = None
        self._quantiles: typing.Optional[typing.Tuple[typing.Optional[int], typing.List[float]]] = None
        self._values: typing.Optional[np.ndarray] = None

    @property
    def variable_name(self) -> str:
        return self.variable.name

    @property
    def variable_id(self) -> str:
        return getattr(self.variable, 'variable_id', None)

    @property
    def standard_name(self) -> str:
        return getattr(self.variable, 'standard_name', None)

    @property
    def ancillary_variables(self) -> typing.Set[str]:
        if self._ancillary_variables is None:
            self._ancillary_variables = set(getattr(self.variable, 'ancillary_variables', "").split())
        return self._ancillary_variables

    @property
    def flags(self) -> typing.Dict[int, str]:
        if self._flags is None:
            self._flags = parse_flags(self.variable)
        return self._flags

    @property
    def statistics_type(self) -> str:
        if self._statistics_type is None:
            current_group = self.variable.group()
            while current_group is not None:
                parent_group = current_group.parent
                if parent_group is None:
                    break
                if parent_group.name == 'statistics':
                    self._statistics_type = current_group.name
                    break
                current_group = parent_group
            else:
                self._statistics_type = ''
        return self._statistics_type

    @property
    def wavelengths(self) -> typing.Tuple[typing.Optional[int], np.ndarray]:
        if self._wavelengths is None:
            try:
                wl_dimension = self.variable.dimensions.index('wavelength')
                wl_values = self._root.group_wavelength(self.variable.group())
                self._wavelengths = (wl_dimension, wl_values)
            except ValueError:
                self._wavelengths = (None, np.empty((), dtype=np.float64))
        return self._wavelengths

    @property
    def quantiles(self) -> typing.Tuple[typing.Optional[int], typing.List[float]]:
        if self._quantiles is None:
            try:
                q_dimension = self.variable.dimensions.index('wavelength')
                q_values = self._root.group_quantile(self.variable.group())
                self._quantiles = (q_dimension, q_values)
            except ValueError:
                self._quantiles = (None, [])
        return self._quantiles
    
    @property
    def cut_size_dimension(self) -> typing.Tuple[typing.Optional[int], np.ndarray]:
        if self._cut_size_dimension is None:
            try:
                cut_dimension = self.variable.dimensions.index('cut_size')
                cut_values = self._root.group_cut_size_dimension(self.variable.group())
                self._cut_size_dimension = (cut_dimension, cut_values)
            except ValueError:
                self._cut_size_dimension = (None, np.empty((), dtype=np.float64))
        return self._cut_size_dimension

    @property
    def cut_size_time(self) -> typing.Optional[np.ndarray]:
        if self._cut_size_time is None:
            if 'cut_size' not in self.ancillary_variables:
                self._cut_size_time = False
            else:
                cut_values = self._root.group_cut_size_time(self.variable.group())
                if cut_values is None:
                    self._cut_size_time = False
                else:
                    self._cut_size_time = cut_values
        return self._cut_size_time if self._cut_size_time is not False else None

    @property
    def times(self) -> typing.Optional[np.ndarray]:
        return self._root.group_time(self.variable.group())

    @property
    def values(self) -> np.ndarray:
        if self._values is None:
            self._values = self.variable[:].data
        return self._values

    @property
    def is_state(self) -> bool:
        return self._root.is_state(self.variable.group())

    @property
    def interval(self) -> typing.Optional[float]:
        return self._root.time_coverage_resolution

    @property
    def time_bounds_ms(self) -> typing.Tuple[int, int]:
        return self._root.time_bounds_ms
    
    def find_dimension_values(self, name: str) -> typing.Optional[Variable]:
        return find_dimension_values(self.variable.group(), name)[1]


class InstrumentSelection:
    def __init__(
            self,
            instrument_id: typing.Optional[str] = None,
            instrument_code: typing.Optional[str] = None,
            require_tags: typing.Iterable[str] = None,
            exclude_tags: typing.Iterable[str] = None,
            station: typing.Optional[str] = None,
            archive: typing.Optional[str] = None,
    ):
        self.instrument_id: typing.Optional[str] = str(instrument_id).upper() if instrument_id else None
        self.instrument_code: typing.Optional[str] = str(instrument_code).lower() if instrument_code else None
        self.require_tags: typing.Set[str] = set(
            [str(t).lower() for t in require_tags]) if require_tags else _EMPTY_SET
        self.exclude_tags: typing.Set[str] = set(
            [str(t).lower() for t in exclude_tags]) if exclude_tags else _EMPTY_SET
        self.station: typing.Optional[str] = str(station).lower() if station else None
        self.archive: typing.Optional[str] = str(archive).lower() if archive else None

        if self.archive and self.archive not in _VALID_ARCHIVES:
            _LOGGER.warning("Invalid archive for selection %r", self)
        if self.station and self.station not in STATIONS:
            _LOGGER.warning("Invalid station for archive selection %r", self)

    def __repr__(self):
        parts = []
        if self.instrument_id:
            parts.append(f"instrument_id='{self.instrument_id}'")
        if self.instrument_code:
            parts.append(f"instrument_code='{self.instrument_code}'")
        if self.require_tags:
            parts.append(f"require_tags={'{'}{','.join(self.require_tags)}{'}'}")
        if self.exclude_tags:
            parts.append(f"exclude_tags={'{'}{','.join(self.exclude_tags)}{'}'}")
        if self.station:
            parts.append(f"station='{self.station}'")
        if self.archive:
            parts.append(f"archive='{self.archive}'")
        return "InstrumentSelection(" + ",".join(parts) + ")"

    def read_index(self, request_station: str, request_archive: str) -> typing.Optional[FileSource]:
        station = self.station if self.station else request_station.lower()
        archive = self.archive if self.archive else request_archive.lower()
        if station not in STATIONS or archive not in _VALID_ARCHIVES:
            return None
        return FileSource(station, archive)

    def index_to_instruments(self, index: ArchiveIndex, everything: bool = True) -> typing.Optional[typing.Set[str]]:
        possible_instruments: typing.Optional[typing.Set[str]] = None

        if self.instrument_id:
            possible_instruments = {self.instrument_id}

        if self.instrument_code:
            selected_instruments: typing.Set[str] = set()
            if possible_instruments is None:
                for instrument_id, codes in index.tags.items():
                    if self.instrument_code not in codes:
                        continue
                    selected_instruments.add(instrument_id)
            else:
                for instrument_id in possible_instruments:
                    codes = index.tags.get(instrument_id)
                    if not codes:
                        continue
                    if self.instrument_code not in codes:
                        continue
                    selected_instruments.add(instrument_id)
            possible_instruments = selected_instruments

        if self.require_tags:
            selected_instruments: typing.Set[str] = set()
            if possible_instruments is None:
                for instrument_id, tags in index.tags.items():
                    if not self.require_tags.issubset(tags):
                        continue
                    selected_instruments.add(instrument_id)
            else:
                for instrument_id in possible_instruments:
                    tags = index.tags.get(instrument_id)
                    if tags is None:
                        continue
                    if not self.require_tags.issubset(tags):
                        continue
                    selected_instruments.add(instrument_id)
            possible_instruments = selected_instruments
        # Can't filter on excluded tags since the index is a union, so an individual file may lack them

        if possible_instruments is None and everything:
            return set(index.known_instrument_ids)
        return possible_instruments

    def accept_file(self, file: FileContext) -> bool:
        if self.instrument_id:
            if self.instrument_id != file.instrument_id:
                return False
        if self.instrument_code:
            if self.instrument_code != file.instrument_code:
                return False
        if self.require_tags:
            if not self.require_tags.issubset(file.tags):
                return False
        if self.exclude_tags:
            if not self.exclude_tags.isdisjoint(file.tags):
                return False
        return True


class Selection(InstrumentSelection):
    class VariableType(enum.Enum):
        Normal = enum.auto()
        State = enum.auto()
        StandardDeviation = enum.auto()
        Quantiles = enum.auto()

    def __init__(
            self,
            instrument_id: typing.Optional[str] = None,
            variable_name: typing.Optional[str] = None,
            variable_id: typing.Optional[str] = None,
            standard_name: typing.Optional[str] = None,
            instrument_code: typing.Optional[str] = None,
            require_tags: typing.Iterable[str] = None,
            exclude_tags: typing.Iterable[str] = None,
            station: typing.Optional[str] = None,
            archive: typing.Optional[str] = None,
            wavelength: typing.Optional[typing.Union[float, typing.Sequence[float]]] = None,
            wavelength_number: typing.Optional[int] = None,
            cut_size: typing.Optional[typing.Union[float, typing.Sequence[float]]] = None,
            cut_size_number: typing.Optional[int] = None,
            quantile: typing.Optional[float] = None,
            variable_type: typing.Optional["Selection.VariableType"] = None,
            dimension_at: typing.Optional[typing.Iterable[typing.Tuple[str, float]]] = None,
    ):
        super().__init__(
            instrument_id=instrument_id,
            instrument_code=instrument_code,
            require_tags=require_tags,
            exclude_tags=exclude_tags,
            station=station,
            archive=archive,
        )
        self.variable_name: typing.Optional[str] = str(variable_name).lower() if variable_name else None
        self.variable_id: typing.Optional[str] = str(variable_id) if variable_id else None
        self.standard_name: typing.Optional[str] = str(standard_name).lower() if standard_name else None

        def float_range(r: typing.Optional[typing.Union[float, typing.Sequence[float]]]) -> typing.Optional[typing.Tuple[typing.Optional[float], typing.Optional[float]]]:
            if r is None:
                return None
            try:
                f = float(r)
                return f, f
            except (TypeError, ValueError):
                pass
            if len(r) == 0:
                return None, None
            if len(r) == 1:
                f = float(r[0])
                return f, f
            return float(r[0]), float(r[1])

        self.wavelength = float_range(wavelength)
        self.wavelength_number: typing.Optional[int] = int(wavelength_number) if wavelength_number is not None else None
        if self.wavelength_number is not None and self.wavelength_number < 0:
            self.wavelength_number = None

        self.cut_size = float_range(cut_size)
        self.cut_size_number: typing.Optional[int] = int(cut_size_number) if cut_size_number is not None else None
        if self.cut_size_number is not None and self.cut_size_number < 0:
            self.cut_size_number = None
            
        self.quantile: typing.Optional[float] = float(quantile) if quantile else None
        if self.quantile is not None and not isfinite(self.quantile):
            self.quantile = None
        self.variable_type = variable_type
        if self.quantile is not None:
            if self.variable_type and self.variable_type != self.VariableType.Quantiles:
                _LOGGER.warning("Quantile type forced for selection %r", self)
            self.variable_type = self.VariableType.Quantiles

        self.dimension_at = dimension_at

        if not self._valid_variable_selection:
            _LOGGER.warning("No variable selected for archive selection %r", self)
        if self.archive and not self._valid_variable_type(self.archive):
            _LOGGER.warning("Invalid variable for archive selection %r", self)
        if self.wavelength and self.wavelength[0] and isfinite(self.wavelength[0]) and self.wavelength[1] and isfinite(self.wavelength[1]) and self.wavelength[0] > self.wavelength[1]:
            _LOGGER.warning("Invalid wavelength specification for archive selection %r", self)
        if self.cut_size and self.cut_size[0] and isfinite(self.cut_size[0]) and self.cut_size[1] and isfinite(self.cut_size[1]) and self.cut_size[0] > self.cut_size[1]:
            _LOGGER.warning("Invalid cut specification with for archive selection %r", self)
        if self.quantile is not None and (self.quantile < 0 or self.quantile > 1.0):
            _LOGGER.warning("Invalid quantile for archive selection %r", self)

    def __repr__(self):
        parts = []
        if self.instrument_id:
            parts.append(f"instrument_id='{self.instrument_id}'")
        if self.variable_name:
            parts.append(f"variable_name='{self.variable_name}'")
        if self.variable_id:
            parts.append(f"variable_id='{self.variable_id}'")
        if self.standard_name:
            parts.append(f"standard_name='{self.standard_name}'")
        if self.instrument_code:
            parts.append(f"instrument_code='{self.instrument_code}'")
        if self.require_tags:
            parts.append(f"require_tags={'{'}{','.join(self.require_tags)}{'}'}")
        if self.exclude_tags:
            parts.append(f"exclude_tags={'{'}{','.join(self.exclude_tags)}{'}'}")
        if self.station:
            parts.append(f"station='{self.station}'")
        if self.archive:
            parts.append(f"archive='{self.archive}'")
        if self.wavelength:
            parts.append(f"wavelength={self.wavelength}")
        if self.wavelength_number is not None:
            parts.append(f"wavelength_number={self.wavelength_number}")
        if self.cut_size is not None:
            parts.append(f"cut_size={self.cut_size}")
        if self.cut_size_number is not None:
            parts.append(f"cut_size_number={self.cut_size_number}")
        if self.quantile is not None:
            parts.append(f"quantile={self.quantile}")
        if self.variable_type:
            parts.append(f"variable_type={self.variable_type}")
        return "Selection(" + ",".join(parts) + ")"

    @property
    def _valid_variable_selection(self) -> bool:
        return bool(self.variable_name or self.variable_id or self.standard_name)

    def _valid_variable_type(self, archive: str) -> bool:
        if self.variable_type in (Selection.VariableType.StandardDeviation, Selection.VariableType.Quantiles):
            if archive not in ("avgh", "avgd", "avgm"):
                return False
        return True

    def _wavelength_index_possible(self, wavelength_count: int) -> bool:
        if self.wavelength_number is not None:
            if not wavelength_count:
                return False
            if self.wavelength_number >= wavelength_count:
                return False
        if self.wavelength:
            if not wavelength_count:
                return False
        return True

    def read_index(self, request_station: str, request_archive: str) -> typing.Optional[FileSource]:
        if not self._valid_variable_selection:
            return None
        archive = self.archive if self.archive else request_archive.lower()
        if not self.archive and not self._valid_variable_type(archive):
            _LOGGER.warning("Invalid variable for archive selection %r on archive %s", self, archive)
            return None
        return super().read_index(request_station, request_archive)

    def index_to_instruments(self, index: ArchiveIndex, everything: bool = True) -> typing.Optional[typing.Set[str]]:
        possible_instruments: typing.Optional[typing.Set[str]] = super().index_to_instruments(index, False)

        if self.variable_name:
            selected_instruments = index.variable_names.get(self.variable_name)
            if not selected_instruments:
                return None
            if possible_instruments:
                possible_instruments = possible_instruments.intersection(selected_instruments)
            else:
                possible_instruments = selected_instruments

        if self.standard_name:
            selected_instruments = index.standard_names.get(self.standard_name)
            if not selected_instruments:
                return None
            if possible_instruments:
                possible_instruments = possible_instruments.intersection(selected_instruments)
            else:
                possible_instruments = selected_instruments

        if self.variable_id:
            variable_ids = index.variable_ids.get(self.variable_id)
            if not variable_ids:
                return None
            selected_instruments: typing.Set[str] = set()
            if possible_instruments is None:
                for instrument_id, wavelength_count in variable_ids.items():
                    if not self._wavelength_index_possible(wavelength_count):
                        continue
                    selected_instruments.add(instrument_id)
            else:
                for instrument_id in possible_instruments:
                    wavelength_count = variable_ids.get(instrument_id)
                    if wavelength_count is None:
                        continue
                    if not self._wavelength_index_possible(wavelength_count):
                        continue
                    selected_instruments.add(instrument_id)
            possible_instruments = selected_instruments
        elif self.wavelength_number is not None or self.wavelength:
            selected_instruments: typing.Set[str] = set()
            for instrument_wavelength in index.variable_ids.values():
                if possible_instruments is None:
                    for instrument_id, wavelength_count in instrument_wavelength.items():
                        if not self._wavelength_index_possible(wavelength_count):
                            continue
                        selected_instruments.add(instrument_id)
                else:
                    for instrument_id in possible_instruments:
                        wavelength_count = instrument_wavelength.get(instrument_id)
                        if not self._wavelength_index_possible(wavelength_count):
                            continue
                        selected_instruments.add(instrument_id)
            possible_instruments = selected_instruments

        if possible_instruments is None and everything:
            return set(index.known_instrument_ids)
        return possible_instruments

    def variable_values(
            self,
            var: VariableContext,
            return_wavelength: bool = False,
            return_cut_size_times: bool = False,
            return_cut_size_dimension: bool = False,
    ):
        if self.variable_id and self.variable_id != var.variable_id:
            return None
        if self.variable_name and self.variable_name != var.variable_name:
            return None
        if self.standard_name and self.standard_name != var.standard_name:
            return None
        if self.variable_type:
            if self.variable_type == self.VariableType.Quantiles:
                if var.statistics_type != 'quantiles' or var.is_state:
                    return None
            elif self.variable_type == self.VariableType.StandardDeviation:
                if var.statistics_type != 'stddev' or var.is_state:
                    return None
            elif self.variable_type == self.VariableType.Normal:
                if var.is_state:
                    return None
            elif self.variable_type == self.VariableType.State:
                if not var.is_state:
                    return None
            else:
                raise RuntimeError

        wavelength_dimension_number: typing.Optional[int] = None
        wavelength_dimension_selector: typing.Optional[int] = None
        possible_wavelengths: typing.Optional[np.ndarray] = None
        if self.wavelength:
            wavelength_dimension_number, possible_wavelengths = var.wavelengths
            wavelength_min, wavelength_max = self.wavelength
            for wl_number in range(len(possible_wavelengths)):
                wavelength = float(possible_wavelengths[wl_number])
                if wavelength_min is not None and wavelength < wavelength_min:
                    continue
                if wavelength_max is not None and wavelength >= wavelength_max and wavelength_min != wavelength_max:
                    continue
                wavelength_dimension_selector = wl_number
                break
            else:
                return None
        elif self.wavelength_number is not None:
            wavelength_dimension_number, possible_wavelengths = var.wavelengths
            if self.wavelength_number >= len(possible_wavelengths):
                return None
            wavelength_dimension_selector = self.wavelength_number
        elif return_wavelength:
            check, possible_wavelengths = var.wavelengths
            if check is None:
                possible_wavelengths = None

        quantile_dimension_number: typing.Optional[int] = None
        quantile_dimension_selector: typing.Optional[typing.Tuple[int, int]] = None
        quantile_fraction: typing.Optional[float] = None
        if self.quantile is not None:
            quantile_dimension_number, possible_quantiles = var.quantiles
            if quantile_dimension_number is None or not possible_quantiles:
                return None
            if len(possible_quantiles) == 1:
                if possible_quantiles[0] != self.quantile:
                    return None
                quantile_dimension_selector = (0, 0)
                quantile_fraction = 0.0
            else:
                q_lower = bisect_left(possible_quantiles, self.quantile)
                if q_lower >= len(possible_quantiles) - 1:
                    q_upper = len(possible_quantiles) - 1
                    q_lower = q_upper - 1
                else:
                    q_upper = q_lower + 1
                quantile_dimension_selector = (q_lower, q_upper)
                q_lower = possible_quantiles[q_lower]
                q_upper = possible_quantiles[q_upper]
                quantile_fraction = (self.quantile - q_lower) / (q_upper - q_lower)

        time_values = var.times

        time_selector = None
        cut_size_dimension_number: typing.Optional[int] = None
        cut_size_dimension_selector: typing.Optional[int] = None
        cut_size_times: typing.Optional[np.ndarray] = None
        possible_cut_size: typing.Optional[np.ndarray] = None
        if self.cut_size:
            cut_size_dimension_number, possible_cut_size = var.cut_size_dimension
            cut_size_min, cut_size_max = self.cut_size
            if cut_size_dimension_number is None:
                if time_values is None:
                    if cut_size_max is not None and isfinite(cut_size_max):
                        return None
                    if return_cut_size_times:
                        cut_size_times = var.cut_size_time
                else:
                    cut_size_times = var.cut_size_time
                    if cut_size_times is None:
                        if cut_size_max is not None and isfinite(cut_size_max):
                            return None
                    else:
                        if cut_size_min is None or not isfinite(cut_size_min):
                            if cut_size_max is None or not isfinite(cut_size_max):
                                time_selector = np.invert(np.isfinite(cut_size_times))
                            else:
                                time_selector = cut_size_times < cut_size_max
                        else:
                            if cut_size_max is None:
                                time_selector = np.any((
                                    np.invert(np.isfinite(cut_size_times)),
                                    cut_size_times >= cut_size_min,
                                ), axis=0)
                                if return_cut_size_times:
                                    result_cut_sizes = cut_size_times[time_selector]
                            elif not isfinite(cut_size_max):
                                time_selector = cut_size_times >= cut_size_min
                            elif cut_size_min != cut_size_max:
                                time_selector = np.all((
                                    cut_size_times >= cut_size_min,
                                    cut_size_times < cut_size_max,
                                ), axis=0)
                            else:
                                time_selector = cut_size_times == cut_size_min
            else:
                for cut_number in range(len(possible_cut_size)):
                    cut_size = possible_cut_size[cut_number]
                    if cut_size_min is None or not isfinite(cut_size_min):
                        if cut_size_max is None or not isfinite(cut_size_max):
                            if isfinite(cut_size):
                                continue
                        else:
                            if cut_size >= cut_size_max:
                                continue
                    else:
                        if cut_size_max is None:
                            if isfinite(cut_size) and cut_size < cut_size_min:
                                continue
                        elif not isfinite(cut_size_max):
                            if not isfinite(cut_size) or cut_size < cut_size_min:
                                continue
                        elif cut_size_min != cut_size_max:
                            if cut_size < cut_size_min or cut_size >= cut_size_max:
                                continue
                        else:
                            if cut_size != cut_size_min:
                                continue
                    cut_size_dimension_selector = cut_number
                    break
                else:
                    return None
        elif self.cut_size_number is not None:
            cut_size_dimension_number, possible_cut_size = var.cut_size_dimension
            if cut_size_dimension_number is None:
                # Number indexing on time dependent data doesn't make sense
                return None
            if self.cut_size_number >= len(possible_cut_size):
                return None
            cut_size_dimension_selector = self.wavelength_number
        else:
            if return_cut_size_dimension:
                check, possible_cut_size = var.cut_size_dimension
                if check is None:
                    possible_cut_size = None
            if return_cut_size_times:
                cut_size_times = var.cut_size_time

        data_values = var.values

        if time_selector is not None:
            assert time_values is not None
            if not np.any(time_selector):
                return None
            time_values = time_values[time_selector]
            data_values = data_values[time_selector]
            if return_cut_size_times and cut_size_times is not None:
                cut_size_times = cut_size_times[time_selector]

        data_selector = list()
        if cut_size_dimension_selector is not None:
            if cut_size_dimension_number >= len(data_selector):
                data_selector.extend([slice(None)] * (cut_size_dimension_number - len(data_selector) + 1))
            data_selector[cut_size_dimension_number] = cut_size_dimension_selector
            if return_cut_size_dimension and possible_cut_size is not None:
                possible_cut_size = possible_cut_size[cut_size_dimension_selector]
        if wavelength_dimension_selector is not None:
            if wavelength_dimension_number >= len(data_selector):
                data_selector.extend([slice(None)] * (wavelength_dimension_number - len(data_selector) + 1))
            data_selector[wavelength_dimension_number] = wavelength_dimension_selector
            if return_wavelength and possible_wavelengths is not None:
                possible_wavelengths = possible_wavelengths[wavelength_dimension_selector]
        if quantile_dimension_selector is not None:
            if quantile_dimension_number >= len(data_selector):
                data_selector.extend([slice(None)] * (quantile_dimension_number - len(data_selector) + 1))
            data_selector[quantile_dimension_number] = quantile_dimension_selector

        if self.dimension_at:
            for dimension, find_value in self.dimension_at:
                try:
                    dimension_number = var.variable.dimensions.index(dimension)
                    dimension_values = var.find_dimension_values(dimension)[:].data.tolist()
                    dimension_index = dimension_values.index(find_value)
                except (ValueError, KeyError):
                    return None
                if dimension_number >= len(data_selector):
                    data_selector.extend([slice(None)] * (dimension_number - len(data_selector) + 1))
                data_selector[dimension_number] = dimension_index

        if data_selector:
            data_values = data_values[tuple(data_selector)]

        if quantile_fraction is not None:
            q_lower = data_values[..., 0]
            q_upper = data_values[..., 1]
            data_values = (q_upper - q_lower) * quantile_fraction + q_lower

        result = [time_values, data_values]

        if return_wavelength:
            if possible_wavelengths is not None:
                result.append(possible_wavelengths)
            else:
                result.append(None)
        if return_cut_size_times:
            if cut_size_times is not None:
                result.append(cut_size_times)
            else:
                result.append(None)
        if return_cut_size_dimension:
            if possible_cut_size is not None:
                result.append(possible_cut_size)
            else:
                result.append(None)

        return tuple(result)


class RealtimeSelection(Selection):
    def __init__(
            self,
            acquisition_field: str,
            instrument_id: typing.Optional[str] = None,
            variable_name: typing.Optional[str] = None,
            variable_id: typing.Optional[str] = None,
            standard_name: typing.Optional[str] = None,
            instrument_code: typing.Optional[str] = None,
            require_tags: typing.Iterable[str] = None,
            exclude_tags: typing.Iterable[str] = None,
            station: typing.Optional[str] = None,
            wavelength: typing.Optional[typing.Union[float, typing.Sequence[float]]] = None,
            wavelength_number: typing.Optional[int] = None,
            cut_size: typing.Optional[typing.Union[float, typing.Sequence[float]]] = None,
            cut_size_number: typing.Optional[int] = None,
            variable_type: typing.Optional["Selection.VariableType"] = Selection.VariableType.Normal,
            dimension_at: typing.Optional[typing.Iterable[typing.Tuple[str, float]]] = None,
            acquisition_index: typing.Optional[typing.Union[str, int]] = None
    ):
        super().__init__(
            instrument_id=instrument_id,
            variable_name=variable_name,
            variable_id=variable_id,
            standard_name=standard_name,
            instrument_code=instrument_code,
            require_tags=require_tags,
            exclude_tags=exclude_tags,
            station=station,
            archive="raw",
            wavelength=wavelength,
            wavelength_number=wavelength_number,
            cut_size=cut_size,
            cut_size_number=cut_size_number,
            variable_type=variable_type,
            dimension_at=dimension_at,
        )
        self.acquisition_field = acquisition_field
        self.acquisition_index = acquisition_index

    def accept_realtime_source(self, instrument_id: str, tags: typing.Set[str], instrument_code: str) -> bool:
        if self.cut_size_number is not None:
            # Can't look this up on realtime
            return False

        if self.instrument_id:
            if self.instrument_id != instrument_id:
                return False
        if self.instrument_code:
            if self.instrument_code != instrument_code:
                return False
        if self.require_tags:
            if not self.require_tags.issubset(tags):
                return False
        if self.exclude_tags:
            if not self.exclude_tags.isdisjoint(tags):
                return False
        return True

    def accept_realtime_data(self, cut_size: typing.Optional[float]) -> bool:
        if self.variable_type and self.variable_type != self.VariableType.Normal:
            return False
        if self.cut_size:
            cut_size_min, cut_size_max = self.cut_size
            if cut_size_max is None:
                if cut_size is not None and cut_size < cut_size_min:
                    return False
            elif not isfinite(cut_size_max):
                if cut_size is None or cut_size < cut_size_min:
                    return False
            elif cut_size_min != cut_size_max:
                if cut_size is None:
                    return False
                if cut_size < cut_size_min or cut_size >= cut_size_max:
                    return False
            else:
                if cut_size is None:
                    return False
                if cut_size != cut_size_min:
                    return False
        return True

    def accept_realtime_message(self) -> bool:
        if self.variable_type and self.variable_type != self.VariableType.State:
            return False
        return False


class FileSequence:
    def __init__(
            self,
            selections: typing.Iterable[InstrumentSelection],
            request_station: str, request_archive: str,
            start_epoch_ms: int, end_epoch_ms: int
    ):
        self._indices: typing.Dict[FileSource, typing.List[InstrumentSelection]] = dict()
        for sel in selections:
            idx = sel.read_index(request_station, request_archive)
            if not idx:
                continue
            idx_sel = self._indices.get(idx)
            if not idx_sel:
                idx_sel = list()
                self._indices[idx] = idx_sel
            idx_sel.append(sel)

        self.start_epoch_ms = start_epoch_ms
        self.end_epoch_ms = end_epoch_ms

        year_start, year_end = containing_year_range(self.start_epoch_ms / 1000.0, self.end_epoch_ms / 1000.0)
        self._year_start = year_start
        self._year_end = year_end

        self._candidate_instruments: typing.Dict[typing.Tuple[int, FileSource], typing.Dict[str, typing.List[InstrumentSelection]]] = dict()

    def selection_index_to_instruments(self, index: ArchiveIndex,
                                       selection: InstrumentSelection) -> typing.Optional[typing.Set[str]]:
        return selection.index_to_instruments(index)

    async def acquire_locks(self, connection: Connection) -> None:
        for idx, selections in self._indices.items():
            await connection.lock_read(index_lock_key(idx.station, idx.archive), self.start_epoch_ms, self.end_epoch_ms)
            for year in range(self._year_start, self._year_end):
                try:
                    index_contents = await connection.read_bytes(
                        index_file_name(idx.station, idx.archive, start_of_year(year))
                    )
                except FileNotFoundError:
                    continue
                archive_index = ArchiveIndex(index_contents)
                for sel in selections:
                    sel_instruments = self.selection_index_to_instruments(archive_index, sel)
                    if not sel_instruments:
                        continue
                    dest_candidates = self._candidate_instruments.get((year, idx))
                    if not dest_candidates:
                        dest_candidates = dict()
                        self._candidate_instruments[(year, idx)] = dest_candidates
                    for instrument in sel_instruments:
                        dest_inst = dest_candidates.get(instrument)
                        if not dest_inst:
                            dest_inst = list()
                            dest_candidates[instrument] = dest_inst
                        dest_inst.append(sel)

        for src in set([idx[1] for idx in self._candidate_instruments.keys()]):
            await connection.lock_read(data_lock_key(src.station, src.archive), self.start_epoch_ms, self.end_epoch_ms)

    async def run(self, connection: Connection) -> "typing.AsyncIterator[typing.Tuple[int, typing.Dict[FileSource, typing.List[typing.Tuple[Dataset, typing.List[InstrumentSelection]]]]]]":
        year_file_sources: typing.Set[FileSource] = set()
        day_file_sources: typing.Set[FileSource] = set()
        for _, src in self._candidate_instruments.keys():
            if src.archive not in ("avgd", "avgm"):
                day_file_sources.add(src)
            else:
                year_file_sources.add(src)

        with TemporaryDirectory() as data_dir:
            for year in range(self._year_start, self._year_end):
                year_start = start_of_year(year)
                year_end = start_of_year(year+1)

                pending_result: typing.Dict[FileSource, typing.List[typing.Tuple[Dataset, typing.List[InstrumentSelection]]]] = dict()
                open_files: typing.List[typing.Tuple[NamedTemporaryFile, Dataset]] = list()

                async def integrate_instrument(src: FileSource, instrument_id: str, file_start: int,
                                               selections: typing.List[InstrumentSelection]):
                    dest_file = NamedTemporaryFile(suffix=".nc", dir=data_dir)
                    try:
                        await connection.read_file(
                            data_file_name(src.station, src.archive, instrument_id, file_start),
                            dest_file
                        )
                        dest_file.flush()
                        open_data = Dataset(dest_file.name, 'r')
                        try:
                            ctx = FileContext(instrument_id, open_data)
                            hit_selections: typing.List[InstrumentSelection] = list()
                            for sel in selections:
                                if not sel.accept_file(ctx):
                                    continue
                                hit_selections.append(sel)
                            if not hit_selections:
                                return

                            add_result = pending_result.get(src)
                            if not add_result:
                                add_result = list()
                                pending_result[src] = add_result
                            add_result.append((open_data, hit_selections))

                            open_files.append((dest_file, open_data))
                            dest_file = None
                            open_data = None
                        finally:
                            if open_data is not None:
                                open_data.close()
                    except FileNotFoundError:
                        return
                    finally:
                        if dest_file is not None:
                            dest_file.close()


                try:
                    for src in year_file_sources:
                        instrument_selections = self._candidate_instruments.get((year, src))
                        if not instrument_selections:
                            continue
                        for instrument, selections in instrument_selections.items():
                            await integrate_instrument(src, instrument, year_start, selections)

                    day_start_range = int(floor(self.start_epoch_ms / (24 * 60 * 60 * 1000))) * 24 * 60 * 60
                    day_start_range = max(day_start_range, year_start)
                    day_end_range = int(ceil(self.end_epoch_ms / (24 * 60 * 60 * 1000))) * 24 * 60 * 60
                    day_end_range = min(day_end_range, year_end)

                    pending_time = day_start_range * 1000

                    if day_file_sources:
                        for day_start in range(day_start_range, day_end_range, 24 * 60 * 60):
                            pending_time = day_start * 1000
                            for src in day_file_sources:
                                instrument_selections = self._candidate_instruments.get((year, src))
                                if not instrument_selections:
                                    continue
                                for instrument, selections in instrument_selections.items():
                                    await integrate_instrument(src, instrument, day_start, selections)

                            if pending_result:
                                yield pending_time, pending_result
                                pending_result.clear()
                            for f, d in open_files:
                                d.close()
                                f.close()
                            open_files.clear()

                    if pending_result:
                        yield pending_time, pending_result
                finally:
                    for f, d in open_files:
                        d.close()
                        f.close()
                    open_files.clear()
