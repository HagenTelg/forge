import typing
import numpy as np
from abc import ABC, abstractmethod
from math import nan, ceil
from netCDF4 import Dataset, Variable
from forge.const import MAX_I64


class SelectedVariable(ABC):
    @property
    def values(self) -> np.ndarray:
        raise NotImplementedError

    @values.setter
    def values(self, value: np.ndarray) -> None:
        self.values[...] = value

    def __getitem__(self, item):
        return self.values[item]

    def __setitem__(self, key, value):
        self.values[key] = value

    def __array__(self):
        return self.values

    def __str__(self) -> str:
        return self.variable.name

    @property
    def shape(self) -> typing.Tuple[int]:
        return self.values.shape

    @property
    def times(self) -> np.ndarray:
        raise NotImplementedError

    @property
    def average_weights(self) -> np.ndarray:
        raise NotImplementedError

    @property
    def parent(self) -> Dataset:
        raise NotImplementedError

    @property
    def variable(self) -> Variable:
        raise NotImplementedError

    @property
    def wavelengths(self) -> typing.List[float]:
        raise NotImplementedError

    @property
    def is_cut_split(self) -> bool:
        raise NotImplementedError

    @property
    def has_multiple_wavelengths(self) -> bool:
        raise NotImplementedError

    @property
    def has_changing_wavelengths(self) -> bool:
        raise NotImplementedError

    @property
    def standard_name(self) -> typing.Optional[str]:
        return getattr(self.variable, "standard_name", None)

    @standard_name.setter
    def standard_name(self, name: typing.Optional[str]) -> None:
        if not name:
            try:
                self.variable.delncattr("standard_name")
            except (AttributeError, RuntimeError):
                pass
            return
        self.variable.standard_name = name

    @abstractmethod
    def get_cut_size_index(self,
                           selector: typing.Optional[typing.Union[float, typing.Callable[[np.ndarray], np.ndarray]]],
                           preserve_dimensions: bool = None) -> typing.Tuple:
        pass

    @abstractmethod
    def get_wavelength_index(self, selector: typing.Union[float, typing.Callable[[np.ndarray], np.ndarray]],
                             preserve_dimensions: bool = None) -> typing.Tuple:
        pass

    @abstractmethod
    def select_cut_size(self) -> typing.Iterator[typing.Tuple[float, typing.Tuple, typing.Tuple]]:
        pass

    @abstractmethod
    def select_wavelengths(
            self,
            tail_index_only: bool = False,
    ) -> typing.Iterator[typing.Tuple[typing.List[float], typing.List[typing.Tuple], typing.Tuple]]:
        pass

    @abstractmethod
    def commit(self) -> None:
        pass

    def __enter__(self) -> "SelectedVariable":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if exc_type is not None:
            return
        self.commit()


class EmptySelectedVariable(SelectedVariable):
    def __init__(
            self,
            origin_times: np.ndarray,
            shape=None,
            wavelengths: typing.List[float] = None,
            dtype=np.float64,
    ):
        self._times = origin_times
        if shape is None:
            shape = (origin_times.shape[0], )
        if np.issubdtype(dtype, np.floating):
            self._values = np.full(shape, nan, dtype=dtype)
        else:
            self._values = np.full(shape, 0, dtype=dtype)
        self._averaged_weights: typing.Optional[np.ndarray] = None
        self._wavelengths = wavelengths or []
        self._root: typing.Optional[Dataset] = None

    @property
    def values(self) -> np.ndarray:
        return self._values

    @values.setter
    def values(self, value: np.ndarray) -> None:
        self.values[...] = value

    @property
    def times(self) -> np.ndarray:
        return self._times

    @property
    def average_weights(self) -> np.ndarray:
        if self._averaged_weights is None:
            self._averaged_weights = np.full(self.times.shape, 0, dtype=np.float64)
        return self._averaged_weights

    @property
    def parent(self) -> Dataset:
        if self._root is None:
            self._root = Dataset("/dev/null", 'w', format='NETCDF4', memory=1)
        return self._root

    @property
    def variable(self) -> Variable:
        var = self.parent.variables.get("empty_variable")
        if var is None:
            var = self.parent.createVariable("empty_variable", 'f8')
        return var

    def __str__(self) -> str:
        return "empty_variable"

    @property
    def wavelengths(self) -> typing.List[float]:
        return self._wavelengths

    @property
    def is_cut_split(self) -> bool:
        return False

    @property
    def has_multiple_wavelengths(self) -> bool:
        return len(self.wavelengths) > 1

    @property
    def has_changing_wavelengths(self) -> bool:
        return False

    def get_cut_size_index(self,
                           selector: typing.Optional[typing.Union[float, typing.Callable[[np.ndarray], np.ndarray]]],
                           preserve_dimensions: bool = None) -> typing.Tuple:
        return (False, )

    def get_wavelength_index(self, selector: typing.Union[float, typing.Callable[[np.ndarray], np.ndarray]],
                             preserve_dimensions: bool = None) -> typing.Tuple:
        if preserve_dimensions is None:
            if selector is None:
                preserve_dimensions = False
            elif isinstance(selector, (int, float)):
                preserve_dimensions = False
            else:
                preserve_dimensions = True
        if preserve_dimensions:
            return ..., [0]
        return ..., False

    def select_cut_size(self) -> typing.Iterator[typing.Tuple[float, typing.Tuple, typing.Tuple]]:
        return iter(())

    def select_wavelengths(
            self,
            tail_index_only: bool = False,
    ) -> typing.Iterator[typing.Tuple[typing.List[float], typing.List[typing.Tuple], typing.Tuple]]:
        return iter(())

    def commit(self) -> None:
        pass


class DataVariable(SelectedVariable):
    def __init__(self, source: Variable):
        self._variable = source
        if len(source.shape):
            self._values = source[...].data
        else:
            self._values = np.array(source[...], dtype=source.dtype)
        if "time" not in source.dimensions:
            self._times = np.array(0, dtype=np.int64)
        else:
            self._times = self._raw_times
            assert self.times.shape == (self.values.shape[0], )
        self._time_origin_indices: typing.Optional[np.ndarray] = None
        self._averaged_weights: typing.Optional[np.ndarray] = None

        self._ancillary_variables: typing.Optional[typing.Set[str]] = None
        self._wavelength_changes: typing.Optional[typing.List[typing.Tuple[int, np.ndarray]]] = None
        self._wavelengths: typing.Optional[typing.List[float]] = None

    @property
    def _raw_times(self) -> np.ndarray:
        source = self.parent
        while source is not None:
            var = source.variables.get("time")
            if var is not None:
                return var[...].data
            source = source.parent
        raise ValueError("time variable not available")

    @classmethod
    def from_data(cls, source: Variable) -> "DataVariable":
        return cls(source)

    @classmethod
    def aligned_with(cls, source: Variable, sibling: SelectedVariable) -> "DataVariable":
        v = cls(source)

        if len(sibling.times.shape) == 0:
            if v._is_constant:
                return v
            v._times = np.array(0, dtype=np.int64)
            if len(v._values.shape) > 1:
                v._values = v._values[-1]
            else:
                v._values = np.array(v._values[-1], dtype=v._values)
            return v

        if sibling.times.shape[0] == 0:
            v._times = np.empty((0,), dtype=v._times.dtype)
            v._values = np.empty((0, *v._values.shape[1:]), dtype=v._values.dtype)
            return v

        if v._is_constant:
            v._times = sibling.times
            v._values = np.full((v._times.shape[0], *v._values.shape), v._values)
            return v

        if sibling.parent == v.parent and isinstance(sibling, DataVariable):
            if sibling._time_origin_indices is None:
                begin_index, end_index = v._time_slice(v._times, int(sibling._times[0]), int(sibling._times[-1]))
                if begin_index == 0 and end_index == v._times.shape[0]:
                    return v
                v._times = sibling._times
                v._values = v._values[begin_index:end_index]
                return v
            v._times = sibling._times
            v._time_origin_indices = sibling._time_origin_indices
            v._values = v._values[v._time_origin_indices]
            return v

        from forge.data.merge.timealign import incoming_before
        indices = incoming_before(sibling.times, v._times)
        if indices.shape[0] == 1 or np.all(indices[1:] == indices[:-1] + 1):
            if indices[0] == 0 and indices[-1] + 1 == v._times.shape[0]:
                return v
            s = slice(int(indices[0]), int(indices[-1]) + 1)
            v._times = v._times[s]
            v._values = v._values[s]
            return v

        v._time_origin_indices = indices
        v._times = v._times[indices]
        v._values = v._values[indices]
        return v

    @classmethod
    def output_for(cls, output: Variable, for_variable: SelectedVariable) -> "DataVariable":
        return cls.aligned_with(output, for_variable)

    @property
    def _is_constant(self) -> bool:
        return len(self.times.shape) == 0

    @property
    def _is_empty(self) -> bool:
        return self.times.shape[0] == 0

    @staticmethod
    def _time_slice(times: np.ndarray, start_ms: int, end_ms: int) -> typing.Tuple[int, int]:
        if start_ms <= times[0]:
            begin_index = 0
        else:
            begin_index = int(np.searchsorted(times, start_ms, side='left'))
        if end_ms >= times[-1]:
            end_index = times.shape[0]
        else:
            end_index = int(np.searchsorted(times, end_ms, side='right'))
        return begin_index, end_index

    def restrict_times(self, start_ms: int, end_ms: int) -> None:
        if self._is_constant or self._is_empty:
            return

        begin_index, end_index = self._time_slice(self.times, start_ms, end_ms)
        if start_ms == 0 and end_index == self.times.shape[0]:
            return

        if begin_index >= self.times.shape[0] or end_index <= 0:
            self._times = np.empty((0, ), dtype=self._times.dtype)
            self._values = np.empty((0, *self._values.shape[1:]), dtype=self._values.dtype)
            self._time_origin_indices = None
            if self._averaged_weights is not None:
                self._averaged_weights = np.empty(0, dtype=self._averaged_weights.dtype)
            return

        self._times = self._times[begin_index:end_index]
        self._values = self._values[begin_index:end_index]
        if self._time_origin_indices is not None:
            self._time_origin_indices = self._time_origin_indices[begin_index:end_index]
        if self._averaged_weights is not None:
            self._averaged_weights = self._averaged_weights[begin_index:end_index]

    def commit(self) -> None:
        if "time" not in self.variable.dimensions:
            if self._is_constant:
                self.variable[:] = self._values
            else:
                if self._is_empty:
                    return
                self.variable[:] = self._values[-1]
            return

        if self._is_constant:
            self.variable[:] = self._values
            return

        if self._is_empty:
            return

        if self._time_origin_indices is None:
            if self._values.shape[0] == self.variable.shape[0]:
                self.variable[:] = self._values
                return
            begin_index, end_index = self._time_slice(self._raw_times, int(self._times[0]), int(self._times[-1]))
            self.variable[begin_index:end_index] = self._values
            return

        raw_times = self._raw_times
        begin_index, end_index = self._time_slice(raw_times, int(self._times[0]), int(self._times[-1]))

        from forge.data.merge.timealign import incoming_before
        indices = incoming_before(raw_times[begin_index:end_index], self._times)

        self.variable[begin_index:end_index] = self._values[indices]

    @property
    def values(self) -> np.ndarray:
        return self._values

    @values.setter
    def values(self, value: np.ndarray) -> None:
        self._values[...] = value

    @property
    def times(self) -> np.ndarray:
        return self._times

    def _make_coverage_weight(self) -> np.ndarray:
        if self._is_constant:
            return np.array(1.0, dtype=np.float64)
        if self._is_empty:
            return np.empty(0, dtype=np.float64)

        source_var = self.parent.variables.get("averaged_time")
        if source_var is None or "time" not in source_var.dimensions:
            return np.full(self.times.shape[0], 1.0, dtype=np.float64)

        from forge.processing.average.calculate import fixed_interval_coverage_weight
        from forge.timeparse import parse_iso8601_duration

        root = self.parent
        while True:
            n = root.parent
            if n is None:
                break
            root = n

        time_coverage_resolution = getattr(root, "time_coverage_resolution", None)
        if time_coverage_resolution is not None:
            time_coverage_resolution = int(round(parse_iso8601_duration(str(time_coverage_resolution)) * 1000))

        if self._time_origin_indices is None:
            selection = slice(*self._time_slice(self._raw_times, int(self.times[0]), int(self.times[-1])))
            return fixed_interval_coverage_weight(
                self.times,
                source_var[selection].data,
                time_coverage_resolution
            )

        weights = fixed_interval_coverage_weight(
            self._raw_times,
            source_var[...].data,
            time_coverage_resolution,
        )
        return weights[self._time_origin_indices]

    @property
    def average_weights(self) -> np.ndarray:
        if self._averaged_weights is None:
            self._averaged_weights = self._make_coverage_weight()
        return self._averaged_weights

    @property
    def parent(self) -> Dataset:
        return self._variable.group()

    @property
    def variable(self) -> Variable:
        return self._variable

    @property
    def ancillary_variables(self) -> typing.Set[str]:
        if self._ancillary_variables is None:
            self._ancillary_variables = set(getattr(self.variable, "ancillary_variables", "").split())
        return self._ancillary_variables

    @property
    def wavelengths(self) -> typing.List[float]:
        if self._wavelengths is None:
            wl = self._wavelength_history
            if not wl:
                self._wavelengths = []
            else:
                raw = self._wavelength_history[-1][1]
                self._wavelengths = [float(f) for f in raw[np.isfinite(raw)]]
        return self._wavelengths

    @property
    def _cut_size_data(self) -> np.ndarray:
        return self.parent.variables["cut_size"][...].data

    @property
    def is_cut_split(self) -> bool:
        if "cut_size" in self.variable.dimensions:
            return True
        if "cut_size" in self.ancillary_variables and self.parent.variables.get("cut_size") is not None:
            return True
        return False

    @property
    def _wavelength_variable(self) -> Variable:
        return self.parent.variables["wavelength"]

    @property
    def _wavelength_history(self) -> typing.List[typing.Tuple[int, np.ndarray]]:
        if self._wavelength_changes is None:
            self._wavelength_changes = list()
            if "wavelength" in self.variable.dimensions or "wavelength" in self.ancillary_variables:
                from forge.timeparse import parse_iso8601_time

                var = self._wavelength_variable
                start_time = -MAX_I64
                history = getattr(var, "change_history", None)
                if history:
                    history = history.strip()
                    for change in history.split("\n"):
                        end_time, *values = change.split(',')
                        end_time = int(ceil(parse_iso8601_time(str(end_time)).timestamp() * 1000.0))

                        def parse_wl(wl: str) -> float:
                            if not wl:
                                return nan
                            return float(wl)

                        if not var.shape:
                            if len(values) != 1:
                                raise ValueError("invalid change history")
                            add = np.array([parse_wl(values[0])], dtype=np.float64)
                        else:
                            if var.shape != (len(values),):
                                raise ValueError("invalid change history")
                            add = np.array([parse_wl(v) for v in values], dtype=np.float64)

                        self._wavelength_changes.append((start_time, add))
                        start_time = end_time

                self._wavelength_changes.append((start_time, var[...].data))
        return self._wavelength_changes

    @property
    def has_multiple_wavelengths(self) -> bool:
        h = self._wavelength_history
        if not h:
            return False
        return h[0][1].shape[0] > 1

    @property
    def has_changing_wavelengths(self) -> bool:
        h = self._wavelength_history
        if not h:
            return False
        if len(h) > 1:
            return True
        return not np.all(np.isfinite(h[0][1]))

    @staticmethod
    def _apply_index(
            values: np.ndarray,
            selector: typing.Optional[typing.Union[float, typing.Callable[[np.ndarray], np.ndarray]]],
            preserve_dimensions: typing.Optional[bool],
    ) -> typing.Union[int, np.ndarray]:
        if selector is None:
            selected = np.invert(np.isfinite(values))
            if preserve_dimensions is None:
                preserve_dimensions = False
        elif isinstance(selector, (int, float)):
            selected = values == float(selector)
            if preserve_dimensions is None:
                preserve_dimensions = False
        else:
            selected = selector(values)
            if preserve_dimensions is None:
                preserve_dimensions = True

        if preserve_dimensions or not selected.shape:
            return selected
        reduced = np.where(selected)[0]
        if reduced.shape[0] == 1:
            return int(reduced[0])
        return reduced

    def get_cut_size_index(self,
                           selector: typing.Optional[typing.Union[float, typing.Callable[[np.ndarray], np.ndarray]]],
                           preserve_dimensions: bool = None) -> typing.Tuple:
        if "cut_size" in self.variable.dimensions:
            selected_data = self._apply_index(self._cut_size_data, selector, preserve_dimensions)
            dimension_number = self.variable.dimensions.index("cut_size")
            return tuple(([slice(None)] * dimension_number) + [selected_data])
        elif "cut_size" in self.ancillary_variables:
            selected_data = self._apply_index(self._cut_size_data, selector, preserve_dimensions)
            return (selected_data, )
        else:
            selected_data = self._apply_index(np.array(nan), selector, preserve_dimensions)
            if bool(selected_data):
                return (slice(None),)
            return (False,)

    def get_wavelength_index(self, selector: typing.Union[float, typing.Callable[[np.ndarray], np.ndarray]],
                             preserve_dimensions: bool = None) -> typing.Tuple:
        history = self._wavelength_history
        if not history:
            selected_data = self._apply_index(np.array(nan), selector, preserve_dimensions)
            if bool(selected_data):
                return (slice(None),)
            return (False,)

        wavelengths = history[-1][1]
        if "wavelength" in self.variable.dimensions:
            selected_data = self._apply_index(wavelengths, selector, preserve_dimensions)
            dimension_number = self.variable.dimensions.index("wavelength")
            from_end = (len(self.variable.dimensions) - 1) - dimension_number
            return tuple([...] + ([slice(None)] * from_end) + [selected_data])
        elif "wavelength" in self.ancillary_variables:
            if np.any(self._apply_index(wavelengths, selector, True)):
                return (...,)
            return (False,)
        else:
            if np.any(self._apply_index(np.array(nan), selector, True)):
                return (...,)
            return (False,)

    def select_cut_size(self) -> typing.Iterator[typing.Tuple[float, typing.Tuple, typing.Tuple]]:
        if "cut_size" in self.variable.dimensions:
            size_data = self._cut_size_data
            dimension_number = self.variable.dimensions.index("cut_size")
            head_idx = [slice(None)] * dimension_number
            for cut_idx in range(size_data.shape[0]):
                yield float(size_data[cut_idx]), tuple(head_idx + [cut_idx]), (..., )
        elif "cut_size" in self.ancillary_variables:
            size_data = self._cut_size_data
            finite_cut_sizes = np.isfinite(size_data)
            unique_cut_sizes = np.unique(size_data[finite_cut_sizes])
            for cut_size in unique_cut_sizes:
                selector = size_data == cut_size
                yield float(cut_size), (selector, ), (selector, )
            whole_air = np.invert(finite_cut_sizes)
            if np.any(whole_air):
                yield nan, (whole_air,), (whole_air,)
        else:
            if self._is_constant:
                yield nan, (...,), (...,)
            yield nan, (slice(None),), (...,)

    def select_wavelengths(
            self,
            tail_index_only: bool = False,
    ) -> typing.Iterator[typing.Tuple[typing.List[float], typing.List[typing.Tuple], typing.Tuple]]:
        history = self._wavelength_history

        if "wavelength" in self.variable.dimensions:
            dimension_number = self.variable.dimensions.index("wavelength")
            from_end = (len(self.variable.dimensions) - 1) - dimension_number
            is_constant = self._is_constant

            def assemble_wavelength_index(time_selector, wavelength_idx):
                tail_idx = [...] + ([slice(None)] * from_end) + [wavelength_idx]
                if tail_index_only or is_constant:
                    return tuple(tail_idx)
                return tuple([time_selector] + tail_idx)
        else:
            def assemble_wavelength_index(time_selector, _):
                if tail_index_only or is_constant:
                    return (...,)
                return (time_selector,)

        for idx_history in range(len(history)):
            start_ms, wavelengths = history[idx_history]
            if idx_history+1 < len(history):
                end_ms = history[idx_history+1][0]
            else:
                end_ms = MAX_I64

            valid_wavelengths = np.isfinite(wavelengths)
            sorted_wavelengths, idx_wavelength = np.unique(wavelengths[valid_wavelengths], return_index=True)
            if sorted_wavelengths.shape[0] == 0:
                continue
            idx_wavelength = np.where(valid_wavelengths)[0][idx_wavelength]

            begin_index, end_index = self._time_slice(self.times, start_ms, end_ms)
            time_selector = slice(begin_index, end_index)

            wavelength_selectors: typing.List[typing.Tuple] = list()
            for idx in idx_wavelength:
                wavelength_selectors.append(assemble_wavelength_index(time_selector, idx))

            yield sorted_wavelengths.tolist(), wavelength_selectors, (time_selector, )
