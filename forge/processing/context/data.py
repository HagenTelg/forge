import typing
import numpy as np
import forge.data.structure.variable as netcdf_var
from abc import ABC, abstractmethod
from math import nan
from netCDF4 import Dataset, Variable
from forge.const import MAX_I64
from forge.data.structure.history import append_history
from forge.data.structure.timeseries import time_coordinate, cutsize_variable, cutsize_coordinate, variable_coordinates
from .variable import SelectedVariable, EmptySelectedVariable, DataVariable
from .selection import VariableSelection


class SelectedData(ABC):
    @property
    def placeholder(self) -> bool:
        raise NotImplementedError

    @property
    def root(self) -> Dataset:
        raise NotImplementedError

    @property
    def times(self) -> np.ndarray:
        raise NotImplementedError

    @classmethod
    def from_file(cls, data: Dataset) -> "SelectedData":
        return FileData(data)

    @classmethod
    def empty_placeholder(cls) -> "SelectedData":
        return EmptySelectedData()

    @classmethod
    def ensure_data(cls, data: typing.Union[Dataset, "SelectedData"]) -> "SelectedData":
        if isinstance(data, SelectedData):
            return data
        return cls.from_file(data)

    @property
    def station(self) -> str:
        station_name = self.root.variables.get("station_name")
        if station_name is not None:
            return str(station_name[0])
        return "NIL"

    @abstractmethod
    def restrict_times(self, start_ms: int, end_ms: int) -> None:
        pass

    def append_history(self, component: str) -> None:
        append_history(self.root, component)

    def set_wavelengths(self, wavelengths: "typing.Union[typing.List[float], typing.Tuple[float, ...], np.ndarray]") -> None:
        data_group = self.root.groups.get("data")
        if data_group is None:
            data_group = self.root.createGroup("data")

        wavelengths = np.array(wavelengths, copy=False, dtype=np.float64)
        assert len(wavelengths.shape) == 1
        assert wavelengths.shape[0] > 0

        wavelengths_dim = data_group.dimensions.get("wavelength")
        if wavelengths_dim is None:
            wavelengths_dim = data_group.createDimension("wavelength", wavelengths.shape[0])
        else:
            if wavelengths_dim.size != wavelengths.shape[0]:
                raise ValueError(f"cannot change the number of wavelengths from {wavelengths_dim.size} to {wavelengths.shape[0]}")

        wavelengths_var = data_group.variables.get("wavelength")
        if wavelengths_var is None:
            wavelengths_var = data_group.createVariable("wavelength", 'f8', (wavelengths_dim.name, ), fill_value=nan)
            netcdf_var.variable_wavelength(wavelengths_var)
            wavelengths_var.coverage_content_type = "coordinate"
        else:
            try:
                wavelengths_var.delncattr("change_history")
            except (AttributeError, RuntimeError):
                pass

        if wavelengths_var.shape != wavelengths.shape:
            raise ValueError(f"cannot change the number of wavelengths from {wavelengths_var.shape} to {wavelengths.shape}")

        wavelengths_var[:] = wavelengths

    @abstractmethod
    def select_variable(
            self,
            variable: typing.Union[typing.Dict[str, typing.Any], str, VariableSelection, typing.Iterable],
            *auxiliary: typing.Union[typing.Dict[str, typing.Any], str, VariableSelection, typing.Iterable],
            always_tuple: bool = False,
            commit_variable: bool = True,
            commit_auxiliary: bool = False,
    ) -> "typing.Iterator[typing.Union[SelectedVariable, typing.Tuple[SelectedVariable, ...]]]":
        pass

    @abstractmethod
    def get_input(
            self,
            for_variable: SelectedVariable,
            selection: typing.Union[typing.Dict[str, typing.Any], str, VariableSelection, typing.Iterable],
            error_when_missing: bool = True
    ) -> SelectedVariable:
        pass

    class OutputContext:
        def __init__(self, var: SelectedVariable):
            self._var: typing.Optional[SelectedVariable] = var

        def __enter__(self) -> SelectedVariable:
            if self._var is None:
                raise RuntimeError("variable already taken from output")
            return self._var

        def __exit__(self, exc_type, exc_val, exc_tb) -> None:
            if exc_type is not None:
                return
            if self._var is not None:
                self._var.commit()

        @property
        def variable(self) -> SelectedVariable:
            if self._var is None:
                raise RuntimeError("variable already taken from output")
            return self._var

        def take(self) -> SelectedVariable:
            if self._var is None:
                raise RuntimeError("variable already taken from output")
            var = self._var
            self._var = None
            return var

    @abstractmethod
    def get_output(
            self,
            for_variable: SelectedVariable,
            name: str,
            error_when_duplicate: bool = False,
            wavelength: bool = False,
            dimensions: "typing.Optional[typing.Tuple[str, ...]]" = None,
            dtype=np.float64,
    ) -> "SelectedData.OutputContext":
        pass


class EmptySelectedData(SelectedData):
    def __init__(self):
        self._root: typing.Optional[Dataset] = None

    @property
    def placeholder(self) -> bool:
        return True

    @property
    def times(self) -> np.ndarray:
        return np.empty(0, dtype=np.int64)

    @property
    def root(self) -> Dataset:
        if self._root is None:
            self._root = Dataset("/dev/null", 'w', format='NETCDF4', memory=1)
        return self._root

    @property
    def station(self) -> str:
        raise "NIL"

    def restrict_times(self, start_ms: int, end_ms: int) -> None:
        pass

    def append_history(self, component: str) -> None:
        pass

    def select_variable(
            self,
            variable: typing.Union[typing.Dict[str, typing.Any], str, VariableSelection, typing.Iterable],
            *auxiliary: typing.Union[typing.Dict[str, typing.Any], str, VariableSelection, typing.Iterable],
            always_tuple: bool = False,
            commit_variable: bool = True,
            commit_auxiliary: bool = False,
    ) -> "typing.Iterator[typing.Union[SelectedVariable, typing.Tuple[SelectedVariable, ...]]]":
        return iter(())

    def get_input(
            self,
            for_variable: SelectedVariable,
            selection: typing.Union[typing.Dict[str, typing.Any], str, VariableSelection, typing.Iterable],
            error_when_missing: bool = True
    ) -> SelectedVariable:
        if error_when_missing:
            raise FileNotFoundError("no inputs available for missing files")
        return EmptySelectedVariable(for_variable.times)

    def get_output(
            self,
            for_variable: SelectedVariable,
            name: str,
            error_when_duplicate: bool = False,
            wavelength: bool = False,
            dimensions: "typing.Optional[typing.Tuple[str, ...]]" = None,
            dtype=np.float64,
    ) -> SelectedData.OutputContext:
        shape = None
        if wavelength:
            if dimensions is None:
                dimensions = ("wavelength", )
            else:
                dimensions = tuple([*dimensions, "wavelength"])
        if dimensions is not None:
            def dimension_size(name: str, root: Dataset) -> int:
                check_dim = root.dimensions.get(name)
                if check_dim is not None:
                    return check_dim.size
                root = root.parent
                if root is None:
                    return 1
                return dimension_size(name, root)

            shape = tuple([for_variable.times.shape[0]] + [
                dimension_size(name, for_variable.parent) for name in dimensions
            ])

        return self.OutputContext(EmptySelectedVariable(
            for_variable.times, shape,
            wavelengths=for_variable.wavelengths if wavelength else None,
            dtype=dtype,
        ))


class FileData(SelectedData):
    def __init__(self, root: Dataset):
        self._root = root
        self._time_start = -MAX_I64
        self._time_end = MAX_I64
        self._times: typing.Optional[np.ndarray] = None

    @property
    def placeholder(self) -> bool:
        return False

    @property
    def root(self) -> Dataset:
        return self._root

    @property
    def times(self) -> np.ndarray:
        if self._times is None:
            def find_time_variable():
                g = self.root.groups.get("data")
                if g is not None:
                    v = g.variables.get("time")
                    if v is not None:
                        return v[:]
                g = self.root.groups.get("upstream")
                if g is not None:
                    v = g.variables.get("time")
                    if v is not None:
                        return v[:]
                return None

            raw = find_time_variable()
            if raw is None or raw.shape[0] == 0:
                self._times = np.empty(0, dtype=np.int64)
            else:
                first = np.searchsorted(raw, self._time_start, side='left')
                last = np.searchsorted(raw, self._time_end, side='right')
                self._times = raw[first:last]

        return self._times

    def restrict_times(self, start_ms: int, end_ms: int) -> None:
        self._time_start = max(self._time_start, start_ms)
        self._time_end = min(self._time_end, end_ms)

    def _find_variable(
            self,
            matcher: typing.Callable[[Variable], bool],
    ) -> typing.Iterator[Variable]:
        def walk_root(root: Dataset):
            for var in list(root.variables.values()):
                if matcher(var):
                    yield var
            for g in list(root.groups.values()):
                yield from walk_root(g)

        yield from walk_root(self.root)

    def select_variable(
            self,
            variable: typing.Union[typing.Dict[str, typing.Any], str, VariableSelection, typing.Iterable],
            *auxiliary: typing.Union[typing.Dict[str, typing.Any], str, VariableSelection, typing.Iterable],
            always_tuple: bool = False,
            commit_variable: bool = True,
            commit_auxiliary: bool = False,
    ) -> "typing.Iterator[typing.Union[SelectedVariable, typing.Tuple[SelectedVariable, ...]]]":
        aux_matchers: typing.List[typing.Callable[[Variable], bool]] = list()
        for aux in auxiliary:
            aux_matchers.append(VariableSelection.matcher(aux))

        for matched_variable in self._find_variable(VariableSelection.matcher(variable)):
            matched_variable = DataVariable.from_data(matched_variable)
            matched_variable.restrict_times(self._time_start, self._time_end)
            if len(matched_variable.times) == 0 or matched_variable.times.shape[0] == 0:
                continue

            matched_aux: typing.List[SelectedVariable] = list()
            for aux in aux_matchers:
                try:
                    hit = next(self._find_variable(aux))
                    matched_aux.append(DataVariable.aligned_with(hit, matched_variable))
                except StopIteration:
                    matched_aux.append(EmptySelectedVariable(matched_variable.times))

            try:
                if not matched_aux and not always_tuple:
                    yield matched_variable
                else:
                    yield matched_variable, *matched_aux
            finally:
                if commit_variable:
                    matched_variable.commit()
                if commit_auxiliary:
                    for aux in matched_aux:
                        aux.commit()

    def get_input(
            self,
            for_variable: SelectedVariable,
            selection: typing.Union[typing.Dict[str, typing.Any], str, VariableSelection, typing.Iterable],
            error_when_missing: bool = True
    ) -> SelectedVariable:
        try:
            hit = next(self._find_variable(VariableSelection.matcher(selection)))
        except StopIteration:
            if error_when_missing:
                raise FileNotFoundError("requested variable not found")
            return EmptySelectedVariable(for_variable.times)

        return DataVariable.aligned_with(hit, for_variable)

    def get_output(
            self,
            for_variable: SelectedVariable,
            name: str,
            error_when_duplicate: bool = False,
            wavelength: bool = False,
            dimensions: "typing.Optional[typing.Tuple[str, ...]]" = None,
            dtype=np.float64,
    ) -> SelectedData.OutputContext:
        def is_same_file() -> bool:
            if isinstance(for_variable, EmptySelectedVariable):
                return False
            for_root = for_variable.parent
            while True:
                if for_root == self.root:
                    return True
                upper = for_root.parent
                if upper is None:
                    break
                for_root = upper
            return False

        def for_time_variable() -> Variable:
            for_root = for_variable.parent
            while for_root is not None:
                var = for_root.variables.get("time")
                if var is not None:
                    return var
                for_root = for_root.parent
            raise ValueError("time variable not available")

        is_constant = len(for_variable.times.shape) == 0

        if is_same_file():
            destination = for_variable.parent
        else:
            direct_time_mapping = False
            destination = self.root.groups.get("data")
            if destination is None:
                destination = self.root.createGroup("data")

            if not is_constant:
                time_var = destination.variables.get("time")
                if time_var is None:
                    time_var = time_coordinate(destination)
                    time_var[:] = for_time_variable()[:]
                    direct_time_mapping = True

            def setup_cut_size():
                if not for_variable.is_cut_split:
                    return
                if "cut_size" in destination.variables:
                    return

                source_cut = for_variable.parent.variables["cut_size"][:].data

                if "cut_size" in for_variable.variable.dimensions:
                    size_var = cutsize_coordinate(destination, source_cut.shape[0])
                    size_var[:] = source_cut[:]
                    return

                size_var = cutsize_variable(destination)
                if direct_time_mapping:
                    size_var[:] = source_cut[:]
                    return

                from forge.data.merge.timealign import incoming_before
                size_idx = incoming_before(destination.variables["time"][:].data, for_time_variable()[:].data)
                size_var[:] = source_cut[size_idx]

            setup_cut_size()

        var = destination.variables.get(name, None)
        if var is not None:
            if error_when_duplicate:
                raise FileExistsError(f"variable {name} already exists")
        else:
            if is_constant:
                dims = list(dimensions or ())
            else:
                dims = ['time'] + list(dimensions or ())
            if wavelength:
                dims.append("wavelength")
            if for_variable.is_cut_split and "cut_size" in for_variable.variable.dimensions:
                dims.insert(1, "cut_size")
            if np.issubdtype(dtype, np.floating):
                fill_value = nan
            else:
                fill_value = False
            var = destination.createVariable(name, dtype, tuple(dims), fill_value=fill_value)
            if for_variable.is_cut_split and "cut_size" not in for_variable.variable.dimensions:
                var.ancillary_variables = "cut_size"
            variable_coordinates(destination, var)

        var = DataVariable.output_for(var, for_variable)
        var.restrict_times(self._time_start, self._time_end)
        return self.OutputContext(var)
