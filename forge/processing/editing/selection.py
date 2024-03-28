import typing
import logging
import numpy as np
from netCDF4 import Dataset, Variable

_LOGGER = logging.getLogger(__name__)


_NEVER_MATCH_VARIABLES = frozenset({
    "time",
    "cut_size",
    "wavelength",
    "averaged_time",
    "averaged_count",
    "system_flags",
})


def ignore_variable(var: Variable) -> bool:
    if len(var.dimensions) < 1:
        return False
    if var.dimensions[0] != 'time':
        return False
    return var.name in _NEVER_MATCH_VARIABLES


class Selection:
    class _Matcher:
        def __init__(self, selection: typing.Dict[str, typing.Any]):
            self.instrument_id: typing.Optional[str] = selection.get("instrument_id", None)
            self.instrument: typing.Optional[str] = selection.get("instrument", None)

            self.require_tags: typing.Set[str] = set()
            self.exclude_tags: typing.Set[str] = set()
            tags: typing.Optional[typing.Union[str, typing.List[str]]] = selection.get("require_tags", None)
            if tags:
                if isinstance(tags, str):
                    tags = tags.split()
                self.require_tags.update(tags)
            tags: typing.Optional[typing.Union[str, typing.List[str]]] = selection.get("exclude_tags", None)
            if tags:
                if isinstance(tags, str):
                    tags = tags.split()
                self.exclude_tags.update(tags)

            self.variable_id: typing.Optional[str] = selection.get("variable_id", None)
            self.variable_id_complete: bool = True
            if self.variable_id:
                self.variable_id = str(self.variable_id)
                if '_' not in self.variable_id:
                    self.variable_id_complete = False
            else:
                self.variable_id = None

            self.variable_name: typing.Optional[str] = selection.get("variable_name", None)
            self.standard_name: typing.Optional[str] = selection.get("standard_name", None)

            self.wavelength: typing.Optional[float] = selection.get("wavelength", None)
            if self.wavelength is not None:
                self.wavelength = float(self.wavelength)

        def matches_file(self, root: Dataset) -> bool:
            if self.instrument_id is not None:
                check = getattr(root, 'instrument_id', None)
                if check is None or check != self.instrument_id:
                    return False

            if self.instrument is not None:
                check = getattr(root, 'instrument', None)
                if check is None or check != self.instrument:
                    return False

            if self.require_tags or self.exclude_tags:
                check = getattr(root, 'forge_tags', None)
                if not check:
                    if self.require_tags:
                        return False
                else:
                    check = set(check.split())
                    if not self.require_tags.issubset(check):
                        return False
                    if not self.exclude_tags.isdisjoint(check):
                        return False

            return True

        def wavelength_index(self, data: Dataset, var: Variable) -> typing.Optional[typing.Iterable[int]]:
            if 'wavelength' in var.dimensions:
                return np.where(data.variables['wavelength'][:].data == self.wavelength)[0]
            else:
                return None

        def matches_variable(self, root: Dataset, data: Dataset, var: Variable) -> bool:
            if self.variable_id is not None:
                check = getattr(var, 'variable_id', None)
                if check is None:
                    return False
                if not self.variable_id_complete:
                    if '_' not in check:
                        if check != self.variable_id:
                            return False
                    else:
                        instrument_id = getattr(root, 'instrument_id', None)
                        if instrument_id is None:
                            return False
                        if self.variable_id + '_' + instrument_id != check:
                            return False
                else:
                    if '_' not in check:
                        instrument_id = getattr(root, 'instrument_id', None)
                        if instrument_id is None:
                            return False
                        if check + '_' + instrument_id != self.variable_id:
                            return False
                    else:
                        if check != self.variable_id:
                            return False

            if self.variable_name is not None:
                if var.name != self.variable_name:
                    return False

            if self.standard_name is not None:
                check = getattr(var, 'standard_name', None)
                if check is None or check != self.standard_name:
                    return False

            if self.wavelength is not None:
                if 'wavelength' in var.dimensions:
                    wavelength = data.variables.get('wavelength')
                    if wavelength is None:
                        return False
                    if not bool(np.isin(self.wavelength, wavelength[:].data, assume_unique=True)):
                        return False
                elif 'wavelength' in getattr(var, 'ancillary_variables', "").split():
                    wavelength = data.variables.get('wavelength')
                    if len(wavelength.shape) != 0:
                        return False
                    if self.wavelength != float(wavelength[:].data):
                        return False
                else:
                    return False

            return True

        def matches_data(self, root: Dataset, data: Dataset) -> bool:
            for var in data.variables.values():
                if ignore_variable(var):
                    continue
                if self.matches_variable(root, data, var):
                    return True
            return False

        def filter_data(self, root: Dataset, data: Dataset) -> bool:
            return self.matches_file(root) and self.matches_data(root, data)

    def __init__(self, parameters: typing.List[typing.Dict[str, typing.Any]]) -> None:
        self._matchers: typing.List["Selection._Matcher"] = list()
        if not isinstance(parameters, list):
            return
        for m in parameters:
            self._matchers.append(self._Matcher(m))

    def filter_data(self, root: Dataset, data: Dataset) -> bool:
        for check in self._matchers:
            if check.filter_data(root, data):
                return True
        return False

    def select_data(self, root: Dataset, data: Dataset) -> typing.Iterator[typing.Tuple[Variable, typing.Tuple]]:
        for var in data.variables.values():
            if ignore_variable(var):
                continue

            selected_wavelengths: typing.Optional[typing.Set[int]] = None
            for m in self._matchers:
                if not m.matches_variable(root, data, var):
                    continue
                if m.wavelength is None:
                    yield var, ()
                    break
                wl_idx = m.wavelength_index(data, var)
                if wl_idx is None:
                    yield var, ()
                    break
                if selected_wavelengths is None:
                    selected_wavelengths = set(wl_idx)
                else:
                    selected_wavelengths.update(wl_idx)
            else:
                if selected_wavelengths is not None:
                    dim_idx = var.dimensions.index('wavelength')
                    if dim_idx == len(var.dimensions) - 1:
                        yield var, (..., np.array(sorted(selected_wavelengths)))
                    else:
                        yield var, tuple([slice(None)] * (dim_idx - 2) + [np.array(sorted(selected_wavelengths))])

    def select_single(self, root: Dataset, data: Dataset) -> typing.Iterator[typing.Tuple[Variable, typing.Tuple]]:
        def _yield_all() -> typing.Iterator[typing.Tuple[Variable, typing.Tuple]]:
            if len(var.shape) <= 1:
                yield var, ()
                return
            for idx in np.ndindex(*var.shape[1:]):
                yield var, idx

        def _yield_wavelengths(indices: typing.List[int]) -> typing.Iterator[typing.Tuple[Variable, typing.Tuple]]:
            if len(var.shape) <= 2:
                for idx in indices:
                    yield var, (idx,)
                return

            dim_idx = var.dimensions.index('wavelength')
            dim_idx -= 1
            effective_shape = list(var.shape[1:])
            effective_shape[dim_idx] = 1
            for var_idx in np.ndindex(*effective_shape):
                for wl_idx in indices:
                    effective_idx = list(var_idx)
                    effective_idx[dim_idx] = wl_idx
                    yield var, tuple(effective_idx)

        for var in data.variables.values():
            if ignore_variable(var):
                continue

            selected_wavelengths: typing.Optional[typing.Set[int]] = None
            for m in self._matchers:
                if not m.matches_variable(root, data, var):
                    continue
                if m.wavelength is None:
                    yield from _yield_all()
                    break
                wl_idx = m.wavelength_index(data, var)
                if wl_idx is None:
                    yield from _yield_all()
                    break
                if selected_wavelengths is None:
                    selected_wavelengths = set(wl_idx)
                else:
                    selected_wavelengths.update(wl_idx)
            else:
                if selected_wavelengths is not None:
                    yield from _yield_wavelengths(sorted(selected_wavelengths))
