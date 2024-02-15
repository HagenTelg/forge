import typing
import logging
import netCDF4
import numpy as np
from forge.data.structure.timeseries import time_coordinate
from .timealign import peer_output_time, incoming_before
from ..state import is_state_group
from ..attrs import copy as copy_attrs
from ..values import create_and_copy_variable

if typing.TYPE_CHECKING:
    from pathlib import Path

_LOGGER = logging.getLogger(__name__)


def _dimension_exists(group: netCDF4.Dataset, name: str) -> bool:
    while group is not None:
        if name in group.dimensions:
            return True
        group = group.parent
    return False


def _copy_dimension(name: str, source_group: netCDF4.Dataset, destination_group: netCDF4.Dataset) -> None:
    source_dimension = None
    while True:
        source_dimension = source_group.dimensions.get(name)
        if source_dimension is not None:
            break
        source_group = source_group.parent
        destination_group = destination_group.parent

    if source_dimension.isunlimited():
        destination_group.createDimension(name, None)
    else:
        destination_group.createDimension(name, source_dimension.size)

    source_dim_var = source_group.variables.get(name)
    if source_dim_var is not None and name not in destination_group.variables:
        create_and_copy_variable(source_dim_var, destination_group)


def _find_time_dimension(root: netCDF4.Dataset) -> typing.Tuple[netCDF4.Dimension, netCDF4.Variable]:
    while True:
        check_dim = root.dimensions.get('time')
        if check_dim is None:
            root = root.parent
            assert root is not None
            continue
        time_var = root.variables['time']
        assert len(time_var.dimensions) == 1
        assert time_var.dimensions[0] == 'time'
        return check_dim, time_var


class _CopyGroupAttributes:
    def __init__(self, group: netCDF4.Dataset):
        self.group = group

    def __call__(self, destination: netCDF4.Dataset):
        copy_attrs(self.group, destination)


class _CopyVariable:
    def __init__(self, variable: netCDF4.Variable):
        self.variable = variable

    def __call__(self, destination: netCDF4.Dataset) -> None:
        if self.variable.name in destination.variables:
            return
        for check_dim in self.variable.dimensions:
            if _dimension_exists(destination, check_dim):
                continue
            _copy_dimension(check_dim, self.variable.group(), destination)
        create_and_copy_variable(self.variable, destination)


class _DataVariable:
    def __init__(self, variable: netCDF4.Variable, time_source: netCDF4.Variable):
        self.variable = variable
        self.time_source = time_source

    def __call__(self, destination: netCDF4.Dataset, alignment: np.ndarray) -> None:
        for check_dim in self.variable.dimensions:
            if _dimension_exists(destination, check_dim):
                continue
            _copy_dimension(check_dim, self.variable.group(), destination)
        output_var = create_and_copy_variable(self.variable, destination, copy_values=False)
        aligned_values = self.variable[:].data[alignment]
        if isinstance(output_var.datatype, netCDF4.VLType):
            for idx in np.ndindex(aligned_values.shape):
                output_var[idx] = aligned_values[idx]
        else:
            output_var[:] = aligned_values


class MergeFlatten:
    def __init__(self, align_state: bool = False, round_times: bool = True):
        self.align_state = align_state
        self.round_times = round_times

        self._copy_group_attrs: typing.Dict[str, typing.List[_CopyGroupAttributes]] = dict()
        self._copy_variables: typing.Dict[str, typing.List[_CopyVariable]] = dict()
        self._data_variables: typing.Dict[str, typing.List[_DataVariable]] = dict()
        self._data_time_sources: typing.Set[netCDF4.Variable] = set()

    def _add_copy_group_attrs(self, group: netCDF4.Dataset, path: str) -> None:
        attrs = self._copy_group_attrs.get(path)
        if not attrs:
            attrs = list()
            self._copy_group_attrs[path] = attrs
        attrs.append(_CopyGroupAttributes(group))

    def _add_copy_variable(self, variable: netCDF4.Variable, path: str) -> None:
        vars = self._copy_variables.get(path)
        if not vars:
            vars = list()
            self._copy_variables[path] = vars
        vars.append(_CopyVariable(variable))

    def _add_state_variable(self, variable: netCDF4.Variable, path: str) -> None:
        if self.align_state:
            self._add_data_variable(variable, path)
            return
        self._add_copy_variable(variable, path)

    def _add_data_variable(self, variable: netCDF4.Variable, path: str) -> None:
        _, time_variable = _find_time_dimension(variable.group())
        self._data_time_sources.add(time_variable)
        vars = self._data_variables.get(path)
        if not vars:
            vars = list()
            self._data_variables[path] = vars
        vars.append(_DataVariable(variable, time_variable))

    def add_source(self, root: netCDF4.Dataset, name: str) -> None:
        self._add_copy_group_attrs(root, f"instrument/{name}")

        is_state = is_state_group(root)

        def _is_data_time_variable(var: netCDF4.Variable, is_state: typing.Optional[bool]) -> bool:
            if len(var.dimensions) != 1:
                return False
            if var.dimensions[0] != 'time':
                return False
            if var.name != 'time':
                return False
            return not is_state or self.align_state

        def _recurse_copy_group(group: netCDF4.Group, path: str) -> None:
            self._add_copy_group_attrs(group, path)
            for var in root.variables:
                self._add_copy_variable(var, path)
            for name, sub in group.groups.items():
                _recurse_copy_group(sub, f"{path}/{name}")

        def _recurse_add_group(group: netCDF4.Group, is_parent_state: typing.Optional[bool],
                               data_path: str, state_path: str, other_path: str) -> None:
            is_state = is_state_group(group)
            if is_state is None:
                is_state = is_parent_state

            if is_state is None:
                self._add_copy_group_attrs(group, other_path)
                for var in group.variables:
                    self._add_copy_variable(var, other_path)
            elif is_state:
                self._add_copy_group_attrs(group, state_path)
                for var in group.variables.values():
                    if len(var.dimensions) < 1 or var.dimensions[0] != 'time':
                        self._add_copy_variable(var, state_path)
                        continue
                    if var.name == 'time':
                        continue
                    self._add_state_variable(var, state_path)
            else:
                self._add_copy_group_attrs(group, data_path)
                for var in group.variables.values():
                    if len(var.dimensions) < 1 or var.dimensions[0] != 'time':
                        self._add_copy_variable(var, data_path)
                        continue
                    if var.name == 'time':
                        continue
                    self._add_data_variable(var, data_path)

            for name, sub in group.groups.items():
                _recurse_add_group(
                    sub, is_state,
                    f"{data_path}/{name}",
                    f"{state_path}/{name}",
                    f"{other_path}/{name}",
                )

        for var in root.variables.values():
            if len(var.dimensions) == 0:
                self._add_copy_variable(var, f"instrument/{name}")
            elif _is_data_time_variable(var, is_state):
                continue
            elif 'time' in var.dimensions:
                if not is_state:
                    self._add_data_variable(var, f"data/{name}")
                else:
                    self._add_state_variable(var, f"state/{name}")
            else:
                if not is_state:
                    self._add_copy_variable(var, f"data/{name}")
                else:
                    self._add_copy_variable(var, f"state/{name}")

        explicit_group = root.groups.get("instrument")
        if explicit_group:
            _recurse_copy_group(explicit_group, f"instrument/{name}")

        explicit_group = root.groups.get("data")
        if explicit_group and not is_state and not is_state_group(explicit_group):
            _recurse_add_group(
                explicit_group, False,
                f"data/{name}",
                f"state/{name}/data",
                f"instrument/{name}/data",
            )

        explicit_group = root.groups.get("state")
        if explicit_group and (is_state or is_state_group(explicit_group)):
            _recurse_add_group(
                explicit_group, False,
                f"data/{name}/state",
                f"state/{name}",
                f"instrument/{name}/state",
            )

        for sub, g in root.groups.items():
            if sub in ("instrument", "data", "state"):
                continue
            _recurse_add_group(
                g, is_state,
                f"data/{name}/{sub}",
                f"state/{name}/{sub}",
                f"instrument/{name}/{sub}",
            )

    def execute(self, output: typing.Union[str, "Path"]) -> netCDF4.Dataset:
        output = netCDF4.Dataset(str(output), 'w', format='NETCDF4')

        for dest_path, source_attrs in self._copy_group_attrs.items():
            g = output.createGroup(dest_path)
            for a in source_attrs:
                a(g)
        for dest_path, source_vars in self._copy_variables.items():
            g = output.createGroup(dest_path)
            for v in source_vars:
                v(g)

        if len(self._data_variables) == 0:
            return output

        output_times = peer_output_time(*[
            v[:].data for v in self._data_time_sources
        ], apply_rounding=self.round_times)

        data_group = output.createGroup("data")
        time_var = time_coordinate(data_group)
        time_var[:] = output_times

        time_align: typing.Dict[netCDF4.Variable, np.ndarray] = dict()
        for dest_path, source_vars in self._data_variables.items():
            g = output.createGroup(dest_path)
            for v in source_vars:
                align_source = v.time_source
                alignment = time_align.get(align_source)
                if alignment is None:
                    alignment = incoming_before(output_times, align_source[:].data)
                    time_align[align_source] = alignment

                v(g, alignment)

        return output
