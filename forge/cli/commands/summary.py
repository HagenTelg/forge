import typing
import asyncio
import logging
import argparse
import sys
import re
import shutil
import numpy as np
from copy import deepcopy
from math import isfinite, sqrt, nan, log10, floor
from netCDF4 import Variable, Dataset, Dimension
from forge.data.dimensions import find_dimension, find_dimension_values
from forge.data.statistics import find_statistics_origin
from forge.data.structure.variable import get_display_units
from . import ParseCommand, ParseArguments
from .netcdf import MergeInstrument
from .get import WavelengthSelector
from ..execute import Execute, ExecuteStage

_LOGGER = logging.getLogger(__name__)


class Command(ParseCommand):
    COMMANDS: typing.List[str] = ["summary"]
    HELP: str = "display a quick summary of data"

    @classmethod
    def available(cls, cmd: ParseArguments.SubCommand, execute: "Execute") -> bool:
        return cmd.is_last

    @classmethod
    def install(cls, cmd: ParseArguments.SubCommand, execute: "Execute",
                parser: argparse.ArgumentParser) -> None:
        if cmd.is_first:
            from .get import Command as GetCommand
            GetCommand.install_pure(cmd, execute, parser)

        parser.add_argument('--use-statistics',
                            dest='use_statistics', action='store_true',
                            help="use average statistics if available")

        parser.add_argument('--separate-instruments',
                            dest='separate_instruments', action='store_true',
                            help="separate rows by instrument metadata")
        parser.add_argument('--separate-cut-size',
                            dest='separate_cut_size', action='store_true',
                            help="separate rows cut size")

    @classmethod
    def instantiate(cls, cmd: ParseArguments.SubCommand, execute: "Execute",
                    parser: argparse.ArgumentParser,
                    args: argparse.Namespace, extra_args: typing.List[str]) -> None:
        from .get import Command as GetCommand, FilterStage
        if cmd.is_first:
            GetCommand.instantiate_pure(cmd, execute, parser, args, extra_args)
        else:
            cls.no_extra_args(parser, extra_args)
        FilterStage.instantiate_if_available(
            execute,
            retain_statistics=args.use_statistics,
        )
        execute.install(MergeInstrument(execute))
        execute.install(_SummaryStage(execute, parser, args))


def _assign_wavelength_suffixes(wavelengths: typing.Iterable[float]) -> typing.List[str]:
    def _named_suffix(wl: float) -> typing.Optional[str]:
        if wl < 400:
            return None
        elif wl < 500:
            return "B"
        elif wl < 600:
            return "G"
        elif wl < 750:
            return "R"
        return None

    unique_suffixes: typing.Set[str] = set()
    wavelength_suffixes: typing.List[str] = list()
    total_wavelengths = 0
    named_wavelengths = 0
    for wl in wavelengths:
        total_wavelengths += 1
        s = _named_suffix(float(wl))
        if not s or s in unique_suffixes:
            continue
        wavelength_suffixes.append(s)
        unique_suffixes.add(s)
        named_wavelengths += 1
    if total_wavelengths != named_wavelengths:
        return [str(i + 1) for i in range(total_wavelengths)]
    return wavelength_suffixes


class _ValueRow:
    _EXTRACT_DECIMALS = re.compile(r"[-+]?\s*\d+\.(\d+)", flags=re.IGNORECASE)
    _EXTRACT_EXPONENT = re.compile(r"[-+]?\s*\d+\.(\d+)\s*e[-+]?\s*\d+", flags=re.IGNORECASE)

    def __init__(self):
        self._sum: float = 0.0
        self._sum_sq: float = 0.0
        self._count: int = 0
        self._maximum_decimals: int = 0
        self._exponent_decimals: int = 0
        self._statistics_stddev: float = nan
        self._variable_name: str = ""
        self._long_name: str = ""
        self._units: str = ""

    def integrate_variable(self, variable: Variable) -> None:
        self._variable_name = variable.name

        number_format = getattr(variable, "C_format", None)
        if number_format:
            v = None
            try:
                v = str(number_format % 0.0)
            except TypeError:
                try:
                    v = str(number_format % 0)
                except TypeError:
                    pass
            if v:
                v = v.strip()
                m = self._EXTRACT_DECIMALS.search(v)
                if m:
                    self._maximum_decimals = max(self._maximum_decimals, len(m.group(1)))
                else:
                    m = self._EXTRACT_EXPONENT.search(v)
                    if m:
                        self._exponent_decimals = max(self._exponent_decimals, len(m.group(1)))

        long_name = getattr(variable, "long_name", None)
        if long_name:
            self._long_name = long_name

        units = get_display_units(variable)
        if units:
            self._units = units

    def integrate_values(self, values: np.ndarray) -> None:
        values = values[np.isfinite(values)]
        if values.size == 0:
            return
        self._sum += float(np.nansum(values))
        self._sum_sq += float(np.nansum(values ** 2))
        self._count += values.size

    def integrate_stddev(self, values: np.ndarray) -> None:
        values = values[np.isfinite(values)]
        if values.size == 0:
            return
        idx = [-1] * len(values.shape)
        self._statistics_stddev = values[tuple(idx)]

    def _format_value(self, value: typing.Optional[float]) -> str:
        if value is None or not isfinite(value):
            return ""
        if self._exponent_decimals:
            fmt = f"%.{self._exponent_decimals}e" % value
            return fmt % value
        if value == 0.0:
            return "0"
        decimals = log10(abs(value))
        if decimals >= 0.0:
            return f"%.{self._maximum_decimals}f" % value
        leading_zeros = floor(-decimals)
        displayed_digits = self._maximum_decimals - leading_zeros
        if displayed_digits >= 2:
            fmt = f"%.{self._maximum_decimals}f"
        elif displayed_digits == 1:
            fmt = f"%.{self._maximum_decimals+1}f"
        else:
            fmt = f"%.{self._maximum_decimals+2}f"
        return fmt % value

    @property
    def mean(self) -> str:
        if not self._count:
            return ""
        return self._format_value(self._sum / self._count)

    @property
    def stddev(self) -> str:
        if self._count <= 1:
            return self._format_value(self._statistics_stddev)
        stddev = self._count * self._sum_sq - self._sum ** 2
        stddev /= self._count * (self._count - 1)
        stddev = sqrt(stddev)
        return self._format_value(stddev)

    @property
    def description(self) -> str:
        desc = self._long_name or self._variable_name
        if self._units:
            desc += f" ({self._units})"
        return desc


class _RowContext:
    def __init__(self, clone: typing.Optional["_RowContext"] = None):
        self._separate_instruments: bool = clone._separate_instruments if clone else False
        self.separate_cut: bool = clone.separate_cut if clone else False

        self.station: str = clone.station if clone else ""
        self._instrument_id: str = clone._instrument_id if clone else None
        self._instrument_fanout: typing.Optional[typing.Tuple] = clone._instrument_fanout if clone else None

        self._variable_id: typing.Optional[str] = clone._variable_id if clone else None
        self._variable_name: typing.Optional[str] = clone._variable_name if clone else None

        self.cut_size: typing.Optional[float] = clone.cut_size if clone else None
        self.wavelength: typing.Optional[float] = clone.wavelength if clone else None
        self.bin_number: typing.Optional[int] = clone.bin_number if clone else None

    def attach_args(self, args: argparse.Namespace):
        self._separate_instruments = args.separate_instruments
        self.separate_cut = args.separate_cut_size

    def __deepcopy__(self, memo):
        y = type(self)(clone=self)
        memo[id(self)] = y
        return y

    def attach_file(self, root: Dataset):
        self._instrument_id = getattr(root, "instrument_id", None)

        station_name = root.variables.get("station_name")
        if station_name is not None:
            self.station = str(station_name[0])

        self._instrument_fanout = (getattr(root, "instrument", None), )
        inst_group = root.groups.get("instrument")
        if inst_group is not None:
            manufacturer = inst_group.variables.get("manufacturer")
            if manufacturer is not None:
                manufacturer = str(manufacturer[0])
            model = inst_group.variables.get("model")
            if model is not None:
                model = str(model[0])
            serial_number = inst_group.variables.get("serial_number")
            if serial_number is not None:
                serial_number = str(serial_number[0])

            if manufacturer or model or serial_number:
                self._instrument_fanout = (manufacturer, model, serial_number)

    def attach_variable(self, variable: Variable):
        self._variable_name = variable.name
        self._variable_id = getattr(variable, "variable_id", None)

    @property
    def uid(self) -> typing.Tuple:
        if self._variable_id and '_' in self._variable_id:
            r = (self._variable_id, self.station, self.wavelength, self.bin_number)
        else:
            r = (self._instrument_id, self.station, self._variable_id or self._variable_name, self.wavelength, self.bin_number)
        if self.separate_cut:
            r = r + (self.cut_size, )
        if self._separate_instruments:
            r = r + self._instrument_fanout
        return r

    def __hash__(self):
        return hash(self.uid)

    def __eq__(self, other):
        if not isinstance(other, _RowContext):
            return False
        return self.uid == other.uid

    def __ne__(self, other):
        if not isinstance(other, _RowContext):
            return True
        return self.uid != other.uid

    @property
    def name_assignment_key(self) -> typing.Tuple:
        if self._variable_id and '_' in self._variable_id:
            r = (self._variable_id, self.station)
        else:
            r = (self._instrument_id, self.station, self._variable_id or self._variable_name)
        if self.separate_cut:
            r = r + (self.cut_size, )
        if self._separate_instruments:
            r = r + self._instrument_fanout
        return r

    def row_name(self, wavelength_suffix: typing.Optional[str] = None, bin_suffix: typing.Optional[str] = None) -> str:
        if not self._variable_id:
            return self._variable_name
        if '_' in self._variable_id:
            prefix, instrument_suffix = self._variable_id.split('_', 1)
        else:
            prefix = self._variable_id
            instrument_suffix = self._instrument_id or ""

        if wavelength_suffix:
            prefix += wavelength_suffix
        if bin_suffix:
            prefix += bin_suffix
        if self.cut_size and isfinite(self.cut_size):
            if self.cut_size < 2.5:
                prefix += "1"
            elif self.cut_size < 10.0:
                prefix += "2"
            else:
                prefix += "0"
        return prefix + "_" + instrument_suffix

    def row_sort_key(self, row_name: str) -> typing.Tuple:
        wavelength_sort = -1
        if self.wavelength is not None and isfinite(self.wavelength):
            wavelength_sort = self.wavelength
        cut_sort = -1
        if self.cut_size is not None and isfinite(self.cut_size):
            cut_sort = self.cut_size
        bin_sort = -1
        if self.bin_number is not None:
            bin_sort = self.bin_number
        return (self._variable_id or "", cut_sort, wavelength_sort, bin_sort, self.station, row_name)

    @property
    def display_wavelength(self) -> str:
        if self.wavelength is None or not isfinite(self.wavelength):
            return ""
        return f"{self.wavelength:.0f}"

    @property
    def display_instrument(self) -> str:
        if not self._separate_instruments:
            return ""
        if not self._instrument_fanout:
            return ""
        r = self._instrument_fanout[0]
        if len(self._instrument_fanout) > 1:
            r += " " + self._instrument_fanout[1]
        if len(self._instrument_fanout) > 2:
            r += " #" + self._instrument_fanout[2]
        return r


class _SummaryStage(ExecuteStage):
    def __init__(self, execute: Execute, parser: argparse.ArgumentParser, args: argparse.Namespace):
        super().__init__(execute)

        self._use_statistics = args.use_statistics
        self._show_instrument = args.separate_instruments
        self._base_context = _RowContext()
        self._base_context.attach_args(args)

        self._wavelength_selector = WavelengthSelector.instantiate_if_available(execute)

    def _process_variable(self, root: Dataset, variable: Variable, context: _RowContext,
                          values: typing.Dict[_RowContext, _ValueRow],
                          is_stddev: bool = False) -> None:
        def integrate_values(context: _RowContext, data: np.ndarray) -> None:
            target = values.get(context)
            if not target:
                target = _ValueRow()
                values[context] = target
            if is_stddev:
                target.integrate_stddev(data)
            else:
                target.integrate_variable(variable)
                target.integrate_values(data)

        def fanout_bin_number(context: _RowContext, data: np.ndarray, dimensions: typing.List[str]) -> None:
            if len(dimensions) < 1 or dimensions == ['time']:
                integrate_values(context, data)
                return

            if dimensions[0] == 'time':
                def dimension_iter() -> typing.Iterator[typing.Tuple[int, np.ndarray]]:
                    for idx in np.ndindex(data.shape[1:]):
                        yield int(np.ravel_multi_index(idx, data.shape[1:])), data[:, idx]
            else:
                def dimension_iter() -> typing.Iterator[typing.Tuple[int, np.ndarray]]:
                    for idx in np.ndindex(data.shape):
                        yield int(np.ravel_multi_index(idx, data.shape)), data[idx]

            for bin_index, sub_data in dimension_iter():
                sub_context = deepcopy(context)
                sub_context.bin_number = bin_index
                integrate_values(sub_context, sub_data)

        def fanout_cut_size(context: _RowContext, data: np.ndarray, dimensions: typing.List[str]) -> None:
            if 'cut_size' not in dimensions:
                if not context.separate_cut:
                    fanout_bin_number(context, data, dimensions)
                    return

                if 'cut_size' not in getattr(variable, 'ancillary_variables', "").split():
                    fanout_bin_number(context, data, dimensions)
                    return
                try:
                    cut_var = variable.group().variables['cut_size']
                except KeyError:
                    _LOGGER.warning("Could not find cut size values for variable %s", variable.name)
                    fanout_bin_number(context, data, dimensions)
                    return
                if not np.issubdtype(cut_var.dtype, np.floating):
                    fanout_bin_number(context, data, dimensions)
                    return

                if len(cut_var.dimensions) != 1 or cut_var.dimensions[0] != 'time':
                    fixed_value = float(cut_var[0])
                    if isfinite(fixed_value):
                        context.cut_size = fixed_value
                    fanout_bin_number(context, data, dimensions)
                    return

                cut_data = cut_var[:].data
                cut_valid_data = np.isfinite(cut_data)
                unique_cut_sizes = np.unique(cut_data[cut_valid_data])
                for cut in unique_cut_sizes:
                    sub_context = deepcopy(context)
                    sub_context.cut_size = cut
                    sub_data = data[cut_data == cut, ...]
                    fanout_bin_number(sub_context, sub_data, dimensions)

                no_cut_data = np.invert(cut_valid_data)
                if np.any(no_cut_data):
                    sub_context = deepcopy(context)
                    sub_context.cut_size = None
                    sub_data = data[no_cut_data, ...]
                    fanout_bin_number(context, sub_data, dimensions)
                return

            dim_idx = dimensions.index('cut_size')
            sub_dimensions = list(dimensions)
            del sub_dimensions[dim_idx]

            if not context.separate_cut:
                for select_idx in range(data.shape[dim_idx]):
                    sub_index: typing.List[typing.Union[slice, int]] = [slice(None)] * len(dimensions)
                    sub_index[dim_idx] = select_idx
                    sub_data = data[tuple(sub_index)]

                    fanout_bin_number(context, sub_data, sub_dimensions)
                return

            try:
                _, cut_var = find_dimension_values(variable.group(), 'cut_size')
            except KeyError:
                _LOGGER.warning("Could not find cut size dimension for variable %s", variable.name)
                fanout_bin_number(context, data, dimensions)
                return

            for select_idx in range(cut_var.shape[0]):
                sub_context = deepcopy(context)

                fixed_value = float(cut_var[select_idx].data)
                if isfinite(fixed_value):
                    sub_context.cut_size = fixed_value
                else:
                    sub_context.cut_size = None

                sub_index: typing.List[typing.Union[slice, int]] = [slice(None)] * len(dimensions)
                sub_index[dim_idx] = select_idx
                sub_data = data[tuple(sub_index)]

                fanout_bin_number(sub_context, sub_data, sub_dimensions)

        def fanout_wavelength(context: _RowContext, data: np.ndarray, dimensions: typing.List[str]) -> None:
            try:
                dim_idx = dimensions.index('wavelength')
                wavelength_dimension, wavelength_variable = find_dimension_values(variable.group(), 'wavelength')
            except (ValueError, KeyError):
                fanout_cut_size(context, data, dimensions)
                return

            if self._wavelength_selector:
                output_wavelengths = self._wavelength_selector(root, variable, retain_statistics=self._use_statistics)
            else:
                output_wavelengths = list(range(wavelength_dimension.size))
            if not output_wavelengths:
                return
            output_wavelengths = sorted(output_wavelengths)

            sub_dimensions = list(dimensions)
            del sub_dimensions[dim_idx]
            for select_idx in output_wavelengths:
                sub_context = deepcopy(context)

                fixed_value = float(wavelength_variable[select_idx].data)
                if isfinite(fixed_value):
                    sub_context.wavelength = fixed_value
                else:
                    sub_context.wavelength = None

                sub_index: typing.List[typing.Union[slice, int]] = [slice(None)] * len(dimensions)
                sub_index[dim_idx] = select_idx
                sub_data = data[tuple(sub_index)]

                fanout_cut_size(sub_context, sub_data, sub_dimensions)

        fanout_wavelength(context, variable[:].data, list(variable.dimensions))

    def _process_file(self, root: Dataset, values: typing.Dict[_RowContext, _ValueRow]):
        context = deepcopy(self._base_context)
        context.attach_file(root)

        def is_statistics(variable: Variable, type_name: str) -> bool:
            check_group = variable.group()
            while check_group is not None:
                if check_group.name == type_name:
                    break
                check_group = check_group.parent
            else:
                return False

            check_group = check_group.parent
            if check_group is None:
                return False
            return check_group.name == 'statistics'

        def is_statistics_stddev(variable: Variable) -> bool:
            if not np.issubdtype(variable.dtype, np.floating):
                return False
            return is_statistics(variable, 'stddev')

        def display_variable(variable: Variable) -> bool:
            if variable.name == 'system_flags':
                return False
            if not np.issubdtype(variable.dtype, np.floating):
                return False
            try:
                name = str(variable.variable_id)
            except AttributeError:
                return False
            if not name:
                return False
            return True

        def integrate_group(root: Dataset) -> None:
            if root.name == 'instrument':
                return

            for var in root.variables.values():
                if len(var.dimensions) == 0 or var.dimensions[0] != 'time':
                    continue
                if var.name in ('time', 'cut_size'):
                    continue
                if var.name == 'wavelength':
                    try:
                        find_dimension(root, 'wavelength')
                        continue
                    except KeyError:
                        pass
                if self._use_statistics and is_statistics_stddev(var):
                    base_var = find_statistics_origin(var)
                    if base_var is None:
                        continue
                    var_context = deepcopy(context)
                    var_context.attach_variable(base_var)
                    self._process_variable(root, var, var_context, values, is_stddev=True)
                else:
                    if not display_variable(var):
                        continue
                    var_context = deepcopy(context)
                    var_context.attach_variable(var)
                    self._process_variable(root, var, var_context, values)

            for sub in root.groups.values():
                integrate_group(sub)

        integrate_group(root)

    @staticmethod
    def _assign_wavelength_suffixes(rows: typing.List[_RowContext]) -> typing.Dict[_RowContext, str]:
        wavelength_rows: typing.List[_RowContext] = list()
        for row in rows:
            if not row.wavelength or not isfinite(row.wavelength):
                continue
            wavelength_rows.append(row)
        wavelength_rows.sort(key=lambda x: x.wavelength)

        wavelength_suffixes: typing.Dict[_RowContext, str] = dict()
        assigned_suffixes = _assign_wavelength_suffixes([row.wavelength for row in wavelength_rows])
        for i in range(len(assigned_suffixes)):
            wavelength_suffixes[wavelength_rows[i]] = assigned_suffixes[i]
        return wavelength_suffixes

    @staticmethod
    def _assign_bin_suffixes(rows: typing.List[_RowContext]) -> typing.Dict[_RowContext, str]:
        bin_rows: typing.List[_RowContext] = list()
        for row in rows:
            if row.bin_number is None:
                continue
            bin_rows.append(row)
        bin_rows.sort(key=lambda x: x.bin_number)

        bin_suffixes: typing.Dict[_RowContext, str] = dict()
        assigned_suffixes = [str(i+1) for i in range(len(bin_rows))]
        for i in range(len(assigned_suffixes)):
            bin_suffixes[bin_rows[i]] = assigned_suffixes[i]
        return bin_suffixes

    def _assemble_rows(self, values: typing.Dict[_RowContext, _ValueRow]) -> typing.List[typing.Tuple[str, _RowContext, _ValueRow]]:
        name_assignment_groups: typing.Dict[typing.Tuple, typing.List[_RowContext]] = dict()
        for context in values.keys():
            key = context.name_assignment_key
            target = name_assignment_groups.get(key)
            if not target:
                target = list()
                name_assignment_groups[key] = target
            target.append(context)

        seen_stations: typing.Set[str] = set()
        sort_rows: typing.List[typing.Tuple[typing.Tuple, str, _RowContext]] = list()
        for assign_rows in name_assignment_groups.values():
            wavelength_suffixes = self._assign_wavelength_suffixes(assign_rows)
            bin_suffixes = self._assign_bin_suffixes(assign_rows)

            for r in assign_rows:
                seen_stations.add(r.station)

                row_name = r.row_name(wavelength_suffixes.get(r), bin_suffixes.get(r))
                sort_rows.append((r.row_sort_key(row_name), row_name, r))
        sort_rows.sort(key=lambda x: x[0])

        output_rows: typing.List[typing.Tuple[str, _RowContext, _ValueRow]] = list()
        for _, name, context in sort_rows:
            if len(seen_stations) > 1:
                name = context.station + ":" + name
            output_rows.append((name, context, values[context]))
        return output_rows

    def _do_output(self, values: typing.Dict[_RowContext, _ValueRow], limit_width: typing.Optional[int] = None) -> None:
        rows = self._assemble_rows(values)

        output_wavelength = False
        for _, ctx, _ in rows:
            output_wavelength = output_wavelength or ctx.wavelength is not None

        columns = [
            ["NAME"],
            ["MEAN"],
            ["STDDEV"],
            ["DESCRIPTION"],
        ]
        wavelength_rows = ["WL"]
        instrument_rows = ["INSTRUMENT"]
        prior_key = None
        last_desc = None
        last_inst = None
        unique_instruments = set()
        for name, ctx, value in rows:
            name_key = ctx.name_assignment_key
            force_changed = name_key != prior_key
            prior_key = name_key

            columns[0].append(name)
            if output_wavelength:
                wavelength_rows.append(ctx.display_wavelength)
            columns[1].append(value.mean)
            columns[2].append(value.stddev)

            desc = value.description
            if force_changed or last_desc != desc:
                columns[3].append(desc)
                last_desc = desc
            else:
                columns[3].append("")

            inst = ctx.display_instrument
            unique_instruments.add(inst)
            if force_changed or last_inst != inst:
                instrument_rows.append(inst)
                last_inst = inst
            else:
                instrument_rows.append("")
        if output_wavelength:
            columns.insert(1, wavelength_rows)
        if len(unique_instruments) > 1 or len(unique_instruments) == 1 and unique_instruments != {""}:
            columns.insert(-1, instrument_rows)

        column_widths: typing.List[int] = list()
        for cidx in range(len(columns)):
            w = 0
            for row in columns[cidx]:
                w = max(w, len(row))
            if cidx != 0:
                w += 1
            column_widths.append(w)

        available_final_width = None
        if limit_width:
            total_width = sum(column_widths[:-1])
            available_final_width = limit_width - total_width
            if available_final_width <= 3:
                available_final_width = None

        for ridx in range(len(columns[0])):
            for cidx in range(len(columns)):
                if cidx == len(columns)-1:
                    value = " " + columns[cidx][ridx]
                    if available_final_width and len(value) > available_final_width:
                        value = value[:available_final_width-3] + "..."
                    print(value)
                else:
                    print(columns[cidx][ridx].rjust(column_widths[cidx]), end="")

    async def __call__(self) -> None:
        values: typing.Dict[_RowContext, _ValueRow] = dict()

        for input_file in self.data_file_progress("Scanning data"):
            input_file = Dataset(input_file, 'r')
            try:
                self._process_file(input_file, values)
            finally:
                input_file.close()

        if not sys.stdout.isatty():
            with self.progress("Writing summary"):
                self._do_output(values)
        else:
            terminal_width = shutil.get_terminal_size(fallback=(0, 24)).columns
            self._do_output(values, terminal_width or None)