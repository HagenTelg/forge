import typing
import asyncio
import logging
import argparse
import time
import re
import sys
import numpy as np
from copy import deepcopy
from math import isfinite, nan, floor, ceil, log2
from netCDF4 import Variable, Dataset, Dimension
from forge.logicaltime import start_of_year, julian_day
from forge.timeparse import parse_interval_argument
from forge.data.flags import parse_flags
from forge.data.state import is_in_state_group
from forge.data.statistics import find_statistics_origin
from forge.data.dimensions import find_dimension_values
from forge.data.merge.timealign import peer_output_time, incoming_before
from . import ParseCommand, ParseArguments
from .netcdf import MergeInstrument
from .get import WavelengthSelector, ArchiveRead
from ..execute import Execute, ExecuteStage

_LOGGER = logging.getLogger(__name__)


class Command(ParseCommand):
    COMMANDS: typing.List[str] = ["export"]
    HELP: str = "write human readable CSV-like data"

    @classmethod
    def available(cls, cmd: ParseArguments.SubCommand, execute: "Execute") -> bool:
        return cmd.is_last

    @classmethod
    def install(cls, cmd: ParseArguments.SubCommand, execute: "Execute",
                parser: argparse.ArgumentParser) -> None:
        if cmd.is_first:
            from .get import Command as GetCommand
            GetCommand.install_pure(cmd, execute, parser)
        cls.install_pure(cmd, execute, parser)

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
            retain_statistics=(args.stddev or args.count or args.quantiles)
        )
        cls.instantiate_pure(cmd, execute, parser, args, extra_args)

    @classmethod
    def install_pure(cls, cmd: ParseArguments.SubCommand, execute: "Execute",
                     parser: argparse.ArgumentParser) -> None:
        parser.add_argument('--mode',
                            choices=['xl', 'excel', 'csv', 'r', 's', 'idl', 'idllegacy'],
                            default='xl',
                            help="base output format")

        parser.add_argument('--station',
                            dest='station_column', action='store_true',
                            help="output a station column")
        parser.set_defaults(station_column=None)

        parser.add_argument('--time-epoch',
                            dest='time_epoch', action='store_true',
                            help="output the number of seconds since Jan 1, 1970")
        parser.set_defaults(time_epoch=None)
        parser.add_argument('--time-excel',
                            dest='time_excel', action='store_true',
                            help="output an Excel compatible time column")
        parser.set_defaults(time_excel=None)
        parser.add_argument('--time-iso',
                            dest='time_iso', action='store_true',
                            help="output an ISO 8601 time column")
        parser.add_argument('--time-fyear',
                            dest='time_fyear', action='store_true',
                            help="output a fractional year time column")
        parser.add_argument('--time-julian',
                            dest='time_julian', action='store_true',
                            help="output a Julian day (days since 12:00 January 1, 4713 BC.)")
        parser.set_defaults(time_julian=None)
        group = parser.add_mutually_exclusive_group()
        group.add_argument('--time-yeardoy',
                            dest='time_yeardoy', action='store_true',
                            help="output year and day of year columns (January 1 = 1.00000)")
        group.add_argument('--time-doy',
                            dest='time_doy', action='store_true',
                            help="output a day of year column (January 1 = 1.00000)")
        parser.set_defaults(time_yeardoy=None)
        parser.set_defaults(time_doy=None)

        parser.add_argument('--cut-string',
                            dest='cut_string', action='store_true',
                            help="output a column with the name of the effective cut size(s)")
        # group = parser.add_mutually_exclusive_group()
        # group.add_argument('--cut-split',
        #                    dest='cut_split', action='store_true',
        #                    help="always split data by cut size even if the input is not split")
        # group.add_argument('--no-cut-split',
        #                    dest='cut_split', action='store_false',
        #                    help="do not split data by cut size")
        # parser.set_defaults(cut_split=None)

        parser.add_argument('--no-stddev',
                            dest='stddev', action='store_false',
                            help="disable standard deviation output when available")
        parser.set_defaults(stddev=True)

        parser.add_argument('--no-count',
                            dest='count', action='store_false',
                            help="disable number of points output when available")
        parser.set_defaults(count=True)

        parser.add_argument('--quantiles',
                            dest='quantiles', action='store_true',
                            help="enable quantile output when available")

        parser.add_argument('--join',
                            choices=['csv', 'space', 'tab'],
                            help="base output join mode")
        parser.add_argument('--join-delimiter',
                            dest='join_delimiter',
                            help="output join field delimiter")
        parser.set_defaults(join_delimiter=None)
        parser.add_argument('--join-quote',
                            dest='join_quote',
                            default='\"',
                            help="output join quote delimiter")
        parser.add_argument('--join-quote-escape',
                            dest='join_quote_escape',
                            help="output join quote escape sequence")
        parser.set_defaults(join_quote_escape=None)

        parser.add_argument('--mvc',
                            dest='mvc',
                            help="string used for missing values")
        parser.add_argument('--mvc-flag',
                            dest='mvc_flag',
                            choices=['none', 'end', 'follow'],
                            help="string used for missing values")

        parser.add_argument('--flags',
                            choices=['hex', '0x', 'breakdown', 'list'],
                            default='hex',
                            help="flags output mode")

        parser.add_argument('--format',
                            help="numeric format specification")
        parser.add_argument('--numeric-only',
                            dest='numeric_only', action='store_true',
                            help="only output numeric values")
        parser.set_defaults(numeric_only=None)

        parser.add_argument('--header-prefix',
                            dest='header_prefix',
                            help="prefix added to header lines")
        parser.add_argument('--header-stderr',
                            dest='header_stderr', action='store_true',
                            help="write the header to stderr instead of stdout")
        parser.set_defaults(header_stderr=None)
        parser.add_argument('--no-header-names',
                            dest='header_names', action='store_false',
                            help="output a header with column names")
        parser.set_defaults(header_names=True)
        parser.add_argument('--header-description',
                            dest='header_description', action='store_true',
                            help="output a header with a description of the variable")
        parser.add_argument('--header-wavelength',
                            dest='header_wavelength', action='store_true',
                            help="output a header with a the wavelength of the variable")
        parser.add_argument('--header-mvcs',
                            dest='header_mvcs', action='store_true',
                            help="output a header with the missing values of the variable")
        parser.add_argument('--header-cut',
                            dest='header_cut', action='store_true',
                            help="output a header with cut size of the column")
        parser.add_argument('--header-flags',
                            dest='header_flags', action='store_true',
                            help="output a header with the values of flags in the variable")

        parser.add_argument('--latest-in-gaps',
                            dest='latest_in_gaps', action='store_true',
                            help="use the latest value in data gaps instead of missing")

        group = parser.add_mutually_exclusive_group()
        group.add_argument('--no-round-times',
                           dest='no_round_times', action='store_true',
                           help="disable time rounding during record creation")
        group.add_argument('--no-fill-times',
                           dest='no_fill_times', action='store_true',
                           help="disable filling time based on source archive spacing")
        group.add_argument('--fill',
                           dest='fill_interval',
                           help="set the record fill interval")

    @classmethod
    def instantiate_pure(cls, cmd: ParseArguments.SubCommand, execute: "Execute",
                         parser: argparse.ArgumentParser,
                         args: argparse.Namespace, extra_args: typing.List[str]) -> None:
        execute.install(MergeInstrument(execute))
        execute.install(_ExportStage(execute, parser, args))


def _assign_wavelength_suffixes(wavelengths: np.ndarray) -> typing.List[str]:
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
    for wl in wavelengths:
        s = _named_suffix(float(wl))
        if not s or s in unique_suffixes:
            return [str(i+1) for i in range(len(wavelengths))]
        wavelength_suffixes.append(s)
        unique_suffixes.add(s)
    return wavelength_suffixes


class _Column:
    @property
    def header_name(self) -> str:
        raise NotImplementedError

    @property
    def mvc(self) -> str:
        raise NotImplementedError

    @property
    def description(self) -> str:
        raise NotImplementedError

    @property
    def time_sources(self) -> typing.List[typing.Tuple[Variable, bool]]:
        return []

    def prepare_file(self, file: Dataset) -> None:
        pass

    def __call__(self, time_value: float, time_indices: typing.List[typing.Optional[int]]) -> str:
        raise NotImplementedError


class _ColumnStation(_Column):
    def __init__(self, name: str = "STN"):
        super().__init__()
        self._name = name
        self._stations: typing.Set[str] = set()
        self._output: str = ""

    @property
    def header_name(self) -> str:
        return self._name

    @property
    def mvc(self) -> str:
        return "ZZZ"

    @property
    def description(self) -> str:
        return "Station code"

    def prepare_file(self, file: Dataset) -> None:
        station_var = file.variables.get("station_name")
        if station_var is not None:
            self._stations.add(str(station_var[0]).upper())
            self._output = ";".join(sorted(self._stations))

    def __call__(self, time_value: float, time_indices: typing.List[typing.Optional[int]]) -> str:
        return self._output


class _ColumnTime(_Column):
    pass


class _ColumnTimeEpoch(_ColumnTime):
    @property
    def header_name(self) -> str:
        return "EPOCH"

    @property
    def mvc(self) -> str:
        return "0"

    @property
    def description(self) -> str:
        return "Epoch time: seconds from 1970-01-01T00:00:00Z"

    def __call__(self, time_value: float, time_indices: typing.List[typing.Optional[int]]) -> str:
        return str(int(round(time_value)))


class _ColumnTimeExcel(_ColumnTime):
    @property
    def header_name(self) -> str:
        return "DateTimeUTC"

    @property
    def mvc(self) -> str:
        return "9999-99-99 99:99:99"

    @property
    def description(self) -> str:
        return "Date String (YYYY-MM-DD hh:mm:ss) UTC"

    def __call__(self, time_value: float, time_indices: typing.List[typing.Optional[int]]) -> str:
        ts = time.gmtime(time_value)
        return f"{ts.tm_year:04}-{ts.tm_mon:02}-{ts.tm_mday:02} {ts.tm_hour:02}:{ts.tm_min:02}:{ts.tm_sec:02}"


class _ColumnTimeISO(_ColumnTime):
    @property
    def header_name(self) -> str:
        return "DateTimeUTC"

    @property
    def mvc(self) -> str:
        return "9999-99-99T99:99:99Z"

    @property
    def description(self) -> str:
        return "Date String (YYYY-MM-DDThh:mm:ssZ) UTC"

    def __call__(self, time_value: float, time_indices: typing.List[typing.Optional[int]]) -> str:
        ts = time.gmtime(time_value)
        return f"{ts.tm_year:04}-{ts.tm_mon:02}-{ts.tm_mday:02}T{ts.tm_hour:02}:{ts.tm_min:02}:{ts.tm_sec:02}Z"


class _ColumnTimeYear(_ColumnTime):
    @property
    def header_name(self) -> str:
        return "Year"

    @property
    def mvc(self) -> str:
        return "9999"

    @property
    def description(self) -> str:
        return "Year"

    def __call__(self, time_value: float, time_indices: typing.List[typing.Optional[int]]) -> str:
        ts = time.gmtime(time_value)
        return f"{ts.tm_year:04}"


class _ColumnTimeDOY(_ColumnTime):
    @property
    def header_name(self) -> str:
        return "DOY"

    @property
    def mvc(self) -> str:
        return "999.99999"

    @property
    def description(self) -> str:
        return "Fractional day of year (Midnight January 1 UTC = 1.00000)"

    def __call__(self, time_value: float, time_indices: typing.List[typing.Optional[int]]) -> str:
        year_start = start_of_year(time.gmtime(time_value).tm_year)
        doy = (time_value - year_start) / (24.0 * 60.0 * 60.0) + 1.0
        return f"{doy:09.5f}"


class _ColumnTimeJulian(_ColumnTime):
    @property
    def header_name(self) -> str:
        return "JulianDay"

    @property
    def mvc(self) -> str:
        return "9999999.99999"

    @property
    def description(self) -> str:
        return "Fractional Julian day (12:00 January 1, 4713 BC UTC)"

    def __call__(self, time_value: float, time_indices: typing.List[typing.Optional[int]]) -> str:
        fractional_day = time_value - floor(time_value / (24 * 60 * 60)) * 24 * 60 * 60
        fractional_day /= 24 * 60 * 60
        ts = time.gmtime(time_value)
        jd = julian_day(ts.tm_year, ts.tm_mon, ts.tm_sec) + fractional_day
        return f"{jd:013.5f}"


class _ColumnTimeFractionalYear(_ColumnTime):
    @property
    def header_name(self) -> str:
        return "FractionalYear"

    @property
    def mvc(self) -> str:
        return "9999.99999999"

    @property
    def description(self) -> str:
        return "Fractional year"

    def __call__(self, time_value: float, time_indices: typing.List[typing.Optional[int]]) -> str:
        year = time.gmtime(time_value).tm_year
        year_start = start_of_year(year)
        year_end = start_of_year(year + 1)
        fyear = year_start + (time_value - year_start) / (year_end - year_start)
        return f"{fyear:013.8f}"


class _ColumnTimeCutString(_Column):
    def __init__(self):
        super().__init__()
        self._cut_time_variables: typing.List[Variable] = list()
        self._always_present_sizes: typing.Set[float] = set()

    @property
    def header_name(self) -> str:
        return "Size"

    @property
    def mvc(self) -> str:
        return "Z"

    @property
    def description(self) -> str:
        return "Semicolon delimited list of cut sizes present"

    @property
    def time_sources(self) -> typing.List[typing.Tuple[Variable, bool]]:
        result: typing.List[typing.Tuple[Variable, bool]] = list()
        for var in self._cut_time_variables:
            _, time_var = find_dimension_values(var.group(), 'time')
            is_state = is_in_state_group(var)
            result.append((time_var, is_state))
        return result

    def prepare_file(self, file: Dataset) -> None:
        def scan_group(root: Dataset) -> None:
            var = root.variables.get("cut_size")
            if var is not None:
                if len(var.dimensions) != 0 and var.dimensions[0] == 'time':
                    self._cut_time_variables.append(var)
                elif var.shape:
                    self._always_present_sizes.update([float(c) for c in var[:].data])
                else:
                    self._always_present_sizes.add(float(var[0].data))
            for g in root.groups.values():
                scan_group(g)
        scan_group(file)

    def __call__(self, time_value: float, time_indices: typing.List[typing.Optional[int]]) -> str:
        effective_sizes: typing.Set[float] = set(self._always_present_sizes)
        for idx in range(len(self._cut_time_variables)):
            time_idx = time_indices[idx]
            if time_idx is None:
                continue
            var = self._cut_time_variables[idx]
            effective_sizes.add(float(var[time_idx].data))
        combined_sizes: typing.List[str] = list()
        for s in sorted(effective_sizes):
            if not isfinite(s):
                combined_sizes.append('')
                continue
            if abs(s - 1.0) < 0.1:
                combined_sizes.append('PM1')
            elif abs(s - 2.5) < 0.1:
                combined_sizes.append('PM25')
            elif abs(s - 10) < 0.1:
                combined_sizes.append('PM10')
            else:
                combined_sizes.append(str(s))
        return ";".join(combined_sizes)


class _ColumnVariable(_Column):
    class Source:
        def __init__(self, file: Dataset, variable: Variable):
            self.file = file
            self.variable = variable
            self._data = self.variable[:].data
            self.indexed_by_time: bool = len(variable.dimensions) > 0 and variable.dimensions[0] == 'time'
            if self.indexed_by_time:
                self.index_suffix: "typing.Tuple[int, ...]" = tuple([0] * (len(variable.dimensions) - 1))
            else:
                self.index_suffix: "typing.Tuple[int, ...]" = tuple([0] * len(variable.dimensions))
            self.name_suffix: str = ""
            self.identifier_suffix: str = ""
            self.wavelength: typing.Optional[float] = None
            self.cut_size: typing.Optional[float] = None

        def __call__(self, time_idx: int) -> typing.Any:
            if not self.indexed_by_time:
                return self._data[self.index_suffix]
            return self._data[(time_idx, *self.index_suffix)]

        def __deepcopy__(self, memo):
            y = type(self)(self.file, self.variable)
            y.indexed_by_time = self.indexed_by_time
            y.index_suffix = self.index_suffix
            y.name_suffix = self.name_suffix
            y.identifier_suffix = self.identifier_suffix
            y.wavelength = self.wavelength
            y.cut_size = self.cut_size
            memo[id(self)] = y
            return y

        def construct_name(self, variable: Variable, modifier_suffix: str = "") -> str:
            try:
                var_code = str(variable.variable_id)
                if "_" in var_code:
                    prefix, suffix = var_code.split("_", 1)
                else:
                    prefix = var_code
                    try:
                        suffix = str(self.file.instrument_id)
                    except AttributeError:
                        suffix = ""
            except AttributeError:
                prefix = variable.name
                suffix = ""
            try:
                archive_suffix = "_" + str(self.file.forge_archive).upper()
            except AttributeError:
                archive_suffix = ""

            if self.name_suffix:
                prefix += self.name_suffix
            if modifier_suffix:
                prefix += modifier_suffix
            if self.identifier_suffix:
                suffix += self.identifier_suffix

            if not suffix:
                return prefix + archive_suffix
            return prefix + "_" + suffix + archive_suffix

        def header_name(self, modifier_suffix: str = "") -> str:
            return self.construct_name(self.variable, modifier_suffix=modifier_suffix)

    def __init__(self, source: "_ColumnVariable.Source"):
        super().__init__()
        self.source = source

    @property
    def header_name(self) -> str:
        return self.source.header_name()

    @property
    def description(self) -> str:
        try:
            return self.source.variable.long_name
        except AttributeError:
            pass
        return self.source.variable.name

    @property
    def sort_key(self) -> typing.Tuple[int, str, int]:
        return 0, self.header_name, 0

    @property
    def time_sources(self) -> typing.List[typing.Tuple[Variable, bool]]:
        _, time_var = find_dimension_values(self.source.variable.group(), 'time')
        is_state = is_in_state_group(self.source.variable)
        return [(time_var, is_state)]


class _ColumnVariableNumeric(_ColumnVariable):
    class Formatter:
        _DIGIT_FORMAT = re.compile(r'(\d+)(\.(\d*))?')
        _FORMAT_CODE = re.compile(r'%([- #0+]*)(\d*)(?:\.(\d+))?(?:hh|h|l|ll|q|L|j|z|Z|t)?([diouxXeEfFgG])')

        @staticmethod
        def _to_mvc(format_code: str) -> str:
            parsed = _ColumnVariableNumeric.Formatter._FORMAT_CODE.search(format_code)
            if not parsed:
                return ""

            flags = parsed.group(1)
            total_width = parsed.group(2)
            fractional_digits = parsed.group(3)
            type_code = parsed.group(4)
            format_code = '%' + flags + total_width

            if total_width:
                total_width = int(total_width)
            else:
                total_width = None
            if fractional_digits:
                format_code += '.' + fractional_digits
                fractional_digits = int(fractional_digits)
            else:
                fractional_digits = 0
            format_code += type_code

            if ' ' in flags and total_width:
                total_width -= 1
            elif '+' in flags and total_width:
                total_width -= 1

            if type_code in ('e', 'E', 'g', 'G'):
                return format_code % float('9.' + ('9' * fractional_digits) + 'E99')
            elif type_code in ('x', 'X'):
                if '#' in flags:
                    total_width = (total_width or 1) - 2
                return format_code % int('F' * max(1, total_width or 0), 16)
            elif type_code == 'o':
                if '#' in flags:
                    total_width = (total_width or 1) - 2
                return format_code % int('7' * max(1, total_width or 0), 8)
            elif type_code not in ('f', 'F'):
                return format_code % int('9' * (total_width or 4))

            base = total_width
            if not base and fractional_digits:
                base = fractional_digits + 1
            if not base:
                base = 4
            base = int('9' * base)
            base /= 10 ** fractional_digits
            return format_code % base

        def __init__(self, parser: argparse.ArgumentParser,
                     format_string: typing.Optional[str], mvc: typing.Optional[str]):
            self._value_converter: typing.Callable[[typing.Union[float, int]], typing.Union[float, int]] = lambda x: x

            self._mvc = mvc

            self._format_string = None
            if format_string:
                digits = _ColumnVariableNumeric.Formatter._DIGIT_FORMAT.fullmatch(format_string)
                if digits:
                    integer_digits = len(digits.group(1))
                    if not digits.group(2):
                        format_string = f'%0{integer_digits}d'
                    else:
                        fractional_digits = len(digits.group(3) or "")
                        integer_digits += 1 + fractional_digits
                        format_string = f'%0{integer_digits}.{fractional_digits}f'

                try:
                    _ = format_string % 0.5
                    self._format_string = format_string
                    self._value_converter = float
                except TypeError:
                    parser.error("Format specification does not contain a value escape ('%')")
                except ValueError:
                    try:
                        _ = format_string % 1
                        self._format_string = format_string
                        self._value_converter = lambda x: int(round(x))
                    except TypeError:
                        parser.error("Format specification does not contain a value escape ('%')")
                    except ValueError:
                        parser.error("Format specification cannot format numbers")

            if self._format_string and not self._mvc:
                self._mvc = self._to_mvc(self._format_string)

        def format(self, value: typing.Union[float, int], is_missing: bool = False) -> str:
            if is_missing or not isfinite(value):
                return self._mvc or ""
            return self._format_string % self._value_converter(value)

        def _from_variable_format(self, variable: Variable) -> typing.Optional[typing.Callable[[typing.Union[float, int], bool], str]]:
            try:
                format_code = variable.C_format
            except AttributeError:
                return None
            # Force zero padding and strip size specifier
            parsed_format = _ColumnVariableNumeric.Formatter._FORMAT_CODE.search(format_code)
            if parsed_format:
                if '0' not in parsed_format.group(1):
                    format_code = '%0' + parsed_format.group(1) + parsed_format.group(2)
                else:
                    format_code = '%' + parsed_format.group(1) + parsed_format.group(2)
                fractional_digits = parsed_format.group(3)
                if fractional_digits:
                    format_code += '.' + fractional_digits
                format_code += parsed_format.group(4)

            mvc = self._mvc
            if mvc is None:
                mvc = self._to_mvc(format_code)

            try:
                _ = format_code % 0.5
                converter = float
            except:
                try:
                    _ = format_code % 1
                    converter = lambda x: int(round(x))
                except:
                    _LOGGER.warning(f"Invalid format code '{format_code}' on variable '{variable.name}'", exc_info=True)
                    return None

            def formatter(value: typing.Union[float, int], is_missing: bool = False) -> str:
                if is_missing or not isfinite(value):
                    return mvc
                return format_code % converter(value)

            return formatter

        def for_variable(self, variable: Variable) -> typing.Callable[[typing.Union[float, int], bool], str]:
            if self._format_string:
                return self.format

            formatter = self._from_variable_format(variable)
            if formatter:
                return formatter

            def auto_format(value: typing.Union[float, int], is_missing: bool = False) -> str:
                if is_missing or not isfinite(value):
                    return self._mvc or ""
                return str(value)

            return auto_format

        def variable_mvc(self, variable: Variable) -> str:
            if self._mvc:
                return self._mvc
            try:
                format_code = variable.C_format
            except AttributeError:
                return ""
            return self._to_mvc(format_code)

    def __init__(self, source: "_ColumnVariableNumeric.Source",
                 formatter: typing.Callable[[typing.Any, bool], str], mvc: str):
        super().__init__(source)
        self.formatter = formatter
        self._mvc = mvc

    def __call__(self, time_value: float, time_indices: typing.List[typing.Optional[int]]) -> str:
        var_idx = time_indices[0]
        if var_idx is None:
            return self.formatter(None, True)
        return self.formatter(self.source(var_idx), False)

    @property
    def mvc(self) -> str:
        return self._mvc


class _ColumnVariableFloat(_ColumnVariableNumeric):
    pass


class _ColumnVariableInteger(_ColumnVariableNumeric):
    pass


class _StatisticsSource(_ColumnVariable.Source):
    def __init__(self, file: Dataset, variable: Variable):
        super().__init__(file, variable)
        self.statistics_origin = find_statistics_origin(self.variable)

    def header_name(self, modifier_suffix: str = "") -> str:
        if self.statistics_origin is not None:
            return self.construct_name(self.statistics_origin, modifier_suffix=modifier_suffix)
        return super().header_name(modifier_suffix=modifier_suffix)


class _ColumnVariableStatisticsCount(_ColumnVariableInteger):
    Source = _StatisticsSource

    @property
    def header_name(self) -> str:
        return self.source.header_name(modifier_suffix="N")

    @property
    def description(self) -> str:
        if self.source.statistics_origin is None:
            return super().description
        try:
            desc = self.source.statistics_origin.long_name
        except AttributeError:
            return super().description
        return desc + " (number of points)"

    @property
    def sort_key(self) -> typing.Tuple[int, str, int]:
        return 1000,  self.source.header_name(), 0


class _ColumnVariableStatisticsStdDev(_ColumnVariableFloat):
    Source = _StatisticsSource

    @property
    def header_name(self) -> str:
        return self.source.header_name(modifier_suffix="g")

    @property
    def description(self) -> str:
        if self.source.statistics_origin is None:
            return super().description
        try:
            desc = self.source.statistics_origin.long_name
        except AttributeError:
            return super().description
        return desc + " (stddev)"

    @property
    def sort_key(self) -> typing.Tuple[int, str, int]:
        return 2000, self.source.header_name(), 0


class _ColumnVariableStatisticsQuantile(_ColumnVariableFloat):
    class Source(_StatisticsSource):
        def __init__(self, file: Dataset, variable: Variable):
            super().__init__(file, variable)
            self.quantile_index = variable.dimensions.index('quantile')
            _, quantile_variable = find_dimension_values(variable.group(), 'quantile')
            self.quantile_values = quantile_variable[:].data

        def __call__(self, time_idx: int, quantile_idx: int) -> float:
            if not self.indexed_by_time:
                idx = list(self.index_suffix)
                idx[self.quantile_index] = quantile_idx
                return self._data[tuple(idx)]
            idx = list(self.index_suffix)
            idx[self.quantile_index - 1] = quantile_idx
            return self._data[(time_idx, *idx)]

        def __deepcopy__(self, memo):
            y = super().__deepcopy__(memo)
            y.quantile_index = self.quantile_index
            y.quantile_values = self.quantile_values
            return y

    def __init__(self, source: "_ColumnVariableStatisticsQuantile.Source",
                 formatter: typing.Callable[[typing.Any, bool], str], mvc: str, q: float):
        super().__init__(source, formatter, mvc)
        self.q = q

        def make_lookup() -> typing.Callable[[int], float]:
            idx_lower = int(np.searchsorted(source.quantile_values, q, side='left'))
            if idx_lower >= len(source.quantile_values):
                return lambda _: nan
            if abs(float(source.quantile_values[idx_lower]) - q) < 1e-6:
                return lambda time_idx: self.source(time_idx, idx_lower)
            if idx_lower == 0:
                return lambda _: nan
            idx_lower -= 1

            idx_upper = int(np.searchsorted(source.quantile_values, q, side='right'))
            if idx_upper == len(source.quantile_values):
                return lambda time_idx: self.source(time_idx, idx_lower)
            if idx_lower == idx_upper:
                return lambda _: nan

            q_lower = float(source.quantile_values[idx_lower])
            q_upper = float(source.quantile_values[idx_upper])
            if not isfinite(q_lower) or not isfinite(q_upper) or q_lower == q_upper:
                return lambda _: nan

            def lookup(time_idx: int) -> float:
                v_lower = self.source(time_idx, idx_lower)
                v_upper = self.source(time_idx, idx_upper)
                return v_lower + (v_upper - v_lower) * (q - q_lower) / (q_upper - q_lower)

            return lookup

        self._lookup = make_lookup()

    def __call__(self, time_value: float, time_indices: typing.List[typing.Optional[int]]) -> str:
        var_idx = time_indices[0]
        if var_idx is None:
            return self.formatter(None, True)
        return self.formatter(self._lookup(var_idx), False)

    @property
    def header_name(self) -> str:
        rq = min(max(round(self.q * 1E5), 1), 99999)
        return self.source.header_name(modifier_suffix=f"q{rq:05d}")

    @property
    def description(self) -> str:
        if self.source.statistics_origin is None:
            return super().description
        try:
            desc = self.source.statistics_origin.long_name
        except AttributeError:
            return super().description
        return desc + f" ({self.q * 100:.3f} quantile)"

    @property
    def sort_key(self) -> typing.Tuple[int, str, int]:
        return 3000, self.source.header_name(), 10000 + round(self.q * 1E5)


class _ColumnVariableMVCFlag(_ColumnVariable):
    def __init__(self, source: "_ColumnVariableMVCFlag.Source", at_end: bool = False):
        super().__init__(source)
        self.at_end = at_end

    @property
    def header_name(self) -> str:
        return super().header_name + "_MVC"

    @property
    def mvc(self) -> str:
        return "1"

    @property
    def description(self) -> str:
        return super().description + " - missing value flag"

    @property
    def sort_key(self) -> typing.Tuple[int, str, int]:
        if self.at_end:
            return 1, self.header_name, 0
        else:
            return 0, self.header_name, 1

    def __call__(self, time_value: float, time_indices: typing.List[typing.Optional[int]]) -> str:
        var_idx = time_indices[0]
        if var_idx is None:
            return "1"
        value = self.source(time_indices[0])
        if value is None or not isfinite(value):
            return "1"
        return "0"


class _ColumnVariableFlags(_ColumnVariable):
    def __init__(self, source: "_ColumnVariableFlags.Source"):
        super().__init__(source)
        self.flag_lookup = parse_flags(source.variable)

    @property
    def sort_key(self) -> typing.Tuple[int, str, int]:
        return -1, self.header_name, 0


class _ColumnVariableFlagsList(_ColumnVariableFlags):
    def __call__(self, time_value: float, time_indices: typing.List[typing.Optional[int]]) -> str:
        var_idx = time_indices[0]
        if var_idx is None:
            return self.mvc
        bits = int(self.source(time_indices[0]))
        set_flags: typing.Set[str] = set()
        for flag_bit, flag_name in self.flag_lookup.items():
            if (bits & flag_bit) == 0:
                continue
            set_flags.add(flag_name)
        return ";".join(sorted(set_flags))


class _ColumnVariableFlagsHex(_ColumnVariableFlags):
    def __init__(self, source: "_ColumnVariableFlagsHex.Source", prefix: str = ""):
        super().__init__(source)
        digits: int = 4
        for bit in self.flag_lookup.keys():
            bit_digit = int(ceil(log2(bit)))
            digits = max(digits, bit_digit+1)
        try:
            original_format = self.source.variable.C_format
            original_width = len(original_format % 0)
            digits = max(digits, original_width)
        except (AttributeError, ValueError, TypeError):
            pass

        self.format = f"{prefix}%0{digits}X"
        self._mvc = prefix + ("F" * digits)

    @property
    def mvc(self) -> str:
        return self._mvc

    def __call__(self, time_value: float, time_indices: typing.List[typing.Optional[int]]) -> str:
        var_idx = time_indices[0]
        if var_idx is None:
            return self.mvc
        bits = int(self.source(time_indices[0]))
        return self.format % bits


class _ColumnVariableFlagsBreakdown(_ColumnVariableFlags):
    def __init__(self, source: "_ColumnVariableFlagsBreakdown.Source", flag: str, bit: int):
        super().__init__(source)
        self.flag = flag
        self.bit = bit

    @property
    def header_name(self) -> str:
        return super().header_name + "_" + self.flag

    @property
    def mvc(self) -> str:
        return "Z"

    @property
    def description(self) -> str:
        return super().description + f" - bit {self.bit:X} - " + self.flag

    @property
    def sort_key(self) -> typing.Tuple[int, str, int]:
        return -1, super().header_name, self.bit

    def __call__(self, time_value: float, time_indices: typing.List[typing.Optional[int]]) -> str:
        var_idx = time_indices[0]
        if var_idx is None:
            return self.mvc
        bits = int(self.source(time_indices[0]))
        return "1" if (bits & self.bit) != 0 else "0"


class _ColumnVariableOther(_ColumnVariable):
    def __init__(self, source: "_ColumnVariable.Source", mvc: typing.Optional[str]):
        super().__init__(source)
        self._mvc = mvc or ""

    def __call__(self, time_value: float, time_indices: typing.List[typing.Optional[int]]) -> str:
        var_idx = time_indices[0]
        if var_idx is None:
            return self.mvc
        return str(self.source(time_indices[0]))

    @property
    def mvc(self) -> str:
        return self._mvc


class _Header:
    def __call__(self, columns: typing.List[_Column]) -> typing.Iterable[typing.Iterable[str]]:
        raise NotImplementedError


class _HeaderWavelength(_Header):
    def __call__(self, columns: typing.List[_Column]) -> typing.Iterable[typing.Iterable[str]]:
        wavelength_headers: typing.List[str] = list()
        for col_idx in range(len(columns)):
            col = columns[col_idx]
            if not isinstance(col, _ColumnVariable):
                continue
            if not col.source.wavelength:
                continue
            if col_idx > len(wavelength_headers):
                wavelength_headers += [""] * (col_idx - len(wavelength_headers))
            wavelength_headers.append(str(int(round(col.source.wavelength))))

        if not wavelength_headers:
            return []
        if len(wavelength_headers) < len(columns):
            wavelength_headers += [""] * (len(columns) - len(wavelength_headers))
        return [wavelength_headers]


class _HeaderFlagsBitList(_Header):
    def __call__(self, columns: typing.List[_Column]) -> typing.Iterable[typing.Iterable[str]]:
        n_headers: int = 0
        bit_lists: typing.List[typing.List[str]] = list()
        for col_idx in range(len(columns)):
            col = columns[col_idx]
            if not isinstance(col, _ColumnVariableFlags):
                continue
            sorted_flags: typing.List[int] = sorted(col.flag_lookup.keys())
            if not sorted_flags:
                continue

            if isinstance(col, _ColumnVariableFlagsHex):
                flag_format = col.format
            else:
                try:
                    flag_format = str(col.source.variable.C_format)
                except AttributeError:
                    flag_format = "%04X"

            n_headers = max(n_headers, len(sorted_flags))
            destination: typing.List[str] = list()
            while col_idx > len(bit_lists):
                bit_lists.append([])
            bit_lists.append(destination)
            for bit in sorted_flags:
                flag = col.flag_lookup[bit]
                destination.append((flag_format % bit) + " - " + flag)

        if not n_headers:
            return []
        rows: typing.List[typing.List[str]] = list()
        for row_idx in range(n_headers):
            bit_row: typing.List[str] = list()
            rows.append(bit_row)
            for col_idx in range(len(columns)):
                if col_idx >= len(bit_lists):
                    bit_row.append("")
                    continue
                col_bits = bit_lists[col_idx]
                row_offset = n_headers - len(col_bits)
                if row_idx < row_offset:
                    bit_row.append("")
                    continue
                bit_row.append(col_bits[row_idx - row_offset])
        return rows


class _HeaderFlagsBreakdownBit(_Header):
    def __call__(self, columns: typing.List[_Column]) -> typing.Iterable[typing.Iterable[str]]:
        bit_headers: typing.List[str] = list()

        for col_idx in range(len(columns)):
            col = columns[col_idx]
            if not isinstance(col, _ColumnVariableFlagsBreakdown):
                continue

            if isinstance(col, _ColumnVariableFlagsHex):
                flag_format = col.format
            else:
                try:
                    flag_format = str(col.source.variable.C_format)
                except AttributeError:
                    flag_format = "%04X"

            if col_idx > len(bit_headers):
                bit_headers += [""] * (col_idx - len(bit_headers))
            bit_headers.append((flag_format % col.bit) + " - " + col.flag)

        if not bit_headers:
            return []
        if len(bit_headers) < len(columns):
            bit_headers += [""] * (len(columns) - len(bit_headers))
        return [bit_headers]


class _HeaderDescription(_Header):
    def __call__(self, columns: typing.List[_Column]) -> typing.Iterable[typing.Iterable[str]]:
        return [
            [c.description for c in columns]
        ]


class _HeaderCutSize(_Header):
    def __call__(self, columns: typing.List[_Column]) -> typing.Iterable[typing.Iterable[str]]:
        cut_size_headers: typing.List[str] = list()
        for col_idx in range(len(columns)):
            col = columns[col_idx]
            if not isinstance(col, _ColumnVariable):
                continue
            if col.source.cut_size is None:
                continue
            if col_idx > len(cut_size_headers):
                cut_size_headers += [""] * (col_idx - len(cut_size_headers))
            if not isfinite(col.source.cut_size):
                cut_size_headers.append("WHOLE")
            else:
                cut_size_headers.append(str(col.source.cut_size))

        if not cut_size_headers:
            return []
        if len(cut_size_headers) < len(columns):
            cut_size_headers += [""] * (len(columns) - len(cut_size_headers))
        return [cut_size_headers]


class _HeaderMVC(_Header):
    def __call__(self, columns: typing.List[_Column]) -> typing.Iterable[typing.Iterable[str]]:
        return [
            [c.mvc for c in columns]
        ]


class _HeaderVariableName(_Header):
    def __call__(self, columns: typing.List[_Column]) -> typing.Iterable[typing.Iterable[str]]:
        return [
            [c.header_name for c in columns]
        ]


class _ExportStage(ExecuteStage):
    def __init__(self, execute: Execute, parser: argparse.ArgumentParser, args: argparse.Namespace):
        super().__init__(execute)

        self._prefix_columns: typing.List[_Column] = list()
        self._headers: typing.List[_Header] = list()

        self._round_times: bool = not args.no_round_times
        self._fill_archive_time: bool = not args.no_fill_times
        try:
            self._fill_interval: typing.Optional[float] = parse_interval_argument(args.fill_interval) if args.fill_interval else None
        except ValueError:
            _LOGGER.debug("Error parsing fill argument", exc_info=True)
            parser.error(f"invalid fill interval: '{args.fill_interval}'")
        self._keep_latest: bool = args.latest_in_gaps

        station = False
        time_epoch = False
        time_excel = True
        time_year = False
        time_doy = False
        time_julian = False
        numeric_only = False
        mvc = None
        mvc_flag = None
        header_stderr = False
        field_join = ','
        field_quote = None
        field_quote_escape = None
        station_header_column_name = "STN"

        if args.mode == 'xl' or args.mode == 'excel':
            mvc = ""
        elif args.mode == 'csv':
            time_excel = False
            time_year = True
            time_doy = True
            mvc = ""
        elif args.mode == 'r' or args.mode =='s':
            time_excel = False
            time_epoch = True
            numeric_only = True
            mvc = "NA"
        elif args.mode == 'idl':
            time_excel = False
            time_julian = True
            time_year = True
            time_doy = True
            mvc_flag = 'follow'
            numeric_only = True
            header_stderr = True
            field_join = ' '
            field_quote = ''
            field_quote_escape = ''
        elif args.mode == 'idllegacy':
            station = True
            time_excel = False
            time_year = True
            time_doy = True
            station_header_column_name = "Station"
        else:
            raise ValueError

        station = args.station_column if args.station_column is not None else station
        time_epoch = args.time_epoch if args.time_epoch is not None else time_epoch
        time_excel = args.time_excel if args.time_excel is not None else time_excel
        time_iso = args.time_iso
        time_fyear = args.time_fyear
        time_julian = args.time_julian if args.time_julian is not None else time_julian
        if args.time_yeardoy is not None:
            time_year = args.time_yeardoy
            time_doy = args.time_yeardoy
        elif args.time_doy is not None:
            time_doy = args.time_doy
        cut_string = args.cut_string
        # cut_split = args.cut_split
        enable_stddev = args.stddev
        enable_count = args.count
        enable_quantiles = args.quantiles

        field_quote = args.join_quote if args.join_quote is not None else field_quote
        field_quote_escape = args.join_quote_escape if args.join_quote_escape is not None else field_quote_escape
        if args.join == 'csv':
            field_join = ','
            if field_quote is None:
                field_quote = '"'
            if field_quote_escape is None:
                field_quote_escape = field_quote * 3
        elif args.join == 'space':
            field_join = ' '
            if field_quote is None:
                field_quote = '"'
            if field_quote_escape is None:
                field_quote_escape = field_quote * 3
        elif args.join == 'tab':
            field_join = '\t'
            if field_quote is None:
                field_quote = ''
            if field_quote_escape is None:
                field_quote_escape = field_quote * 3
        else:
            if field_join is None:
                field_join = ','
            if field_quote is None:
                field_quote = '"'
            if field_quote_escape is None:
                field_quote_escape = field_quote * 3
        self._field_join = field_join
        self._field_quote = field_quote
        self._field_quote_escape = field_quote_escape

        mvc = args.mvc if args.mvc is not None else mvc
        mvc_flag = args.mvc_flag if args.mvc_flag is not None else mvc_flag
        flags = args.flags
        numeric_format = args.format
        numeric_only = args.numeric_only if args.numeric_only is not None else numeric_only
        self._header_prefix = args.header_prefix or ""
        self._header_stderr = args.header_stderr if args.header_stderr is not None else header_stderr
        header_names = args.header_names
        header_description = args.header_description
        header_wavelength = args.header_wavelength
        header_mvcs = args.header_mvcs
        header_cut = args.header_cut
        header_flags = args.header_flags

        if self._header_stderr:
            execute.stderr_is_output = True

        if station:
            self._prefix_columns.append(_ColumnStation(station_header_column_name))
        if time_excel:
            self._prefix_columns.append(_ColumnTimeExcel())
        if time_epoch:
            self._prefix_columns.append(_ColumnTimeEpoch())
        if time_iso:
            self._prefix_columns.append(_ColumnTimeISO())
        if time_fyear:
            self._prefix_columns.append(_ColumnTimeFractionalYear())
        if time_julian:
            self._prefix_columns.append(_ColumnTimeJulian())
        if time_year:
            self._prefix_columns.append(_ColumnTimeYear())
        if time_doy:
            self._prefix_columns.append(_ColumnTimeDOY())
        if cut_string:
            self._prefix_columns.append(_ColumnTimeCutString())

        if header_wavelength:
            self._headers.append(_HeaderWavelength())
        if header_flags:
            if flags != 'breakdown':
                self._headers.append(_HeaderFlagsBitList())
            else:
                self._headers.append(_HeaderFlagsBreakdownBit())
        if header_description:
            self._headers.append(_HeaderDescription())
        if header_cut:
            self._headers.append(_HeaderCutSize())
        if header_mvcs:
            self._headers.append(_HeaderMVC())
        if header_names:
            self._headers.append(_HeaderVariableName())

        numeric_formatter = _ColumnVariableNumeric.Formatter(parser, numeric_format, mvc)
        wavelength_selector = WavelengthSelector.instantiate_if_available(execute)

        def fanout_variable(
                file: Dataset, variable: Variable,
                cls: typing.Type[_ColumnVariable],
                *args,
                is_statistics: bool = False,
                **kwargs,
        ) -> typing.List[_ColumnVariable]:
            source = cls.Source(file, variable)

            def fanout_dimension(
                    source: "_ColumnVariable.Source",
                    index_number: int, dimension_idx: int
            ) -> typing.List[_ColumnVariable]:
                if dimension_idx >= len(variable.dimensions):
                    return [cls(deepcopy(source), *args, **kwargs)]
                if variable.dimensions[dimension_idx] in ('cut_size', 'wavelength'):
                    return fanout_dimension(source, index_number + 1, dimension_idx + 1)
                if variable.dimensions[dimension_idx] == 'quantile' and isinstance(source, _ColumnVariableStatisticsQuantile.Source):
                    return fanout_dimension(source, index_number + 1, dimension_idx + 1)

                result: typing.List[_ColumnVariable] = list()
                for idx in range(variable.shape[dimension_idx]):
                    sub_source = deepcopy(source)
                    sub_idx = list(sub_source.index_suffix)
                    sub_idx[index_number] = idx
                    sub_source.index_suffix = tuple(sub_idx)
                    sub_source.name_suffix += str(idx + 1)
                    result.extend(fanout_dimension(sub_source, index_number + 1, dimension_idx + 1))
                return result

            def fanout_cut_size(source: "_ColumnVariable.Source", start_dimension_idx: int) -> typing.List[_ColumnVariable]:
                try:
                    cut_size_dimension_idx = variable.dimensions.index('cut_size')
                    cut_size_dimension, cut_size_variable = find_dimension_values(variable.group(), 'cut_size')
                except (ValueError, KeyError):
                    return fanout_dimension(source, 0, start_dimension_idx)
                cut_size_data = cut_size_variable[:].data

                suffixes: typing.List[str] = list()
                for c in cut_size_data:
                    c = float(c)
                    if not isfinite(c):
                        suffixes.append('')
                    elif c >= 10.0:
                        suffixes.append('0')
                    elif c >= 2.5:
                        suffixes.append('2')
                    else:
                        suffixes.append('1')

                result: typing.List[_ColumnVariable] = list()
                for idx in range(cut_size_dimension.size):
                    sub_source = deepcopy(source)
                    sub_idx = list(sub_source.index_suffix)
                    sub_idx[cut_size_dimension_idx - start_dimension_idx] = idx
                    sub_source.index_suffix = tuple(sub_idx)
                    sub_source.name_suffix += suffixes[idx]
                    sub_source.cut_size = float(cut_size_data[idx])
                    result.extend(fanout_dimension(sub_source, 0, start_dimension_idx))
                return result

            def fanout_wavelength(source: "_ColumnVariable.Source", start_dimension_idx: int) -> typing.List[_ColumnVariable]:
                try:
                    wavelength_dimension_idx = variable.dimensions.index('wavelength')
                    wavelength_dimension, wavelength_variable = find_dimension_values(variable.group(), 'wavelength')
                except (ValueError, KeyError):
                    return fanout_cut_size(source, start_dimension_idx)
                wavelength_data = wavelength_variable[:].data

                if wavelength_selector:
                    output_wavelengths = wavelength_selector(file, variable, retain_statistics=is_statistics)
                else:
                    output_wavelengths = list(range(wavelength_dimension.size))
                if not output_wavelengths:
                    return []
                output_wavelengths = sorted(output_wavelengths)
                suffixes = _assign_wavelength_suffixes(wavelength_data[output_wavelengths])

                result: typing.List[_ColumnVariable] = list()
                for indirect_idx in range(len(output_wavelengths)):
                    idx = output_wavelengths[indirect_idx]
                    sub_source = deepcopy(source)
                    sub_idx = list(sub_source.index_suffix)
                    sub_idx[wavelength_dimension_idx - start_dimension_idx] = idx
                    sub_source.index_suffix = tuple(sub_idx)
                    sub_source.name_suffix += suffixes[indirect_idx]
                    sub_source.wavelength = float(wavelength_data[idx])
                    result.extend(fanout_cut_size(sub_source, start_dimension_idx))
                return result

            if len(variable.dimensions) == 0 or variable.dimensions[0] != 'time':
                source.index_suffix = tuple([0] * len(variable.dimensions))
                return fanout_wavelength(source, 0)
            else:
                source.index_suffix = tuple([0] * (len(variable.dimensions) - 1))
                return fanout_wavelength(source, 1)

        def display_variable(variable: Variable) -> bool:
            if variable.name == 'system_flags':
                return True
            try:
                name = str(variable.variable_id)
            except AttributeError:
                return False
            if not name:
                return False
            return True

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

        def is_statistics_count(variable: Variable) -> bool:
            if not np.issubdtype(variable.dtype, np.integer):
                return False
            return is_statistics(variable, 'valid_count')

        def is_statistics_quantile(variable: Variable) -> bool:
            if not np.issubdtype(variable.dtype, np.floating):
                return False
            if not is_statistics(variable, 'quantiles'):
                return False
            return 'quantile' in variable.dimensions

        def construct_variable(file: Dataset, variable: Variable) -> typing.List[_ColumnVariable]:
            if len(variable.dimensions) == 0 or variable.dimensions[0] != 'time':
                if variable.name == 'wavelength':
                    # Handled in name fanout
                    return []
                if variable.name == 'system_flags':
                    # Invalid case
                    return []
                # Otherwise, if it's got variable name so handle it normally even if it's constant
                if not display_variable(variable):
                    return []
            else:
                if is_statistics_stddev(variable):
                    if not enable_stddev:
                        return []
                    base_var = find_statistics_origin(variable)
                    if not display_variable(base_var if base_var is not None else variable):
                        return []
                    return fanout_variable(file, variable, _ColumnVariableStatisticsStdDev,
                                           numeric_formatter.for_variable(variable),
                                           numeric_formatter.variable_mvc(variable),
                                           is_statistics=True)
                elif is_statistics_count(variable):
                    if not enable_count:
                        return []
                    base_var = find_statistics_origin(variable)
                    if not display_variable(base_var if base_var is not None else variable):
                        return []
                    return fanout_variable(file, variable, _ColumnVariableStatisticsCount,
                                           numeric_formatter.for_variable(variable),
                                           numeric_formatter.variable_mvc(variable),
                                           is_statistics=True)
                elif is_statistics_quantile(variable):
                    if not enable_quantiles:
                        return []
                    base_var = find_statistics_origin(variable)
                    if not display_variable(base_var if base_var is not None else variable):
                        return []
                    result: typing.List[_ColumnVariable] = list()
                    for q in (0.00135, 0.00621, 0.02275, 0.05, 0.06681, 0.15866, 0.25, 0.30854, 0.50,
                              0.69146, 0.75, 0.84134, 0.93319, 0.95, 0.97725, 0.99379, 0.99865):
                        result.extend(fanout_variable(
                            file, variable, _ColumnVariableStatisticsQuantile,
                            numeric_formatter.for_variable(variable),
                            numeric_formatter.variable_mvc(variable),
                            q,
                            is_statistics=True,
                        ))
                    return result
                elif not display_variable(variable):
                    return []

            if variable.name == 'system_flags':
                if numeric_only:
                    return []
                if not np.issubdtype(variable.dtype, np.integer):
                    return []
                if flags == '0x':
                    return fanout_variable(file, variable, _ColumnVariableFlagsHex, prefix='0x')
                elif flags == 'list':
                    return fanout_variable(file, variable, _ColumnVariableFlagsList)
                elif flags == 'breakdown':
                    result: typing.List[_ColumnVariable] = list()
                    for bit, flag in parse_flags(variable).items():
                        result.extend(fanout_variable(file, variable, _ColumnVariableFlagsBreakdown, flag, bit))
                    return result
                else:
                    return fanout_variable(file, variable, _ColumnVariableFlagsHex)

            if np.issubdtype(variable.dtype, np.floating):
                return fanout_variable(file, variable, _ColumnVariableFloat,
                                       numeric_formatter.for_variable(variable),
                                       numeric_formatter.variable_mvc(variable)) + (
                    fanout_variable(file, variable, _ColumnVariableMVCFlag, mvc_flag == 'end')
                    if mvc_flag in ('follow', 'end') else []
                )
            elif np.issubdtype(variable.dtype, np.integer):
                return fanout_variable(file, variable, _ColumnVariableInteger,
                                       numeric_formatter.for_variable(variable),
                                       numeric_formatter.variable_mvc(variable)) + (
                    fanout_variable(file, variable, _ColumnVariableMVCFlag, mvc_flag == 'end')
                    if mvc_flag in ('follow', 'end') else []
                )
            if numeric_only:
                return []

            return fanout_variable(file, variable, _ColumnVariableOther, mvc)

        self._construct_variable: typing.Callable[[Dataset, Variable], typing.List[_ColumnVariable]] = construct_variable

    async def __call__(self) -> None:
        variable_columns: typing.List[_ColumnVariable] = list()

        def integrate_group(file: Dataset, root: Dataset) -> None:
            if root.name == 'instrument':
                return

            for var in root.variables.values():
                if var.name == 'time':
                    continue
                variable_columns.extend(self._construct_variable(file, var))

            for sub in root.groups.values():
                integrate_group(file, sub)

        def make_index_lookup(output_times: np.ndarray, column_sources: typing.List[typing.List[Variable]]) -> typing.List[typing.List[np.ndarray]]:
            alignment_variable_indices: typing.Dict[Variable, np.ndarray] = dict()
            alignment_indices: typing.List[typing.List[np.ndarray]] = list()
            for vars in column_sources:
                var_indices: typing.List[np.ndarray] = list()
                alignment_indices.append(var_indices)
                for var in vars:
                    indices = alignment_variable_indices.get(var)
                    if indices is None:
                        indices = incoming_before(output_times, var[:].data)
                        alignment_variable_indices[var] = indices
                    var_indices.append(indices)
            return alignment_indices

        def join_fields(raw: typing.Iterable[str]) -> str:
            if not self._field_quote:
                return self._field_join.join(raw)
            escaped_fields: typing.List[str] = list()
            for f in raw:
                if not self._field_join in f:
                    escaped_fields.append(f)
                    continue
                if not self._field_quote_escape:
                    escaped_fields.append(self._field_quote + f + self._field_quote)
                    continue
                f = f.replace(self._field_quote, self._field_quote_escape)
                escaped_fields.append(self._field_quote + f + self._field_quote)
            return self._field_join.join(escaped_fields)

        open_files: typing.List[Dataset] = list()
        try:
            unique_stations: typing.Set[str] = set()
            file_stations: typing.Dict[Dataset, str] = dict()
            for input_file in self.data_file_progress("Scanning data"):
                input_file = Dataset(input_file, 'r')
                open_files.append(input_file)
                integrate_group(input_file, input_file)

                station_var = input_file.variables.get("station_name")
                if station_var is not None:
                    stn = str(station_var[0]).upper()
                    unique_stations.add(stn)
                    file_stations[input_file] = stn

            if len(variable_columns) == 0:
                _LOGGER.debug('No variables to export')
                if not self.exec.stderr_is_output:
                    sys.stderr.write("No data to export\n")
                return

            with self.progress("Aligning data time"):
                if len(unique_stations) > 1:
                    for col in variable_columns:
                        col.source.identifier_suffix += "_" + file_stations.get(col.source.file, "")

                variable_columns.sort(key=lambda x: x.sort_key)
                all_columns = self._prefix_columns + variable_columns

                for col in all_columns:
                    for file in open_files:
                        col.prepare_file(file)

                state_time_variables: typing.Set[Variable] = set()
                data_time_variables: typing.Set[Variable] = set()
                time_variable_lookup: typing.List[typing.List[Variable]] = list()
                source_is_state: typing.List[typing.List[bool]] = list()
                for col in all_columns:
                    col_time_sources: typing.List[Variable] = list()
                    time_variable_lookup.append(col_time_sources)

                    col_is_state: typing.List[bool] = list()
                    source_is_state.append(col_is_state)

                    for var, is_state in col.time_sources:
                        col_time_sources.append(var)
                        col_is_state.append(is_state)
                        if is_state:
                            state_time_variables.add(var)
                        else:
                            data_time_variables.add(var)

                if len(data_time_variables) == 0:
                    data_time_variables = state_time_variables

                if self._fill_interval:
                    begin_time = min([
                        int(v[:].data[0]) for v in data_time_variables
                    ])
                    end_time = max([
                        int(v[:].data[-1]) for v in data_time_variables
                    ])
                    interval_ms = int(ceil(self._fill_interval * 1000))
                    begin_rounded = int(floor(begin_time / interval_ms) * interval_ms)
                    end_rounded = int(floor(end_time / interval_ms) * interval_ms)
                    if end_rounded < end_time:
                        end_rounded += interval_ms
                    output_times = np.arange(begin_rounded, end_rounded + interval_ms, interval_ms, dtype=np.int64)
                elif self._fill_archive_time:
                    output_times = ArchiveRead.nominal_record_spacing(self.exec)
                else:
                    output_times = None
                if output_times is None:
                    output_times = peer_output_time(*[
                        v[:].data for v in data_time_variables
                    ], apply_rounding=self._round_times)

                if output_times.shape[0] == 0:
                    _LOGGER.debug('No time to export')
                    if not self.exec.stderr_is_output:
                        sys.stderr.write("No values in data to export\n")
                    return
                state_time_variables.clear()
                data_time_variables.clear()

                alignment_indices = make_index_lookup(output_times, time_variable_lookup)
                time_variable_lookup.clear()

            def do_export():
                for header_generator in self._headers:
                    for header_columns in header_generator(all_columns):
                        header_line = self._header_prefix + join_fields(header_columns)
                        try:
                            if self._header_stderr:
                                sys.stderr.write(header_line + "\n")
                                sys.stderr.flush()
                            else:
                                print(header_line, flush=True)
                        except BrokenPipeError:
                            pass

                column_values: typing.List[str] = list()
                for row_idx in range(output_times.shape[0]):
                    column_values.clear()
                    row_time = float(output_times[row_idx]) / 1000.0
                    for col_idx in range(len(all_columns)):
                        col = all_columns[col_idx]
                        col_aligned_indices = alignment_indices[col_idx]

                        source_indices: typing.List[typing.Optional[int]] = [
                            int(v[row_idx]) for v in col_aligned_indices
                        ]

                        if not self._keep_latest and row_idx > 0:
                            for source_num in range(len(source_indices)):
                                if source_is_state[col_idx][source_num]:
                                    continue
                                if col_aligned_indices[source_num][row_idx-1] != source_indices[source_num]:
                                    continue
                                # A repeat index means that there was a gap: multiple values inserted into different
                                # rows.  So we set it to unavailable if it wasn't state (which always has this true).
                                source_indices[source_num] = None

                        column_values.append(col(row_time, source_indices))
                    try:
                        print(join_fields(column_values))
                    except BrokenPipeError:
                        _LOGGER.debug("Output pipe closed, export aborted", exc_info=True)
                        break

            if not sys.stdout.isatty() and not self.exec.stderr_is_output:
                with self.progress("Exporting data"):
                    do_export()
            else:
                do_export()
        finally:
            for f in open_files:
                f.close()

