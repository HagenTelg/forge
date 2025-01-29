import typing
import asyncio
import logging
import argparse
import enum
import time
import shutil
import re
import os
import numpy as np
from math import floor, ceil
from pathlib import Path
from netCDF4 import Dataset, Variable, Group, EnumType, VLType
from forge.const import STATIONS, MAX_I64
from forge.timeparse import parse_time_bounds_arguments, parse_iso8601_time
from forge.formattime import format_iso8601_time
from forge.logicaltime import containing_year_range, start_of_year, end_of_year_ms, year_bounds_ms, containing_epoch_month_range, start_of_epoch_month_ms
from forge.archive.client import index_lock_key, index_file_name, data_lock_key, data_file_name
from forge.archive.client.connection import Connection, LockDenied, LockBackoff
from forge.archive.client.archiveindex import ArchiveIndex as BaseArchiveIndex
from forge.data.state import is_state_group, is_in_state_group
from forge.data.attrs import copy as copy_attrs
from forge.data.values import create_and_copy_variable
from forge.data.statistics import find_statistics_origin
from forge.data.merge.timeselect import selected_time_range
from ..execute import Execute, ExecuteStage, Progress, mkstemp_like
from .parse import split_tagged_regex
from . import ParseCommand, ParseArguments

_LOGGER = logging.getLogger(__name__)
_PLAIN_SPLIT = re.compile(r'[\s;,:]+')
_ALIAS_MATCH = re.compile(
    r'(?:(?P<instrument_id>[ASGMNQ][1-9]{2})(?P<instrument_record>[am]))'
    r'|(?P<auxiliary_code>(X[A-Z1-9])+a)'
    r'|(?:XI(?P<intensives_contam>C)?(?P<intensives_record>[sl]))'
)


class Command(ParseCommand):
    COMMANDS: typing.List[str] = ["get"]
    HELP: str = "read data from the archive"

    @classmethod
    def available(cls, cmd: ParseArguments.SubCommand, execute: "Execute") -> bool:
        return cmd.is_first

    @classmethod
    def install(cls, cmd: ParseArguments.SubCommand, execute: "Execute",
                parser: argparse.ArgumentParser) -> None:
        cls.install_pure(cmd, execute, parser)
        if cmd.is_last:
            from .export import Command as ExportCommand
            ExportCommand.install_pure(cmd, execute, parser)

    @classmethod
    def instantiate(cls, cmd: ParseArguments.SubCommand, execute: Execute,
                    parser: argparse.ArgumentParser,
                    args: argparse.Namespace, extra_args: typing.List[str]) -> None:
        read = cls.instantiate_pure(cmd, execute, parser, args, extra_args)
        if cmd.is_last:
            execute.install(FilterStage(execute, read.data_selection))
            from .export import Command as ExportCommand
            ExportCommand.instantiate_pure(cmd, execute, parser, args, extra_args)

    STATION_DESCRIPTION = r"""
The station specification consists of one or more station codes (usually GAW station IDs), separated by commas, 
  semicolons, colons, or spaces.
However, spaces and semicolons require quoting to prevent shell interpretation.  
The special value 'ALLSTATIONS' can also be used to select all possible stations.  
The station selection is not case sensitive.
"""
    DATA_DESCRIPTION = r"""
The data selection consists of one or more variable selectors or modifiers separated by commas.
If a component of the data selector begins with the form 'KEY:', then it is interpreted with respect to the key.
Otherwise, it is a raw regular expression selecting a variable code.
So a data selection like 'BsG_S11' selects the 'BsG' (green scattering) variable of the instrument 'S11'.
If no instrument suffix is provided, then variables from all instruments will match.
Likewise, if no wavelength suffix is present then all wavelengths will match.
So simply 'Bs' selects scattering from all instruments on all wavelengths, while 'Bs_S11' selects scattering
  on all wavelengths from the 'S11' instrument.
Wavelength selection only has any effect if the output of the command is wavelength limiting, so it is generally
  only meaningful when exporting CSV data.
The data selection can also 'EVERYTHING' (case insensitive) to select all variables (subject to active restrictions,
  below).
Aliases are also available to select data like old style records.
These aliases are of the form '<INSTRUMENT>a' or '<INSTRUMENT>m'.
So a specification like 'S11a' selects average/analysis variables from the S11 instrument.
There are also 'XIs' and 'XIl' aliases available for intensives variables in edited, clean, or averaged data.
For contaminated averaged data, there are 'XICs' and 'XICl' aliases.
Keys are not case sensitive. 
The key 'TAG' or 'TAGS' means that subsequent variables selected are required to have the specified tags (separated
  by semicolons, colons, or spaces) present.
This can be inverted by prefixing the tag with a '-', so that the variable must NOT have that tag present.
So 'TAG:cpc' selects variables from any type of CPC.
The key 'INSTRUMENT' or 'INSTRUMENT_ID' means that subsequent variables are only selected if from the given
  instrument ID as a regular expression (case insensitive).
So 'INSTRUMENT:S11' selects variables from the 'S11' instrument.
The key 'INSTRUMENT_TYPE' or 'SOURCE_TYPE' means that subsequent variables are only selected if from the given
  instrument type as a regular expression (case insensitive).
So 'SOURCE_TYPE:admagic250cpc' selects variables from a MAGIC 250 CPC.
Refer to the metadata for instrument type codes.
The key 'STATION', 'STATION_NAME' or 'STATION_CODE' means that subsequent variables are only selected if from the given
  station as a regular expression (case insensitive).
This is used to limit subsequent variables to the given station from a wider 'global' station selection.
The key 'VARIABLE_TYPE' or 'TYPE' means that subsequent variables are only selected if they are of the specified 
  type.
Variable types not case sensitive and must be one of 'ANY' (or empty), 'NORMAL'/'STANDARD'/'TIMESERIES'/'VARIABLE',
  'STATE', or 'OTHER'/'CONSTANT'/'CONST'/'PARAMETERS'.
So 'TYPE:STATE' eans that subsequent variables are only selected if they are classified as instrument state
  (usually not averaged and infrequently changing).
The key 'VARIABLE_NAME, 'NETCDF_VARIABLE', or 'NCVAR' selects variables based on the NetCDF variable name
  as a regular expression (case insensitive).
So 'NCVAR:scattering_coefficient' selects any scattering coefficient variables based on the NetCDF variable name.
The key 'STANDARD_NAME' or 'STDNAME' selects variables based on the CF conveientions standard name assigned.
So 'STANDARD_NAME:mole_fraction_of_ozone_in_air' selects ozone concentrations.
The key 'VARIABLE_ID' selects data as the raw variable ID described above.
So 'VARIABLE_ID:BsG_S11' is the same as just 'BsG_S11'.
"""
    TIME_DESCRIPTION = r"""
The time selection can either be a single time (e.x. '2024-02-01') which implies an end time (on day in this case),
  or a range of times '2024-02-01 2024-02-10' which selects a range of data (start inclusive, end exclusive).
All times are in UTC.
The start time may be 'undef' or 'none' for the beginning of data and the end time may be 'now' for the current time.
A week of year is specified as '<YEAR>w<WEEK>' (e.x. '2024w3').
A quarter of a year is specified as '<YEAR>Q<QUARTER>' (e.x. '2024Q1').
A fractional year is specified as '<YEAR>.<FRACTIONAL>' (e.x '2024.1234')
A start time and an interval can also be used (e.x. '2024-02-01 12h').
A DOY may be specified as '<YEAR>:<DOY>' or simply '<DOY>' with the year inferred (e.x. '2024:91').
ISO8601 times are interpreted in UTC only (e.x. '2024-02-12T12:00:00Z').
Both simple specifications ('s', 'm', 'h', 'd', and 'w') as well as ISO8601 durations (e.x. 'PT12H') are supported.
Finally, the time specification can also just be 'FOREVER' (case insensitive) to select all possible data.
"""
    ARCHIVE_DESCRIPTION = r"""
If the command ends with a series of valid archives (separated by commas, semicolons, colons, or spaces),
  then the data is read from those archives instead of the default 'raw' archive.
Archive selections are not case sensitive.
Available archive are 'raw', 'edited', 'clean', 'avgh', 'avgd', 'avgm', or 'ALLARCHIVES' for all available archives.
"""

    @classmethod
    def install_pure(cls, cmd: ParseArguments.SubCommand, execute: Execute,
                     parser: argparse.ArgumentParser, can_filter: bool = True) -> None:
        parser.add_argument('station',
                            help="station code")
        parser.add_argument('data',
                            help="data selections")
        parser.add_argument('time',
                            help="time selection")

        parser.add_argument('--incremental-lock',
                            dest='incremental_lock', action='store_true',
                            help="perform archive locking per year")

        if can_filter:
            parser.add_argument('--keep-all',
                                dest='keep_all', action='store_true',
                                help="retain all data, instead of filtering to the selection")

        parser.epilog = cls.STATION_DESCRIPTION + " " + cls.DATA_DESCRIPTION + " " + cls.TIME_DESCRIPTION + " " + cls.ARCHIVE_DESCRIPTION

    @classmethod
    def instantiate_pure(cls, cmd: ParseArguments.SubCommand, execute: Execute,
                         parser: argparse.ArgumentParser,
                         args: argparse.Namespace, extra_args: typing.List[str]) -> "ArchiveRead":
        read = ArchiveRead(execute, parser, args, extra_args)
        execute.install(read)
        return read


class _ArchiveIndex(BaseArchiveIndex):
    def __init__(self, station: str, json_data: bytes):
        super().__init__(json_data)
        self.station = station

    def match_variable_name(self, match: "re.Pattern") -> typing.Set[str]:
        result: typing.Set[str] = set()
        for check, contents in self.variable_names.items():
            if not match.fullmatch(check):
                continue
            result.update(contents)
        return result

    def match_standard_name(self, match: "re.Pattern") -> typing.Set[str]:
        result: typing.Set[str] = set()
        for check, contents in self.standard_names.items():
            if not match.fullmatch(check):
                continue
            result.update(contents)
        return result

    def match_variable_id(self, match: "re.Pattern") -> typing.Set[str]:
        def match_permute(name: str, instrument_id: str, wavelength_count: int) -> bool:
            if match.fullmatch(name):
                return True

            if '_' not in name:
                fullname = name + '_' + instrument_id
                if match.fullmatch(fullname):
                    return True

            if wavelength_count:
                wavelength_suffixes = [str(i+1) for i in range(wavelength_count)]
                if wavelength_count == 1 or wavelength_count == 3:
                    wavelength_suffixes.extend(('B', 'G', 'R'))
                if '_' in name:
                    prefix, suffix = name.split('_', 1)
                    for wl_suffix in wavelength_suffixes:
                        fullname = prefix + wl_suffix
                        if match.fullmatch(fullname):
                            return True
                        fullname += '_' + suffix
                        if match.fullmatch(fullname):
                            return True
                else:
                    for wl_suffix in wavelength_suffixes:
                        fullname = name + wl_suffix
                        if match.fullmatch(fullname):
                            return True
                        fullname += '_' + instrument_id
                        if match.fullmatch(fullname):
                            return True

            return False

        result: typing.Set[str] = set()
        for name, contents in self.variable_ids.items():
            for instrument_id, wavelength_count in contents.items():
                if instrument_id in result:
                    continue
                if not match_permute(name, instrument_id, wavelength_count):
                    continue
                result.add(instrument_id)
        return result


class DataSelection:
    _TOP_LEVEL_METADATA_VARIABLES: typing.Set[str] = frozenset({
        'station_name',
        'lat',
        'lon',
        'alt',
        'station_inlet_height',
    })
    _TOP_LEVEL_METADATA_GROUPS: typing.Set[str] = frozenset({
        'instrument',
    })

    class _VariableType(enum.Enum):
        TIMESERIES = "timeseries"
        STATE = "state"
        OTHER = "other"

    class _FileMatch:
        def __init__(self, require_tags: typing.Set[str], exclude_tags: typing.Set[str],
                     instrument_id: typing.Optional["re.Pattern"], instrument_type: typing.Optional["re.Pattern"],
                     station: typing.Optional["re.Pattern"]):
            self.require_tags = require_tags
            self.exclude_tags = exclude_tags
            self.instrument_id = instrument_id
            self.instrument_type = instrument_type
            self.station = station

        def __str__(self):
            parts: typing.List[str] = list()
            if self.require_tags or self.exclude_tags:
                parts.append(f"TAGS:{';'.join(sorted(self.require_tags) + ['-'+t for t in sorted(self.exclude_tags)])}")
            if self.instrument_id:
                parts.append(f"INSTRUMENT:{self.instrument_id.pattern}")
            if self.instrument_type:
                parts.append(f"TYPE:{self.instrument_type.pattern}")
            if self.station:
                parts.append(f"STATION:{self.station.pattern}")
            return ','.join(parts)

        def index_instrument_id_matches(self, index: _ArchiveIndex, instrument_id: str) -> bool:
            if self.instrument_id:
                if not self.instrument_id.fullmatch(instrument_id):
                    return False

            if self.station:
                if not self.station.fullmatch(index.station):
                    return False

            if self.instrument_type:
                for check in index.instrument_codes_for_instrument_id(instrument_id):
                    if self.instrument_type.fullmatch(check):
                        break
                else:
                    return False

            if self.require_tags:
                if not self.require_tags.issubset(index.tags_for_instrument_id(instrument_id)):
                    return False
            # Can't filter on excluded tags since the index is a union, so an individual file may lack them

            return True

        def file_matches(self, root: Dataset) -> bool:
            if self.instrument_id:
                if not self.instrument_id.fullmatch(str(getattr(root, 'instrument_id', ""))):
                    return False

            if self.station:
                station_name = root.variables.get("station_name")
                if station_name is None:
                    return False
                if not self.station.fullmatch(str(station_name[0])):
                    return False

            if self.instrument_type:
                try:
                    instrument_type = str(root.instrument)
                except AttributeError:
                    return False
                if not self.instrument_type.fullmatch(instrument_type):
                    return False

            if self.require_tags or self.exclude_tags:
                tags = set(str(getattr(root, 'forge_tags', "")).split())
                if self.require_tags:
                    if not self.require_tags.issubset(tags):
                        return False
                if self.exclude_tags:
                    if not self.exclude_tags.isdisjoint(tags):
                        return False

            return True

    class _VariableMatch:
        def __init__(self, file: "DataSelection._FileMatch",
                     variable_type: typing.Optional["DataSelection._VariableType"]):
            self.file = file
            self.variable_type = variable_type

        def __str__(self):
            parts: typing.List[str] = list()
            if self.variable_type:
                parts.append(self.variable_type.value.upper())
            file_match = str(self.file)
            if file_match:
                parts.append(file_match)
            if not parts:
                return ""
            return "@" + ",".join(parts)

        def integrate_index(self, index: _ArchiveIndex, instrument_ids: typing.Set[str]) -> None:
            raise NotImplementedError

        def variable_matches(self, variable: Variable, is_state: typing.Optional[bool]) -> bool:
            if self.variable_type:
                if 'time' not in variable.dimensions:
                    if self.variable_type != DataSelection._VariableType.OTHER:
                        return False
                else:
                    if is_state:
                        if self.variable_type != DataSelection._VariableType.STATE:
                            return False
                    else:
                        if self.variable_type != DataSelection._VariableType.TIMESERIES:
                            return False
            return True

        @staticmethod
        def _variable_wavelengths(variable: Variable) -> typing.Tuple[int, np.ndarray]:
            group = variable.group()
            while group is not None:
                wavelength_dimension = group.dimensions.get('wavelength')
                if wavelength_dimension is None:
                    group = group.parent
                    continue

                wavelength_variable = group.variables.get('wavelength')
                if wavelength_variable is not None:
                    return wavelength_dimension.size, wavelength_variable[:].data
                else:
                    return wavelength_dimension.size, np.empty((), dtype=np.float64)
            return 0, np.empty((), dtype=np.float64)

        def accepted_wavelengths(self, variable: Variable) -> typing.Set[int]:
            count, _ = self._variable_wavelengths(variable)
            return set(range(count))

    class _AllVariables(_VariableMatch):
        def __init__(self, file: "DataSelection._FileMatch",
                     variable_type: typing.Optional["DataSelection._VariableType"]):
            super().__init__(file, variable_type)

        def __str__(self):
            return "EVERYTHING" + super().__str__()

        def integrate_index(self, index: _ArchiveIndex, instrument_ids: typing.Set[str]) -> None:
            for check_id in index.known_instrument_ids:
                if check_id in instrument_ids:
                    continue
                if not self.file.index_instrument_id_matches(index, check_id):
                    continue
                instrument_ids.add(check_id)

    class _VariableRegex(_VariableMatch):
        def __init__(self, file: "DataSelection._FileMatch",
                     variable_type: typing.Optional["DataSelection._VariableType"],
                     match: "re.Pattern"):
            super().__init__(file, variable_type)
            self.match = match

        def __str__(self):
            return self.match.pattern + super().__str__()

    class _VariableName(_VariableRegex):
        def __str__(self):
            return "NCVAR:" + super().__str__()

        def integrate_index(self, index: _ArchiveIndex, instrument_ids: typing.Set[str]) -> None:
            for check_id in index.match_variable_name(self.match):
                if check_id in instrument_ids:
                    continue
                if not self.file.index_instrument_id_matches(index, check_id):
                    continue
                instrument_ids.add(check_id)

        def variable_matches(self, variable: Variable, is_state: typing.Optional[bool]) -> bool:
            if not super().variable_matches(variable, is_state):
                return False
            return bool(self.match.fullmatch(variable.name))

    class _StandardName(_VariableRegex):
        def __str__(self):
            return "STDNAME:" + super().__str__()

        def integrate_index(self, index: _ArchiveIndex, instrument_ids: typing.Set[str]) -> None:
            for check_id in index.match_standard_name(self.match):
                if check_id in instrument_ids:
                    continue
                if not self.file.index_instrument_id_matches(index, check_id):
                    continue
                instrument_ids.add(check_id)

        def variable_matches(self, variable: Variable, is_state: typing.Optional[bool]) -> bool:
            if not super().variable_matches(variable, is_state):
                return False
            try:
                standard_name = str(variable.standard_name)
            except AttributeError:
                return False
            return bool(self.match.fullmatch(standard_name))

    class _VariableID(_VariableRegex):
        def integrate_index(self, index: _ArchiveIndex, instrument_ids: typing.Set[str]) -> None:
            for check_id in index.match_variable_id(self.match):
                if check_id in instrument_ids:
                    continue
                if not self.file.index_instrument_id_matches(index, check_id):
                    continue
                instrument_ids.add(check_id)

        @staticmethod
        def _wavelength_suffix(wl: float) -> typing.Optional[str]:
            if wl < 400:
                return None
            elif wl < 500:
                return "B"
            elif wl < 600:
                return "G"
            elif wl < 750:
                return "R"
            return None

        @staticmethod
        def _get_instrument_id(variable: Variable) -> typing.Optional[str]:
            group = variable.group()
            while group is not None:
                instrument_id = getattr(group, 'instrument_id', None)
                if instrument_id:
                    return str(instrument_id)
                group = group.parent
            return None

        def variable_matches(self, variable: Variable, is_state: typing.Optional[bool]) -> bool:
            if not super().variable_matches(variable, is_state):
                return False

            try:
                name = str(variable.variable_id)
            except AttributeError:
                return False
            if self.match.fullmatch(name):
                return True

            instrument_id = None
            if '_' not in name:
                instrument_id = self._get_instrument_id(variable)
                if instrument_id:
                    fullname = name + '_' + instrument_id
                    if self.match.fullmatch(fullname):
                        return True

            if 'wavelength' in variable.dimensions:
                wavelength_suffixes: typing.Set[str] = set()
                n_wavelengths, wavelengths = self._variable_wavelengths(variable)
                wavelength_suffixes.update([str(i + 1) for i in range(n_wavelengths)])
                for wl in wavelengths:
                    suffix = self._wavelength_suffix(float(wl))
                    if suffix:
                        wavelength_suffixes.add(suffix)

                if '_' in name:
                    prefix, suffix = name.split('_', 1)
                    for wl_suffix in wavelength_suffixes:
                        fullname = prefix + wl_suffix
                        if self.match.fullmatch(fullname):
                            return True
                        fullname += '_' + suffix
                        if self.match.fullmatch(fullname):
                            return True
                elif instrument_id:
                    for wl_suffix in wavelength_suffixes:
                        fullname = name + wl_suffix
                        if self.match.fullmatch(fullname):
                            return True
                        fullname += '_' + instrument_id
                        if self.match.fullmatch(fullname):
                            return True
                else:
                    for wl_suffix in wavelength_suffixes:
                        fullname = name + wl_suffix
                        if self.match.fullmatch(fullname):
                            return True

            return False

        def accepted_wavelengths(self, variable: Variable) -> typing.Set[int]:
            try:
                name = str(variable.variable_id)
            except AttributeError:
                return set()
            if self.match.fullmatch(name):
                return super().accepted_wavelengths(variable)

            instrument_id = None
            if '_' not in name:
                instrument_id = self._get_instrument_id(variable)
                if instrument_id:
                    fullname = name + '_' + instrument_id
                    if self.match.fullmatch(fullname):
                        return super().accepted_wavelengths(variable)

            result: typing.Set[int] = set()
            n_wavelengths, wavelengths = self._variable_wavelengths(variable)

            def get_wavelength_suffixes() -> typing.Iterator[typing.Tuple[str, int]]:
                for idx in range(n_wavelengths):
                    yield str(idx+1), idx
                    suffix = self._wavelength_suffix(float(wavelengths[idx]))
                    if suffix:
                        yield suffix, idx

            if '_' in name:
                prefix, suffix = name.split('_', 1)
                for wl_suffix, wl_idx in get_wavelength_suffixes():
                    fullname = prefix + wl_suffix
                    if self.match.fullmatch(fullname):
                        result.add(wl_idx)
                        continue
                    fullname += '_' + suffix
                    if self.match.fullmatch(fullname):
                        result.add(wl_idx)
                        continue
            elif instrument_id:
                for wl_suffix, wl_idx in get_wavelength_suffixes():
                    fullname = name + wl_suffix
                    if self.match.fullmatch(fullname):
                        result.add(wl_idx)
                        continue
                    fullname += '_' + instrument_id
                    if self.match.fullmatch(fullname):
                        result.add(wl_idx)
                        continue
            else:
                for wl_suffix, wl_idx in get_wavelength_suffixes():
                    fullname = name + wl_suffix
                    if self.match.fullmatch(fullname):
                        result.add(wl_idx)

            return result

    def __init__(self, selection: str, parser: argparse.ArgumentParser):
        self._variables: typing.List[DataSelection._VariableMatch] = list()

        require_tags: typing.Set[str] = set()
        exclude_tags: typing.Set[str] = set()
        instrument_id: typing.Optional["re.Pattern"] = None
        instrument_type: typing.Optional["re.Pattern"] = None
        station: typing.Optional["re.Pattern"] = None
        variable_type: typing.Optional[DataSelection._VariableType] = None
        for component_type, component_value in split_tagged_regex(selection):
            component_type = component_type.lower()
            if component_type == 'tag' or component_type == 'tags':
                component_value = component_value.strip().lower()
                require_tags.clear()
                exclude_tags.clear()
                for tag in _PLAIN_SPLIT.split(component_value):
                    tag = tag.strip()
                    if tag.startswith('-'):
                        exclude_tags.add(tag[1:])
                    elif tag.startswith('+'):
                        require_tags.add(tag[1:])
                    else:
                        require_tags.add(tag)
                continue
            elif component_type == 'instrument' or component_type == 'instrument_id':
                component_value = component_value.strip()
                if not component_value:
                    instrument_id = None
                else:
                    try:
                        instrument_id = re.compile(component_value, flags=re.IGNORECASE)
                    except re.error:
                        _LOGGER.debug(f"Error parsing instrument ID {component_value}", exc_info=True)
                        parser.error(f"Error parsing instrument ID '{component_value}'")
                continue
            elif component_type == 'instrument_type' or component_type == 'source_type':
                component_value = component_value.strip()
                if not component_value:
                    instrument_type = None
                else:
                    try:
                        instrument_type = re.compile(component_value, flags=re.IGNORECASE)
                    except re.error:
                        _LOGGER.debug(f"Error parsing instrument type {component_value}", exc_info=True)
                        parser.error(f"Error parsing instrument type '{component_value}'")
                continue
            elif component_type == 'station' or component_type == 'station_name' or component_type == 'station_code':
                component_value = component_value.strip()
                if not component_value:
                    station = None
                else:
                    try:
                        station = re.compile(component_value, flags=re.IGNORECASE)
                    except re.error:
                        _LOGGER.debug(f"Error parsing station {component_value}", exc_info=True)
                        parser.error(f"Error parsing station '{component_value}'")
                continue
            elif component_type == 'variable_type' or component_type == 'type':
                component_value = component_value.lower().strip()
                if component_value == 'any' or component_value == '':
                    variable_type = None
                elif component_value in ('normal', 'standard', 'timeseries', 'variable'):
                    variable_type = DataSelection._VariableType.TIMESERIES
                elif component_value == 'state':
                    variable_type = DataSelection._VariableType.STATE
                elif component_value in ('other', 'constant', 'const', 'parameters'):
                    variable_type = DataSelection._VariableType.OTHER
                else:
                    parser.error(f"Unrecognized variable type '{component_value}'")
                continue

            file_match = self._FileMatch(require_tags, exclude_tags, instrument_id, instrument_type, station)

            if component_type == '' and component_value == 'everything':
                self._variables.append(self._AllVariables(file_match, variable_type))
                continue
            if component_type == '':
                alias = _ALIAS_MATCH.fullmatch(component_value)
                if alias:
                    match_id = alias.group('instrument_id')
                    if match_id:
                        file_match.instrument_id = re.compile(match_id)
                        match_record = alias.group('instrument_record')
                        if match_record == 'm':
                            self._variables.append(self._VariableID(
                                file_match,
                                variable_type if variable_type else DataSelection._VariableType.TIMESERIES,
                                re.compile(r'T.*|P.*|U.*|I.*|V.*|A.*|C.*|TD.*')
                            ))
                        elif match_record == 'a':
                            self._variables.append(self._VariableID(
                                file_match,
                                variable_type if variable_type else DataSelection._VariableType.TIMESERIES,
                                re.compile(r'Bs|Bbs|Ba|Be|N|Nb|Ns|Nn|X|Ir|T1?|P1?|U1?|Q1?')
                            ))
                            self._variables.append(self._VariableName(
                                file_match,
                                variable_type if variable_type else DataSelection._VariableType.TIMESERIES,
                                re.compile(r'system_flags')
                            ))
                        else:
                            raise RuntimeError
                        continue

                    match_aux = alias.group('auxiliary_code')
                    if match_aux:
                        file_match.instrument_id = re.compile(match_aux)
                        self._variables.append(self._VariableID(
                            file_match,
                            variable_type if variable_type else DataSelection._VariableType.TIMESERIES,
                            re.compile(r'.+')
                        ))
                        self._variables.append(self._VariableName(
                            file_match,
                            variable_type if variable_type else DataSelection._VariableType.TIMESERIES,
                            re.compile(r'system_flags')
                        ))

                    intensives_code = alias.group('intensives_record')
                    if intensives_code:
                        if alias.group('intensives_contam'):
                            file_match.instrument_id = re.compile(r'XIC')
                        else:
                            file_match.instrument_id = re.compile(r'XI')
                        if intensives_code == 's':
                            self._variables.append(self._VariableID(
                                file_match,
                                DataSelection._VariableType.TIMESERIES,
                                re.compile(r'N|BsG|BaG|BeG|ZSSAG|ZAngBsG|ZAngBaG|ZBfGr|ZGG|ZRFEG')
                            ))
                        elif intensives_code == 'l':
                            self._variables.append(self._VariableID(
                                file_match,
                                DataSelection._VariableType.TIMESERIES,
                                re.compile(r'N|Bs|Ba|Be|ZSSA|ZAngBs|ZAngBa|ZBfr|ZG|ZRFE')
                            ))
                        else:
                            raise RuntimeError
                        continue

                    raise RuntimeError

            if component_type == 'variable_name' or component_type == 'netcdf_variable' or component_type == 'ncvar':
                try:
                    match = re.compile(component_value, flags=re.IGNORECASE)
                except re.error:
                    _LOGGER.debug(f"Error parsing variable name {component_value}", exc_info=True)
                    parser.error(f"Error parsing variable name '{component_value}'")
                    raise
                self._variables.append(self._VariableName(file_match, variable_type, match))
            elif component_type == 'standard_name' or component_type == 'stdname':
                try:
                    match = re.compile(component_value, flags=re.IGNORECASE)
                except re.error:
                    _LOGGER.debug(f"Error parsing standard name {component_value}", exc_info=True)
                    parser.error(f"Error parsing standard name '{component_value}'")
                    raise
                self._variables.append(self._StandardName(file_match, variable_type, match))
            elif component_type == '' or component_type == 'variable_id':
                try:
                    match = re.compile(component_value)
                except re.error:
                    _LOGGER.debug(f"Error parsing variable ID {component_value}", exc_info=True)
                    parser.error(f"Error parsing variable ID '{component_value}'")
                    raise
                self._variables.append(self._VariableID(file_match, variable_type, match))
            else:
                _LOGGER.debug(f"Unrecognized selection type {component_type} with value {component_value}")
                parser.error(f"Unrecognized selection '{component_type}:{component_value}'")

    def __str__(self):
        parts: typing.List[str] = [str(v) for v in self._variables]
        return " | ".join(parts)

    def index_instrument_ids(self, index: _ArchiveIndex) -> typing.Set[str]:
        result: typing.Set[str] = set()
        for var in self._variables:
            var.integrate_index(index, result)
        return result

    def accept_any_in_file(self, file: Dataset) -> bool:
        def walk_variables(group: Dataset, check: DataSelection._VariableMatch,
                           is_parent_state: typing.Optional[bool] = None) -> bool:
            if group.name == "instrument":
                return False

            is_state = is_state_group(group)
            if is_state is None:
                is_state = is_parent_state

            for var in group.variables.values():
                if check.variable_matches(var, is_state):
                    return True

            for sub in group.groups.values():
                if walk_variables(sub, check, is_state):
                    return True

            return False

        for var in self._variables:
            if not var.file.file_matches(file):
                continue
            if walk_variables(file, var):
                return True
        return False

    def accept_whole_file(self, file: Dataset,
                          not_before_ms: typing.Optional[int] = None,
                          not_after_ms: typing.Optional[int] = None,
                          accept_statistics: bool = False) -> bool:
        time_coverage_start = getattr(file, 'time_coverage_start', None)
        if time_coverage_start is not None:
            time_coverage_start = int(floor(parse_iso8601_time(str(time_coverage_start)).timestamp() * 1000.0))
            if not_before_ms is not None and time_coverage_start < not_before_ms:
                return False
        elif not_before_ms is not None:
            return False

        time_coverage_end = getattr(file, 'time_coverage_end', None)
        if time_coverage_end is not None:
            time_coverage_end = int(ceil(parse_iso8601_time(str(time_coverage_end)).timestamp() * 1000.0))
            if not_after_ms is not None and time_coverage_end > not_after_ms:
                return False
        elif not_after_ms is not None:
            return False

        def is_dimension_variable(var: Variable) -> bool:
            if len(var.dimensions) != 1:
                return False
            if var.dimensions[0] == 'time':
                return False
            group = var.group()
            while group is not None:
                if var.name in group.dimensions:
                    return True
                group = group.parent
            return False

        for name, var in file.variables.items():
            if len(var.dimensions) == 0 and name in self._TOP_LEVEL_METADATA_VARIABLES:
                continue
            if not self.accept_variable(file, var):
                if not is_dimension_variable(var):
                    return False

        def walk_group(file: Dataset, group: Dataset) -> bool:
            for var in group.variables.values():
                if self.accept_variable(file, var):
                    continue
                if is_dimension_variable(var):
                    continue
                if accept_statistics:
                    base_var = find_statistics_origin(var)
                    if base_var is not None and self.accept_variable(file, base_var):
                        continue
                return False
            for g in group.groups.values():
                if not walk_group(file, g):
                    return False
            return True

        for name, group in file.groups.items():
            if name in self._TOP_LEVEL_METADATA_GROUPS:
                continue
            if not walk_group(file, group):
                return False

        return True

    def accept_variable(self, file: Dataset, var: Variable) -> bool:
        is_state = is_in_state_group(var)

        for check in self._variables:
            if not check.file.file_matches(file):
                continue
            if check.variable_matches(var, is_state):
                return True
        if var.name == 'cut_size':
            for sibling in var.group().variables.values():
                if 'cut_size' not in getattr(sibling, 'ancillary_variables', "").split():
                    continue
                for check in self._variables:
                    if not check.file.file_matches(file):
                        continue
                    if check.variable_matches(sibling, is_state):
                        return True
        return False

    def filter_file(self, input_file: Dataset, output_file: Dataset,
                    not_before_ms: typing.Optional[int] = None,
                    not_after_ms: typing.Optional[int] = None,
                    accept_statistics: bool = False) -> bool:
        copy_attrs(input_file, output_file)

        any_accepted: bool = False

        def copy_accepted(source_var: Variable, group_path: typing.List[str]) -> None:
            nonlocal any_accepted

            destination_group: Dataset = output_file
            source_group = input_file
            for p in group_path:
                source_group = source_group.groups[p]
                check_group = destination_group.groups.get(p)
                if check_group is None:
                    check_group = destination_group.createGroup(p)
                    copy_attrs(source_group, check_group)
                destination_group = check_group

            created_time_var: typing.Optional[Variable] = None

            for dimension_name in source_var.dimensions:
                source_group = source_var.group()
                target_group = destination_group
                while True:
                    source_dimension = source_group.dimensions.get(dimension_name)
                    if source_dimension is None:
                        source_group = source_group.parent
                        target_group = target_group.parent
                        continue

                    if dimension_name in target_group.dimensions:
                        break

                    if source_dimension.isunlimited():
                        target_group.createDimension(dimension_name, None)
                    else:
                        target_group.createDimension(dimension_name, source_dimension.size)

                    source_dim_var = source_group.variables.get(dimension_name)
                    if source_dim_var is not None and len(source_dim_var.dimensions) == 1 and source_dim_var.dimensions[0] == dimension_name:
                        assert dimension_name not in target_group.variables

                        if dimension_name != 'time' or (not_before_ms is None and not_after_ms is None):
                            create_and_copy_variable(source_dim_var, target_group)
                        else:
                            created_time_var = create_and_copy_variable(source_dim_var, target_group, copy_values=False)

                    break

            def apply_time_selection() -> typing.Optional[typing.Tuple]:
                if len(source_var.dimensions) == 0 or source_var.dimensions[0] != 'time':
                    return (...,)

                source_group = source_var.group()
                while True:
                    source_time_var = source_group.variables.get('time')
                    if source_time_var is not None:
                        break
                    source_group = source_group.parent

                if not_after_ms is None and not_after_ms is None:
                    if created_time_var is not None:
                        created_time_var[:] = source_time_var[:]
                    return (...,)

                time_selector = selected_time_range(
                    source_time_var[:].data,
                    not_before_ms or -MAX_I64,
                    not_after_ms or MAX_I64,
                    is_in_state_group(source_var),
                )
                if time_selector is None:
                    return None
                time_selector = slice(*time_selector)

                if created_time_var is not None:
                    created_time_var[:] = source_time_var[time_selector]

                return time_selector, ...

            source_selector = apply_time_selection()
            if source_selector is None:
                return

            nonlocal any_accepted
            any_accepted = True

            if isinstance(source_var.datatype, EnumType) and source_var.datatype.name not in destination_group.enumtypes:
                destination_group.createEnumType(source_var.datatype.name, source_var.datatype.dtype,
                                                 source_var.datatype.enum_dict)

            # Might already exist if it's a dimension variable
            destination_var = destination_group.variables.get(source_var.name)
            if destination_var is None:
                destination_var = create_and_copy_variable(source_var, destination_group, copy_values=False)
            source_values = source_var[source_selector]
            if isinstance(destination_var.datatype, VLType):
                for idx in np.ndindex(source_values.shape):
                    destination_var[idx] = source_values[idx]
            else:
                destination_var[...] = source_values[...]

        def filter_group(source_group: Group, path: typing.List[str]) -> None:
            for name, var in source_group.variables.items():
                if self.accept_variable(input_file, var):
                    copy_accepted(var, path)
                    continue
                if accept_statistics:
                    base_var = find_statistics_origin(var)
                    if base_var is not None and self.accept_variable(input_file, base_var):
                        copy_accepted(var, path)
                        continue

            for name, group in source_group.groups.items():
                filter_group(group, path + [name])

        def direct_copy_group(source_group: Group, destination_root: Dataset) -> None:
            destination = destination_root.createGroup(source_group.name)
            copy_attrs(source_group, destination)

            for name, input_dimension in source_group.dimensions.items():
                destination.createDimension(name, input_dimension.size)
            for name, input_enum in source_group.enumtypes.items():
                destination.createEnumType(name, input_enum.datatype, input_enum.enum_dict)
            for name, input_variable in source_group.variables.items():
                create_and_copy_variable(input_variable, destination)

            for input_group in source_group.groups.values():
                direct_copy_group(input_group, destination)

        for name, var in input_file.variables.items():
            if len(var.dimensions) == 0 and name in self._TOP_LEVEL_METADATA_VARIABLES:
                create_and_copy_variable(var, output_file)
                continue
            if not self.accept_variable(input_file, var):
                continue
            copy_accepted(var, [])

        for name, group in input_file.groups.items():
            if name in self._TOP_LEVEL_METADATA_GROUPS:
                direct_copy_group(group, output_file)
                continue
            filter_group(group, [name])

        return any_accepted

    def accepted_wavelengths(self, file: Dataset, var: Variable, accept_statistics: bool = False) -> typing.Set[int]:
        assert 'wavelength' in var.dimensions

        is_state = is_in_state_group(var)
        result: typing.Set[int] = set()
        for check in self._variables:
            if not check.file.file_matches(file):
                continue
            if check.variable_matches(var, is_state):
                result.update(check.accepted_wavelengths(var))
                continue
            if accept_statistics:
                base_var = find_statistics_origin(var)
                if base_var is not None and check.variable_matches(base_var, is_state):
                    result.update(check.accepted_wavelengths(base_var))
                    continue
        return result


class ArchiveRead(ExecuteStage):
    _VALID_ARCHIVES: typing.Set[str] = frozenset({
        'raw', 'edited', 'clean', 'avgh', 'avgd', 'avgm'
    })

    def __init__(self, execute: Execute, parser: argparse.ArgumentParser,
                 args: argparse.Namespace, extra_args: typing.List[str]):
        super().__init__(execute)
        try:
            self.keep_all: bool = args.keep_all
        except AttributeError:
            self.keep_all: bool = True

        self.incremental_lock = args.incremental_lock

        self.stations: typing.Set[str] = set()
        for stn in _PLAIN_SPLIT.split(args.station.strip()):
            stn = stn.lower().strip()
            if stn == "allstations":
                self.stations.update(STATIONS)
                continue
            if not stn or stn not in STATIONS:
                parser.error(f"Unknown station '{stn.upper()}'")
            self.stations.add(stn)
        if not self.stations:
            parser.error(f"No valid station specified")

        self.archives: typing.Set[str] = {'raw'}
        if extra_args:
            try:
                candidate_archives: typing.Set[str] = set()
                for a in _PLAIN_SPLIT.split(extra_args[-1].strip()):
                    a = a.lower().strip()
                    if a == "allarchives":
                        candidate_archives.update(self._VALID_ARCHIVES)
                        continue
                    if not a or a not in self._VALID_ARCHIVES:
                        _LOGGER.debug("Archive argument match failed for %s", a)
                        raise ValueError
                    candidate_archives.add(a)
                if candidate_archives:
                    self.archives = candidate_archives
                    extra_args = extra_args[:-1]
            except ValueError:
                pass

        time_args = [args.time] + extra_args
        try:
            start, end = parse_time_bounds_arguments(time_args)
        except ValueError:
            _LOGGER.debug("Error parsing time arguments", exc_info=True)
            parser.error(f"The time specification '{' '.join(time_args)}' is not valid")
            raise
        self.start_ms = int(floor(start.timestamp() * 1000))
        self.end_ms = int(ceil(end.timestamp() * 1000))

        self.data_selection = DataSelection(args.data, parser)

        _LOGGER.debug("Archive read on %s", self)

    def __str__(self):
        return f"{(' '.join(self.stations)).upper()}/{(' '.join(self.archives)).upper()} at ({format_iso8601_time(self.start_ms / 1000.0)},{format_iso8601_time(self.end_ms / 1000.0)}) selected: {str(self.data_selection)}"

    async def _filter_file(self, output_file: Path, archive: str, archive_path: str,
                           created_files: typing.List[Path], filter_tasks: typing.List[asyncio.Future]) -> None:
        def apply_filter(output_file: Path, archive: str, archive_path: str):
            contents = Dataset(str(output_file), 'r+')
            try:
                if not self.data_selection.accept_any_in_file(contents):
                    try:
                        output_file.unlink()
                    except (OSError, FileNotFoundError):
                        pass
                    _LOGGER.debug(f"File {archive_path} rejected by post-fetch filtering")
                    return
                if len(self.archives) != 1:
                    # Set this so merging can differentiate when needed
                    contents.forge_archive = archive.upper()
            finally:
                contents.close()

        async def filter_task():
            await asyncio.get_event_loop().run_in_executor(self.netcdf_executor, apply_filter,
                                                           output_file, archive, archive_path)
            _LOGGER.debug(f"Fetched archive file {archive_path} into '{output_file}'")
            created_files.append(output_file)

        filter_tasks.append(asyncio.ensure_future(filter_task()))

    async def _fetch_file(self, connection: Connection, archive: str, archive_path: str,
                          created_files: typing.List[Path], filter_tasks: typing.List[asyncio.Future]) -> bool:
        archive_parts = Path(archive_path)
        output_file = self.data_path / archive_parts.name
        if output_file.exists():
            fd, temp_name = mkstemp_like(output_file)
            output_file = Path(self.data_path.name) / temp_name
            write_file = os.fdopen(fd, mode='wb')
        else:
            write_file = output_file.open("wb")

        try:
            with write_file as f:
                await connection.read_file(archive_path, f)
        except FileNotFoundError:
            try:
                output_file.unlink()
            except (OSError, FileNotFoundError):
                pass
            _LOGGER.debug(f"File {archive_path} does not exist in the archive")
            return False

        await self._filter_file(output_file, archive, archive_path, created_files, filter_tasks)
        return True

    async def _read_year(self, station: str, archive: str, connection: Connection, current_year: int,
                         progress: Progress, fraction_start: float, fraction_end: float) -> typing.List[Path]:
        year_start = start_of_year(current_year)
        try:
            index_contents = await connection.read_bytes(
                index_file_name(station, archive, year_start))
        except FileNotFoundError:
            _LOGGER.debug(f"No index for {station.upper()}/{archive.upper()}/{current_year}")
            return []
        index = _ArchiveIndex(station, index_contents)

        read_instrument_ids = self.data_selection.index_instrument_ids(index)
        if not read_instrument_ids:
            _LOGGER.debug(f"No instrument ID matches for {station.upper()}/{archive.upper()}/{current_year}")
            return []
        _LOGGER.debug(f"Instrument ID matches for {station.upper()}/{archive.upper()}/{current_year}: {' '.join(read_instrument_ids)}")

        await connection.lock_read(data_lock_key(station, archive),
                                   self.start_ms, self.end_ms)

        progress.set_title(f"Reading {station.upper()}/{archive.upper()}/{current_year} data")

        filter_tasks: typing.List[asyncio.Future] = list()
        completed_files: int = 0
        total_files: int = 0

        async def progress_filter(timeout: typing.Optional = 0):
            nonlocal completed_files

            if not filter_tasks:
                return

            done, pending = await asyncio.wait(filter_tasks, timeout=timeout, return_when=asyncio.FIRST_COMPLETED)
            for v in done:
                v.result()
            completed_files += len(done)
            filter_tasks.clear()
            filter_tasks.extend(pending)

            step_complete = completed_files / total_files
            progress(fraction_start + step_complete * (fraction_end - fraction_start))

        created_files: typing.List[Path] = list()
        if archive in ('avgd', 'avgm'):
            total_files = len(read_instrument_ids)
            for instrument_id in read_instrument_ids:
                await progress_filter()
                if not await self._fetch_file(
                    connection, archive,
                    data_file_name(station, archive, instrument_id, year_start),
                    created_files, filter_tasks
                ):
                    completed_files += 1
        else:
            year_start_ms = int(floor(year_start * 1000))
            year_end_ms = end_of_year_ms(current_year)
            read_start_day = int(floor(max(year_start_ms, self.start_ms) / (24 * 60 * 60 * 1000)))
            read_end_day = int(ceil(min(year_end_ms, self.end_ms) / (24 * 60 * 60 * 1000)))
            total_days = read_end_day - read_start_day
            total_files = len(read_instrument_ids) * total_days
            for instrument_id in read_instrument_ids:
                for day_number in range(read_start_day, read_end_day):
                    await progress_filter()
                    if not await self._fetch_file(
                        connection, archive,
                        data_file_name(station, archive, instrument_id, day_number * 24 * 60 * 60),
                        created_files, filter_tasks,
                    ):
                        completed_files += 1

        _LOGGER.debug(f"Waiting for read completion on {len(filter_tasks)} files")
        while filter_tasks:
            await progress_filter(None)

        return created_files

    async def _read_single_lock(self, connection: Connection, station: str, archive: str):
        start_year, end_year = containing_year_range(self.start_ms / 1000.0, self.end_ms / 1000.0)

        _LOGGER.debug("Reading archive for %s/%s", station, archive)

        backoff = LockBackoff()
        with self.progress(f"Reading {station.upper()}/{archive.upper()} data") as progress:
            while True:
                created_files: typing.List[Path] = list()
                try:
                    async with connection.transaction():
                        await connection.lock_read(index_lock_key(station, archive),
                                                   self.start_ms, self.end_ms)

                        total_years = end_year - start_year
                        per_year_fraction = 1.0 / total_years

                        for current_year in range(start_year, end_year):
                            fraction_start = (current_year - start_year) * per_year_fraction
                            fraction_end = fraction_start + per_year_fraction
                            created_files.extend(await self._read_year(
                                station, archive, connection, current_year, progress,
                                fraction_start, fraction_end,
                            ))
                except LockDenied as ld:
                    progress.set_title(f"Waiting for {station.upper()}/{archive.upper()}: {ld.status}", reset=True)
                    for f in created_files:
                        try:
                            f.unlink()
                        except (OSError, FileNotFoundError):
                            pass
                    created_files.clear()
                    await backoff()
                    continue
                _LOGGER.debug("Read %d files for %s/%s", len(created_files), station, archive)
                break

    async def _read_incremental_lock(self, connection: Connection, station: str, archive: str):
        start_year, end_year = containing_year_range(self.start_ms / 1000.0, self.end_ms / 1000.0)

        for current_year in range(start_year, end_year):
            backoff = LockBackoff()

            _LOGGER.debug("Reading archive for %s/%s/%d", station, archive, current_year)

            with self.progress(f"Reading {station.upper()}/{archive.upper()}/{current_year} data") as progress:
                while True:
                    created_files: typing.List[Path] = list()
                    try:
                        async with connection.transaction():
                            year_start_ms, year_end_ms = year_bounds_ms(current_year)
                            await connection.lock_read(index_lock_key(station, archive),
                                                       max(year_start_ms, self.start_ms), min(year_end_ms, self.end_ms))
                            created_files.extend(await self._read_year(
                                station, archive, connection, current_year, progress,
                                0.0, 1.0,
                            ))
                    except LockDenied as ld:
                        progress.set_title(f"Waiting for {station.upper()}/{archive.upper()}/{current_year}: {ld.status}", reset=True)
                        for f in created_files:
                            try:
                                f.unlink()
                            except (OSError, FileNotFoundError):
                                pass
                        created_files.clear()
                        await backoff()
                        continue
                    _LOGGER.debug("Read %d files for %s/%s/%d", len(created_files), station, archive, current_year)
                    break

    async def __call__(self) -> None:
        self.ensure_writable()

        _LOGGER.debug("Starting archive read")
        begin_time = time.monotonic()
        async with await self.exec.archive_connection() as connection:
            for station in self.stations:
                for archive in self.archives:
                    if not self.incremental_lock:
                        await self._read_single_lock(connection, station, archive)
                    else:
                        await self._read_incremental_lock(connection, station, archive)
        end_time = time.monotonic()
        _LOGGER.debug("Archive read completed in %.3f seconds", end_time - begin_time)

    @classmethod
    def nominal_record_spacing(cls, execute: Execute) -> typing.Optional[np.ndarray]:
        from .average import AverageStage
        if not execute.stages:
            return None
        read = execute.stages[0]
        if not isinstance(read, ArchiveRead):
            return None
        if len(read.archives) != 1:
            return None
        for check in reversed(execute.stages):
            if isinstance(check, AverageStage):
                return None
        selected_archive = next(iter(read.archives))
        if selected_archive == 'avgh':
            start = int(floor(read.start_ms / (60 * 60 * 1000))) * 60 * 60 * 1000
            end = int(floor(read.end_ms / (60 * 60 * 1000))) * 60 * 6 * 10000
            if end < read.end_ms:
                end += 60 * 60 * 1000
            return np.arange(start, end, 60 * 60 * 1000, dtype=np.int64)
        elif selected_archive == 'avgd':
            start = int(floor(read.start_ms / (24 * 60 * 60 * 1000))) * 24 * 60 * 60 * 1000
            end = int(floor(read.end_ms / (24 * 60 * 60 * 1000))) * 24 * 60 * 6 * 10000
            if end < read.end_ms:
                end += 24 * 60 * 60 * 1000
            return np.arange(start, end, 24 * 60 * 60 * 1000, dtype=np.int64)
        elif selected_archive == 'avgm':
            start_month_number, end_month_number = containing_epoch_month_range(
                read.start_ms / 1000.0,
                read.end_ms / 1000.0,
            )
            result = np.empty((end_month_number - start_month_number, ), dtype=np.int64)
            for mon in range(start_month_number, end_month_number):
                result[mon - start_month_number] = start_of_epoch_month_ms(mon)
            return result
        return None


class FilterStage(ExecuteStage):
    def __init__(
            self, execute: Execute,
            data_selection: DataSelection,
            start_ms: typing.Optional[int] = None,
            end_ms: typing.Optional[int] = None,
            retain_statistics: bool = False,
    ) -> None:
        super().__init__(execute)
        self.data_selection = data_selection
        self.start_ms = start_ms
        self.end_ms = end_ms
        self._retain_statistics = retain_statistics

    @classmethod
    def instantiate_if_available(cls, execute: Execute, retain_statistics: bool = False) -> None:
        from .select import SelectStage

        for stage in reversed(execute.stages):
            if isinstance(stage, ArchiveRead):
                if stage.keep_all:
                    return
                execute.install(cls(execute, stage.data_selection, stage.start_ms, stage.end_ms,
                                    retain_statistics=retain_statistics))
            elif isinstance(stage, SelectStage):
                return

    def _apply_filter(self, input_path: Path, output_path: Path) -> None:
        input_root = Dataset(str(input_path), 'r')
        try:
            if self.data_selection.accept_whole_file(
                    input_root,
                    self.start_ms, self.end_ms,
                    accept_statistics=self._retain_statistics,
            ):
                input_root.close()
                input_root = None
                shutil.move(str(input_path), str(output_path))
                _LOGGER.debug("Filter accepted whole file '%s'", str(output_path))
            else:
                output_root = Dataset(str(output_path), 'w', format='NETCDF4')
                try:
                    keep_file = self.data_selection.filter_file(
                        input_root, output_root,
                        self.start_ms, self.end_ms,
                        accept_statistics=self._retain_statistics,
                    )
                finally:
                    output_root.close()
                if not keep_file:
                    _LOGGER.debug("Filter rejected file '%s'", str(output_path))
                    try:
                        output_path.unlink()
                    except (OSError, FileNotFoundError):
                        pass
                else:
                    _LOGGER.debug("Filter accepted partial file '%s'", str(output_path))
        finally:
            if input_root is not None:
                input_root.close()

    async def __call__(self) -> None:
        _LOGGER.debug("Starting archive data filter")
        begin_time = time.monotonic()
        with self.data_replacement() as output_base:
            filter_tasks: typing.List[asyncio.Future] = list()
            with self.progress("Filtering data") as progress:
                input_files = list(self.data_files())
                completed_files: int = 0

                async def progress_filter(timeout: typing.Optional = 0):
                    nonlocal completed_files

                    if not filter_tasks:
                        return

                    done, pending = await asyncio.wait(filter_tasks, timeout=timeout, return_when=asyncio.FIRST_COMPLETED)
                    for v in done:
                        v.result()
                    completed_files += len(done)
                    filter_tasks.clear()
                    filter_tasks.extend(pending)

                    progress(completed_files / len(input_files))

                for input_path in input_files:
                    output_path = output_base / input_path.name

                    filter_tasks.append(asyncio.ensure_future(asyncio.get_event_loop().run_in_executor(
                        self.netcdf_executor, self._apply_filter, input_path, output_path
                    )))
                    await progress_filter()

                _LOGGER.debug(f"Waiting for filter completion on {len(filter_tasks)} files")
                while filter_tasks:
                    await progress_filter(None)


        end_time = time.monotonic()
        _LOGGER.debug("Archive data filter completed in %.3f seconds", end_time - begin_time)


class WavelengthSelector:
    def __init__(self, data_selection: DataSelection):
        self.data_selection = data_selection

    @classmethod
    def instantiate_if_available(cls, execute: Execute) -> typing.Optional["WavelengthSelector"]:
        from .select import SelectStage

        for stage in reversed(execute.stages):
            if isinstance(stage, ArchiveRead):
                if stage.keep_all:
                    return None
                return cls(stage.data_selection)
            elif isinstance(stage, SelectStage):
                return cls(stage.data_selection)
        return None

    def __call__(self, file: Dataset, var: Variable, retain_statistics: bool = False) -> typing.Set[int]:
        return self.data_selection.accepted_wavelengths(file, var, accept_statistics=retain_statistics)
