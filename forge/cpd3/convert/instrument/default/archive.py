import typing
import logging
import time
import sys
import re
import numpy as np
import forge.cpd3.variant as variant
from math import floor, ceil, nan, inf
from netCDF4 import Dataset, Variable
from forge.const import MAX_I64, __version__
from forge.timeparse import parse_iso8601_time, parse_iso8601_duration
from forge.data.state import is_in_state_group
from forge.data.flags import parse_flags
from forge.data.attrs import cell_methods as parse_cell_methods
from forge.data.merge.timeselect import selected_time_range
from forge.cpd3.archive.selection import FileMatch
from forge.cpd3.identity import Identity, Name
from .flags import CPD3Flag
from .units import units as unit_lookup
from .converter import convert_format_code
from ..lookup import instrument_data

_LOGGER = logging.getLogger(__name__)
_CODE_SUFFIX = re.compile(r"\D+(\d*)")


def _find_dimension_values(data: Dataset, name: str) -> Variable:
    while True:
        if name in data.dimensions:
            var = data.variables.get(name)
            if var is not None:
                return var
        data = data.parent
        if data is None:
            raise KeyError(f"Dimension {name} not found")


def _find_variable_values(data: Dataset, name: str) -> Variable:
    while True:
        var = data.variables.get(name)
        if var is not None:
            return var
        data = data.parent
        if data is None:
            raise KeyError(f"Variable {name} not found")


class Converter:
    class OutputVariable:
        def __init__(self, converter: "Converter", forge_variable: Variable, cpd3_name: Name,
                     array_dimensions: int = 0):
            self.converter = converter
            self.input_variable = forge_variable
            self.output_name = cpd3_name
            self.array_dimensions = array_dimensions
            self.is_state = is_in_state_group(self.input_variable)
            self.ignore_meta: bool = False

        @property
        def source_metadata(self) -> typing.Dict[str, typing.Any]:
            return {
                "Source": self.converter.source_metadata,
                "Processing": self.converter.processing_metadata,
            }

        def _sibling_cpd3_variable(self, forge_variable: str) -> typing.Optional[str]:
            forge_sibling = self.input_variable.group().variables.get(forge_variable)
            if forge_sibling is None:
                return None
            try:
                variable_id = forge_sibling.variable_id
            except AttributeError:
                return None
            if '_' in variable_id:
                return variable_id
            suffix = self.output_name.variable.split('_', 1)[1]
            return variable_id + "_" + suffix

        def generate_metadata(self) -> variant.Metadata:
            if np.issubdtype(self.input_variable.dtype, np.floating):
                value_meta_type = variant.MetadataReal
            else:
                value_meta_type = variant.MetadataInteger

            if self.array_dimensions == 0:
                meta = value_meta_type(self.source_metadata)
                value_meta = meta
            elif self.array_dimensions == 1:
                meta = variant.MetadataArray(self.source_metadata)
                value_meta = value_meta_type()
                meta["Children"] = value_meta
            else:
                meta = variant.MetadataMatrix(self.source_metadata)
                value_meta = value_meta_type()
                meta["Children"] = value_meta

            if self.is_state:
                meta["Smoothing"] = {"Mode": "None"}
            else:
                cell_methods = parse_cell_methods(self.input_variable)
                time_averaging_method = cell_methods.get("time")
                if time_averaging_method == "point":
                    meta["Smoothing"] = {"Mode": "None"}
                elif time_averaging_method == "last":
                    meta["Smoothing"] = {"Mode": "DifferenceInitial"}
                else:
                    for variable, method in cell_methods.items():
                        if method == "vector_magnitude":
                            vector_magnitude = self._sibling_cpd3_variable(variable)
                            if vector_magnitude:
                                meta["Smoothing"] = {
                                    "Mode": "Vector2D",
                                    "Parameters": {
                                        "Magnitude": vector_magnitude,
                                        "Direction": self.output_name.variable,
                                    }
                                }
                        elif method == "vector_direction":
                            vector_direction = self._sibling_cpd3_variable(variable)
                            if vector_direction:
                                meta["Smoothing"] = {
                                    "Mode": "Vector2D",
                                    "Parameters": {
                                        "Direction": vector_direction,
                                        "Magnitude": self.output_name.variable,
                                    }
                                }

            units = getattr(self.input_variable, "units", None)
            if units:
                units = str(units)
                value_meta['Units'] = units
                units = unit_lookup.get(units)
                if units:
                    if units.units:
                        value_meta['Units'] = units.units
                    if units.format:
                        value_meta['Format'] = units.format

            fmt = getattr(self.input_variable, "C_format", None)
            if fmt:
                fmt = convert_format_code(str(fmt))
                if fmt:
                    value_meta["Format"] = fmt

            description = getattr(self.input_variable, "long_name", None)
            if description:
                value_meta["Description"] = str(description)

            channel = getattr(self.input_variable, "channel", None)
            if channel is None:
                channel = getattr(self.input_variable, "address", None)
            if channel is not None:
                try:
                    channel = int(channel)
                    value_meta["Channel"] = channel
                except (TypeError, ValueError):
                    value_meta["Channel"] = str(channel)

            return meta

        def convert_value(self, forge_value: np.ndarray) -> typing.Any:
            if len(forge_value.shape) <= 1:
                return forge_value.tolist()
            out = variant.Matrix(forge_value.flatten().tolist())
            out.shape = list(forge_value.shape)
            return out

    class OutputSystemFlags(OutputVariable):
        _EMPTY_FLAGS = set()

        def __init__(self, converter: "Converter", forge_variable: Variable, cpd3_name: Name):
            super().__init__(converter, forge_variable, cpd3_name)
            self._forge_flags: typing.Dict[int, str] = parse_flags(forge_variable)
            self._cpd3_flags: typing.Dict[str, CPD3Flag] = dict()
            if self.converter.instrument_type:
                self._cpd3_flags = instrument_data(self.converter.instrument_type, 'flags', 'lookup')

            self._flag_map: typing.List[typing.Tuple[int, CPD3Flag]] = list()
            for bits, flag_name in self._forge_flags.items():
                dest_flag = self._cpd3_flags.get(flag_name)
                if not dest_flag:
                    if flag_name == "abnormal_data_wild_fire" or flag_name == "abnormal_data_dust":
                        dest_flag = CPD3Flag(
                            "EBASFlag110",
                            "Episode data checked and accepted by data originator. Valid measurement",
                        )
                    elif flag_name.startswith("data_contamination_"):
                        dest_flag = CPD3Flag(
                            "ContaminateForge" + (''.join([s.title() for s in flag_name.split('_')[2:]])),
                            f"Forge contamination flag {flag_name}"
                        )
                    else:
                        continue
                self._flag_map.append((bits, dest_flag))

        def generate_metadata(self) -> variant.Metadata:
            meta = variant.MetadataFlags(self.source_metadata)
            meta["Description"] = "Instrument flags"

            all_bits = 0
            for _, cpd3_flag in self._flag_map:
                flag_data: typing.Dict[str, typing.Any] = {
                    "Origin": ["forge.cpd3.convert.archive"]
                }
                if cpd3_flag.description:
                    flag_data["Description"] = cpd3_flag.description
                if cpd3_flag.bit:
                    flag_data["Bits"] = cpd3_flag.bit
                    all_bits |= cpd3_flag.bit
                meta.children[cpd3_flag.code] = flag_data

            meta["Format"] = "FFFF"
            if all_bits:
                digits = int(ceil(all_bits.bit_length() / (4 * 4))) * 4
                meta["Format"] = "F" * digits
            return meta

        def convert_value(self, forge_value: np.ndarray) -> typing.Set[str]:
            forge_flags = int(forge_value)
            if forge_flags == 0:
                return self._EMPTY_FLAGS
            cpd3_flags: typing.Set[str] = set()
            for bits, output_flag in self._flag_map:
                if (forge_flags & bits) == 0:
                    continue
                cpd3_flags.add(output_flag.code)
            return cpd3_flags

    class OutputDataVariable(OutputVariable):
        def __init__(self, converter: "Converter", forge_variable: Variable, cpd3_name: Name,
                     array_dimensions: int = 0):
            super().__init__(converter, forge_variable, cpd3_name, array_dimensions)
            self.output_hash: typing.Optional[str] = None
            self.keyframe_map: typing.Optional[typing.List[float]] = None

        def convert_value(self, forge_value: np.ndarray) -> typing.Any:
            converted = super().convert_value(forge_value)
            if self.keyframe_map:
                base = variant.Keyframe()
                for i in range(min(len(self.keyframe_map), len(converted))):
                    base[self.keyframe_map[i]] = converted[i]
                converted = base
            if self.output_hash:
                base = dict()
                base[self.output_hash] = converted
                converted = base
            return converted

    class OutputWavelengthSelected(OutputDataVariable):
        def __init__(self, converter: "Converter", forge_variable: Variable, cpd3_name: Name,
                     array_dimensions: int, wavelength: float):
            super().__init__(converter, forge_variable, cpd3_name, array_dimensions)
            self.wavelength = wavelength

        def generate_metadata(self) -> variant.Metadata:
            meta = super().generate_metadata()
            if isinstance(meta, variant.MetadataChildren):
                meta.children["Wavelength"] = self.wavelength
            else:
                meta["Wavelength"] = self.wavelength
            return meta

    def __init__(self, station: str, archive: str, root: Dataset, matchers: typing.List[FileMatch]):
        self.root = root
        self.matchers = matchers
        self.station = station
        self.archive = archive

        self.instrument_id = str(self.root.instrument_id)
        self.instrument_type = str(getattr(self.root, 'instrument', ""))

        self._record_interval: typing.Optional[float] = None
        time_coverage_resolution = getattr(self.root, "time_coverage_resolution", None)
        if time_coverage_resolution is not None:
            self._record_interval = parse_iso8601_duration(str(time_coverage_resolution))

        self._file_start_time = parse_iso8601_time(str(self.root.time_coverage_start)).timestamp()
        self._file_end_time = parse_iso8601_time(str(self.root.time_coverage_end)).timestamp()

        self.processing_metadata: typing.List[typing.Dict[str, typing.Any]] = list()
        history = getattr(self.root, "history", None)
        if history is not None:
            for item in str(history).split('\n'):
                processed_at, processed_by, processed_version, command = item.split(',', 3)
                processed_at = parse_iso8601_time(processed_at).timestamp()
                self.processing_metadata.append({
                    "At": processed_at,
                    "By": processed_by,
                    "Revision": processed_version,
                    "Environment": command,
                })
        self.processing_metadata.append({
            "At": time.time(),
            "By": "forge.cpd3.convert.archive",
            "Revision": __version__,
            "Environment": ' '.join(sys.argv),
        })

        self.source_metadata: typing.Dict[str, typing.Any] = {
            "ForgeInstrument": self.instrument_type,
            "Name": self.instrument_id,
        }
        inst_group = self.root.groups.get("instrument")
        if inst_group is not None:
            manufacturer = inst_group.variables.get("manufacturer")
            if manufacturer is not None:
                self.source_metadata["Manufacturer"] = str(manufacturer[0])
            model = inst_group.variables.get("model")
            if model is not None:
                self.source_metadata["Model"] = str(model[0])
            serial_number = inst_group.variables.get("serial_number")
            if serial_number is not None:
                self.source_metadata["SerialNumber"] = str(serial_number[0])
            firmware_version = inst_group.variables.get("firmware_version")
            if firmware_version is not None:
                self.source_metadata["FirmwareVersion"] = str(firmware_version[0])

    def output_variable(
            self,
            variable: Variable,
            cpd3_name: Name,
            array_dimensions: int = 0,
            wavelength: typing.Optional[float] = None,
            statistics: typing.Optional[FileMatch.Statistics] = None,
    ) -> "Converter.OutputVariable":
        if variable.name == "system_flags":
            return self.OutputSystemFlags(self, variable, cpd3_name)

        if wavelength is not None:
            var = self.OutputWavelengthSelected(self, variable, cpd3_name, array_dimensions, wavelength)
        else:
            var = self.OutputVariable(self, variable, cpd3_name, array_dimensions)
        if statistics:
            var.ignore_meta = True
        if statistics == FileMatch.Statistics.Quantiles and 'quantile' in variable.dimensions:
            var.output_hash = "Quantiles"
            var.keyframe_map = _find_dimension_values(variable.group(), 'quantile')[:].data.tolist()
        # elif statistics == FileMatch.Statistics.StdDev:
        #     var.output_hash = "StandardDeviation"
        return var

    def _generate_output(
            self,
            output: typing.List[typing.Tuple[Identity, typing.Any]],
            input_variable: Variable,
            output_variable: "Converter.OutputVariable",
            data_selector: typing.List[typing.Any],
            cpd3_data: typing.Optional[Name],
            cpd3_meta: typing.Optional[Name],
            start: typing.Optional[float],
            end: typing.Optional[float],
    ) -> None:
        if cpd3_meta and not output_variable.ignore_meta:
            meta_start = self._file_start_time
            if start is not None and start > meta_start:
                meta_start = start
            meta_end = self._file_end_time
            if end is not None and end < meta_end:
                meta_end = end
            if meta_start < meta_end:
                output.append((
                    Identity(name=cpd3_meta, start=meta_start, end=meta_end),
                    output_variable.generate_metadata())
                )

        if not cpd3_data:
            return

        time_dim = _find_dimension_values(input_variable.group(), 'time')
        time_data = time_dim[:].data
        if not time_data.shape or time_data.shape[0] < 1:
            return
        data_start = time_data
        data_end = np.concatenate((
            time_data[1:],
            np.array((int(ceil(self._file_end_time * 1000)),), dtype=time_data.dtype)
        ))
        if not output_variable.is_state:
            data_start = data_start[data_selector[0]]
            data_end = data_end[data_selector[0]]

        selected_data = selected_time_range(
            data_start,
            int(floor(start * 1000)) if start is not None else -MAX_I64,
            int(ceil(end * 1000)) if end is not None else MAX_I64,
            output_variable.is_state
        )
        if selected_data is None:
            return
        start_idx, end_idx = selected_data

        if not output_variable.is_state:
            data_values = input_variable[:].data[tuple(data_selector)]
        else:
            data_values = input_variable[:].data[(slice(None), *data_selector[1:])]
        data_values = data_values[slice(start_idx, end_idx), ...]
        data_start = data_start[slice(start_idx, end_idx)]
        data_end = data_end[slice(start_idx, end_idx)]

        for idx in range(data_start.shape[0]):
            converted_value = output_variable.convert_value(data_values[idx])
            converted_start = float(data_start[idx] / 1000.0)
            converted_end = float(data_end[idx] / 1000.0)
            if not output_variable.is_state and self._record_interval and converted_end >= converted_start + self._record_interval * 2:
                converted_end = converted_start + self._record_interval

            output.append((
                Identity(name=cpd3_data, start=converted_start, end=converted_end),
                converted_value,
            ))

    def _convert_variable(self,  output: typing.List[typing.Tuple[Identity, typing.Any]], group: Dataset,
                          var: Variable, statistics: typing.Optional[FileMatch.Statistics] = None) -> None:
        if var.name == 'time':
            return
        if len(var.dimensions) < 1 or var.dimensions[0] != 'time':
            return
        variable_id = getattr(var, 'variable_id', None)
        if not variable_id:
            return
        variable_id = str(variable_id)
        ancillary_variables = set(getattr(var, "ancillary_variables", "").split())

        try:
            cut_dim_idx: typing.Optional[int] = var.dimensions.index('cut_size')
        except (ValueError, KeyError):
            cut_dim_idx: typing.Optional[int] = None
        if cut_dim_idx is not None:
            cut_values: typing.Optional[np.ndarray] = _find_dimension_values(group, 'cut_size')[:].data
        elif 'cut_size' in ancillary_variables:
            cut_values: typing.Optional[np.ndarray] = _find_variable_values(group, 'cut_size')[:].data
        else:
            cut_values: typing.Optional[np.ndarray] = None

        cpd3_flavors: typing.Dict[typing.Tuple[str, ...], typing.Union[np.ndarray, slice]] = dict()
        if cut_values is None:
            for m in self.matchers:
                r = m.matches_flavors(nan, statistics)
                if r is None:
                    continue
                r = tuple(sorted(r))
                cpd3_flavors[r] = slice(None)
        else:
            for cut in np.unique(cut_values):
                cut = float(cut)
                for m in self.matchers:
                    r = m.matches_flavors(cut, statistics)
                    if r is None:
                        continue
                    r = tuple(sorted(r))
                    existing = cpd3_flavors.get(r)
                    if existing is None:
                        cpd3_flavors[r] = (cut_values == cut)
                    else:
                        cpd3_flavors[r] = np.any((
                            cut_values == cut,
                            existing,
                        ), axis=0)
        if not cpd3_flavors:
            return

        try:
            wl_dim_idx: typing.Optional[int] = var.dimensions.index('wavelength')
        except (ValueError, KeyError):
            wl_dim_idx: typing.Optional[int] = None

        if wl_dim_idx is None:
            cpd3_variable: typing.Optional[str] = None
            include_data: bool = False
            include_meta: bool = False
            start_time: typing.Optional[float] = inf
            end_time: typing.Optional[float] = -inf
            for m in self.matchers:
                if not m.include_data and not m.include_meta:
                    continue
                if 'wavelength' in ancillary_variables:
                    r = m.matches_variable_wavelengths(variable_id, self.instrument_id, [
                        float(group.variables['wavelength'][0]),
                    ])
                    if not r:
                        continue
                    cpd3_variable = r[0][0]
                else:
                    r = m.matches_variable_id(variable_id, self.instrument_id)
                    if not r:
                        continue
                    cpd3_variable = r

                include_data = include_data or m.include_data
                include_meta = include_meta or m.include_meta
                if not cpd3_variable:
                    start_time = m.start
                    end_time = m.end
                else:
                    start_time = min(start_time, m.start) if start_time and m.start else None
                    end_time = max(end_time, m.end) if end_time and m.end else None
            if not cpd3_variable:
                return
            if not include_data and not include_meta:
                return

            for flavors, selector in cpd3_flavors.items():
                output_name = Name(self.station, self.archive, cpd3_variable, flavors)
                if cut_dim_idx is not None:
                    data_selector = ([slice(None)] * cut_dim_idx) + [selector]
                else:
                    data_selector = [selector]

                unselected_dimensions: int = 0
                for didx in range(1, len(var.shape)):
                    if didx >= len(data_selector):
                        unselected_dimensions += 1
                    elif data_selector[didx] is True:
                        unselected_dimensions += 1
                        data_selector[didx] = slice(None)

                output_variable = self.output_variable(
                    var, output_name,
                    array_dimensions=unselected_dimensions,
                    statistics=statistics,
                )
                self._generate_output(
                    output, var, output_variable, data_selector,
                    output_name if include_data else None,
                    output_name.to_metadata() if include_meta else None,
                    start_time, end_time
                )
        else:
            wl_dim = _find_dimension_values(group, 'wavelength')
            wl_values: typing.List[float] = wl_dim[:].data.tolist()
            wl_output: typing.Dict[
                int, typing.Tuple[str, bool, bool, typing.Optional[float], typing.Optional[float], float]] = dict()
            for m in self.matchers:
                if not m.include_data and not m.include_meta:
                    continue
                for hit_var, hit_idx in m.matches_variable_wavelengths(variable_id, self.instrument_id, wl_values):
                    existing = wl_output.get(hit_idx)
                    if not existing:
                        wl_output[hit_idx] = (
                        hit_var, m.include_data, m.include_meta, m.start, m.end, wl_values[hit_idx])
                    else:
                        wl_output[hit_idx] = (
                            hit_var,
                            existing[1] or m.include_data,
                            existing[2] or m.include_meta,
                            min(existing[3], m.start) if existing[3] and m.start else None,
                            max(existing[4], m.end) if existing[4] and m.start else None,
                            existing[5],
                        )
            if not wl_output:
                return

            for wl_selector, (cpd3_variable, include_data, include_meta,
                              start_time, end_time, wavelength) in wl_output.items():
                for flavors, selector in cpd3_flavors.items():
                    output_name = Name(self.station, self.archive, cpd3_variable, flavors)
                    if cut_dim_idx is not None:
                        data_selector = ([slice(None)] * cut_dim_idx) + [selector]
                    else:
                        data_selector = [selector]
                    if wl_dim_idx >= len(data_selector):
                        data_selector += [slice(None)] * (wl_dim_idx - len(data_selector) + 1)
                    data_selector[wl_dim_idx] = wl_selector

                    unselected_dimensions: int = 0
                    for didx in range(1, len(var.shape)):
                        if didx >= len(data_selector):
                            unselected_dimensions += 1
                        elif data_selector[didx] is True:
                            unselected_dimensions += 1
                            data_selector[didx] = slice(None)

                    output_variable = self.output_variable(
                        var, output_name,
                        array_dimensions=unselected_dimensions,
                        wavelength=wavelength,
                        statistics=statistics,
                    )
                    self._generate_output(
                        output, var, output_variable, data_selector,
                        output_name if include_data else None,
                        output_name.to_metadata() if include_meta else None,
                        start_time, end_time
                    )

    def _convert_group(self, output: typing.List[typing.Tuple[Identity, typing.Any]], group: Dataset,
                       statistics: typing.Optional[FileMatch.Statistics] = None) -> None:
        for var in group.variables.values():
            self._convert_variable(output, group, var, statistics)

        for g in group.groups.values():
            if statistics == FileMatch.Statistics.Root:
                if g.name == 'quantiles':
                    self._convert_group(output, g, FileMatch.Statistics.Quantiles)
                else:
                    self._convert_group(output, g, FileMatch.Statistics.Other)
                continue
            self._convert_group(output, g)

    def convert_data(self, output: typing.List[typing.Tuple[Identity, typing.Any]]) -> None:
        for var in self.root.variables.values():
            self._convert_variable(output, self.root, var)

        for name, g in self.root.groups.items():
            if name == 'statistics':
                self._convert_group(output, g, FileMatch.Statistics.Root)
            else:
                self._convert_group(output, g)


def convert(station: str, archive: str, root: Dataset, matchers: typing.List[FileMatch],
            output: typing.List[typing.Tuple[Identity, typing.Any]]) -> None:
    Converter(station, archive, root, matchers).convert_data(output)
