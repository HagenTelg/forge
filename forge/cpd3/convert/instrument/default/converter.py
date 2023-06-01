import typing
import time
import sys
import numpy as np
import re
import enum
import forge.cpd3.variant as cpd3_variant
from math import isfinite, nan, ceil
from netCDF4 import Dataset, Group, Variable
from forge.const import __version__
from forge.timeparse import parse_iso8601_time, parse_iso8601_duration
from forge.cpd3.identity import Name, Identity
from ..lookup import instrument_data
from .flags import CPD3Flag
from .units import units as unit_lookup


_CUT_WHOLE = set()
_CUT_PM1 = {"pm1"}
_CUT_PM25 = {"pm25"}
_CUT_PM10 = {"pm10"}

_FLAGS_EMPTY = set()


_FORMAT_FLOAT = re.compile(
    r'%[0# +-]*(\d*)(?:\.(\d*))L?[fF]'
)


def convert_format_code(code: str) -> typing.Optional[str]:
    m = _FORMAT_FLOAT.fullmatch(code)
    if m:
        total_width = m.group(1)
        if total_width:
            total_width = int(total_width)
        else:
            total_width = 0
        decimals = m.group(2)
        if decimals:
            decimals = int(decimals)
        else:
            decimals = 0

        if not total_width and not decimals:
            return None

        fmt = "0" * total_width
        if decimals:
            fmt = fmt[:-(decimals+1)] + "." + "0" * decimals
        if not fmt:
            return None
        if fmt[0] != "0":
            fmt = "0" + fmt
        return fmt
    return None


class Converter:
    def __init__(self, station: str, root: Dataset):
        self.station = station
        self.root = root
        self.instrument: str = root.instrument
        self.source: str = root.instrument_id

        self.flag_lookup: typing.Dict[str, CPD3Flag] = dict()
        if self.instrument:
            instrument_flags = instrument_data(self.instrument, 'flags', 'lookup')
            if instrument_flags:
                self.flag_lookup.update(instrument_flags)

        self.source_metadata: typing.Dict[str, typing.Any] = {
            "ForgeInstrument": self.instrument,
            "Name": self.source,
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

        self.expected_record_interval: typing.Optional[float] = None
        time_coverage_resolution = getattr(self.root, "time_coverage_resolution", None)
        if time_coverage_resolution is not None:
            self.expected_record_interval = parse_iso8601_duration(str(time_coverage_resolution))

        self.file_start_time: typing.Optional[float] = None
        time_coverage_start = getattr(self.root, "time_coverage_start", None)
        if time_coverage_start is not None:
            self.file_start_time = parse_iso8601_time(str(time_coverage_start)).timestamp()

        self.file_end_time: typing.Optional[float] = None
        time_coverage_end = getattr(self.root, "time_coverage_end", None)
        if time_coverage_end is not None:
            self.file_end_time = parse_iso8601_time(str(time_coverage_end)).timestamp()

        self.system_start_time: typing.Optional[float] = None
        acquisition_start_time = getattr(self.root, "acquisition_start_time", None)
        if acquisition_start_time is not None:
            self.system_start_time = parse_iso8601_time(str(acquisition_start_time)).timestamp()

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
            "By": "forge.cpd3.convert.instrument",
            "Revision": __version__,
            "Environment": ' '.join(sys.argv),
        })

    def insert_metadata(self, meta: cpd3_variant.Metadata) -> None:
        meta["Source"] = self.source_metadata
        meta["Processing"] = self.processing_metadata

    def record_converter(self, group: Group) -> typing.Optional["RecordConverter"]:
        if group.name == "instrument":
            return None
        if group.name == "data":
            return DataRecord(self, group)
        elif group.name == "upstream":
            return DataRecord(self, group)
        elif group.name == "state":
            return StateRecord(self, group)
        raise ValueError(f"unrecognized group {group.name}")

    def convert(self) -> typing.List[typing.Tuple[Identity, typing.Any]]:
        result: typing.List[typing.Tuple[Identity, typing.Any]] = list()

        for g in self.root.groups.values():
            converter = self.record_converter(g)
            if not converter:
                continue
            converter.convert(result)

        return result


class RecordConverter:
    _WAVELENGTH_CODES = [
        ("B", 400.0, 500.0),
        ("G", 500.0, 600.0),
        ("R", 600.0, 750.0),
        ("Q", 750.0, 900.0),
    ]

    def __init__(self, converter: Converter, group: Group):
        self.converter = converter
        self.group = group

        self.wavelength_suffix: typing.List[str] = list()

        wavelengths = self.group.variables.get("wavelength")
        if wavelengths is not None:
            def lookup_code(wl) -> typing.Optional[str]:
                if wl is None:
                    return None
                if getattr(wl, 'mask', False):
                    return None
                if not isfinite(wl):
                    return None
                for code, min, max in self._WAVELENGTH_CODES:
                    if min <= wl < max:
                        return code
                return None

            if len(wavelengths.dimensions) == 0 or len(wavelengths) == 1:
                code = lookup_code(wavelengths[0])
                if code:
                    self.wavelength_suffix.append(code)
            elif len(wavelengths) == 3:
                codes = []
                for i in range(len(wavelengths)):
                    code = lookup_code(wavelengths[i])
                    if not code:
                        break
                    codes.append(code)
                else:
                    for code in codes:
                        self.wavelength_suffix.append(code)

        wavelengths = self.group.dimensions.get("wavelength")
        if wavelengths is not None:
            for i in range(len(self.wavelength_suffix), wavelengths.size):
                self.wavelength_suffix.append(f"{i+1}")

    def convert(self, result: typing.List[typing.Tuple[Identity, typing.Any]]) -> None:
        raise NotImplementedError


class VariableConverter:
    def __init__(self, record: RecordConverter, variable: Variable):
        self.record = record
        self.variable = variable

        ancillary_variables = getattr(self.variable, "ancillary_variables", None)
        if ancillary_variables:
            self.ancillary_variables: typing.Set[str] = set(ancillary_variables.split(' '))
        else:
            self.ancillary_variables: typing.Set[str] = set()

    def insert_metadata(self, meta: cpd3_variant.Metadata) -> None:
        self.record.converter.insert_metadata(meta)

        units = getattr(self.variable, "units", None)
        if units:
            meta['Units'] = units
            units = unit_lookup.get(units)
            if units:
                if units.units:
                    meta['Units'] = units.units
                if units.format:
                    meta['Format'] = units.format

        fmt = getattr(self.variable, "C_format", None)
        if fmt:
            fmt = convert_format_code(str(fmt))
            if fmt:
                meta["Format"] = fmt

        description = getattr(self.variable, "long_name", None)
        if description:
            meta["Description"] = str(description)

        channel = getattr(self.variable, "channel", None)
        if channel is None:
            channel = getattr(self.variable, "address", None)
        if channel is not None:
            try:
                channel = int(channel)
                meta["Channel"] = channel
            except (TypeError, ValueError):
                meta["Channel"] = channel

        if "standard_temperature" in self.ancillary_variables:
            standard_temperature = self.record.group.variables.get("standard_temperature")
            if standard_temperature is not None:
                meta["ReportT"] = float(standard_temperature[0])
        if "standard_pressure" in self.ancillary_variables:
            standard_pressure = self.record.group.variables.get("standard_pressure")
            if standard_pressure is not None:
                meta["ReportP"] = float(standard_pressure[0])

        standard_name = getattr(self.variable, "standard_name", None)
        comment = getattr(self.variable, "comment", None)
        if comment:
            if standard_name == "number_concentration_of_aerosol_particles_at_stp_in_air" or standard_name == "number_concentration_of_ambient_aerosol_particles_in_air":
                meta["NoteFlow"] = str(comment)
            elif self.variable.name == "sample_flow":
                meta["NoteFlow"] = str(comment)
            else:
                meta["Comment"] = str(comment)

    @staticmethod
    def convert_float(v) -> float:
        if v is None:
            return nan
        if getattr(v, 'mask', False):
            return nan
        v = float(v)
        if not isfinite(v):
            return nan
        return v

    class ConversionType(enum.Enum):
        FLOAT = enum.auto()
        INTEGER = enum.auto()
        STRING = enum.auto()
        ARRAYFLOAT = enum.auto()

        @property
        def converter(self) -> typing.Callable[[typing.Any], typing.Any]:
            if self == self.FLOAT:
                return VariableConverter.convert_float
            elif self == self.INTEGER:
                def c(v):
                    if v is None:
                        return None
                    if getattr(v, 'mask', False):
                        return None
                    return int(v)
                return c
            elif self == self.STRING:
                def c(v):
                    if v is None:
                        return None
                    if getattr(v, 'mask', False):
                        return None
                    return str(v)
                return c
            elif self == self.ARRAYFLOAT:
                return lambda v: [VariableConverter.convert_float(i) for i in v]
            else:
                raise ValueError("invalid conversion type")

        @property
        def metadata(self) -> typing.Type[cpd3_variant.Metadata]:
            if self == self.FLOAT:
                return cpd3_variant.MetadataReal
            elif self == self.INTEGER:
                return cpd3_variant.MetadataInteger
            elif self == self.STRING:
                return cpd3_variant.MetadataString
            elif self == self.ARRAYFLOAT:
                return cpd3_variant.MetadataArray
            else:
                raise ValueError("invalid conversion type")

    @property
    def conversion_type(self) -> "VariableConverter.ConversionType":
        if np.issubdtype(self.variable.dtype, np.floating):
            if len(self.variable.dimensions) == 2:
                return self.ConversionType.ARRAYFLOAT
            return self.ConversionType.FLOAT
        elif np.issubdtype(self.variable.dtype, np.integer):
            return self.ConversionType.INTEGER
        elif self.variable.dtype == str:
            return self.ConversionType.STRING
        return self.ConversionType.INTEGER

    def convert(self, result: typing.List[typing.Tuple[Identity, typing.Any]]) -> None:
        raise NotImplementedError


class DataRecord(RecordConverter):
    def __init__(self, converter: Converter, group: Group):
        super().__init__(converter, group)

        self.times: typing.List[typing.Tuple[float, float, typing.Set[str]]] = list()
        self.coverage: typing.List[typing.Optional[float]] = list()

        self.cut_size_lookup: typing.Dict[typing.Optional[float], typing.Set[str]] = {
            None: set(),
        }
        record_cut_size = self.group.variables.get("cut_size")
        if record_cut_size is not None:
            self.cut_size_lookup.clear()
            if not record_cut_size.shape:
                record_cut_size = [float(record_cut_size[0])]
            for diameter in record_cut_size:
                if not diameter:
                    self.cut_size_lookup[None] = _CUT_WHOLE
                    continue
                diameter = float(diameter)
                if not isfinite(diameter):
                    self.cut_size_lookup[None] = _CUT_WHOLE
                elif diameter == 1.0:
                    self.cut_size_lookup[diameter] = _CUT_PM1
                elif diameter == 2.5:
                    self.cut_size_lookup[diameter] = _CUT_PM25
                elif diameter == 10.0:
                    self.cut_size_lookup[diameter] = _CUT_PM10

        record_times = self.group.variables["time"]
        record_averaged_time = self.group.variables.get("averaged_time")
        for i in range(len(record_times)):
            start_time: float = float(record_times[i]) / 1000.0

            if i != len(record_times) - 1:
                end_time: float = float(record_times[i + 1]) / 1000.0
                if self.converter.expected_record_interval:
                    expected_end_time: float = start_time + self.converter.expected_record_interval
                    if expected_end_time < end_time:
                        end_time = expected_end_time
            else:
                if self.converter.expected_record_interval:
                    end_time: float = start_time + self.converter.expected_record_interval
                elif self.converter.file_end_time:
                    end_time: float = self.converter.file_end_time
                # elif len(self.times) != 0:
                #     prior_interval = self.times[-1][1] - self.times[-1][0]
                #     end_time: float = start_time + prior_interval
                else:
                    raise ValueError("unable to determine record end time")

            if self.converter.file_start_time and start_time < self.converter.file_start_time:
                start_time = self.converter.file_start_time
            if self.converter.file_end_time and end_time > self.converter.file_end_time:
                end_time = self.converter.file_end_time

            cut_size = None
            if record_cut_size:
                if i > len(record_cut_size):
                    cut_size = record_cut_size[0]
                else:
                    cut_size = record_cut_size[i]
                if not cut_size or not isfinite(cut_size):
                    cut_size = None
                else:
                    cut_size = float(cut_size)
            cut_size = self.cut_size_lookup[cut_size]

            self.times.append((start_time, end_time, cut_size))

            coverage_fraction: typing.Optional[float] = None
            if self.converter.expected_record_interval and record_averaged_time is not None:
                coverage_fraction = (float(
                    record_averaged_time[i]) / 1000.0) / self.converter.expected_record_interval
                if coverage_fraction >= 1.0:
                    coverage_fraction = None
                elif coverage_fraction < 0.0:
                    coverage_fraction = 0.0
            self.coverage.append(coverage_fraction)

    def global_fanout(self, result: typing.List[typing.Tuple[Identity, typing.Any]], name: Name, value: typing.Any,
                      use_cut_size: bool = True) -> None:

        start_time: float = self.converter.file_start_time
        end_time: float = self.converter.file_end_time
        if not start_time and len(self.times) > 0:
            start_time = self.times[0][0]
        if not end_time and len(self.times) > 0:
            end_time = self.times[-1][1]

        if not start_time or (self.converter.system_start_time and self.converter.system_start_time < start_time):
            start_time = self.converter.system_start_time

        if not start_time or not end_time:
            return

        if not use_cut_size:
            result.append((Identity(name=name, start=start_time, end=end_time), value))
            return

        for cut_flavors in self.cut_size_lookup.values():
            result.append((Identity(name=name,
                                    flavors=name.flavors | cut_flavors,
                                    start=start_time, end=end_time),
                           value))

    def value_convert(self, result: typing.List[typing.Tuple[Identity, typing.Any]],
                      name: Name, variable: Variable,
                      c: typing.Callable[[typing.Any], typing.Any],
                      use_cut_size: bool = True) -> None:
        for i in range(len(self.times)):
            v = c(variable[i, ...])
            flavors = name.flavors
            if use_cut_size:
                flavors = flavors | self.times[i][2]
            result.append((Identity(name=name,
                                    flavors=flavors,
                                    start=self.times[i][0], end=self.times[i][1]),
                           v))

    def value_coverage(self, result: typing.List[typing.Tuple[Identity, typing.Any]],
                       name: Name, use_cut_size: bool = True) -> None:
        for i in range(len(self.times)):
            cover = self.coverage[i]
            if cover is None:
                continue
            flavors = set(name.flavors)
            flavors.add("cover")
            if use_cut_size:
                flavors |= self.times[i][2]
            result.append((Identity(name=name,
                                    flavors=flavors,
                                    start=self.times[i][0], end=self.times[i][1]),
                           cover))

    class FlagsConverter(VariableConverter):
        def __init__(self, record: "DataRecord", variable: Variable):
            super().__init__(record, variable)
            self.record: "DataRecord" = record
            self.base_name = Name(self.record.converter.station, 'raw',
                                  'F1_' + self.record.converter.source)

        def metadata(self) -> cpd3_variant.Metadata:
            meta = cpd3_variant.MetadataFlags()
            meta["Description"] = "Instrument flags"
            self.record.converter.insert_metadata(meta)

            all_bits = 0
            for flag in self.record.converter.flag_lookup.values():
                flag_data: typing.Dict[str, typing.Any] = {
                    "Origin": ["forge.cpd3.convert.instrument"]
                }
                if flag.description:
                    flag_data["Description"] = flag.description
                if flag.bit:
                    flag_data["Bits"] = flag.bit
                    all_bits |= flag.bit
                meta.children[flag.code] = flag_data

            meta["Format"] = "FFFF"
            if all_bits:
                digits = int(ceil(all_bits.bit_length() / (4 * 4))) * 4
                meta["Format"] = "F" * digits

            return meta

        def convert(self, result: typing.List[typing.Tuple[Identity, typing.Any]]) -> None:
            self.record.global_fanout(result, self.base_name.to_metadata(), self.metadata())

            flag_meanings = self.variable.flag_meanings.split(' ')
            flag_masks = self.variable.flag_masks
            bit_lookup: typing.Dict[int, typing.Set[str]] = dict()
            for i in range(len(flag_meanings)):
                flag_name = flag_meanings[i]
                cpd3_flag = self.record.converter.flag_lookup.get(flag_name)
                if cpd3_flag is None:
                    continue
                if len(flag_meanings) == 1:
                    flag_bits = int(flag_masks)
                else:
                    flag_bits = flag_masks[i]
                bit_lookup[flag_bits] = {cpd3_flag.code}

            def c(bits) -> typing.Set[str]:
                if bits is None:
                    return _FLAGS_EMPTY
                if bits.mask:
                    return _FLAGS_EMPTY
                bits = int(bits)
                if bits == 0:
                    return _FLAGS_EMPTY
                check = bit_lookup.get(bits)
                if check is not None:
                    return check
                flag_set: typing.Set[str] = set()
                for bit, flag in bit_lookup.items():
                    bit = int(bit)
                    if (bits & bit) != 0:
                        flag_set.update(flag)
                return flag_set

            self.record.value_convert(result, self.base_name, self.variable, c)

    class GenericConverter(VariableConverter):
        def __init__(self, record: "DataRecord", variable: Variable):
            super().__init__(record, variable)
            self.record: "DataRecord" = record

            variable_name: str = variable.variable_id
            if "_" not in variable_name:
                if "wavelength" in self.ancillary_variables and len(self.record.wavelength_suffix) == 1:
                    variable_name = variable_name + self.record.wavelength_suffix[0]

                variable_name = variable_name + "_" + self.record.converter.source
            self.base_name = Name(self.record.converter.station, 'raw', variable_name)

        @staticmethod
        def _split_colon_fields(raw: str) -> typing.Dict[str, str]:
            result: typing.Dict[str, str] = dict()
            if not raw:
                return result

            prior_key: typing.Optional[str] = None
            fields = raw.split(':')
            for field in fields[:-1]:
                field = field.strip()
                key = field.split()[-1].strip()
                prior_value = field[:(-len(key))].strip()

                if prior_key:
                    result[prior_key] = prior_value
                prior_key = key

            if prior_key:
                result[prior_key] = fields[-1].strip()

            return result

        def _sibling_variable_code(self, name: str, apply_suffix: bool = False) -> typing.Optional[str]:
            if not name:
                return None
            variable = self.record.group.variables.get(name)
            if variable is None:
                return None

            variable_name: str = variable.variable_id
            if "_" not in variable_name:
                if "wavelength" in self.ancillary_variables and len(self.record.wavelength_suffix) == 1:
                    variable_name = variable_name + self.record.wavelength_suffix[0]

                if apply_suffix:
                    variable_name = variable_name + "_" + self.record.converter.source

            return variable_name

        def metadata(self) -> cpd3_variant.Metadata:
            conversion_type = self.conversion_type
            metadata_type = conversion_type.metadata

            meta = metadata_type()
            self.insert_metadata(meta)

            if "wavelength" in self.ancillary_variables:
                wavelengths = self.variable.group().variables.get("wavelength")
                if wavelengths is not None and len(wavelengths.dimensions) == 0 or len(wavelengths) == 1:
                    wl = float(wavelengths[0])
                    if isfinite(wl):
                        meta["Wavelength"] = wl

            cell_methods = self._split_colon_fields(getattr(self.variable, "cell_methods", None))
            time_averaging_method = cell_methods.get("time")
            if time_averaging_method == "point":
                meta["Smoothing"] = {"Mode": "None"}
            elif time_averaging_method == "last":
                meta["Smoothing"] = {"Mode": "DifferenceInitial"}
            else:
                vector_magnitude = self._sibling_variable_code(cell_methods.get("vector_magnitude"))
                if vector_magnitude:
                    meta["Smoothing"] = {
                        "Mode": "Vector2D",
                        "Parameters": {
                            "Magnitude": vector_magnitude,
                            "Direction": self.variable.variable_id,
                        }
                    }
                vector_direction = self._sibling_variable_code(cell_methods.get("vector_angle"))
                if vector_direction:
                    meta["Smoothing"] = {
                        "Mode": "Vector2D",
                        "Parameters": {
                            "Direction": vector_direction,
                            "Magnitude": self.variable.variable_id,
                        }
                    }

            if conversion_type == self.ConversionType.ARRAYFLOAT:
                if 'time' in self.variable.dimensions:
                    meta["Count"] = int(self.variable.shape[-2])
                else:
                    meta["Count"] = int(self.variable.shape[-1])
                children = cpd3_variant.MetadataReal()
                meta["Children"] = children
                if meta.get("Units"):
                    children["Units"] = meta["Units"]
                if meta.get("Format"):
                    children["Format"] = meta["Format"]

            return meta

        def convert(self, result: typing.List[typing.Tuple[Identity, typing.Any]]) -> None:
            use_cut_size = "cut_size" in self.ancillary_variables

            self.record.global_fanout(result, self.base_name.to_metadata(), self.metadata(),
                                      use_cut_size=use_cut_size)

            self.record.value_convert(result, self.base_name, self.variable,
                                      self.conversion_type.converter,
                                      use_cut_size=use_cut_size)
            self.record.value_coverage(result, self.base_name, use_cut_size=use_cut_size)

    class ConstantConverter(GenericConverter):
        @property
        def conversion_type(self) -> VariableConverter.ConversionType:
            if np.issubdtype(self.variable.dtype, np.floating):
                if len(self.variable.dimensions) == 1:
                    return self.ConversionType.ARRAYFLOAT
            return super().conversion_type

        def metadata(self) -> cpd3_variant.Metadata:
            meta = super().metadata()
            if "Smoothing" not in meta:
                cell_methods = self._split_colon_fields(getattr(self.variable, "cell_methods", None))
                if not cell_methods.get("time"):
                    meta["Smoothing"] = {"Mode": "None"}
            return meta

        def convert(self, result: typing.List[typing.Tuple[Identity, typing.Any]]) -> None:
            use_cut_size = "cut_size" in self.ancillary_variables

            self.record.global_fanout(result, self.base_name.to_metadata(), self.metadata(),
                                      use_cut_size=use_cut_size)

            self.record.global_fanout(
                result, self.base_name,
                self.conversion_type.converter(self.variable[...]),
                use_cut_size=use_cut_size
            )

    class WavelengthConverter(GenericConverter):
        def __init__(self, record: "DataRecord", variable: Variable):
            super().__init__(record, variable)

            self.base_name: typing.List[Name] = list()
            for code in self.record.wavelength_suffix:
                variable_name: str = variable.variable_id
                if "_" not in variable_name:
                    variable_name = variable_name + code + "_" + self.record.converter.source
                else:
                    prefix, suffix = variable_name.split("_", 1)
                    variable_name = prefix + code + "_" + suffix
                self.base_name.append(Name(self.record.converter.station, 'raw', variable_name))

            self.wavelength_dimension_index: int = self.variable.dimensions.index("wavelength")

            self.wavelengths: typing.List[float] = list()
            wavelengths = self.variable.group().variables["wavelength"]
            for i in range(len(wavelengths)):
                self.wavelengths.append(self.convert_float(wavelengths[i]))

        @property
        def conversion_type(self) -> VariableConverter.ConversionType:
            if np.issubdtype(self.variable.dtype, np.floating):
                if len(self.variable.dimensions) == 3:
                    return self.ConversionType.ARRAYFLOAT
                return self.ConversionType.FLOAT
            return super().conversion_type

        def wavelength_metadata(self, wavelength_index: int) -> cpd3_variant.Metadata:
            return super().metadata()

        def convert(self, result: typing.List[typing.Tuple[Identity, typing.Any]]) -> None:
            use_cut_size = "cut_size" in self.ancillary_variables

            base_converter = self.conversion_type.converter

            def index_converter(i: int) -> typing.Callable[[typing.Any], typing.Any]:
                def cnv(v: np.ndarray) -> typing.Any:
                    return base_converter(
                        v[tuple([
                            np.s_[:] if dimension != self.wavelength_dimension_index else i
                            for dimension in range(1, len(v.shape) + 1)
                        ])])
                return cnv

            for i in range(len(self.base_name)):
                wavelength_name = self.base_name[i]
                wavelength_center = self.wavelengths[i]

                meta = self.wavelength_metadata(i)
                meta["Wavelength"] = wavelength_center
                self.record.global_fanout(result, wavelength_name.to_metadata(), meta,
                                          use_cut_size=use_cut_size)

                self.record.value_convert(result, wavelength_name, self.variable,
                                          index_converter(i),
                                          use_cut_size=use_cut_size)

                self.record.value_coverage(result, wavelength_name, use_cut_size=use_cut_size)

    _IGNORED_VARIABLES = frozenset({
        "time", "averaged_time", "averaged_count",
        "cut_size",
        "wavelength",
        "standard_temperature", "standard_pressure",
    })

    def variable_converter(self, variable: Variable) -> typing.Optional[VariableConverter]:
        if variable.name == "system_flags":
            return self.FlagsConverter(self, variable)
        if variable.name in self._IGNORED_VARIABLES:
            return None
        if not getattr(variable, "variable_id", None):
            return None
        if "time" not in variable.dimensions:
            return self.ConstantConverter(self, variable)
        if "wavelength" in variable.dimensions:
            return self.WavelengthConverter(self, variable)
        return self.GenericConverter(self, variable)

    def convert(self, result: typing.List[typing.Tuple[Identity, typing.Any]]) -> None:
        for var in self.group.variables.values():
            converter = self.variable_converter(var)
            if not converter:
                continue
            converter.convert(result)


class StateRecord(RecordConverter):
    def __init__(self, converter: Converter, group: Group):
        super().__init__(converter, group)

        self.times: typing.List[typing.Tuple[float, float]] = list()

        record_times = self.group.variables["time"]
        for i in range(len(record_times)):
            start_time: float = float(record_times[i]) / 1000.0

            if i != len(record_times) - 1:
                end_time: float = float(record_times[i + 1]) / 1000.0
            else:
                if self.converter.file_end_time:
                    end_time: float = self.converter.file_end_time
                else:
                    raise ValueError("unable to determine state end time")

            self.times.append((start_time, end_time))

    def global_fanout(self, result: typing.List[typing.Tuple[Identity, typing.Any]], name: Name,
                      value: typing.Any) -> None:
        start_time: float = self.converter.file_start_time
        end_time: float = self.converter.file_end_time

        if len(self.times) > 0 and (not start_time or (self.times[0][0] and self.times[0][0] < start_time)):
            start_time = self.times[0][0]
        if not end_time and len(self.times) > 0:
            end_time = self.times[-1][1]

        if not start_time or (self.converter.system_start_time and self.converter.system_start_time < start_time):
            start_time = self.converter.system_start_time

        if not start_time or not end_time:
            return

        result.append((Identity(name=name, start=start_time, end=end_time), value))

    def value_convert(self, result: typing.List[typing.Tuple[Identity, typing.Any]],
                      name: Name, variable: Variable,
                      c: typing.Callable[[typing.Any], typing.Any]) -> None:
        for i in range(len(self.times)):
            v = c(variable[i, ...])
            result.append((Identity(name=name, start=self.times[i][0], end=self.times[i][1]), v))

    class GenericConverter(VariableConverter):
        def __init__(self, record: "StateRecord", variable: Variable):
            super().__init__(record, variable)
            self.record: "StateRecord" = record

            variable_name: str = variable.variable_id
            if "_" not in variable_name:
                if "wavelength" in self.ancillary_variables and len(self.record.wavelength_suffix) == 1:
                    variable_name = variable_name + self.record.wavelength_suffix[0]

                variable_name = variable_name + "_" + self.record.converter.source
            self.base_name = Name(self.record.converter.station, 'raw', variable_name)

        def metadata(self) -> cpd3_variant.Metadata:
            conversion_type = self.conversion_type
            metadata_type = conversion_type.metadata

            meta = metadata_type()
            self.insert_metadata(meta)
            meta["Smoothing"] = {"Mode": "None"}

            if "wavelength" in self.ancillary_variables:
                wavelengths = self.variable.group().variables.get("wavelength")
                if wavelengths is not None and len(wavelengths.dimensions) == 0 or len(wavelengths) == 1:
                    wl = float(wavelengths[0])
                    if isfinite(wl):
                        meta["Wavelength"] = wl

            if conversion_type == self.ConversionType.ARRAYFLOAT:
                if 'time' in self.variable.dimensions:
                    meta["Count"] = int(self.variable.shape[-2])
                else:
                    meta["Count"] = int(self.variable.shape[-1])
                children = cpd3_variant.MetadataReal()
                meta["Children"] = children
                if meta.get("Units"):
                    children["Units"] = meta["Units"]
                if meta.get("Format"):
                    children["Format"] = meta["Format"]

            return meta

        def convert(self, result: typing.List[typing.Tuple[Identity, typing.Any]]) -> None:
            self.record.global_fanout(result, self.base_name.to_metadata(), self.metadata())

            self.record.value_convert(result, self.base_name, self.variable,
                                      self.conversion_type.converter)

    class WavelengthConverter(GenericConverter):
        def __init__(self, record: "StateRecord", variable: Variable):
            super().__init__(record, variable)

            self.base_name: typing.List[Name] = list()
            for code in self.record.wavelength_suffix:
                variable_name: str = variable.variable_id
                if "_" not in variable_name:
                    variable_name = variable_name + code + "_" + self.record.converter.source
                else:
                    prefix, suffix = variable_name.split("_", 1)
                    variable_name = prefix + code + "_" + suffix
                self.base_name.append(Name(self.record.converter.station, 'raw', variable_name))

            self.wavelength_dimension_index: int = self.variable.dimensions.index("wavelength")

            self.wavelengths: typing.List[float] = list()
            wavelengths = self.variable.group().variables["wavelength"]
            for i in range(len(wavelengths)):
                self.wavelengths.append(self.convert_float(wavelengths[i]))

        @property
        def conversion_type(self) -> VariableConverter.ConversionType:
            if np.issubdtype(self.variable.dtype, np.floating):
                if len(self.variable.dimensions) == 3:
                    return self.ConversionType.ARRAYFLOAT
                return self.ConversionType.FLOAT
            return super().conversion_type

        def convert(self, result: typing.List[typing.Tuple[Identity, typing.Any]]) -> None:
            base_converter = self.conversion_type.converter

            def index_converter(i: int) -> typing.Callable[[typing.Any], typing.Any]:
                def cnv(v: typing.Any) -> typing.Any:
                    return base_converter(
                        v[tuple([
                            np.s_[:] if dimension != self.wavelength_dimension_index else i
                            for dimension in range(1, len(v.shape) + 1)
                        ])])
                return cnv

            for i in range(len(self.base_name)):
                wavelength_name = self.base_name[i]
                wavelength_center = self.wavelengths[i]

                meta = self.metadata()
                meta["Wavelength"] = wavelength_center
                self.record.global_fanout(result, wavelength_name.to_metadata(), meta)

                self.record.value_convert(result, wavelength_name, self.variable,
                                          index_converter(i))

    _IGNORED_VARIABLES = frozenset({
        "time",
        "wavelength",
        "standard_temperature", "standard_pressure",
    })

    def variable_converter(self, variable: Variable) -> typing.Optional[VariableConverter]:
        if variable.name in self._IGNORED_VARIABLES:
            return None
        if not getattr(variable, "variable_id", None):
            return None
        if "wavelength" in variable.dimensions:
            return self.WavelengthConverter(self, variable)
        return self.GenericConverter(self, variable)

    def convert(self, result: typing.List[typing.Tuple[Identity, typing.Any]]) -> None:
        for var in self.group.variables.values():
            converter = self.variable_converter(var)
            if not converter:
                continue
            converter.convert(result)
