import typing
import netCDF4
import forge.cpd3.variant as cpd3_variant
from math import isfinite
from forge.cpd3.identity import Identity, Name
from ..default.converter import Converter as BaseConverter, DataRecord as BaseRecord, VariableConverter, RecordConverter, StateRecord


class DataRecord(BaseRecord):
    def __init__(self, converter: BaseConverter, group: netCDF4.Group, at_stp: bool):
        super().__init__(converter, group)
        self.at_stp = at_stp

    class FlagsConverter(BaseRecord.FlagsConverter):
        def metadata(self) -> cpd3_variant.Metadata:
            meta: cpd3_variant.MetadataFlags = super().metadata()
            if self.record.at_stp:
                meta.children["STP"] = {
                    "Origin": ["forge.cpd3.convert.instrument"],
                    "Bits": 0x0200,
                    "Description": "Data reported at STP",
                }
            return meta

        def convert(self, result: typing.List[typing.Tuple[Identity, typing.Any]]) -> None:
            flags: typing.List[typing.Tuple[Identity, typing.Any]] = list()
            super().convert(flags)
            if self.record.at_stp:
                for _, s in flags:
                    if not isinstance(s, set):
                        continue
                    s.add("STP")
            result.extend(flags)


class PolarStateRecord(StateRecord):
    def variable_converter(self, variable: netCDF4.Variable) -> typing.Optional[VariableConverter]:
        # Conversions handled by the non-polar records
        if variable.name in ("angle", "zero_temperature", "zero_pressure"):
            return None
        return super().variable_converter(variable)


class Parameters(RecordConverter):
    def __init__(self, converter: "Converter", group: netCDF4.Group):
        super().__init__(converter, group)
        self.converter: "Converter" = converter

        variable_name = "ZEE_" + self.converter.source
        self.base_name = Name(self.converter.station, 'raw', variable_name)

    def metadata(self) -> cpd3_variant.Metadata:
        meta = cpd3_variant.MetadataHash()
        self.converter.insert_metadata(meta)
        meta["Description"] = "EE command configuration data"
        meta["Smoothing"] = {"Mode": "None"}
        meta.children["Lines"] = cpd3_variant.MetadataArray({
            "Description": "Raw response lines, in order",
            "Children": cpd3_variant.MetadataString({"Description": "EE response line"})
        })
        return meta

    def value(self) -> typing.Dict[str, typing.Any]:
        result: typing.Dict[str, typing.Any] = dict()

        raw_data = self.group.variables.get("instrument_parameters")
        if raw_data is not None:
            result["Lines"] = raw_data[0].split('\n')

        return result

    def convert(self, result: typing.List[typing.Tuple[Identity, typing.Any]]) -> None:
        start_time: float = self.converter.file_start_time
        if start_time is None or not isfinite(start_time):
            return

        meta_start_time = start_time
        end_time: float = self.converter.file_end_time
        if self.converter.system_start_time and self.converter.system_start_time < meta_start_time:
            meta_start_time = self.converter.system_start_time

        result.append((Identity(name=self.base_name.to_metadata(), start=meta_start_time, end=end_time),
                       self.metadata()))
        result.append((Identity(name=self.base_name, start=start_time, end=end_time),
                       self.value()))


class Converter(BaseConverter):
    def record_converter(self, group: netCDF4.Group) -> typing.Optional[RecordConverter]:
        if group.name == "data":
            standard_t = group.variables.get("standard_temperature")
            standard_p = group.variables.get("standard_pressure")
            return DataRecord(
                self, group,
                standard_t is not None and isfinite(float(standard_t[0])) and
                standard_p is not None and isfinite(float(standard_p[0]))
            )
        elif group.name == "zero":
            return StateRecord(self, group)
        elif group.name == "spancheck":
            return StateRecord(self, group)
        elif group.name == "polar_zero":
            return PolarStateRecord(self, group)
        elif group.name == "polar_spancheck":
            return PolarStateRecord(self, group)
        elif group.name == "parameters":
            return Parameters(self, group)
        return super().record_converter(group)


def convert(station: str, root: netCDF4.Dataset) -> typing.List[typing.Tuple[Identity, typing.Any]]:
    return Converter(station, root).convert()
