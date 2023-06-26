import typing
import netCDF4
import forge.cpd3.variant as cpd3_variant
from math import isfinite
from forge.cpd3.identity import Identity, Name
from ..default.converter import Converter as BaseConverter, DataRecord as BaseRecord, StateRecord, RecordConverter, VariableConverter


class WeatherCode(StateRecord):
    def variable_converter(self, variable: netCDF4.Variable) -> typing.Optional[VariableConverter]:
        if variable.name == "synop_weather_code":
            converter: StateRecord.GenericConverter = super().variable_converter(variable)
            converter.base_name.variable = "WX2_" + variable.variable_id
            return converter
        return super().variable_converter(variable)


class Parameters(RecordConverter):
    def __init__(self, converter: "Converter", group: netCDF4.Group):
        super().__init__(converter, group)
        self.converter: "Converter" = converter

        variable_name = "ZPARAMETERS_" + self.converter.source
        self.base_name = Name(self.converter.station, 'raw', variable_name)

    def metadata(self) -> cpd3_variant.Metadata:
        meta = cpd3_variant.MetadataHash()
        self.converter.insert_metadata(meta)
        meta["Description"] = "Parameter settings"
        meta["Smoothing"] = {"Mode": "None"}
        meta.children["System"] = cpd3_variant.MetadataArray({
            "Description": "System parameters",
            "Children": cpd3_variant.MetadataString({
                "Description": "Parameter line",
            })
        })
        meta.children["Weather"] = cpd3_variant.MetadataArray({
            "Description": "Weather parameters",
            "Children": cpd3_variant.MetadataString({
                "Description": "Parameter line",
            })
        })

        return meta

    def value(self) -> typing.Dict[str, typing.Any]:
        result: typing.Dict[str, typing.Any] = dict()

        def add_array_lines(target: str, variable: str) -> None:
            var = self.group.variables.get(variable)
            if var is None:
                return
            result[target] = str(var[0]).split('\n')

        add_array_lines("System", "system_parameters")
        add_array_lines("Weather", "weather_parameters")

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
        if group.name == "state":
            return WeatherCode(self, group)
        elif group.name == "parameters":
            return Parameters(self, group)
        return super().record_converter(group)


def convert(station: str, root: netCDF4.Dataset) -> typing.List[typing.Tuple[Identity, typing.Any]]:
    return Converter(station, root).convert()
