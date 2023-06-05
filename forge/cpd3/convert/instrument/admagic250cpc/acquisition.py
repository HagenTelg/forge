import typing
import netCDF4
import forge.cpd3.variant as cpd3_variant
from math import isfinite
from forge.cpd3.identity import Identity, Name
from forge.acquisition.instrument.admagic250cpc.parameters import Parameters as ParameterStorage
from ..default.converter import Converter as BaseConverter, RecordConverter, VariableConverter


class Parameters(RecordConverter):
    def __init__(self, converter: "Converter", group: netCDF4.Group):
        super().__init__(converter, group)
        self.converter: "Converter" = converter

        variable_name = "ZPARAMETERS_" + self.converter.source
        self.base_name = Name(self.converter.station, 'raw', variable_name)

    def metadata(self) -> cpd3_variant.Metadata:
        meta = cpd3_variant.MetadataHash()
        self.converter.insert_metadata(meta)
        meta["Description"] = "Instrument parameter data"
        meta["Smoothing"] = {"Mode": "None"}
        meta.children["Raw"] = cpd3_variant.MetadataString({
            "Description": "Raw parameters data",
        })

        def declare_value(name: str, metadata_type: typing.Type[cpd3_variant.Metadata]) -> None:
            var = self.group.variables.get(name)
            if var is None:
                return
            var_meta = metadata_type()
            VariableConverter.translate_metadata(var, var_meta)
            meta.children[name] = var_meta

        for name in ParameterStorage.INTEGER_PARAMETERS:
            declare_value(name, cpd3_variant.MetadataInteger)
        for name in ParameterStorage.FLOAT_PARAMETERS:
            declare_value(name, cpd3_variant.MetadataReal)

        def declare_temperature(name: str) -> None:
            var = self.group.variables.get(name)
            if var is None:
                return
            var_meta = cpd3_variant.MetadataHash()

            description = getattr(var, "long_name", None)
            if description:
                var_meta["Description"] = str(description)

            var_meta.children["Setpoint"] = cpd3_variant.MetadataReal({
                "Description": "Temperature setpoint",
                "Units": "°C",
                "Format": "000.0",
            })
            var_meta.children["Mode"] = cpd3_variant.MetadataString({
                "Description": "Temperature control mode information",
            })
            meta.children[name] = var_meta

        for name in ParameterStorage.TEMPERATURE_PARAMETERS:
            declare_temperature(name)

        return meta

    def value(self) -> typing.Dict[str, typing.Any]:
        result: typing.Dict[str, typing.Any] = dict()

        def add_value(target: str, variable: str, c: typing.Callable[[typing.Any], typing.Any]) -> None:
            var = self.group.variables.get(variable)
            if var is None:
                return
            val = var[0]
            result[target] = c(val)

        add_value("Raw", "instrument_parameters", str)
        for name in ParameterStorage.INTEGER_PARAMETERS:
            add_value(name, name, int)
        for name in ParameterStorage.FLOAT_PARAMETERS:
            add_value(name, name, float)

        def add_temperature(name: str) -> None:
            var = self.group.variables.get(name)
            if var is None:
                return
            val = var[0]
            contents = {
                "Setpoint": float(val),
            }
            var = self.group.variables.get(name + "_mode")
            if var is not None:
                val = var[0]
                contents["Mode"] = str(val)

            result[name] = contents

        for name in ParameterStorage.TEMPERATURE_PARAMETERS:
            add_temperature(name)

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
        if group.name == "parameters":
            return Parameters(self, group)
        return super().record_converter(group)


def convert(station: str, root: netCDF4.Dataset) -> typing.List[typing.Tuple[Identity, typing.Any]]:
    return Converter(station, root).convert()
