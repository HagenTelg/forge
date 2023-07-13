import typing
import netCDF4
from math import isfinite
import forge.cpd3.variant as cpd3_variant
from forge.cpd3.identity import Identity, Name
from ..default.converter import Converter as BaseConverter, DataRecord as BaseRecord, StateRecord, RecordConverter, VariableConverter


class DataRecord(BaseRecord):
    class FlagsConverter(BaseRecord.FlagsConverter):
        def metadata(self) -> cpd3_variant.Metadata:
            meta: cpd3_variant.MetadataFlags = super().metadata()
            meta.children["STP"] = {
                "Origin": ["forge.cpd3.convert.instrument"],
                "Bits": 0x0200,
                "Description": "Data reported at STP",
            }
            return meta

        def convert(self, result: typing.List[typing.Tuple[Identity, typing.Any]]) -> None:
            flags: typing.List[typing.Tuple[Identity, typing.Any]] = list()
            super().convert(flags)
            for _, s in flags:
                if not isinstance(s, set):
                    continue
                s.add("STP")
            result.extend(flags)

    class AbsorptionWavelengthConverter(BaseRecord.WavelengthConverter):
        def wavelength_metadata(self, wavelength_index: int) -> cpd3_variant.Metadata:
            meta: cpd3_variant.Metadata = super().wavelength_metadata(wavelength_index)

            params_meta: typing.Dict[str, typing.Any] = meta['Processing'][-1].get('Parameters')
            if not params_meta:
                params_meta = dict()
                meta['Processing'][-1]['Parameters'] = params_meta

            params_meta['Efficiency'] = self.record.converter.ebc_efficiency

            return meta

    def variable_converter(self, variable: netCDF4.Variable) -> typing.Optional[VariableConverter]:
        if "wavelength" in variable.dimensions and getattr(variable, "variable_id", None) in ("Ba", "Bac", "X"):
            return self.AbsorptionWavelengthConverter(self, variable)
        return super().variable_converter(variable)


class Parameters(RecordConverter):
    def __init__(self, converter: "Converter", group: netCDF4.Group):
        super().__init__(converter, group)
        self.converter: "Converter" = converter

        variable_name = "ZPARAMETERS_" + self.converter.source
        self.base_name = Name(self.converter.station, 'raw', variable_name)

    def metadata(self) -> cpd3_variant.Metadata:
        meta = cpd3_variant.MetadataArray()
        self.converter.insert_metadata(meta)
        meta["Description"] = "List of all system parameters"
        meta["Smoothing"] = {"Mode": "None"}
        meta["Children"] = cpd3_variant.MetadataString({
            "Description": "System parameter line",
        })

        return meta

    def value(self) -> typing.Optional[typing.List[str]]:
        var = self.group.variables.get("instrument_parameters")
        if var is None:
            return
        return str(var[0]).split('\n')

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
    def __init__(self, station: str, root: netCDF4.Dataset):
        super().__init__(station, root)

        self.ebc_efficiency: typing.Optional[float] = None

        params_group = self.root.groups.get("parameters")
        params_meta: typing.Dict[str, typing.Any] = self.processing_metadata[-1].get('Parameters')
        if not params_meta:
            params_meta = dict()
            self.processing_metadata[-1]['Parameters'] = params_meta
        if params_group is not None:
            ebc_efficiency = params_group.variables.get("mass_absorption_efficiency")
            if ebc_efficiency is not None:
                self.ebc_efficiency = float(ebc_efficiency[0])

    def record_converter(self, group: netCDF4.Group) -> typing.Optional[RecordConverter]:
        if group.name == "data":
            return DataRecord(self, group)
        elif group.name == "state":
            return None
        elif group.name == "parameters":
            return Parameters(self, group)
        return super().record_converter(group)


def convert(station: str, root: netCDF4.Dataset) -> typing.List[typing.Tuple[Identity, typing.Any]]:
    return Converter(station, root).convert()
