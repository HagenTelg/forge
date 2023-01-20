import typing
import netCDF4
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
                self.processing_metadata[-1]['Parameters'] = params_meta

            if wavelength_index < len(self.record.converter.ebc_efficiency):
                params_meta['Efficiency'] = self.record.converter.ebc_efficiency[wavelength_index]

            return meta

    def variable_converter(self, variable: netCDF4.Variable) -> typing.Optional[VariableConverter]:
        if "wavelength" in variable.dimensions and getattr(variable, "variable_id", None) == "Ba":
            return self.AbsorptionWavelengthConverter(self, variable)
        return super().variable_converter(variable)


class Converter(BaseConverter):
    def __init__(self, station: str, root: netCDF4.Dataset):
        super().__init__(station, root)

        self.ebc_efficiency: typing.List[float] = []
        # Slow reporting, so discard this
        self.expected_record_interval = None

        params_group = self.root.groups.get("parameters")
        params_meta: typing.Dict[str, typing.Any] = self.processing_metadata[-1].get('Parameters')
        if not params_meta:
            params_meta = dict()
            self.processing_metadata[-1]['Parameters'] = params_meta
        if params_group is not None:
            report_temperature = params_group.variables.get("instrument_standard_temperature")
            if report_temperature is not None:
                params_meta['SampleT'] = float(report_temperature[0])
            report_pressure = params_group.variables.get("instrument_standard_pressure")
            if report_pressure is not None:
                params_meta['SampleP'] = float(report_pressure[0])
            mean_ratio = params_group.variables.get("mean_ratio")
            if mean_ratio is not None:
                params_meta['MeanRatio'] = float(mean_ratio[0])
            spot_size = params_group.variables.get("spot_area")
            if spot_size is not None:
                params_meta['SpotSize'] = float(spot_size[0])

            ebc_efficiency = params_group.variables.get("mass_absorption_efficiency")
            if ebc_efficiency is not None:
                for i in range(len(ebc_efficiency)):
                    self.ebc_efficiency.append(float(ebc_efficiency[i]))

    def record_converter(self, group: netCDF4.Group) -> typing.Optional[RecordConverter]:
        if group.name == "data":
            return DataRecord(self, group)
        elif group.name == "parameters":
            return None
        return super().record_converter(group)


def convert(station: str, root: netCDF4.Dataset) -> typing.List[typing.Tuple[Identity, typing.Any]]:
    return Converter(station, root).convert()
