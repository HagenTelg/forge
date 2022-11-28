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


class FilterRecord(StateRecord):
    class SpotConverter(StateRecord.GenericConverter):
        def __init__(self, record: "StateRecord", variable: netCDF4.Variable):
            super().__init__(record, variable)
            self.base_name = Name(self.record.converter.station, 'raw', "ZSPOT_" + self.record.converter.source)

        def metadata(self) -> cpd3_variant.Metadata:
            meta = cpd3_variant.MetadataHash()
            self.insert_metadata(meta)

            meta["Smoothing"] = {"Mode": "None"}
            meta.pop("Format", None)
            meta.pop("Units", None)

            wavelengths = self.variable.group().variables["wavelength"]
            for i in range(len(wavelengths)):
                code = self.record.wavelength_suffix[i]
                wl_meta = cpd3_variant.MetadataReal()
                meta.children['In' + code] = wl_meta
                wl_meta["Description"] = "Spot start normalized intensity"
                wl_meta["Wavelength"] = float(wavelengths[i])
                wl_meta["Format"] = "00.0000000"

            return meta

        def convert(self, result: typing.List[typing.Tuple[Identity, typing.Any]]) -> None:
            self.record.global_fanout(result, self.base_name.to_metadata(), self.metadata())

            float_converter = self.ConversionType.FLOAT.converter

            def cnv(v: typing.Any) -> typing.Any:
                result = dict()
                for i in range(len(self.record.wavelength_suffix)):
                    code = self.record.wavelength_suffix[i]
                    wl_v = float_converter(v[i])
                    result['In' + code] = wl_v
                return result

            self.record.value_convert(result, self.base_name, self.variable, cnv)

    def variable_converter(self, variable: netCDF4.Variable) -> typing.Optional[VariableConverter]:
        if variable.name == "spot_normalization":
            return self.SpotConverter(self, variable)
        return super().variable_converter(variable)


class Converter(BaseConverter):
    def record_converter(self, group: netCDF4.Group) -> typing.Optional[RecordConverter]:
        if group.name == "state":
            return FilterRecord(self, group)
        elif group.name == "data":
            return DataRecord(self, group)
        elif group.name == "parameters":
            return None
        return super().record_converter(group)


def convert(station: str, root: netCDF4.Dataset) -> typing.List[typing.Tuple[Identity, typing.Any]]:
    return Converter(station, root).convert()
