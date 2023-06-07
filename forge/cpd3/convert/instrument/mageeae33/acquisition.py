import typing
import netCDF4
import forge.cpd3.variant as cpd3_variant
from math import isfinite
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
            self.spot_two = record.group.variables.get('spot_two_normalization')

        def metadata(self) -> cpd3_variant.Metadata:
            meta = cpd3_variant.MetadataHash()
            self.insert_metadata(meta)


            meta["Description"] = "Spot sampling parameters"
            meta["Smoothing"] = {"Mode": "None"}
            meta.pop("Format", None)
            meta.pop("Units", None)

            wavelengths = self.variable.group().variables["wavelength"]
            for spot in range(2):
                spot_meta = cpd3_variant.MetadataArray()
                meta.children['In' + str(spot+1)] = spot_meta
                spot_meta["Description"] = f"Spot {spot+1} start normalized intensities"
                spot_meta["Wavelengths"] = [float(wavelengths[i]) for i in range(len(wavelengths))]

                child_meta = cpd3_variant.MetadataReal()
                spot_meta["Children"] = child_meta
                child_meta["Description"] = "Spot start normalized intensity"
                child_meta["Format"] = "00.0000000"

            return meta

        def convert(self, result: typing.List[typing.Tuple[Identity, typing.Any]]) -> None:
            self.record.global_fanout(result, self.base_name.to_metadata(), self.metadata())

            raw_converter = self.ConversionType.ARRAYFLOAT.converter

            for i in range(len(self.record.times)):
                v = dict()
                v['In1'] = raw_converter(self.variable[i, ...])
                if self.spot_two:
                    v['In2'] = raw_converter(self.spot_two[i, ...])
                result.append((Identity(name=self.base_name,
                                        start=self.record.times[i][0], end=self.record.times[i][1]), v))

    def variable_converter(self, variable: netCDF4.Variable) -> typing.Optional[VariableConverter]:
        if variable.name == "spot_one_normalization":
            return self.SpotConverter(self, variable)
        elif variable.name == "spot_two_normalization":
            return None
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
        meta["Description"] = "SG parameter read data"
        meta["Smoothing"] = {"Mode": "None"}
        meta.children["Frame"] = cpd3_variant.MetadataString({
            "Description": "Whole frame data (including trailing data record if applicable)",
        })
        meta.children["Sigma"] = cpd3_variant.MetadataArray({
            "Description": "Absorption mass efficiency settings in channel order",
            "Children": cpd3_variant.MetadataReal({
                "Description": "Absorption mass efficiency",
            })
        })

        return meta

    def value(self) -> typing.Dict[str, typing.Any]:
        result: typing.Dict[str, typing.Any] = dict()

        def add_value(target: str, variable: str, c: typing.Callable[[typing.Any], typing.Any]) -> None:
            var = self.group.variables.get(variable)
            if var is None:
                return
            val = var[0]
            result[target] = c(val)

        def add_array(target: str, variable: str, c: typing.Callable[[typing.Any], typing.Any]) -> None:
            var = self.group.variables.get(variable)
            if var is None:
                return
            out = list()
            for i in range(len(var)):
                val = var[i]
                if val.mask:
                    val = None
                else:
                    val = c(val)
                out.append(val)
            result[target] = out

        add_value("Frame", "instrument_parameters", str)
        add_array("Sigma", "mass_absorption_efficiency", float)

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
            return FilterRecord(self, group)
        elif group.name == "data":
            return DataRecord(self, group)
        elif group.name == "parameters":
            return Parameters(self, group)
        return super().record_converter(group)


def convert(station: str, root: netCDF4.Dataset) -> typing.List[typing.Tuple[Identity, typing.Any]]:
    return Converter(station, root).convert()
