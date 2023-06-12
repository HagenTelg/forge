import typing
import netCDF4
import numpy as np
from forge.cpd3.identity import Identity, Name
from ..default.converter import Converter as BaseConverter, DataRecord, RecordConverter, VariableConverter


class SizedDataConverter(DataRecord):
    class DiameterConverter(DataRecord.GenericConverter):
        def __init__(self, record: DataRecord, variable: netCDF4.Variable):
            super().__init__(record, variable)

            self.diameter_names: typing.List[Name] = list()
            diameters = self.variable.group().variables["diameter"]
            for i in range(len(diameters)):
                d = float(diameters[i])
                if d == 1.0:
                    self.diameter_names.append(Name(
                        station=self.base_name.station,
                        archive=self.base_name.archive,
                        variable=self.base_name.variable,
                        flavors=self.base_name.flavors | {"pm1"},
                    ))
                elif d == 2.5:
                    self.diameter_names.append(Name(
                        station=self.base_name.station,
                        archive=self.base_name.archive,
                        variable=self.base_name.variable,
                        flavors=self.base_name.flavors | {"pm25"},
                    ))
                elif d == 10.0:
                    self.diameter_names.append(Name(
                        station=self.base_name.station,
                        archive=self.base_name.archive,
                        variable=self.base_name.variable,
                        flavors=self.base_name.flavors | {"pm10"},
                    ))
                else:
                    self.diameter_names.append(self.base_name)

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
                    return base_converter(v[i, ...])
                return cnv

            for i in range(len(self.diameter_names)):
                diameter_name = self.diameter_names[i]

                meta = self.metadata()
                self.record.global_fanout(result, diameter_name.to_metadata(), meta,
                                          use_cut_size=False)

                self.record.value_convert(result, diameter_name, self.variable,
                                          index_converter(i),
                                          use_cut_size=False)

                self.record.value_coverage(result, diameter_name, use_cut_size=False)

    def variable_converter(self, variable: netCDF4.Variable) -> typing.Optional[VariableConverter]:
        if variable.name == "mass_concentration":
            return self.DiameterConverter(self, variable)
        return super().variable_converter(variable)


class Converter(BaseConverter):
    def record_converter(self, group: netCDF4.Group) -> typing.Optional[RecordConverter]:
        if group.name == "data":
            return SizedDataConverter(self, group)
        return super().record_converter(group)


def convert(station: str, root: netCDF4.Dataset) -> typing.List[typing.Tuple[Identity, typing.Any]]:
    return Converter(station, root).convert()
