import typing
import netCDF4
import numpy as np
import forge.cpd3.variant as cpd3_variant
from math import isfinite
from forge.cpd3.identity import Identity, Name
from ..default.converter import Converter as BaseConverter, DataRecord as BaseRecord, StateRecord, RecordConverter, VariableConverter


class DataRecord(BaseRecord):
    class ArrayUnrollConverter(BaseRecord.GenericConverter):
        def __init__(self, record: "DataRecord", variable: netCDF4.Variable):
            super().__init__(record, variable)

            self.base_name: typing.List[Name] = list()
            for i in range(record.group.dimensions[variable.dimensions[-1]].size):
                variable_name: str = variable.variable_id
                variable_name = variable_name + str(i) + "_" + self.record.converter.source
                self.base_name.append(Name(self.record.converter.station, 'raw', variable_name))

        @property
        def conversion_type(self) -> VariableConverter.ConversionType:
            return self.ConversionType.FLOAT

        def convert(self, result: typing.List[typing.Tuple[Identity, typing.Any]]) -> None:
            use_cut_size = "cut_size" in self.ancillary_variables

            base_converter = self.conversion_type.converter

            def index_converter(i: int) -> typing.Callable[[typing.Any], typing.Any]:
                def cnv(v: np.ndarray) -> typing.Any:
                    return base_converter(
                        v[tuple([
                            np.s_[:] if dimension != 1 else i
                            for dimension in range(1, len(v.shape) + 1)
                        ])])

                return cnv

            for i in range(len(self.base_name)):
                index_name = self.base_name[i]

                meta = self.metadata()
                if i == 0:
                    meta["Description"] = "Filter bypass volume"
                elif i == 1:
                    meta["Description"] = "Filter position 1 (blank) volume"
                else:
                    meta["Description"] = f"Filter position {i} volume"
                self.record.global_fanout(result, index_name.to_metadata(), self.metadata(),
                                          use_cut_size=use_cut_size)

                self.record.value_convert(result, index_name, self.variable,
                                          index_converter(i),
                                          use_cut_size=use_cut_size)

                self.record.value_coverage(result, index_name, use_cut_size=use_cut_size)

    def variable_converter(self, variable: netCDF4.Variable) -> typing.Optional[VariableConverter]:
        if variable.name in ("filter_pressure_drop", "sample_flow"):
            return None
        if variable.name == "total_volume":
            return self.ArrayUnrollConverter(self, variable)
        return super().variable_converter(variable)


class CarouselRecord(StateRecord):
    class TotalConverter(StateRecord.GenericConverter):
        def __init__(self, record: "StateRecord", variable: netCDF4.Variable):
            VariableConverter.__init__(self, record, variable)
            self.record: "StateRecord" = record
            self.base_name = Name(self.record.converter.station, 'raw', "ZTOTAL_" + self.record.converter.source)

        def metadata(self) -> cpd3_variant.Metadata:
            meta = cpd3_variant.MetadataHash()
            self.insert_metadata(meta)

            meta["Smoothing"] = {"Mode": "None"}
            meta["Description"] = "Accumulator totals"
            meta.pop("Format", None)
            meta.pop("Units", None)

            meta.children["Ff"] = cpd3_variant.MetadataInteger({
                "Description": "Accumulator ID",
                "Format": "0000000000",
            })

            for i in range(9):
                a_meta = cpd3_variant.MetadataHash()
                meta.children[f"Qt{i}"] = a_meta
                a_meta["Description"] = "Single variable total"
                a_meta.children["Time"] = cpd3_variant.MetadataReal({
                    "Description": "Total number of seconds accumulated",
                    "Format": "0000000",
                    "Units": "s",
                })
                if i == 0:
                    desc = "Filter bypass volume"
                elif i == 1:
                    desc = "Filter position 1 (blank) volume"
                else:
                    desc = f"Filter position {i} volume"
                a_meta.children["Total"] = cpd3_variant.MetadataReal({
                    "Description": desc,
                    "Format": "0000.00000",
                    "Units": "m³",
                    "Indices": [i],
                    "ReportP": 1013.25,
                    "ReportT": 0.0,
                })

            return meta

        def convert(self, result: typing.List[typing.Tuple[Identity, typing.Any]]) -> None:
            array_converter = self.ConversionType.ARRAYFLOAT.converter
            int_converter = self.ConversionType.INTEGER.converter

            filter_start_time = self.record.group.variables.get("completed_start_time")
            accumulated_seconds = self.record.group.variables.get("final_accumulated_time")

            first_start_time = None
            for i in range(len(self.record.times)):
                contents = dict()

                effective_start = None
                if filter_start_time is not None:
                    effective_start = int_converter(filter_start_time[i])
                    if effective_start:
                        contents["Ff"] = int(effective_start / 1000)
                        effective_start = effective_start / 1000.0

                effective_end = self.record.times[i][0]
                if i > 0 and not effective_start or effective_start >= effective_end:
                    effective_start = self.record.times[i-1][0]

                if not effective_start or not isfinite(effective_start):
                    continue
                if effective_start >= effective_end:
                    continue
                if first_start_time is None:
                    first_start_time = effective_start

                volumes = array_converter(self.variable[i, ...])
                if accumulated_seconds is not None:
                    seconds = array_converter(accumulated_seconds[i, ...])
                else:
                    seconds = []

                for i in range(len(volumes)):
                    sub = {
                        "Total": volumes[i]
                    }
                    if i < len(seconds):
                        sub["Time"] = seconds[i]
                    contents[f"Qt{i}"] = sub

                result.append((Identity(name=self.base_name, start=effective_start, end=effective_end), contents))

            start_time: float = self.record.converter.file_start_time
            end_time: float = self.record.converter.file_end_time

            if len(self.record.times) > 0 and (not start_time or (self.record.times[0][0] and self.record.times[0][0] < start_time)):
                start_time = self.record.times[0][0]
            if not end_time and len(self.record.times) > 0:
                end_time = self.record.times[-1][1]

            if not start_time or (self.record.converter.system_start_time and self.record.converter.system_start_time < start_time):
                start_time = self.record.converter.system_start_time

            if not start_time or (first_start_time and first_start_time < start_time):
                start_time = first_start_time

            if not start_time or not end_time:
                return

            result.append((Identity(name=self.base_name.to_metadata(), start=start_time, end=end_time), self.metadata()))

    def variable_converter(self, variable: netCDF4.Variable) -> typing.Optional[VariableConverter]:
        if variable.name == "final_volume":
            return self.TotalConverter(self, variable)
        return None


class Converter(BaseConverter):
    def record_converter(self, group: netCDF4.Group) -> typing.Optional[RecordConverter]:
        if group.name == "data":
            return DataRecord(self, group)
        elif group.name == "completed_carousel":
            return CarouselRecord(self, group)
        return super().record_converter(group)


def convert(station: str, root: netCDF4.Dataset) -> typing.List[typing.Tuple[Identity, typing.Any]]:
    return Converter(station, root).convert()
