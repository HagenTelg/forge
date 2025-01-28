import typing
import numpy as np
import forge.data.structure.variable as netcdf_var
import forge.data.structure.timeseries as netcdf_timeseries
from math import nan
from forge.data.structure.stp import standard_temperature, standard_pressure
from .converter import InstrumentConverter, Identity, Selection, read_archive


class Converter(InstrumentConverter):
    CAROUSEL_SIZE = 8

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._average_interval: typing.Optional[float] = None

    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"aerosol"}

    @property
    def instrument_type(self) -> typing.Optional[str]:
        return "filtercarousel"

    @property
    def average_interval(self) -> typing.Optional[float]:
        return self._average_interval

    @property
    def flow_source(self) -> typing.Optional[str]:
        return f"Q_Q{self.instrument_id[1:]}"

    def pressure_drop_source(self, filter_idx: int) -> typing.Optional[str]:
        return f"Pd_P{self.instrument_id[1:-1]}{filter_idx}"

    # Specialized since we need the time of the end of the carousel
    def load_total(self) -> "Converter.Data":
        def convert_loaded(values: typing.List[typing.Tuple[Identity, typing.Any, float]]):
            convert_values = list()
            convert_times: typing.List[int] = list()
            for idx in range(len(values)):
                identity = values[idx][0]
                time_point = identity.end
                if not time_point:
                    continue
                if time_point >= self.file_end:
                    continue
                if time_point < self.file_start:
                    if idx + 1 >= len(values) or values[idx + 1][0].end > self.file_start:
                        pass
                    else:
                        continue

                converted = dict(values[idx][1])
                convert_times.append(int(round(time_point * 1000)))
                convert_values.append(converted)

            if len(convert_times) == 0:
                return np.empty((0,), dtype=np.int64), np.empty((0,), dtype=dict)
            return np.array(convert_times, dtype=np.int64), np.array(convert_values, dtype=dict)

        return self.Data(*convert_loaded(read_archive([Selection(
            start=self.file_start - 9 * 4 * 24 * 60 * 60,  # Need the prior carousel, so extend by more than the max length
            end=self.file_end,
            stations=[self.station],
            archives=[self.archive],
            variables=[f"ZTOTAL_{self.instrument_id}"],
            include_meta_archive=False,
            include_default_station=False,
            lacks_flavors=["cover", "stats"],
        )])))

    def run(self) -> bool:
        data_Qt = [
            self.load_variable(f"Qt{i}_{self.instrument_id}") for i in range(self.CAROUSEL_SIZE+1)
        ]
        if not any([v.time.shape != 0 for v in data_Qt]):
            return False
        self._average_interval = self.calculate_average_interval(np.concatenate([v.time for v in data_Qt]))
        if not super().run():
            return False

        flow_variable = self.flow_source
        if flow_variable:
            data_Q = self.load_variable(flow_variable)
        else:
            data_Q = self.Data(np.empty((0,), np.int64), np.empty((0,), np.float64))
        if data_Q.time.shape[0] == 0:
            data_Q = self.load_variable(f"Q_{self.instrument_id}")

        data_Pd = []
        for idx in range(self.CAROUSEL_SIZE):
            var = self.pressure_drop_source(idx+1)
            if var is None:
                data_Pd.append(self.Data(np.empty((0,), np.int64), np.empty((0,), np.float64)))
            else:
                data_Pd.append(self.load_variable(var))
            if data_Pd[-1].time.shape[0] == 0:
                data_Pd[-1] = self.load_variable(f"Pd{idx+1}_{self.instrument_id}")

        def convert_Ff(x):
            if x is None:
                return 0
            x = int(x)
            if x < 1722470400:  # 2024-08-01 (before first deployment)
                x *= 1000
            return x

        data_Fn = self.load_state(f"Fn_{self.instrument_id}", dtype=np.uint64)
        data_Ff = self.load_state(f"Ff_{self.instrument_id}", dtype=np.uint64, convert=convert_Ff)
        data_Fp = self.load_state(f"Fp_{self.instrument_id}", dtype=np.uint64)
        carousel = self.load_total()

        g, times = self.data_group(data_Qt)
        standard_temperature(g)
        standard_pressure(g)
        self.declare_system_flags(g, times)

        var_Q = g.createVariable("sample_flow", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_sample_flow(var_Q)
        netcdf_timeseries.variable_coordinates(g, var_Q)
        var_Q.variable_id = "Q"
        var_Q.coverage_content_type = "physicalMeasurement"
        var_Q.cell_methods = "time: mean"
        var_Q.long_name = "flow through the active filter"
        var_Q.ancillary_variables = "standard_temperature standard_pressure"
        if flow_variable:
            var_Q.measurement_source_override = flow_variable
        self.apply_data(times, var_Q, data_Q)

        g.createDimension("total_volume", self.CAROUSEL_SIZE+1)
        var_Qt = g.createVariable("total_volume", "f8", ("time", "total_volume"), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_Qt)
        var_Qt.variable_id = "Qt"
        var_Qt.coverage_content_type = "physicalMeasurement"
        var_Qt.cell_methods = "time: last"
        var_Qt.long_name = "total volume through each filter with the first (zero) as the bypass line"
        var_Qt.units = "m3"
        var_Qt.C_format = "%10.5f"
        var_Qt.ancillary_variables = "standard_temperature standard_pressure"
        for idx in range(self.CAROUSEL_SIZE+1):
            self.apply_data(
                times, var_Qt, data_Qt[idx].time, data_Qt[idx].value, (idx,),
            )

        g.createDimension("filter_pressure_drop", self.CAROUSEL_SIZE)
        var_Pd = g.createVariable("filter_pressure_drop", "f8", ("time", "filter_pressure_drop"), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_Pd)
        var_Pd.variable_id = "Pd"
        var_Pd.coverage_content_type = "physicalMeasurement"
        var_Pd.cell_methods = "time: mean"
        var_Pd.long_name = "pressure drop across each filter in the carousel"
        var_Pd.units = "hPa"
        var_Pd.C_format = "%5.1f"
        for idx in range(self.CAROUSEL_SIZE):
            self.apply_data(
                times, var_Pd, data_Pd[idx].time, data_Pd[idx].value, (idx,),
            )

        selected_idx = 0
        for idx in range(1, len(data_Qt)):
            if data_Qt[idx].time.shape[0] > data_Qt[selected_idx].time.shape[0]:
                selected_idx = idx
        self.apply_coverage(g, times, f"Qt{selected_idx}_{self.instrument_id}")

        g, times = self.state_group([data_Ff, data_Fn, data_Fp])

        if data_Ff.time.shape[0] != 0:
            var_Ff = g.createVariable("carousel_start_time", "u8", ("time",), fill_value=False)
            netcdf_timeseries.variable_coordinates(g, var_Ff)
            var_Ff.variable_id = "Ff"
            var_Ff.coverage_content_type = "auxiliaryInformation"
            var_Ff.cell_methods = "time: point"
            var_Ff.long_name = "start time of the carousel"
            var_Ff.units = "milliseconds since 1970-01-01 00:00:00"
            self.apply_state(times, var_Ff, data_Ff)

        var_Fn = g.createVariable("active_filter", "u8", ("time",), fill_value=False)
        netcdf_timeseries.variable_coordinates(g, var_Fn)
        var_Fn.variable_id = "Fn"
        var_Fn.coverage_content_type = "auxiliaryInformation"
        var_Fn.cell_methods = "time: point"
        var_Fn.long_name = "currently accumulating filter number or zero for the bypass"
        var_Fn.C_format = "%2llu"
        self.apply_state(times, var_Fn, data_Fn)

        if data_Fp.time.shape[0] != 0:
            var_Fp = g.createVariable("measurement_filter", "u8", ("time",), fill_value=False)
            netcdf_timeseries.variable_coordinates(g, var_Fp)
            var_Fp.variable_id = "Fp"
            var_Fp.coverage_content_type = "auxiliaryInformation"
            var_Fp.cell_methods = "time: point"
            var_Fp.long_name = "desired measurement carousel filter number or zero when complete"
            var_Fp.C_format = "%2llu"
            self.apply_state(times, var_Fp, data_Fp)

        if carousel.time.shape[0] != 0:
            g, times = self.state_group([carousel], name="completed_carousel")
            standard_temperature(g)
            standard_pressure(g)

            data_Ff = []
            data_Qt = []
            data_seconds = []
            for carousel_data in carousel.value:
                data_Ff.append(int(carousel_data.get("Ff", 0)) * 1000)
                add_Qt = [nan] * (self.CAROUSEL_SIZE+1)
                add_seconds = [nan] * (self.CAROUSEL_SIZE+1)
                for i in range(self.CAROUSEL_SIZE+1):
                    pos_data = carousel_data.get(f"Qt{i}")
                    if not pos_data:
                        continue
                    add_Qt[i] = float(pos_data.get("Total", nan))
                    add_seconds[i] = float(pos_data.get("Time", nan))

                data_Qt.append(add_Qt)
                data_seconds.append(add_seconds)

            data_Ff = np.array(data_Ff, dtype=np.uint64)
            data_Qt = np.array(data_Qt, dtype=np.float64)
            data_seconds = np.array(data_seconds, dtype=np.float64)

            var_Ff = g.createVariable("completed_start_time", "u8", ("time",), fill_value=False)
            netcdf_timeseries.variable_coordinates(g, var_Ff)
            var_Ff.coverage_content_type = "auxiliaryInformation"
            var_Ff.cell_methods = "time: point"
            var_Ff.long_name = "start time of the completed carousel"
            var_Ff.units = "milliseconds since 1970-01-01 00:00:00"
            self.apply_state(times, var_Ff, carousel.time, data_Ff)

            g.createDimension("final_volume", self.CAROUSEL_SIZE + 1)
            var_Qt = g.createVariable("final_volume", "f8", ("time", "final_volume"), fill_value=nan)
            netcdf_timeseries.variable_coordinates(g, var_Qt)
            var_Qt.coverage_content_type = "physicalMeasurement"
            var_Qt.cell_methods = "time: point"
            var_Qt.long_name = "final volume through each filter in the completed carousel with the first (zero) as the bypass line"
            var_Qt.units = "m3"
            var_Qt.C_format = "%10.5f"
            var_Qt.ancillary_variables = "standard_temperature standard_pressure"
            self.apply_state(times, var_Qt, carousel.time, data_Qt)

            g.createDimension("final_accumulated_time", self.CAROUSEL_SIZE + 1)
            var_seconds = g.createVariable("final_accumulated_time", "f8", ("time", "final_accumulated_time"), fill_value=nan)
            netcdf_timeseries.variable_coordinates(g, var_seconds)
            var_seconds.coverage_content_type = "physicalMeasurement"
            var_seconds.cell_methods = "time: point"
            var_seconds.long_name = "final amount of sampling time on the completed carousel with the first (zero) as the bypass line"
            var_seconds.units = "seconds"
            var_seconds.C_format = "%7.0f"
            self.apply_state(times, var_seconds, carousel.time, data_seconds)

        return True