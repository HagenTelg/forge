import typing
import numpy as np
import forge.data.structure.variable as netcdf_var
import forge.data.structure.timeseries as netcdf_timeseries
from math import nan
from .converter import InstrumentConverter


class Converter(InstrumentConverter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._average_interval: typing.Optional[float] = None

    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"aerosol", "cpc", "tsi3781cpc"}

    @property
    def instrument_type(self) -> typing.Optional[str]:
        return "tsi3781cpc"

    @property
    def average_interval(self) -> typing.Optional[float]:
        return self._average_interval

    @property
    def split_monitor(self) -> typing.Optional[bool]:
        return None

    def run(self) -> bool:
        data_N = self.load_variable(f"N_{self.instrument_id}")
        if data_N.time.shape[0] == 0:
            return False
        self._average_interval = self.calculate_average_interval(data_N.time)
        if not super().run():
            return False

        data_Q = self.load_variable(f"Q_{self.instrument_id}")
        data_P = self.load_variable(f"P_{self.instrument_id}")
        data_T1 = self.load_variable(f"T1_{self.instrument_id}")
        data_T2 = self.load_variable(f"T2_{self.instrument_id}")
        data_T3 = self.load_variable(f"T3_{self.instrument_id}")
        data_A = self.load_variable(f"A_{self.instrument_id}")
        data_PCT = self.load_variable(f"PCT_{self.instrument_id}")

        g, times = self.data_group([data_N])
        self.declare_system_flags(g, times)

        var_N = g.createVariable("number_concentration", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_number_concentration(var_N)
        netcdf_timeseries.variable_coordinates(g, var_N)
        var_N.variable_id = "N"
        var_N.coverage_content_type = "physicalMeasurement"
        var_N.cell_methods = "time: mean"
        self.apply_data(times, var_N, data_N)

        split_monitor = self.split_monitor
        if split_monitor is None:
            split_monitor = self.calculate_split_monitor(data_T1.time)
        if not split_monitor:
            mon_g = g
            mon_times = times
        elif data_T1.time.shape[0] > 0:
            mon_g, mon_times = self.data_group([data_T1], name='status', fill_gaps=False)
        else:
            mon_g, mon_times = None, None
            split_monitor = True

        if mon_g is not None:
            var_Q = mon_g.createVariable("sample_flow", "f8", ("time",), fill_value=nan)
            netcdf_var.variable_sample_flow(var_Q)
            netcdf_timeseries.variable_coordinates(mon_g, var_Q)
            var_Q.variable_id = "Q"
            var_Q.coverage_content_type = "physicalMeasurement"
            var_Q.cell_methods = "time: mean"
            var_Q.C_format = "%5.3f"
            self.apply_data(mon_times, var_Q, data_Q)

            var_P = mon_g.createVariable("pressure", "f8", ("time",), fill_value=nan)
            netcdf_var.variable_air_pressure(var_P)
            netcdf_timeseries.variable_coordinates(mon_g, var_P)
            var_P.variable_id = "P"
            var_P.coverage_content_type = "physicalMeasurement"
            var_P.cell_methods = "time: mean"
            var_P.long_name = "absolute pressure"
            self.apply_data(mon_times, var_P, data_P)

            var_T1 = mon_g.createVariable("saturator_temperature", "f8", ("time",), fill_value=nan)
            netcdf_var.variable_temperature(var_T1)
            netcdf_timeseries.variable_coordinates(mon_g, var_T1)
            var_T1.variable_id = "T1"
            var_T1.coverage_content_type = "physicalMeasurement"
            var_T1.cell_methods = "time: mean"
            var_T1.long_name = "saturator temperature"
            self.apply_data(mon_times, var_T1, data_T1)

            var_T2 = mon_g.createVariable("growth_tube_temperature", "f8", ("time",), fill_value=nan)
            netcdf_var.variable_temperature(var_T2)
            netcdf_timeseries.variable_coordinates(mon_g, var_T2)
            var_T2.variable_id = "T2"
            var_T2.coverage_content_type = "physicalMeasurement"
            var_T2.cell_methods = "time: mean"
            var_T2.long_name = "growth tube temperature"
            self.apply_data(mon_times, var_T2, data_T2)

            var_T3 = mon_g.createVariable("optics_temperature", "f8", ("time",), fill_value=nan)
            netcdf_var.variable_temperature(var_T3)
            netcdf_timeseries.variable_coordinates(mon_g, var_T3)
            var_T3.variable_id = "T3"
            var_T3.coverage_content_type = "physicalMeasurement"
            var_T3.cell_methods = "time: mean"
            var_T3.long_name = "optics block temperature"
            self.apply_data(mon_times, var_T3, data_T3)

            var_A = mon_g.createVariable("laser_current", "f8", ("time",), fill_value=nan)
            netcdf_timeseries.variable_coordinates(mon_g, var_A)
            var_A.variable_id = "A"
            var_A.coverage_content_type = "physicalMeasurement"
            var_A.cell_methods = "time: mean"
            var_A.long_name = "laser current"
            var_A.units = "mA"
            var_A.C_format = "%3.0f"
            self.apply_data(mon_times, var_A, data_A)

            var_PCT = mon_g.createVariable("nozzle_pressure_drop", "f8", ("time",), fill_value=nan)
            netcdf_timeseries.variable_coordinates(mon_g, var_PCT)
            var_PCT.variable_id = "PCT"
            var_PCT.coverage_content_type = "physicalMeasurement"
            var_PCT.cell_methods = "time: mean"
            var_PCT.long_name = "normalized pressure drop across the nozzle"
            var_PCT.units = "%"
            var_PCT.C_format = "%3.0f"
            self.apply_data(mon_times, var_PCT, data_PCT)

        if not split_monitor:
            self.apply_cut_size(g, times, [
                (var_N, data_N),
                (var_Q, data_Q),
                (var_P, data_P),
                (var_T1, data_T1),
                (var_T2, data_T2),
                (var_T3, data_T3),
                (var_A, data_A),
                (var_PCT, data_PCT),
            ])
        else:
            self.apply_cut_size(g, times, [
                (var_N, data_N),
            ])
        self.apply_coverage(g, times, f"N_{self.instrument_id}")

        self.apply_instrument_metadata(f"N_{self.instrument_id}", manufacturer="TSI", model="3781")

        return True