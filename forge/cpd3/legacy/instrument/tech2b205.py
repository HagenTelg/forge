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
        return {"ozone", "tech2b205"}

    @property
    def instrument_type(self) -> typing.Optional[str]:
        return "tech2b205"

    @property
    def average_interval(self) -> typing.Optional[float]:
        return self._average_interval

    def run(self) -> bool:
        data_X = self.load_variable(f"X_{self.instrument_id}")
        if data_X.time.shape[0] == 0:
            return False
        self._average_interval = self.calculate_average_interval(data_X.time)
        if not super().run():
            return False

        data_Q = self.load_variable(f"Q_{self.instrument_id}")
        data_P = self.load_variable(f"P_{self.instrument_id}")
        data_T = self.load_variable(f"T_{self.instrument_id}")

        g, times = self.data_group([data_X])
        self.declare_system_flags(g, times)

        var_X = g.createVariable("ozone_mixing_ratio", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_ozone(var_X)
        netcdf_timeseries.variable_coordinates(g, var_X)
        var_X.variable_id = "X"
        var_X.coverage_content_type = "physicalMeasurement"
        var_X.cell_methods = "time: mean"
        self.apply_data(times, var_X, data_X)

        var_P = g.createVariable("sample_pressure", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_air_pressure(var_P)
        netcdf_timeseries.variable_coordinates(g, var_P)
        var_P.variable_id = "P"
        var_P.coverage_content_type = "physicalMeasurement"
        var_P.long_name = "cell pressure"
        self.apply_data(times, var_P, data_P)

        var_T = g.createVariable("sample_temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_air_temperature(var_T)
        netcdf_timeseries.variable_coordinates(g, var_T)
        var_T.variable_id = "T"
        var_T.coverage_content_type = "physicalMeasurement"
        var_T.long_name = "cell temperature"
        self.apply_data(times, var_T, data_T)

        var_Q = g.createVariable("sample_flow", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_sample_flow(var_Q)
        netcdf_timeseries.variable_coordinates(g, var_Q)
        var_Q.variable_id = "Q"
        var_Q.coverage_content_type = "physicalMeasurement"
        var_Q.C_format = "%5.3f"
        self.apply_data(times, var_Q, data_Q)

        self.apply_instrument_metadata(f"X_{self.instrument_id}", manufacturer="2B Tech", model="205")

        return True
