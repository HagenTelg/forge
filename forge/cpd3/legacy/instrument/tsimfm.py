import typing
import numpy as np
import forge.data.structure.variable as netcdf_var
import forge.data.structure.timeseries as netcdf_timeseries
from math import nan
from forge.data.structure.stp import standard_temperature, standard_pressure
from .converter import InstrumentConverter


class Converter(InstrumentConverter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._average_interval: typing.Optional[float] = None

    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"aerosol", "tsimfm"}

    @property
    def instrument_type(self) -> typing.Optional[str]:
        return "tsimfm"

    @property
    def average_interval(self) -> typing.Optional[float]:
        return self._average_interval

    def run(self) -> bool:
        data_Q = self.load_variable(f"Q_{self.instrument_id}")
        if data_Q.time.shape[0] == 0:
            return False
        self._average_interval = self.calculate_average_interval(data_Q.time)
        if not super().run():
            return False

        data_P = self.load_variable(f"P_{self.instrument_id}")
        data_T = self.load_variable(f"T_{self.instrument_id}")
        data_U = self.load_variable(f"U_{self.instrument_id}")

        g, times = self.data_group([data_Q])
        standard_temperature(g)
        standard_pressure(g)
        self.declare_system_flags(g, times)

        var_Q = g.createVariable("sample_flow", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_sample_flow(var_Q)
        netcdf_timeseries.variable_coordinates(g, var_Q)
        var_Q.variable_id = "Q"
        var_Q.coverage_content_type = "physicalMeasurement"
        var_Q.cell_methods = "time: mean"
        var_Q.C_format = "%6.3f"
        var_Q.ancillary_variables = "standard_temperature standard_pressure"
        self.apply_data(times, var_Q, data_Q)

        var_P = g.createVariable("pressure", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_air_pressure(var_P)
        netcdf_timeseries.variable_coordinates(g, var_P)
        var_P.variable_id = "P"
        var_P.coverage_content_type = "physicalMeasurement"
        var_P.cell_methods = "time: mean"
        self.apply_data(times, var_P, data_P)

        var_T = g.createVariable("temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_air_temperature(var_T)
        netcdf_timeseries.variable_coordinates(g, var_T)
        var_T.variable_id = "T"
        var_T.coverage_content_type = "physicalMeasurement"
        var_T.cell_methods = "time: mean"
        self.apply_data(times, var_T, data_T)

        if data_U.time.shape[0] > 0:
            var_U = g.createVariable("humidity", "f8", ("time",), fill_value=nan)
            netcdf_var.variable_air_rh(var_U)
            netcdf_timeseries.variable_coordinates(g, var_U)
            var_U.variable_id = "U"
            var_U.coverage_content_type = "physicalMeasurement"
            var_U.cell_methods = "time: mean"
            self.apply_data(times, var_U, data_U)
        else:
            var_U = None

        self.apply_cut_size(g, times, [
            (var_Q, data_Q),
            (var_P, data_P),
            (var_T, data_T),
            (var_U, data_U),
        ])
        self.apply_coverage(g, times, f"Q_{self.instrument_id}")

        self.apply_instrument_metadata(f"Q_{self.instrument_id}", manufacturer="TSI", generic_model="4000")

        return True