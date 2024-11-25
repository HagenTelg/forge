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
        return {"aerosol", "met"}

    @property
    def average_interval(self) -> typing.Optional[float]:
        return self._average_interval

    def run(self) -> bool:
        data_WS = self.load_variable(f"WS1?_{self.instrument_id}")
        if data_WS.time.shape[0] == 0:
            return False
        self._average_interval = self.calculate_average_interval(data_WS.time)
        if not super().run():
            return False

        data_WD = self.load_variable(f"WD1?_{self.instrument_id}")
        data_T = self.load_variable(f"T1?_{self.instrument_id}")
        data_U = self.load_variable(f"U1?_{self.instrument_id}")
        data_TD = self.load_variable(f"TD1?_{self.instrument_id}")
        data_P = self.load_variable(f"P1?_{self.instrument_id}")

        g, times = self.data_group([data_WS, data_WD, data_T, data_P, data_U])

        var_WS = g.createVariable("wind_speed", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_wind_speed(var_WS)
        netcdf_timeseries.variable_coordinates(g, var_WS)
        var_WS.variable_id = "WS1"
        var_WS.coverage_content_type = "physicalMeasurement"
        var_WS.cell_methods = "time: mean wind_direction: vector_direction"
        self.apply_data(times, var_WS, data_WS)

        var_WD = g.createVariable("wind_direction", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_wind_direction(var_WD)
        netcdf_timeseries.variable_coordinates(g, var_WD)
        var_WD.variable_id = "WD1"
        var_WD.coverage_content_type = "physicalMeasurement"
        var_WD.cell_methods = "time: mean wind_speed: vector_magnitude"
        self.apply_data(times, var_WD, data_WD)

        if data_P.time.shape[0] > 0:
            var_P = g.createVariable("ambient_pressure", "f8", ("time",), fill_value=nan)
            netcdf_var.variable_air_pressure(var_P)
            netcdf_timeseries.variable_coordinates(g, var_P)
            var_P.variable_id = "P"
            var_P.coverage_content_Pype = "physicalMeasurement"
            var_P.cell_methods = "time: mean"
            var_P.long_name = "ambient pressure"
            self.apply_data(times, var_P, data_P)

        if data_T.time.shape[0] > 0:
            var_T = g.createVariable("ambient_temperature", "f8", ("time",), fill_value=nan)
            netcdf_var.variable_air_temperature(var_T)
            netcdf_timeseries.variable_coordinates(g, var_T)
            var_T.variable_id = "T1"
            var_T.coverage_content_type = "physicalMeasurement"
            var_T.cell_methods = "time: mean"
            var_T.long_name = "ambient air temperature"
            self.apply_data(times, var_T, data_T)

        if data_U.time.shape[0] > 0:
            var_U = g.createVariable("ambient_humidity", "f8", ("time",), fill_value=nan)
            netcdf_var.variable_air_rh(var_U)
            netcdf_timeseries.variable_coordinates(g, var_U)
            var_U.variable_id = "U1"
            var_U.coverage_content_type = "physicalMeasurement"
            var_U.cell_methods = "time: mean"
            var_U.long_name = "ambient air humidity"
            self.apply_data(times, var_U, data_U)

        if data_TD.time.shape[0] > 0:
            var_TD = g.createVariable("ambient_dewpoint", "f8", ("time",), fill_value=nan)
            netcdf_var.variable_air_dewpoint(var_TD)
            netcdf_timeseries.variable_coordinates(g, var_TD)
            var_TD.variable_id = "TD1"
            var_TD.coverage_content_type = "physicalMeasurement"
            var_TD.cell_methods = "time: mean"
            var_TD.long_name = "ambient air dewpoint"
            self.apply_data(times, var_TD, data_TD)

        self.apply_coverage(g, times, f"WS1?_{self.instrument_id}")

        self.apply_instrument_metadata(f"WS1?_{self.instrument_id}")

        return True