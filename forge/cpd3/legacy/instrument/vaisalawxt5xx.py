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
        return {"aerosol", "met", "vaisalawxt5xx"}

    @property
    def instrument_type(self) -> typing.Optional[str]:
        return "vaisalawxt5xx"

    @property
    def average_interval(self) -> typing.Optional[float]:
        return self._average_interval

    def run(self) -> bool:
        data_WS = self.load_variable(f"WS_{self.instrument_id}")
        if data_WS.time.shape[0] == 0:
            return False
        self._average_interval = self.calculate_average_interval(data_WS.time)
        if not super().run():
            return False

        data_WD = self.load_variable(f"WD_{self.instrument_id}")
        data_ZWSGust = self.load_variable(f"ZWSGust_{self.instrument_id}")
        data_P = self.load_variable(f"P_{self.instrument_id}")
        data_U1 = self.load_variable(f"U1_{self.instrument_id}")
        data_T1 = self.load_variable(f"T1_{self.instrument_id}")
        data_T2 = self.load_variable(f"T2_{self.instrument_id}")
        data_T3 = self.load_variable(f"T3_{self.instrument_id}")
        data_T4 = self.load_variable(f"Ld_{self.instrument_id}")
        data_WI = self.load_variable(f"WI_{self.instrument_id}")
        data_VA = self.load_variable(f"VA_{self.instrument_id}")
        data_Ld = self.load_variable(f"Ld_{self.instrument_id}")    

        g, times = self.data_group([data_WS])
        self.declare_system_flags(g, times)

        var_WS = g.createVariable("wind_speed", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_wind_speed(var_WS)
        netcdf_timeseries.variable_coordinates(g, var_WS)
        var_WS.variable_id = "WS"
        var_WS.coverage_content_type = "physicalMeasurement"
        var_WS.cell_methods = "time: mean wind_direction: vector_direction"
        self.apply_data(times, var_WS, data_WS)

        var_WD = g.createVariable("wind_direction", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_wind_direction(var_WD)
        netcdf_timeseries.variable_coordinates(g, var_WD)
        var_WD.variable_id = "WD"
        var_WD.coverage_content_type = "physicalMeasurement"
        var_WD.cell_methods = "time: mean wind_speed: vector_magnitude"
        self.apply_data(times, var_WD, data_WD)

        var_ZWSGust = g.createVariable("wind_gust_speed", "f8", ("time",), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_ZWSGust)
        var_ZWSGust.variable_id = "ZWSGust"
        var_ZWSGust.coverage_content_type = "physicalMeasurement"
        var_ZWSGust.cell_methods = "time: mean"
        var_ZWSGust.long_name = "averaged wind gust speed"
        var_ZWSGust.standard_name = "wind_speed_of_gust"
        var_ZWSGust.units = "m s-1"
        var_ZWSGust.C_format = "%4.1f"
        self.apply_data(times, var_ZWSGust, data_ZWSGust)

        var_P = g.createVariable("ambient_pressure", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_air_pressure(var_P)
        netcdf_timeseries.variable_coordinates(g, var_P)
        var_P.variable_id = "P"
        var_P.coverage_content_type = "physicalMeasurement"
        var_P.cell_methods = "time: mean"
        var_P.long_name = "ambient pressure"
        self.apply_data(times, var_P, data_P)

        var_U1 = g.createVariable("ambient_humidity", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_air_rh(var_U1)
        netcdf_timeseries.variable_coordinates(g, var_U1)
        var_U1.variable_id = "U1"
        var_U1.coverage_content_type = "physicalMeasurement"
        var_U1.cell_methods = "time: mean"
        var_U1.long_name = "ambient relative humidity"
        self.apply_data(times, var_U1, data_U1)

        var_T1 = g.createVariable("ambient_temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_air_temperature(var_T1)
        netcdf_timeseries.variable_coordinates(g, var_T1)
        var_T1.variable_id = "T1"
        var_T1.coverage_content_type = "physicalMeasurement"
        var_T1.cell_methods = "time: mean"
        var_T1.long_name = "ambient air temperature"
        self.apply_data(times, var_T1, data_T1)

        var_T2 = g.createVariable("internal_temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_temperature(var_T2)
        netcdf_timeseries.variable_coordinates(g, var_T2)
        var_T2.variable_id = "T2"
        var_T2.coverage_content_type = "physicalMeasurement"
        var_T2.cell_methods = "time: mean"
        var_T2.long_name = "instrument internal temperature"
        self.apply_data(times, var_T2, data_T2)

        var_T3 = g.createVariable("heater_temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_temperature(var_T3)
        netcdf_timeseries.variable_coordinates(g, var_T3)
        var_T3.variable_id = "T3"
        var_T3.coverage_content_type = "physicalMeasurement"
        var_T3.cell_methods = "time: mean"
        var_T3.long_name = "heater temperature"
        self.apply_data(times, var_T3, data_T3)

        if data_T4.time.shape[0] > 0:
            var_T4 = g.createVariable("auxiliary_temperature", "f8", ("time",), fill_value=nan)
            netcdf_var.variable_temperature(var_T4)
            netcdf_timeseries.variable_coordinates(g, var_T4)
            var_T4.variable_id = "T4"
            var_T4.coverage_content_type = "physicalMeasurement"
            var_T4.cell_methods = "time: mean"
            var_T4.long_name = "auxiliary temperature sensor"
            self.apply_data(times, var_T4, data_T4)
            
        if data_VA.time.shape[0] > 0:
            var_VA = g.createVariable("solar_radiation", "f8", ("time",), fill_value=nan)
            netcdf_timeseries.variable_coordinates(g, var_VA)
            var_VA.variable_id = "VA"
            var_VA.coverage_content_type = "physicalMeasurement"
            var_VA.cell_methods = "time: mean"
            var_VA.long_name = "solar radiation intensity"
            var_VA.standard_name = "solar_irradiance"
            var_VA.units = "W m-2"
            var_VA.C_format = "%7.2f"
            self.apply_data(times, var_VA, data_VA)
            
        if data_Ld.time.shape[0] > 0:
            var_Ld = g.createVariable("level_sensor", "f8", ("time",), fill_value=nan)
            netcdf_timeseries.variable_coordinates(g, var_Ld)
            var_Ld.variable_id = "Ld"
            var_Ld.coverage_content_type = "physicalMeasurement"
            var_Ld.cell_methods = "time: mean"
            var_Ld.long_name = "measured distance from level sensor"
            var_Ld.units = "m"
            var_Ld.C_format = "%5.2f"
            self.apply_data(times, var_Ld, data_Ld)

        self.apply_coverage(g, times, f"WS_{self.instrument_id}")

        self.apply_instrument_metadata(f"WS_{self.instrument_id}", manufacturer="Vaisala", generic_model="WXT5xx")

        return True