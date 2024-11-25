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
        return {"aerosol", "size", "opc", "csdpops"}

    @property
    def instrument_type(self) -> typing.Optional[str]:
        return "csdpops"

    @property
    def average_interval(self) -> typing.Optional[float]:
        return self._average_interval

    def run(self) -> bool:
        data_N = self.load_variable(f"N_{self.instrument_id}")
        if data_N.time.shape[0] == 0:
            return False
        self._average_interval = self.calculate_average_interval(data_N.time)
        if not super().run():
            return False

        data_Ns = self.load_array_state(f"Ns_{self.instrument_id}")
        data_Nb = self.load_array_variable(f"Nb_{self.instrument_id}")
        data_Q = self.load_variable(f"Q_{self.instrument_id}")
        data_P = self.load_variable(f"P_{self.instrument_id}")
        data_T1 = self.load_variable(f"T1_{self.instrument_id}")
        data_T2 = self.load_variable(f"T2_{self.instrument_id}")
        data_T3 = self.load_variable(f"T3_{self.instrument_id}")
        data_I = self.load_variable(f"I_{self.instrument_id}")
        data_Ig = self.load_variable(f"Ig_{self.instrument_id}")
        data_ZLASERMON = self.load_variable(f"ZLASERMON_{self.instrument_id}")
        data_ZPUMPFB = self.load_variable(f"ZPUMPFB_{self.instrument_id}")

        g, times = self.data_group([data_N])
        self.declare_system_flags(g, times)

        var_N = g.createVariable("number_concentration", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_number_concentration(var_N)
        netcdf_timeseries.variable_coordinates(g, var_N)
        var_N.variable_id = "N"
        var_N.coverage_content_type = "physicalMeasurement"
        var_N.cell_methods = "time: mean"
        self.apply_data(times, var_N, data_N)

        g.createDimension("diameter", data_Nb.value.shape[1])
        var_diameter = g.createVariable("diameter", "f8", ("diameter",), fill_value=nan)
        netcdf_var.variable_size_distribution_Dp(var_diameter)
        var_diameter.variable_id = "Ns"
        var_diameter.coverage_content_type = "coordinate"
        var_diameter.cell_methods = "time: mean"
        diameter_values = data_Ns.value[-1]
        assign_diameters = min(diameter_values.shape[0], data_Nb.value.shape[1])
        var_diameter[:assign_diameters] = diameter_values[:assign_diameters]

        var_Nb = g.createVariable("number_distribution", "f8", ("time", "diameter"), fill_value=nan)
        netcdf_var.variable_size_distribution_dN(var_Nb)
        netcdf_timeseries.variable_coordinates(g, var_Nb)
        var_Nb.variable_id = "Nb"
        var_Nb.coverage_content_type = "physicalMeasurement"
        self.apply_data(times, var_Nb, data_Nb)

        var_Q = g.createVariable("sample_flow", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_sample_flow(var_Q)
        netcdf_timeseries.variable_coordinates(g, var_Q)
        var_Q.variable_id = "Q"
        var_Q.coverage_content_type = "physicalMeasurement"
        var_Q.cell_methods = "time: mean"
        var_Q.C_format = "%5.3f"
        self.apply_data(times, var_Q, data_Q)

        var_P = g.createVariable("pressure", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_pressure(var_P)
        netcdf_timeseries.variable_coordinates(g, var_P)
        var_P.variable_id = "P"
        var_P.coverage_content_type = "physicalMeasurement"
        var_P.cell_methods = "time: mean"
        var_P.long_name = "board pressure"
        self.apply_data(times, var_P, data_P)

        var_T1 = g.createVariable("temperature_of_pressure", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_temperature(var_T1)
        netcdf_timeseries.variable_coordinates(g, var_T1)
        var_T1.variable_id = "T1"
        var_T1.coverage_content_type = "physicalMeasurement"
        var_T1.cell_methods = "time: mean"
        var_T1.long_name = "temperature of pressure sensor"
        self.apply_data(times, var_T1, data_T1)

        var_T2 = g.createVariable("laser_temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_temperature(var_T2)
        netcdf_timeseries.variable_coordinates(g, var_T2)
        var_T2.variable_id = "T2"
        var_T2.coverage_content_type = "physicalMeasurement"
        var_T2.cell_methods = "time: mean"
        var_T2.long_name = "laser temperature"
        self.apply_data(times, var_T2, data_T2)

        var_T3 = g.createVariable("internal_temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_temperature(var_T3)
        netcdf_timeseries.variable_coordinates(g, var_T3)
        var_T3.variable_id = "T3"
        var_T3.coverage_content_type = "physicalMeasurement"
        var_T3.cell_methods = "time: mean"
        var_T3.long_name = "internal temperature"
        self.apply_data(times, var_T3, data_T3)

        var_I = g.createVariable("baseline", "f8", ("time",), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_I)
        var_I.variable_id = "I"
        var_I.coverage_content_type = "physicalMeasurement"
        var_I.cell_methods = "time: mean"
        var_I.long_name = "baseline value"
        var_I.C_format = "%4.0f"
        self.apply_data(times, var_I, data_I)

        var_Ig = g.createVariable("baseline_stddev", "f8", ("time",), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_Ig)
        var_Ig.variable_Igd = "Ig"
        var_Ig.coverage_content_type = "physicalMeasurement"
        var_Ig.cell_methods = "time: mean"
        var_Ig.long_name = "baseline standard deviation"
        var_Ig.C_format = "%4.0f"
        self.apply_data(times, var_Ig, data_Ig)

        var_ZLASERMON = g.createVariable("laser_monitor", "f8", ("time",), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_ZLASERMON)
        var_ZLASERMON.variable_ZLASERMONd = "ZLASERMON"
        var_ZLASERMON.coverage_content_type = "physicalMeasurement"
        var_ZLASERMON.cell_methods = "time: mean"
        var_ZLASERMON.long_name = "laser monitor"
        var_ZLASERMON.C_format = "%4.0f"
        self.apply_data(times, var_ZLASERMON, data_ZLASERMON)

        var_ZPUMPFB = g.createVariable("pump_feedback", "f8", ("time",), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_ZPUMPFB)
        var_ZPUMPFB.variable_ZPUMPFBd = "ZPUMPFB"
        var_ZPUMPFB.coverage_content_type = "physicalMeasurement"
        var_ZPUMPFB.cell_methods = "time: mean"
        var_ZPUMPFB.long_name = "pump feedback"
        var_ZPUMPFB.C_format = "%3.0f"
        self.apply_data(times, var_ZPUMPFB, data_ZPUMPFB)

        self.apply_coverage(g, times, f"N_{self.instrument_id}")

        self.apply_instrument_metadata(f"N_{self.instrument_id}", manufacturer="NOAA/CSL", model="POPS")

        return True