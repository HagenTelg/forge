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
        return {"ozone", "thermo49iq"}

    @property
    def instrument_type(self) -> typing.Optional[str]:
        return "thermo49iq"

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

        data_P1 = self.load_variable(f"P1_{self.instrument_id}")
        data_T1 = self.load_variable(f"T1_{self.instrument_id}")
        data_T2 = self.load_variable(f"T2_{self.instrument_id}")
        data_P2 = self.load_variable(f"P2_{self.instrument_id}")
        data_Q = self.load_variable(f"Q_{self.instrument_id}")
        data_C1 = self.load_variable(f"C1_{self.instrument_id}")
        data_C2 = self.load_variable(f"C2_{self.instrument_id}")
        data_VA1 = self.load_variable(f"VA1_{self.instrument_id}")
        data_VA2 = self.load_variable(f"VA2_{self.instrument_id}")

        g, times = self.data_group([data_X])
        self.declare_system_flags(g, times)

        var_X = g.createVariable("ozone_mixing_ratio", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_ozone(var_X)
        netcdf_timeseries.variable_coordinates(g, var_X)
        var_X.variable_id = "X"
        var_X.coverage_content_type = "physicalMeasurement"
        var_X.cell_methods = "time: mean"
        self.apply_data(times, var_X, data_X)

        var_P1 = g.createVariable("sample_pressure", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_air_pressure(var_P1)
        netcdf_timeseries.variable_coordinates(g, var_P1)
        var_P1.variable_id = "P"
        var_P1.coverage_content_type = "physicalMeasurement"
        var_P1.long_name = "photometer A pressure"
        self.apply_data(times, var_P1, data_P1)

        var_T1 = g.createVariable("sample_temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_air_temperature(var_T1)
        netcdf_timeseries.variable_coordinates(g, var_T1)
        var_T1.variable_id = "T1"
        var_T1.coverage_content_type = "physicalMeasurement"
        var_T1.long_name = "sample bench temperature"
        self.apply_data(times, var_T1, data_T1)

        var_T2 = g.createVariable("lamp_temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_temperature(var_T2)
        netcdf_timeseries.variable_coordinates(g, var_T2)
        var_T2.variable_id = "T2"
        var_T2.coverage_content_type = "physicalMeasurement"
        var_T2.long_name = "lamp temperature"
        self.apply_data(times, var_T2, data_T2)

        var_P2 = g.createVariable("pump_pressure", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_temperature(var_P2)
        netcdf_timeseries.variable_coordinates(g, var_P2)
        var_P2.variable_id = "P2"
        var_P2.coverage_content_type = "physicalMeasurement"
        var_P2.long_name = "pump pressure"
        self.apply_data(times, var_P2, data_P2)

        var_Q = g.createVariable("cell_a_flow", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_sample_flow(var_Q)
        netcdf_timeseries.variable_coordinates(g, var_Q)
        var_Q.variable_id = "Q"
        var_Q.coverage_content_type = "physicalMeasurement"
        var_Q.long_name = "air flow rate through cell A"
        self.apply_data(times, var_Q, data_Q)

        var_C1 = g.createVariable("cell_a_count_rate", "f8", ("time",), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_C1)
        var_C1.variable_id = "C1"
        var_C1.coverage_content_type = "physicalMeasurement"
        var_C1.long_name = "cell A intensity count rate"
        var_C1.units = "Hz"
        var_C1.C_format = "%7.0f"
        self.apply_data(times, var_C1, data_C1)

        var_C2 = g.createVariable("cell_b_count_rate", "f8", ("time",), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_C2)
        var_C2.variable_id = "C2"
        var_C2.coverage_content_type = "physicalMeasurement"
        var_C2.long_name = "cell B intensity count rate"
        var_C2.units = "Hz"
        var_C2.C_format = "%7.0f"
        self.apply_data(times, var_C2, data_C2)

        var_VA1 = g.createVariable("lamp_current", "f8", ("time",), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_VA1)
        var_VA1.variable_id = "VA1"
        var_VA1.coverage_content_type = "physicalMeasurement"
        var_VA1.long_name = "lamp current"
        var_VA1.units = "mA"
        var_VA1.C_format = "%5.2f"
        self.apply_data(times, var_VA1, data_VA1)

        var_VA2 = g.createVariable("lamp_heater_current", "f8", ("time",), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_VA2)
        var_VA2.variable_id = "VA2"
        var_VA2.coverage_content_type = "physicalMeasurement"
        var_VA2.long_name = "lamp heater current"
        var_VA2.units = "A"
        var_VA2.C_format = "%5.2f"
        self.apply_data(times, var_VA2, data_VA2)

        self.apply_coverage(g, times, f"X_{self.instrument_id}")

        self.apply_instrument_metadata(f"X_{self.instrument_id}", manufacturer="Thermo", model="49iQ")

        return True
