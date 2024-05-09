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
        return {"ozone", "thermo49"}

    @property
    def instrument_type(self) -> typing.Optional[str]:
        return "thermo49"

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

        data_P = self.load_variable(f"P_{self.instrument_id}")
        data_T1 = self.load_variable(f"T1_{self.instrument_id}")
        data_T2 = self.load_variable(f"T2_{self.instrument_id}")
        data_T3 = self.load_variable(f"T3_{self.instrument_id}")
        data_Q1 = self.load_variable(f"Q1_{self.instrument_id}")
        data_Q2 = self.load_variable(f"Q2_{self.instrument_id}")
        data_Q3 = self.load_variable(f"Q3_{self.instrument_id}")
        data_C1 = self.load_variable(f"C1_{self.instrument_id}")
        data_C2 = self.load_variable(f"C2_{self.instrument_id}")
        data_V1 = self.load_variable(f"V1_{self.instrument_id}")
        data_V2 = self.load_variable(f"V2_{self.instrument_id}")

        g, times = self.data_group([data_X])
        self.declare_system_flags(g, times)

        var_X = g.createVariable("ozone_mixing_ratio", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_ozone(var_X)
        netcdf_timeseries.variable_coordinates(g, var_X)
        var_X.variable_id = "X"
        var_X.coverage_content_type = "physicalMeasurement"
        var_X.cell_methods = "time: mean"
        self.apply_data(times, var_X, data_X)

        # Check for old src/lrc ingest with the lrc at low resolution, then split into a low time resolution group
        if self._average_interval:
            lrc_check_average_interval = self.calculate_average_interval(data_C1.time)
            if lrc_check_average_interval and abs(lrc_check_average_interval - self._average_interval) > 60:
                g, times = self.data_group([data_C1], name="status")

        var_P = g.createVariable("sample_pressure", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_air_pressure(var_P)
        netcdf_timeseries.variable_coordinates(g, var_P)
        var_P.variable_id = "P"
        var_P.coverage_content_type = "physicalMeasurement"
        var_P.long_name = "sample bench pressure"
        self.apply_data(times, var_P, data_P)

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
        var_T2.long_name = "measurement lamp temperature"
        self.apply_data(times, var_T2, data_T2)

        var_Q1 = g.createVariable("cell_a_flow", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_flow(var_Q1)
        netcdf_timeseries.variable_coordinates(g, var_Q1)
        var_Q1.variable_id = "Q1"
        var_Q1.coverage_content_type = "physicalMeasurement"
        var_Q1.long_name = "air flow rate through cell A"
        var_Q1.C_format = "%6.3f"
        self.apply_data(times, var_Q1, data_Q1)

        var_Q2 = g.createVariable("cell_b_flow", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_flow(var_Q2)
        netcdf_timeseries.variable_coordinates(g, var_Q2)
        var_Q2.variable_id = "Q2"
        var_Q2.coverage_content_type = "physicalMeasurement"
        var_Q2.long_name = "air flow rate through cell B"
        var_Q2.C_format = "%6.3f"
        self.apply_data(times, var_Q2, data_Q2)

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
        
        if data_Q3.time.shape[0] > 0:
            var_Q3 = g.createVariable("ozonator_flow", "f8", ("time",), fill_value=nan)
            netcdf_var.variable_sample_flow(var_Q3)
            netcdf_timeseries.variable_coordinates(g, var_Q3)
            var_Q3.variable_id = "Q3"
            var_Q3.coverage_content_type = "physicalMeasurement"
            var_Q3.long_name = "ozonator flow rate"
            var_Q3.C_format = "%5.3f"
            self.apply_data(times, var_Q3, data_Q3)
        
        if data_T3.time.shape[0] > 0:
            var_T3 = g.createVariable("ozonator_temperature", "f8", ("time",), fill_value=nan)
            netcdf_var.variable_temperature(var_T3)
            netcdf_timeseries.variable_coordinates(g, var_T3)
            var_T3.variable_id = "T3"
            var_T3.coverage_content_type = "physicalMeasurement"
            var_T3.long_name = "ozonator lamp temperature"
            self.apply_data(times, var_T3, data_T3)
        
        if data_V1.time.shape[0] > 0:
            var_V1 = g.createVariable("lamp_voltage", "f8", ("time",), fill_value=nan)
            netcdf_timeseries.variable_coordinates(g, var_V1)
            var_V1.variable_id = "V1"
            var_V1.coverage_content_type = "physicalMeasurement"
            var_V1.long_name = "measurement lamp voltage"
            var_V1.units = "V"
            var_V1.C_format = "%4.1f"
            self.apply_data(times, var_V1, data_V1)
        
        if data_V2.time.shape[0] > 0:
            var_V2 = g.createVariable("ozonator_voltage", "f8", ("time",), fill_value=nan)
            netcdf_timeseries.variable_coordinates(g, var_V2)
            var_V2.variable_id = "V2"
            var_V2.coverage_content_type = "physicalMeasurement"
            var_V2.long_name = "ozonator lamp voltage"
            var_V2.units = "V"
            var_V2.C_format = "%4.1f"
            self.apply_data(times, var_V2, data_V2)

        self.apply_coverage(g, times, f"X_{self.instrument_id}")

        self.apply_instrument_metadata(f"X_{self.instrument_id}", manufacturer="Thermo", generic_model="49")

        return True
