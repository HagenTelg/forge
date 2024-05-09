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
        return {"ozone", "teledynen500"}

    @property
    def instrument_type(self) -> typing.Optional[str]:
        return "teledynen500"

    @property
    def average_interval(self) -> typing.Optional[float]:
        return self._average_interval

    def run(self) -> bool:
        data_X1 = self.load_variable(f"X1_{self.instrument_id}")
        if data_X1.time.shape[0] == 0:
            return False
        self._average_interval = self.calculate_average_interval(data_X1.time)
        if not super().run():
            return False

        data_X2 = self.load_variable(f"X2_{self.instrument_id}")
        data_X3 = self.load_variable(f"X3_{self.instrument_id}")
        data_P = self.load_variable(f"P_{self.instrument_id}")
        data_T1 = self.load_variable(f"T1_{self.instrument_id}")
        data_T2 = self.load_variable(f"T2_{self.instrument_id}")
        data_T3 = self.load_variable(f"T3_{self.instrument_id}")

        g, times = self.data_group([data_X1])
        self.declare_system_flags(g, times)

        var_X1 = g.createVariable("nitrogen_dioxide_mixing_ratio", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_no2(var_X1)
        netcdf_timeseries.variable_coordinates(g, var_X1)
        var_X1.variable_id = "X1"
        var_X1.coverage_content_type = "physicalMeasurement"
        var_X1.cell_methods = "time: mean"
        self.apply_data(times, var_X1, data_X1)

        var_X2 = g.createVariable("nitrogen_monoxide_mixing_ratio", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_no(var_X2)
        netcdf_timeseries.variable_coordinates(g, var_X2)
        var_X2.variable_id = "X2"
        var_X2.coverage_content_type = "physicalMeasurement"
        var_X2.cell_methods = "time: mean"
        self.apply_data(times, var_X2, data_X2)

        var_X3 = g.createVariable("nox_mixing_ratio", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_nox(var_X3)
        netcdf_timeseries.variable_coordinates(g, var_X3)
        var_X3.variable_id = "X3"
        var_X3.coverage_content_type = "physicalMeasurement"
        var_X3.cell_methods = "time: mean"
        self.apply_data(times, var_X3, data_X3)

        var_P = g.createVariable("pressure", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_air_pressure(var_P)
        netcdf_timeseries.variable_coordinates(g, var_P)
        var_P.variable_id = "P"
        var_P.coverage_content_type = "physicalMeasurement"
        self.apply_data(times, var_P, data_P)

        var_T1 = g.createVariable("manifold_temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_temperature(var_T1)
        netcdf_timeseries.variable_coordinates(g, var_T1)
        var_T1.variable_id = "T1"
        var_T1.coverage_content_T1ype = "physicalMeasurement"
        var_T1.long_name = "internal manifold temperature"
        self.apply_data(times, var_T1, data_T1)

        var_T2 = g.createVariable("oven_temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_temperature(var_T2)
        netcdf_timeseries.variable_coordinates(g, var_T2)
        var_T2.variable_id = "T2"
        var_T2.coverage_content_T2ype = "physicalMeasurement"
        var_T2.long_name = "optical assembly oven temperature"
        self.apply_data(times, var_T2, data_T2)

        var_T3 = g.createVariable("box_temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_temperature(var_T3)
        netcdf_timeseries.variable_coordinates(g, var_T3)
        var_T3.variable_id = "T3"
        var_T3.coverage_content_T3ype = "physicalMeasurement"
        var_T3.long_name = "instrument internal box temperature"
        self.apply_data(times, var_T3, data_T3)

        self.apply_instrument_metadata(f"X1_{self.instrument_id}", manufacturer="Teledyne", model="N500")

        return True
