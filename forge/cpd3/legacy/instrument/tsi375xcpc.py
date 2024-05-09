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
        return {"aerosol", "cpc", "tsi375xcpc"}

    @property
    def instrument_type(self) -> typing.Optional[str]:
        return "tsi375xcpc"

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

        data_Q = self.load_variable(f"Q_{self.instrument_id}")
        data_Qu = self.load_variable(f"Qu_{self.instrument_id}")
        data_P = self.load_variable(f"P_{self.instrument_id}")
        data_Pd1 = self.load_variable(f"Pd1_{self.instrument_id}")
        data_Pd2 = self.load_variable(f"Pd2_{self.instrument_id}")
        data_T1 = self.load_variable(f"T1_{self.instrument_id}")
        data_T2 = self.load_variable(f"T2_{self.instrument_id}")
        data_T3 = self.load_variable(f"T3_{self.instrument_id}")
        data_T4 = self.load_variable(f"T4_{self.instrument_id}")
        data_A = self.load_variable(f"A_{self.instrument_id}")

        g = self.apply_instrument_metadata(f"N_{self.instrument_id}", manufacturer="TSI", generic_model="375x")
        model = g.variables.get("model")
        if model is not None:
            model = str(model[0])

        g, times = self.data_group([data_N])
        self.declare_system_flags(g, times)

        var_N = g.createVariable("number_concentration", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_number_concentration(var_N)
        netcdf_timeseries.variable_coordinates(g, var_N)
        var_N.variable_id = "N"
        var_N.coverage_content_type = "physicalMeasurement"
        var_N.cell_methods = "time: mean"
        self.apply_data(times, var_N, data_N)

        var_Q = g.createVariable("sample_flow", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_sample_flow(var_Q)
        netcdf_timeseries.variable_coordinates(g, var_Q)
        var_Q.variable_id = "Q"
        var_Q.coverage_content_type = "physicalMeasurement"
        var_Q.cell_methods = "time: mean"
        var_Q.C_format = "%5.3f"
        self.apply_data(times, var_Q, data_Q)

        if data_Qu.time.shape[0] != 0:
            var_Qu = g.createVariable("inlet_flow", "f8", ("time",), fill_value=nan)
            netcdf_var.variable_flow(var_Qu)
            netcdf_timeseries.variable_coordinates(g, var_Qu)
            var_Qu.variable_id = "Qu"
            var_Qu.coverage_content_type = "physicalMeasurement"
            var_Qu.cell_methods = "time: mean"
            var_Qu.C_format = "%5.3f"
            var_Qu.long_name = "inlet flow rate"
            self.apply_data(times, var_Qu, data_Qu)
        else:
            var_Qu = None

        var_P = g.createVariable("inlet_pressure", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_air_pressure(var_P)
        netcdf_timeseries.variable_coordinates(g, var_P)
        var_P.variable_id = "P"
        var_P.coverage_content_type = "physicalMeasurement"
        var_P.cell_methods = "time: mean"
        var_P.long_name = "ambient pressure at the instrument inlet"
        self.apply_data(times, var_P, data_P)

        if data_Pd1.time.shape[0] != 0:
            var_Pd1 = g.createVariable("nozzle_pressure_drop", "f8", ("time",), fill_value=nan)
            netcdf_var.variable_delta_pressure(var_Pd1)
            netcdf_timeseries.variable_coordinates(g, var_Pd1)
            var_Pd1.variable_id = "Pd2"
            var_Pd1.coverage_content_type = "physicalMeasurement"
            var_Pd1.cell_methods = "time: mean"
            var_Pd1.C_format = "%6.2f"
            var_Pd1.long_name = "nozzle pressure drop"
            self.apply_data(times, var_Pd1, data_Pd1)
        else:
            var_Pd1 = None

        var_Pd2 = g.createVariable("orifice_pressure_drop", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_delta_pressure(var_Pd2)
        netcdf_timeseries.variable_coordinates(g, var_Pd2)
        var_Pd2.variable_id = "Pd2"
        var_Pd2.coverage_content_type = "physicalMeasurement"
        var_Pd2.cell_methods = "time: mean"
        var_Pd2.C_format = "%4.0f"
        var_Pd2.long_name = "orifice pressure drop"
        self.apply_data(times, var_Pd2, data_Pd2)

        var_T1 = g.createVariable("initiator_temperature" if model == "3789" else "saturator_temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_temperature(var_T1)
        netcdf_timeseries.variable_coordinates(g, var_T1)
        var_T1.variable_id = "T1"
        var_T1.coverage_content_type = "physicalMeasurement"
        var_T1.cell_methods = "time: mean"
        var_T1.long_name = "initiator temperature" if model == "3789" else "saturator temperature"
        self.apply_data(times, var_T1, data_T1)

        var_T2 = g.createVariable("conditioner_temperature" if model == "3789" else "condenser_temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_temperature(var_T2)
        netcdf_timeseries.variable_coordinates(g, var_T2)
        var_T2.variable_id = "T2"
        var_T2.coverage_content_type = "physicalMeasurement"
        var_T2.cell_methods = "time: mean"
        var_T2.long_name = "conditioner temperature" if model == "3789" else "condenser temperature"
        self.apply_data(times, var_T2, data_T2)

        if data_T3.time.shape[0] != 0:
            var_T3 = g.createVariable("optics_temperature", "f8", ("time",), fill_value=nan)
            netcdf_var.variable_temperature(var_T3)
            netcdf_timeseries.variable_coordinates(g, var_T3)
            var_T3.variable_id = "T3"
            var_T3.coverage_content_type = "physicalMeasurement"
            var_T3.cell_methods = "time: mean"
            var_T3.long_name = "optics block temperature"
            self.apply_data(times, var_T3, data_T3)
        else:
            var_T3 = None

        var_T4 = g.createVariable("cabinet_temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_temperature(var_T4)
        netcdf_timeseries.variable_coordinates(g, var_T4)
        var_T4.variable_id = "T4"
        var_T4.coverage_content_type = "physicalMeasurement"
        var_T4.cell_methods = "time: mean"
        var_T4.long_name = "internal cabinet temperature"
        self.apply_data(times, var_T4, data_T4)

        if data_A.time.shape[0] != 0:
            var_A = g.createVariable("laser_current", "f8", ("time",), fill_value=nan)
            netcdf_var.variable_temperature(var_A)
            netcdf_timeseries.variable_coordinates(g, var_A)
            var_A.variable_id = "A"
            var_A.coverage_content_type = "physicalMeasurement"
            var_A.cell_methods = "time: mean"
            var_A.long_name = "laser current"
            var_A.units = "mA"
            var_A.C_format = "%3.0f"
            self.apply_data(times, var_A, data_A)
        else:
            var_A = None

        self.apply_cut_size(g, times, [
            (var_N, data_N),
            (var_Q, data_Q),
            (var_Qu, data_Qu),
            (var_T1, data_T1),
            (var_T2, data_T2),
            (var_T3, data_T3),
            (var_T4, data_T4),
            (var_P, data_P),
            (var_Pd1, data_Pd1),
            (var_Pd2, data_Pd2),
            (var_A, data_A),
        ])
        self.apply_coverage(g, times, f"N_{self.instrument_id}")

        return True