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
        return {"aerosol", "cpc"}

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

        data_T1 = self.load_variable(f"T1_{self.instrument_id}")
        data_T2 = self.load_variable(f"T2_{self.instrument_id}")
        data_V1 = self.load_variable(f"V1_{self.instrument_id}")
        data_V2 = self.load_variable(f"V2_{self.instrument_id}")
        data_raw_inputs = self.load_array_variable(f"ZINPUTS_{self.instrument_id}")
        data_Nb = self.load_array_variable(f"Nb_{self.instrument_id}")
        data_Ns = self.load_array_variable(f"Ns_{self.instrument_id}")

        g, times = self.data_group([data_N])

        var_N = g.createVariable("number_concentration", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_number_concentration(var_N)
        netcdf_timeseries.variable_coordinates(g, var_N)
        var_N.variable_id = "N"
        var_N.coverage_content_type = "physicalMeasurement"
        var_N.cell_methods = "time: mean"
        self.apply_data(times, var_N, data_N)

        var_T1 = g.createVariable("optics_temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_temperature(var_T1)
        netcdf_timeseries.variable_coordinates(g, var_T1)
        var_T1.variable_id = "T1"
        var_T1.coverage_content_type = "physicalMeasurement"
        var_T1.cell_methods = "time: mean"
        var_T1.long_name = "optics block temperature"
        self.apply_data(times, var_T1, data_T1)

        var_T2 = g.createVariable("electronics_temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_temperature(var_T2)
        netcdf_timeseries.variable_coordinates(g, var_T2)
        var_T2.variable_id = "T2"
        var_T2.coverage_content_type = "physicalMeasurement"
        var_T2.cell_methods = "time: mean"
        var_T2.long_name = "electronics temperature"
        self.apply_data(times, var_T2, data_T2)

        var_V1 = g.createVariable("first_stage_monitor", "f8", ("time",), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_V1)
        var_V1.variable_id = "V1"
        var_V1.coverage_content_type = "physicalMeasurement"
        var_V1.cell_methods = "time: mean"
        var_V1.long_name = "first stage monitor voltage"
        var_V1.units = "V"
        var_V1.C_format = "%5.3f"
        self.apply_data(times, var_V1, data_V1)

        var_V2 = g.createVariable("baseline_monitor", "f8", ("time",), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_V2)
        var_V2.variable_id = "V2"
        var_V2.coverage_content_type = "physicalMeasurement"
        var_V2.cell_methods = "time: mean"
        var_V2.long_name = "baseline monitor voltage"
        var_V2.units = "V"
        var_V2.C_format = "%5.3f"
        self.apply_data(times, var_V2, data_V2)

        if data_raw_inputs.time.shape[0] > 0:
            g.createDimension("analog_input", data_raw_inputs.value.shape[1])
            var_raw_inputs = g.createVariable("analog_input", "f8", ("time", "analog_input"), fill_value=nan)
            netcdf_timeseries.variable_coordinates(g, var_raw_inputs)
            var_raw_inputs.variable_id = "ZINPUTS"
            var_raw_inputs.coverage_content_type = "physicalMeasurement"
            var_raw_inputs.long_name = "raw input values from all analog channels"
            var_raw_inputs.C_format = "%5.3f"
            var_raw_inputs.units = "V"
            self.apply_data(times, var_raw_inputs, data_raw_inputs)
        else:
            var_raw_inputs = None

        n_diameters = max(
            data_Nb.value.shape[1] if data_Nb.value.shape[0] > 0 else 0,
            data_Ns.value.shape[1] if data_Ns.value.shape[0] > 0 else 0,
        )
        if n_diameters > 0:
            g.createDimension("diameter", n_diameters)

        if n_diameters > 0 and len(data_Ns.value.shape) == 2 and data_Ns.value.shape[0] > 0 and data_Ns.value.shape[1] > 0:
            var_diameter = g.createVariable("diameter", "f8", ("diameter",), fill_value=nan)
            netcdf_var.variable_size_distribution_Dp(var_diameter)
            var_diameter.variable_id = "Ns"
            var_diameter.coverage_content_type = "coordinate"
            var_diameter.cell_methods = "time: mean"
            assign_diameters = data_Ns.value.shape[1]
            diameter_values = data_Ns.value[-1]
            var_diameter[:assign_diameters] = diameter_values[:assign_diameters]

        if n_diameters > 0 and data_Nb.time.shape[0] > 0 and len(data_Nb.value.shape) == 2 and data_Nb.value.shape[1] > 0:
            var_Nb = g.createVariable("number_distribution", "f8", ("time", "diameter"), fill_value=nan)
            netcdf_var.variable_size_distribution_dN(var_Nb)
            netcdf_timeseries.variable_coordinates(g, var_Nb)
            var_Nb.variable_id = "Nb"
            var_Nb.coverage_content_type = "physicalMeasurement"
            var_Nb.long_name = "binned number concentration (dN) with ADC overflow in the final bin"
            n_add = n_diameters - data_Nb.value.shape[1]
            if n_add > 0:
                value_Nb = np.pad(data_Nb.value, ((0, 0), (0, n_add)), mode='constant', constant_values=nan)
            else:
                value_Nb = data_Nb.value
            self.apply_data(times, var_Nb, data_Nb.time, value_Nb)
        else:
            var_Nb = None

        self.apply_cut_size(g, times, [
            (var_N, data_N),
            (var_T1, data_T1),
            (var_T2, data_T2),
            (var_V1, data_V1),
            (var_V2, data_V2),
        ])
        self.apply_coverage(g, times, f"N_{self.instrument_id}")

        self.apply_instrument_metadata(f"N_{self.instrument_id}", manufacturer="DMT", model="BCP")

        return True