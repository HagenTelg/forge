import typing
import numpy as np
import forge.data.structure.variable as netcdf_var
import forge.data.structure.timeseries as netcdf_timeseries
from math import nan
from .converter import InstrumentConverter, read_archive, Selection


class Converter(InstrumentConverter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._average_interval: typing.Optional[float] = None

    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"aerosol", "size", "opc", "pmslasair"}

    @property
    def instrument_type(self) -> typing.Optional[str]:
        return "pmslasair"

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
        data_V = self.load_variable(f"V_{self.instrument_id}")

        g, times = self.data_group([data_N])
        self.declare_system_flags(g, times)

        var_N = g.createVariable("number_concentration", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_number_concentration(var_N)
        netcdf_timeseries.variable_coordinates(g, var_N)
        var_N.variable_id = "N"
        var_N.coverage_content_type = "physicalMeasurement"
        var_N.cell_methods = "time: mean"
        self.apply_data(times, var_N, data_N)

        if data_Nb.time.shape[0] != 0 and len(data_Nb.value.shape) == 2:
            g.createDimension("diameter", data_Nb.value.shape[1])
            if data_Ns.time.shape[0] != 0 and len(data_Ns.value.shape) == 2:
                var_diameter = g.createVariable("diameter", "f8", ("diameter",), fill_value=nan)
                netcdf_var.variable_size_distribution_Dp(var_diameter)
                var_diameter.variable_id = "Ns"
                var_diameter.coverage_content_type = "coordinate"
                var_diameter.cell_methods = "time: mean"
                diameter_values = data_Ns.value[-1]
                assign_diameters = min(diameter_values.shape[0], data_Nb.value.shape[1])
                var_diameter[:assign_diameters] = diameter_values[:assign_diameters]
            else:
                var_diameter = None

            var_Nb = g.createVariable("number_distribution", "f8", ("time", "diameter"), fill_value=nan)
            netcdf_var.variable_size_distribution_dN(var_Nb)
            netcdf_timeseries.variable_coordinates(g, var_Nb)
            var_Nb.variable_id = "Nb"
            var_Nb.coverage_content_type = "physicalMeasurement"
            self.apply_data(times, var_Nb, data_Nb)
        else:
            var_diameter = None
            var_Nb = None

        var_Q = g.createVariable("sample_flow", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_sample_flow(var_Q)
        netcdf_timeseries.variable_coordinates(g, var_Q)
        var_Q.variable_id = "Q"
        var_Q.coverage_content_type = "physicalMeasurement"
        var_Q.cell_methods = "time: mean"
        var_Q.C_format = "%6.3f"
        self.apply_data(times, var_Q, data_Q)

        var_V = g.createVariable("laser_reference", "f8", ("time",), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_V)
        var_V.variable_id = "V"
        var_V.long_name = "laser reference voltage"
        var_V.units = "V"
        var_V.coverage_content_type = "physicalMeasurement"
        var_V.cell_methods = "time: mean"
        var_V.C_format = "%6.3f"
        self.apply_data(times, var_V, data_V)

        self.apply_coverage(g, times, f"N_{self.instrument_id}")

        self.apply_instrument_metadata(f"N_{self.instrument_id}", manufacturer="PMS", generic_model="LASAIR")

        return True