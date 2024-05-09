import typing
import numpy as np
import forge.data.structure.variable as netcdf_var
import forge.data.structure.timeseries as netcdf_timeseries
from math import nan
from .converter import InstrumentConverter, Group


class Converter(InstrumentConverter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._average_interval: typing.Optional[float] = None

    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"aerosol", "size"}

    @property
    def average_interval(self) -> typing.Optional[float]:
        return self._average_interval

    def add_other_data(self, times: np.ndarray, g: Group) -> None:
        pass

    def run(self) -> bool:
        data_Nb = self.load_array_variable(f"Nb_{self.instrument_id}")
        data_Nn = self.load_array_variable(f"Nn_{self.instrument_id}")
        if data_Nb.time.shape[0] == 0 and data_Nn.time.shape[0] == 0:
            return False
        self._average_interval = self.calculate_average_interval(data_Nb.time if data_Nb.time.shape[0] > 0 else data_Nn.time)
        if not super().run():
            return False

        data_Ns = self.load_array_state(f"Ns_{self.instrument_id}")

        g, times = self.data_group([data_Nb, data_Nn])

        n_diameters = max(
            data_Nb.value.shape[1] if data_Nb.value.shape[0] > 0 else 0,
            data_Nn.value.shape[1] if data_Nn.value.shape[0] > 0 else 0,
            data_Ns.value.shape[1] if data_Ns.value.shape[0] > 0 else 0
        )

        g.createDimension("diameter", n_diameters)
        if data_Ns.value.shape[1] > 0:
            var_diameter = g.createVariable("diameter", "f8", ("diameter",), fill_value=nan)
            netcdf_var.variable_size_distribution_Dp(var_diameter)
            var_diameter.variable_id = "Ns"
            var_diameter.coverage_content_type = "coordinate"
            var_diameter.cell_methods = "time: mean"
            assign_diameters = data_Ns.value.shape[1]
            diameter_values = data_Ns.value[-1]
            var_diameter[:assign_diameters] = diameter_values[:assign_diameters]

        if data_Nb.time.shape[0] > 0:
            var_Nb = g.createVariable("number_distribution", "f8", ("time", "diameter"), fill_value=nan)
            netcdf_var.variable_size_distribution_dN(var_Nb)
            netcdf_timeseries.variable_coordinates(g, var_Nb)
            var_Nb.variable_id = "Nb"
            var_Nb.coverage_content_Vype = "physicalMeasurement"
            n_add = n_diameters - data_Nb.value.shape[1]
            if n_add > 0:
                value_Nb = np.pad(data_Nb.value, ((0, 0), (0, n_add)), mode='constant', constant_values=nan)
            else:
                value_Nb = data_Nb.value
            self.apply_data(times, var_Nb, data_Nb.time, value_Nb)

        if data_Nn.time.shape[0] > 0:
            var_Nn = g.createVariable("normalized_number_distribution", "f8", ("time", "diameter"), fill_value=nan)
            netcdf_var.variable_size_distribution_dNdlogDp(var_Nn)
            netcdf_timeseries.variable_coordinates(g, var_Nn)
            var_Nn.variable_id = "Nn"
            var_Nn.coverage_content_Vype = "physicalMeasurement"
            n_add = n_diameters - data_Nn.value.shape[1]
            if n_add > 0:
                value_Nn = np.pad(data_Nn.value, ((0, 0), (0, n_add)), mode='constant', constant_values=nan)
            else:
                value_Nn = data_Nn.value
            self.apply_data(times, var_Nn, data_Nn.time, value_Nn)

        self.add_other_data(times, g)

        if data_Nb.time.shape[0] > data_Nn.time.shape[0]:
            self.apply_coverage(g, times, f"Nb_{self.instrument_id}")
            self.apply_instrument_metadata(f"Nb_{self.instrument_id}")
        else:
            self.apply_coverage(g, times, f"Nn_{self.instrument_id}")
            self.apply_instrument_metadata(f"Nn_{self.instrument_id}")

        return True