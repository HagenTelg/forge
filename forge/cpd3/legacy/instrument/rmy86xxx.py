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
        return {"aerosol", "met", "rmy86xxx"}

    @property
    def instrument_type(self) -> typing.Optional[str]:
        return "rmy86xxx"

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

        self.apply_coverage(g, times, f"WS_{self.instrument_id}")

        self.apply_instrument_metadata(f"WS_{self.instrument_id}", manufacturer="Gill Instruments", generic_model="86xxx")

        return True