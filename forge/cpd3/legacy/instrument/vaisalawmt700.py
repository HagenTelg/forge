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
        return {"aerosol", "met", "vaisalawmt700"}

    @property
    def instrument_type(self) -> typing.Optional[str]:
        return "vaisalawmt700"

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
        data_T1 = self.load_variable(f"T1_{self.instrument_id}")
        data_T2 = self.load_variable(f"T2_{self.instrument_id}")

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

        var_T1 = g.createVariable("sonic_temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_air_temperature(var_T1)
        netcdf_timeseries.variable_coordinates(g, var_T1)
        var_T1.variable_id = "T1"
        var_T1.coverage_content_type = "physicalMeasurement"
        var_T1.cell_methods = "time: mean"
        var_T1.long_name = "air temperature used for speed of sound calculations"
        self.apply_data(times, var_T1, data_T1)

        var_T2 = g.createVariable("transducer_temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_temperature(var_T2)
        netcdf_timeseries.variable_coordinates(g, var_T2)
        var_T2.variable_id = "T2"
        var_T2.coverage_content_type = "physicalMeasurement"
        var_T2.cell_methods = "time: mean"
        var_T2.long_name = "temperature of ultrasonic transducer"
        self.apply_data(times, var_T2, data_T2)

        self.apply_coverage(g, times, f"WS_{self.instrument_id}")

        def meta_extra(meta) -> typing.Dict[str, typing.Tuple[str, str]]:
            result: typing.Dict[str, typing.Tuple[str, str]] = dict()

            pcb_number = meta.get("PCBNumber")
            if pcb_number:
                result["pcb_number"] = pcb_number

            return result

        self.apply_instrument_metadata(f"WS_{self.instrument_id}", manufacturer="Vaisala", model="WMT700", extra=meta_extra)

        return True