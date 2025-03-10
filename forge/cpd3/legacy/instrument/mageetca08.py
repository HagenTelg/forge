import typing
import numpy as np
import forge.data.structure.variable as netcdf_var
import forge.data.structure.timeseries as netcdf_timeseries
from math import nan, isfinite
from .converter import InstrumentConverter, read_archive, Selection


class Converter(InstrumentConverter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._average_interval: typing.Optional[float] = None

    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"aerosol", "mass", "mageetca08"}

    @property
    def instrument_type(self) -> typing.Optional[str]:
        return "mageetca08"

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

        data_X2 = self.load_variable(f"X2_{self.instrument_id}", convert=lambda x: float(x) * 1E3 if x is not None and isfinite(x) else nan)
        data_Q1 = self.load_variable(f"Q1_{self.instrument_id}")
        data_Q2 = self.load_variable(f"Q2_{self.instrument_id}")
        data_T1 = self.load_variable(f"T1_{self.instrument_id}")
        data_T2 = self.load_variable(f"T2_{self.instrument_id}")
        data_T3 = self.load_variable(f"T3_{self.instrument_id}")
        data_TD3 = self.load_variable(f"TD3_{self.instrument_id}")
        data_P = self.load_variable(f"P_{self.instrument_id}")

        g, times = self.data_group([data_X1])

        var_X1 = g.createVariable("total_carbon_concentration", "f8", ("time", ), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_X1)
        var_X1.variable_id = "X1"
        var_X1.coverage_content_type = "physicalMeasurement"
        var_X1.cell_methods = "time: mean"
        var_X1.long_name = "total carbon concentration"
        var_X1.units = "ug m-3"
        var_X1.C_format = "%8.3f"
        self.apply_data(times, var_X1, data_X1)

        var_X2 = g.createVariable("ambient_co2_concentration", "f8", ("time", ), fill_value=nan)
        netcdf_var.variable_co2(var_X2)
        netcdf_timeseries.variable_coordinates(g, var_X2)
        var_X2.variable_id = "X2"
        var_X2.coverage_content_type = "physicalMeasurement"
        var_X2.cell_methods = "time: mean"
        var_X2.long_name = "fractional concentration of carbon dioxide in ambient air"
        self.apply_data(times, var_X2, data_X2)

        self.apply_coverage(g, times, f"X1_{self.instrument_id}")

        system_flags_time = self.load_variable(f"F1?_{self.instrument_id}", convert=bool, dtype=np.bool_).time
        self._average_interval = self.calculate_average_interval(np.concatenate([data_Q1.time, system_flags_time]))
        g, times = self.data_group([data_Q1, system_flags_time], name='status')
        self.declare_system_flags(g, times)

        var_Q1 = g.createVariable("sample_flow", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_sample_flow(var_Q1)
        netcdf_timeseries.variable_coordinates(g, var_Q1)
        var_Q1.variable_id = "Q1"
        var_Q1.coverage_content_type = "physicalMeasurement"
        var_Q1.cell_methods = "time: mean"
        var_Q1.C_format = "%6.3f"
        self.apply_data(times, var_Q1, data_Q1)

        var_Q2 = g.createVariable("analytic_flow", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_flow(var_Q2)
        netcdf_timeseries.variable_coordinates(g, var_Q2)
        var_Q2.variable_id = "Q2"
        var_Q2.coverage_content_type = "physicalMeasurement"
        var_Q2.cell_methods = "time: mean"
        var_Q2.long_name = "analytic flow"
        self.apply_data(times, var_Q2, data_Q2)

        var_T1 = g.createVariable("chamber_1_temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_temperature(var_T1)
        netcdf_timeseries.variable_coordinates(g, var_T1)
        var_T1.variable_id = "T1"
        var_T1.coverage_content_type = "physicalMeasurement"
        var_T1.cell_methods = "time: mean"
        var_T1.long_name = "measurement chamber 1 temperature"
        self.apply_data(times, var_T1, data_T1)

        var_T2 = g.createVariable("chamber_2_temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_temperature(var_T2)
        netcdf_timeseries.variable_coordinates(g, var_T2)
        var_T2.variable_id = "T2"
        var_T2.coverage_content_type = "physicalMeasurement"
        var_T2.cell_methods = "time: mean"
        var_T2.long_name = "measurement chamber 2 temperature"
        self.apply_data(times, var_T2, data_T2)

        var_T3 = g.createVariable("licor_temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_temperature(var_T3)
        netcdf_timeseries.variable_coordinates(g, var_T3)
        var_T3.variable_id = "T3"
        var_T3.coverage_content_type = "physicalMeasurement"
        var_T3.cell_methods = "time: mean"
        var_T3.long_name = "LI-COR temperature"
        self.apply_data(times, var_T3, data_T3)

        var_TD3 = g.createVariable("licor_dewpoint", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_dewpoint(var_TD3)
        netcdf_timeseries.variable_coordinates(g, var_TD3)
        var_TD3.variable_id = "TD3"
        var_TD3.coverage_content_type = "physicalMeasurement"
        var_TD3.cell_methods = "time: mean"
        var_TD3.long_name = "LI-COR dewpoint"
        self.apply_data(times, var_TD3, data_TD3)

        var_P = g.createVariable("licor_pressure", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_pressure(var_P)
        netcdf_timeseries.variable_coordinates(g, var_P)
        var_P.variable_id = "P"
        var_P.coverage_content_type = "physicalMeasurement"
        var_P.cell_methods = "time: mean"
        var_P.long_name = "LI-COR pressure"
        self.apply_data(times, var_P, data_P)

        self.apply_coverage(g, times, f"F1_{self.instrument_id}")

        self.apply_instrument_metadata(f"X1_{self.instrument_id}", manufacturer="Magee", model="TC08")

        return True

    def reapply_system_flags(
            self,
            flags_data: "InstrumentConverter.Data",
            bit_to_flag: typing.Dict[int, str],
            g=None,
    ) -> None:
        if g is None:
            g = self.root.groups["status"]
        return super().reapply_system_flags(flags_data, bit_to_flag, g)
