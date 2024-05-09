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
        return {"aerosol", "cpc", "tsi3783cpc"}

    @property
    def instrument_type(self) -> typing.Optional[str]:
        return "tsi3783cpc"

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

        data_Qu = self.load_variable(f"Qu_{self.instrument_id}")
        data_P1 = self.load_variable(f"P1_{self.instrument_id}")
        data_P2 = self.load_variable(f"P2_{self.instrument_id}")
        data_Tu = self.load_variable(f"Tu_{self.instrument_id}")
        data_T1 = self.load_variable(f"T1_{self.instrument_id}")
        data_T2 = self.load_variable(f"T2_{self.instrument_id}")
        data_T3 = self.load_variable(f"T3_{self.instrument_id}")
        data_T4 = self.load_variable(f"T4_{self.instrument_id}")
        data_T5 = self.load_variable(f"T5_{self.instrument_id}")
        data_A = self.load_variable(f"A_{self.instrument_id}")
        data_PCT = self.load_variable(f"PCT_{self.instrument_id}")
        data_V1 = self.load_variable(f"V1_{self.instrument_id}")
        data_V2 = self.load_variable(f"V2_{self.instrument_id}")

        g, times = self.data_group([data_N])
        self.declare_system_flags(g, times)

        var_N = g.createVariable("number_concentration", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_number_concentration(var_N)
        netcdf_timeseries.variable_coordinates(g, var_N)
        var_N.variable_id = "N"
        var_N.coverage_content_type = "physicalMeasurement"
        var_N.cell_methods = "time: mean"
        self.apply_data(times, var_N, data_N)

        var_Qu = g.createVariable("inlet_flow", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_flow(var_Qu)
        netcdf_timeseries.variable_coordinates(g, var_Qu)
        var_Qu.variable_id = "Qu"
        var_Qu.coverage_content_type = "physicalMeasurement"
        var_Qu.cell_methods = "time: mean"
        var_Qu.C_format = "%5.3f"
        var_Qu.long_name = "inlet flow rate"
        self.apply_data(times, var_Qu, data_Qu)

        var_P1 = g.createVariable("pressure", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_air_pressure(var_P1)
        netcdf_timeseries.variable_coordinates(g, var_P1)
        var_P1.variable_id = "P1"
        var_P1.coverage_content_type = "physicalMeasurement"
        var_P1.cell_methods = "time: mean"
        var_P1.long_name = "absolute pressure at instrument inlet"
        self.apply_data(times, var_P1, data_P1)

        var_P2 = g.createVariable("vacuum_pressure", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_pressure(var_P2)
        netcdf_timeseries.variable_coordinates(g, var_P2)
        var_P2.variable_id = "P2"
        var_P2.coverage_content_type = "physicalMeasurement"
        var_P2.cell_methods = "time: mean"
        var_P2.long_name = "vacuum pressure at instrument outlet"
        self.apply_data(times, var_P2, data_P2)

        var_Tu = g.createVariable("inlet_temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_air_temperature(var_Tu)
        netcdf_timeseries.variable_coordinates(g, var_Tu)
        var_Tu.variable_id = "Tu"
        var_Tu.coverage_content_type = "physicalMeasurement"
        var_Tu.cell_methods = "time: mean"
        var_Tu.long_name = "air temperature at the instrument inlet"
        self.apply_data(times, var_Tu, data_Tu)

        var_T1 = g.createVariable("saturator_temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_temperature(var_T1)
        netcdf_timeseries.variable_coordinates(g, var_T1)
        var_T1.variable_id = "T1"
        var_T1.coverage_content_type = "physicalMeasurement"
        var_T1.cell_methods = "time: mean"
        var_T1.long_name = "saturator temperature"
        self.apply_data(times, var_T1, data_T1)

        var_T2 = g.createVariable("growth_tube_temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_temperature(var_T2)
        netcdf_timeseries.variable_coordinates(g, var_T2)
        var_T2.variable_id = "T2"
        var_T2.coverage_content_type = "physicalMeasurement"
        var_T2.cell_methods = "time: mean"
        var_T2.long_name = "growth tube temperature"
        self.apply_data(times, var_T2, data_T2)

        var_T3 = g.createVariable("optics_temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_temperature(var_T3)
        netcdf_timeseries.variable_coordinates(g, var_T3)
        var_T3.variable_id = "T3"
        var_T3.coverage_content_type = "physicalMeasurement"
        var_T3.cell_methods = "time: mean"
        var_T3.long_name = "optics block temperature"
        self.apply_data(times, var_T3, data_T3)

        var_T4 = g.createVariable("water_seperator_temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_temperature(var_T4)
        netcdf_timeseries.variable_coordinates(g, var_T4)
        var_T4.variable_id = "T4"
        var_T4.coverage_content_type = "physicalMeasurement"
        var_T4.cell_methods = "time: mean"
        var_T4.long_name = "water separator temperature"
        self.apply_data(times, var_T4, data_T4)

        var_T5 = g.createVariable("cabinet_temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_temperature(var_T5)
        netcdf_timeseries.variable_coordinates(g, var_T5)
        var_T5.variable_id = "T5"
        var_T5.coverage_content_type = "physicalMeasurement"
        var_T5.cell_methods = "time: mean"
        var_T5.long_name = "temperature inside the cabinet"
        self.apply_data(times, var_T5, data_T5)

        var_A = g.createVariable("laser_current", "f8", ("time",), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_A)
        var_A.variable_id = "A"
        var_A.coverage_content_type = "physicalMeasurement"
        var_A.cell_methods = "time: mean"
        var_A.long_name = "laser current"
        var_A.units = "mA"
        var_A.C_format = "%3.0f"
        self.apply_data(times, var_A, data_A)

        var_PCT = g.createVariable("nozzle_pressure_drop", "f8", ("time",), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_PCT)
        var_PCT.variable_id = "PCT"
        var_PCT.coverage_content_type = "physicalMeasurement"
        var_PCT.cell_methods = "time: mean"
        var_PCT.long_name = "normalized pressure drop across the nozzle"
        var_PCT.units = "%"
        var_PCT.C_format = "%3.0f"
        self.apply_data(times, var_PCT, data_PCT)

        var_V1 = g.createVariable("photodetector_voltage", "f8", ("time",), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_V1)
        var_V1.variable_id = "V1"
        var_V1.coverage_content_type = "physicalMeasurement"
        var_V1.cell_methods = "time: mean"
        var_V1.long_name = "average photodetector voltage"
        var_V1.units = "mV"
        var_V1.C_format = "%3.0f"
        self.apply_data(times, var_V1, data_V1)

        var_V2 = g.createVariable("pulse_height", "f8", ("time",), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_V2)
        var_V2.variable_id = "V2"
        var_V2.coverage_content_type = "physicalMeasurement"
        var_V2.cell_methods = "time: mean"
        var_V2.long_name = "average pulse height"
        var_V2.units = "mV"
        var_V2.C_format = "%3.0f"
        self.apply_data(times, var_V2, data_V2)

        self.apply_cut_size(g, times, [
            (var_N, data_N),
            (var_Qu, data_Qu),
            (var_P1, data_P1),
            (var_P2, data_P2),
            (var_Tu, data_Tu),
            (var_T1, data_T1),
            (var_T2, data_T2),
            (var_T3, data_T3),
            (var_T4, data_T4),
            (var_T5, data_T5),
            (var_A, data_A),
            (var_PCT, data_PCT),
            (var_V1, data_V1),
            (var_V2, data_V2),
        ])
        self.apply_coverage(g, times, f"N_{self.instrument_id}")

        self.apply_instrument_metadata(f"N_{self.instrument_id}", manufacturer="TSI", model="3783")

        return True