#!/usr/bin/env python3

import typing
import os
import forge.data.structure.variable as netcdf_var
import forge.data.structure.timeseries as netcdf_timeseries
from math import nan
from forge.const import STATIONS as VALID_STATIONS
from forge.cpd3.legacy.raw.write import InstrumentTimeConversion as C
from forge.cpd3.legacy.instrument.converter import InstrumentConverter

STATION = os.path.basename(__file__).split('.', 1)[0].lower()
assert STATION in VALID_STATIONS


class Met(InstrumentConverter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._average_interval: typing.Optional[float] = None

    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"aerosol", "met"}

    @property
    def average_interval(self) -> typing.Optional[float]:
        return self._average_interval

    def run(self) -> bool:
        data_WS = self.load_variable(f"WS1?_{self.instrument_id}")
        if data_WS.time.shape[0] == 0:
            return False
        self._average_interval = self.calculate_average_interval(data_WS.time)
        if not super().run():
            return False

        data_WD = self.load_variable(f"WD1?_{self.instrument_id}")
        data_T = self.load_variable(f"T1?_{self.instrument_id}")
        data_U = self.load_variable(f"U1?_{self.instrument_id}")
        data_P = self.load_variable(f"P1?_{self.instrument_id}")
        data_X1 = self.load_variable(f"X1_{self.instrument_id}")
        data_X2 = self.load_variable(f"X2_{self.instrument_id}")
        data_X3 = self.load_variable(f"X3_{self.instrument_id}")
        data_X4 = self.load_variable(f"X4_{self.instrument_id}")
        data_X5 = self.load_variable(f"X5_{self.instrument_id}")
        data_X6 = self.load_variable(f"X6_{self.instrument_id}")
        data_X7 = self.load_variable(f"X7_{self.instrument_id}")

        g, times = self.data_group([data_WS, data_WD, data_T, data_P, data_X1])

        if data_WS.time.shape[0] > 0:
            var_WS = g.createVariable("wind_speed", "f8", ("time",), fill_value=nan)
            netcdf_var.variable_wind_speed(var_WS)
            netcdf_timeseries.variable_coordinates(g, var_WS)
            var_WS.variable_id = "WS1"
            var_WS.coverage_content_type = "physicalMeasurement"
            var_WS.cell_methods = "time: mean wind_direction: vector_direction"
            self.apply_data(times, var_WS, data_WS)

        if data_WD.time.shape[0] > 0:
            var_WD = g.createVariable("wind_direction", "f8", ("time",), fill_value=nan)
            netcdf_var.variable_wind_direction(var_WD)
            netcdf_timeseries.variable_coordinates(g, var_WD)
            var_WD.variable_id = "WD1"
            var_WD.coverage_content_type = "physicalMeasurement"
            var_WD.cell_methods = "time: mean wind_speed: vector_magnitude"
            self.apply_data(times, var_WD, data_WD)

        if data_P.time.shape[0] > 0:
            var_P = g.createVariable("ambient_pressure", "f8", ("time",), fill_value=nan)
            netcdf_var.variable_air_pressure(var_P)
            netcdf_timeseries.variable_coordinates(g, var_P)
            var_P.variable_id = "P"
            var_P.coverage_content_type = "physicalMeasurement"
            var_P.cell_methods = "time: mean"
            var_P.long_name = "ambient pressure"
            self.apply_data(times, var_P, data_P)

        if data_T.time.shape[0] > 0:
            var_T = g.createVariable("ambient_temperature", "f8", ("time",), fill_value=nan)
            netcdf_var.variable_air_temperature(var_T)
            netcdf_timeseries.variable_coordinates(g, var_T)
            var_T.variable_id = "T1"
            var_T.coverage_content_type = "physicalMeasurement"
            var_T.cell_methods = "time: mean"
            var_T.long_name = "ambient air temperature"
            self.apply_data(times, var_T, data_T)

        if data_U.time.shape[0] > 0:
            var_U = g.createVariable("ambient_humidity", "f8", ("time",), fill_value=nan)
            netcdf_var.variable_air_rh(var_U)
            netcdf_timeseries.variable_coordinates(g, var_U)
            var_U.variable_id = "U1"
            var_U.coverage_content_type = "physicalMeasurement"
            var_U.cell_methods = "time: mean"
            var_U.long_name = "ambient air humidity"
            self.apply_data(times, var_U, data_U)

        if data_X1.time.shape[0] > 0:
            var_X1 = g.createVariable("sulfur_dioxide_mixing_ratio", "f8", ("time",), fill_value=nan)
            netcdf_timeseries.variable_coordinates(g, var_X1)
            var_X1.variable_id = "X1"
            var_X1.coverage_content_type = "physicalMeasurement"
            var_X1.cell_methods = "time: mean"
            var_X1.long_name = "fraction concentration of sulfur dioxide"
            var_X1.standard_name = "mole_fraction_of_sulfur_dioxide_in_air"
            var_X1.units = "1e-9"  # canonical ppb
            var_X1.C_format = "%9.2f"
            self.apply_data(times, var_X1, data_X1)

        if data_X2.time.shape[0] > 0:
            var_X2 = g.createVariable("nitrogen_monoxide_mixing_ratio", "f8", ("time",), fill_value=nan)
            netcdf_var.variable_no(var_X2)
            netcdf_timeseries.variable_coordinates(g, var_X2)
            var_X2.variable_id = "X2"
            var_X2.coverage_content_type = "physicalMeasurement"
            var_X2.cell_methods = "time: mean"
            self.apply_data(times, var_X2, data_X2)

        if data_X3.time.shape[0] > 0:
            var_X3 = g.createVariable("nitrogen_dioxide_mixing_ratio", "f8", ("time",), fill_value=nan)
            netcdf_var.variable_no2(var_X3)
            netcdf_timeseries.variable_coordinates(g, var_X3)
            var_X3.variable_id = "X3"
            var_X3.coverage_content_type = "physicalMeasurement"
            var_X3.cell_methods = "time: mean"
            self.apply_data(times, var_X3, data_X3)

        if data_X4.time.shape[0] > 0:
            var_X4 = g.createVariable("nox_mixing_ratio", "f8", ("time",), fill_value=nan)
            netcdf_var.variable_nox(var_X4)
            netcdf_timeseries.variable_coordinates(g, var_X4)
            var_X4.variable_id = "X4"
            var_X4.coverage_content_type = "physicalMeasurement"
            var_X4.cell_methods = "time: mean"
            self.apply_data(times, var_X4, data_X4)

        if data_X5.time.shape[0] > 0:
            var_X5 = g.createVariable("ozone_mixing_ratio", "f8", ("time",), fill_value=nan)
            netcdf_var.variable_ozone(var_X5)
            netcdf_timeseries.variable_coordinates(g, var_X5)
            var_X5.variable_id = "X5"
            var_X5.coverage_content_type = "physicalMeasurement"
            var_X5.cell_methods = "time: mean"
            self.apply_data(times, var_X5, data_X5)

        if data_X6.time.shape[0] > 0:
            var_X6 = g.createVariable("mass_concentration", "f8", ("time",), fill_value=nan)
            netcdf_timeseries.variable_coordinates(g, var_X6)
            var_X6.variable_id = "X6"
            var_X6.coverage_content_type = "physicalMeasurement"
            var_X6.cell_methods = "time: mean"
            var_X6.long_name = "mass concentration of PM2.5 particles"
            var_X6.units = "ug m-3"
            var_X6.C_format = "%7.2f"
            self.apply_data(times, var_X6, data_X6)
            
        if data_X7.time.shape[0] > 0:
            var_X7 = g.createVariable("hydrogen_sulfide_mixing_ratio", "f8", ("time",), fill_value=nan)
            netcdf_timeseries.variable_coordinates(g, var_X7)
            var_X7.variable_id = "X7"
            var_X7.coverage_content_type = "physicalMeasurement"
            var_X7.cell_methods = "time: mean"
            var_X7.long_name = "fraction concentration of hydrogen sulfide"
            var_X7.standard_name = "mole_fraction_of_hydrogen_sulfide_in_air"
            var_X7.units = "1e-9"  # canonical ppb
            var_X7.C_format = "%9.2f"
            self.apply_data(times, var_X7, data_X7)

        return True

    def analyze_flags_mapping_bug(
            self,
            variable: str = None,
            flags_map: typing.Dict[str, typing.Union[str, typing.Tuple[str, int]]] = None,
            bit_shift: int = 16,
            only_fixed_assignment: bool = False,
    ) -> None:
        return None


C.run(STATION, {
    "A11": [ C('clap'), ],
    "N61": [ C('tsi377xcpc', start='2015-06-25'), ],
    "S11": [ C('tsi3563nephelometer'), ],
    "XM1": [ C(Met, start='2013-05-27', end='2016-01-02'), ],
})
