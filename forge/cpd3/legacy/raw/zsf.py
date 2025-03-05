#!/usr/bin/env python3

import typing
import os
import forge.data.structure.variable as netcdf_var
import forge.data.structure.timeseries as netcdf_timeseries
import numpy as np
from math import nan
from forge.const import STATIONS as VALID_STATIONS
from forge.cpd3.legacy.instrument.converter import WavelengthConverter, InstrumentConverter
from forge.cpd3.legacy.raw.write import InstrumentTimeConversion as C
from forge.cpd3.legacy.instrument.mageeae33 import Converter as MageeAE33
from forge.cpd3.legacy.instrument.ecotechnephelometer import Converter as EcotechNephelometer
from forge.data.structure.stp import standard_temperature, standard_pressure

STATION = os.path.basename(__file__).split('.', 1)[0].lower()
assert STATION in VALID_STATIONS


class LegacyMAAP(WavelengthConverter):
    WAVELENGTHS = [
        (670.0, "R"),
    ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._average_interval: typing.Optional[float] = None

    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"aerosol", "absorption", "thermomaap"}

    @property
    def instrument_type(self) -> typing.Optional[str]:
        return "thermomaap"

    @property
    def average_interval(self) -> typing.Optional[float]:
        return self._average_interval

    def run(self) -> bool:
        data_Bac = self.load_wavelength_variable("Ba")
        if not any([v.time.shape[0] != 0 for v in data_Bac]):
            return False
        self._average_interval = self.calculate_average_interval(np.concatenate([v.time for v in data_Bac]))
        if not super().run():
            return False

        data_T1 = self.load_variable(f"T_{self.instrument_id}")

        g, times = self.data_group(data_Bac)
        standard_temperature(g)
        standard_pressure(g)
        self.declare_system_flags(g, times)

        var_Bac = g.createVariable("light_absorption", "f8", ("time", "wavelength"), fill_value=nan)
        netcdf_var.variable_absorption(var_Bac, is_stp=True)
        netcdf_timeseries.variable_coordinates(g, var_Bac)
        var_Bac.variable_id = "Bac"
        var_Bac.coverage_content_type = "physicalMeasurement"
        var_Bac.cell_methods = "time: mean"
        var_Bac.ancillary_variables = "standard_temperature standard_pressure"
        self.apply_wavelength_data(times, var_Bac, data_Bac)

        if data_T1.time.shape[0] > 0:
            var_T1 = g.createVariable("sample_temperature", "f8", ("time",), fill_value=nan)
            netcdf_var.variable_air_temperature(var_T1)
            netcdf_timeseries.variable_coordinates(g, var_T1)
            var_T1.variable_id = "T1"
            var_T1.coverage_content_T1ype = "physicalMeasurement"
            var_T1.cell_methods = "time: mean"
            self.apply_data(times, var_T1, data_T1)

        self.apply_instrument_metadata(
            [f"Bac{code}_{self.instrument_id}" for _, code in self.WAVELENGTHS],
            manufacturer="Thermo", model="MAAP",
        )
        return True


def instrument_id_override(converter: typing.Type[InstrumentConverter], instrument_id: str) -> typing.Type[InstrumentConverter]:
    class Override(converter):
        def load_variable(self, variable: str, *args, **kwargs):
            suffix = "_" + self.instrument_id
            if variable.endswith(suffix):
                variable = variable[:-len(suffix)]
                variable += "_" + instrument_id
            return super().load_variable(variable, *args, **kwargs)

        def load_array_variable(self, variable: str, *args, **kwargs):
            suffix = "_" + self.instrument_id
            if variable.endswith(suffix):
                variable = variable[:-len(suffix)]
                variable += "_" + instrument_id
            return super().load_array_variable(variable, *args, **kwargs)

        def load_state(self, variable: str, *args, **kwargs):
            suffix = "_" + self.instrument_id
            if variable.endswith(suffix):
                variable = variable[:-len(suffix)]
                variable += "_" + instrument_id
            return super().load_state(variable, *args, **kwargs)

        def load_array_state(self, variable: str, *args, **kwargs):
            suffix = "_" + self.instrument_id
            if variable.endswith(suffix):
                variable = variable[:-len(suffix)]
                variable += "_" + instrument_id
            return super().load_array_state(variable, *args, **kwargs)

        def apply_instrument_metadata(self, variable: typing.Union[str, typing.List[str]], *args, **kwargs):
            suffix = "_" + self.instrument_id
            if isinstance(variable, str):
                variable = [variable]
            for i in range(len(variable)):
                check = variable[i]
                if check.endswith(suffix):
                    check = check[:-len(suffix)]
                    check += "_" + instrument_id
                    variable[i] = check
            return super().apply_instrument_metadata(variable, *args, **kwargs)

        def apply_coverage(self, g, group_times, variable: str, *args, **kwargs):
            suffix = "_" + self.instrument_id
            if variable.endswith(suffix):
                variable = variable[:-len(suffix)]
                variable += "_" + instrument_id
            return super().apply_coverage(g, group_times, variable, *args, **kwargs)

    return Override


C.run(STATION, {
    "A11": [
        C(LegacyMAAP, end='2014-12-31'),
        C('mageeae33', start='2022-09-06', end='2022-09-08'),
    ],
    "A81": [
        C(instrument_id_override(MageeAE33, "81"), start='2022-05-02T15:12:00Z', end='2022-05-11T06:04:00Z'),
        C('mageeae33', start='2022-05-11T06:04:00Z', end='2025-02-19'),
    ],
    "S11": [ C('tsi3563nephelometer', start='2010-03-03'), ],
    "S12": [
        C(instrument_id_override(EcotechNephelometer, "A40").with_added_tag("secondary"), start='2022-09-07', end='2022-09-12T06:13:00Z'),
        C("ecotechnephelometer+secondary", start='2022-09-12T06:13:00Z', end='2025-02-20'),
    ],
})
