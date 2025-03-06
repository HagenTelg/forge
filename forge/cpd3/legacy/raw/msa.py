#!/usr/bin/env python3

import typing
import os
import numpy as np
import forge.data.structure.variable as netcdf_var
import forge.data.structure.timeseries as netcdf_timeseries
from math import nan
from forge.const import STATIONS as VALID_STATIONS
from forge.cpd3.legacy.raw.write import InstrumentTimeConversion as C
from forge.cpd3.legacy.instrument.converter import WavelengthConverter
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

        var_T1 = g.createVariable("sample_temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_air_temperature(var_T1)
        netcdf_timeseries.variable_coordinates(g, var_T1)
        var_T1.variable_id = "T1"
        var_T1.coverage_content_T1ype = "physicalMeasurement"
        var_T1.cell_methods = "time: mean"
        self.apply_data(times, var_T1, data_T1)

        self.apply_cut_size(g, times, [
            (var_T1, data_T1),
        ], [
            (var_Bac, data_Bac),
        ])
        selected_idx = 0
        for wlidx in range(len(self.WAVELENGTHS)):
            if data_Bac[wlidx].time.shape[0] > data_Bac[selected_idx].time.shape[0]:
                selected_idx = wlidx
        self.apply_coverage(g, times, f"Ba{self.WAVELENGTHS[selected_idx][1]}_{self.instrument_id}")

        self.apply_instrument_metadata(
            [f"Bac{code}_{self.instrument_id}" for _, code in self.WAVELENGTHS],
            manufacturer="Thermo", model="MAAP",
        )

        return True


C.run(STATION, {
    "A11": [ C(LegacyMAAP, end='2015-01-01'), ],
    "A21": [ C('thermomaap', start='2017-01-25'), ],
    "A81": [ C('mageeae33', start='2017-01-25'), ],
    "S11": [
        C('ecotechnephelometer', end='2015-01-01'),
        C('ecotechnephelometer', start='2017-01-25'),
    ],
})
