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

STATION = os.path.basename(__file__).split('.', 1)[0].lower()
assert STATION in VALID_STATIONS


class SMPS(WavelengthConverter):
    WAVELENGTHS = [
        (450.0, "B"),
        (550.0, "G"),
        (700.0, "R"),
    ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._average_interval: typing.Optional[float] = None

    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"aerosol", "scattering", "size"}

    @property
    def average_interval(self) -> typing.Optional[float]:
        return self._average_interval

    def run(self) -> bool:
        data_Bs = self.load_wavelength_variable("Bs")
        if not any([v.time.shape != 0 for v in data_Bs]):
            return False
        self._average_interval = self.calculate_average_interval(np.concatenate([v.time for v in data_Bs]))
        if not super().run():
            return False

        g, times = self.data_group(data_Bs, fill_gaps=False)

        var_Bs = g.createVariable("scattering_coefficient", "f8", ("time", "wavelength"), fill_value=nan)
        netcdf_var.variable_total_scattering(var_Bs)
        netcdf_timeseries.variable_coordinates(g, var_Bs)
        var_Bs.variable_id = "Bs"
        var_Bs.coverage_content_type = "physicalMeasurement"
        var_Bs.cell_methods = "time: mean"
        self.apply_wavelength_data(times, var_Bs, data_Bs)

        return True


C.run(STATION, {
    "A11": [ C('clap', start='2018-07-08'), ],
    "N81": [ C(SMPS, start='2019-01-01', end='2019-04-25'), ],
    "S11": [ C('tsi3563nephelometer'), ],
    "S12": [
        C('tsi3563nephelometer+secondary', start='2021-01-20', end='2022-06-25'),
        C('tsi3563nephelometer+secondary', start='2024-09-26', end='2025-02-27'),
    ],
})
