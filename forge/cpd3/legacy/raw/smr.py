#!/usr/bin/env python3

import typing
import os
import forge.data.structure.variable as netcdf_var
import forge.data.structure.timeseries as netcdf_timeseries
import numpy as np
from math import nan
from forge.const import STATIONS as VALID_STATIONS
from forge.cpd3.legacy.instrument.converter import InstrumentConverter
from forge.cpd3.legacy.raw.write import InstrumentTimeConversion as C

STATION = os.path.basename(__file__).split('.', 1)[0].lower()
assert STATION in VALID_STATIONS


class LegacyCPC(InstrumentConverter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._average_interval: typing.Optional[float] = None

    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"aerosol", "cpc"}

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

        g, times = self.data_group([data_N])

        var_N = g.createVariable("number_concentration", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_number_concentration(var_N)
        netcdf_timeseries.variable_coordinates(g, var_N)
        var_N.variable_id = "N"
        var_N.coverage_content_type = "physicalMeasurement"
        var_N.cell_methods = "time: mean"
        self.apply_data(times, var_N, data_N)

        self.apply_cut_size(g, times, [
            (var_N, data_N),
        ])
        self.apply_coverage(g, times, f"N_{self.instrument_id}")

        return True


C.run(STATION, {
    "A11": [ C('clap', start='2015-01-01'), ],
    "A12": [ C('clap+secondary', start='2021-05-10', end='2021-09-10'), ],
    "A21": [ C('thermomaap', start='2018-01-18', end='2024-12-06'), ],
    "A81": [ C('mageeae33', start='2018-01-17'), ],
    "N61": [ C(LegacyCPC, start='2005-06-01', end='2017-01-01'), ],
    "N71": [ C('tsi375xcpc', start='2024-12-11'), ],
    "S11": [
        C('tsi3563nephelometer', start='2006-01-01', end='2024-12-11T15:00:00Z '),
        C('acoemnex00nephelometer', start='2024-12-11T15:00:00Z '),
    ],
    "S12": [
        C('tsi3563nephelometer+secondary', start='2020-09-28', end='2021-02-18 '),
    ],
})
