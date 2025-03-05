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


C.run(STATION, {
    "A11": [ C('clap', start='2021-09-22'), ],
    "A12": [
        C('psap3w', end='2021-09-22'),
        C('psap3w+secondary', start='2021-09-22', end='2022-03-24'),
    ],
    "N71": [ C('tsi375xcpc', start='2025-03-01'), ],
    "S11": [ C('ecotechnephelometer', end='2024-05-30'), ],
})
