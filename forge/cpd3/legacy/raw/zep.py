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
    "A21": [ C('thermomaap', start='2018-04-21', end='2018-04-21'), ],
    "A31": [ C('thermomaap', start='2018-04-21'), ],
    "S11": [ C('tsi3563nephelometer', start='2010-01-01', end='2018-04-21'), ],
    "S12": [ C('ecotechnephelometer+secondary', start='2018-04-06', end='2018-04-22'), ],
    "S13": [ C('ecotechnephelometer+secondary', start='2018-04-21', end='2018-04-22'), ],
    "S41": [
        C('tsi3563nephelometer+secondary', start='2013-09-18', end='2018-04-21'),
        C('tsi3563nephelometer', start='2018-04-21'),
    ],
})
