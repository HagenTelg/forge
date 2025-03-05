#!/usr/bin/env python3

import typing
import os
from forge.const import STATIONS as VALID_STATIONS
from forge.cpd3.legacy.raw.write import InstrumentTimeConversion as C

STATION = os.path.basename(__file__).split('.', 1)[0].lower()
assert STATION in VALID_STATIONS


C.run(STATION, {
    "A11": [ C('clap', start='2012-06-03'), ],
    "S11": [ C('tsi3563nephelometer'), ],
    "XM1": [ C('generic_met', start=' 2014-02-07', end='2021-01-13'), ],
})
