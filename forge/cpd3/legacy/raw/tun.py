#!/usr/bin/env python3

import typing
import os
from forge.const import STATIONS as VALID_STATIONS
from forge.cpd3.legacy.raw.write import InstrumentTimeConversion as C

STATION = os.path.basename(__file__).split('.', 1)[0].lower()
assert STATION in VALID_STATIONS


C.run(STATION, {
    "G81": [C('thermo49'), ],
    "G82": [C('tech2b205+secondary', start='2016-08-31', end='2019-01-09'), ],
})
