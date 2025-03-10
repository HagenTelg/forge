#!/usr/bin/env python3

import typing
import os
from forge.const import STATIONS as VALID_STATIONS
from forge.cpd3.legacy.raw.write import InstrumentTimeConversion as C
from forge.cpd3.legacy.instrument.thermo49 import Converter as Thermo49

STATION = os.path.basename(__file__).split('.', 1)[0].lower()
assert STATION in VALID_STATIONS


C.run(STATION, {
    "G81": [
        C('thermo49', end='2015-12-20'),
        C(Thermo49.with_instrument_override(serial_number="75576380"), start='2015-12-20', end='2023-11-08T01:02:00Z'),
        C('thermo49iq', start='2024-01-04'),
    ],
    "G82": [ C('thermo49+secondary', start='2023-11-08', end='2024-01-13'), ],
})
