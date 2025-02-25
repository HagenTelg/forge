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
        C(Thermo49.with_instrument_override(serial_number="416806715") , end='2024-08-14'),
        C('thermo49iq', start='2024-08-14'),
    ],
    "S11": [ C('tsi3563nephelometer', start='2021-05-12'), ],
})
