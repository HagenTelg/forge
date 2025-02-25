#!/usr/bin/env python3

import typing
import os
from forge.const import STATIONS as VALID_STATIONS
from forge.cpd3.legacy.raw.write import InstrumentTimeConversion as C

STATION = os.path.basename(__file__).split('.', 1)[0].lower()
assert STATION in VALID_STATIONS


C.run(STATION, {
    "A11": [
        C('psap1w', start='2009-10-13', end='2012-04-30'),
        C('psap1w+secondary', start='2012-04-30', end='2015-03-25'),
        C('clap', start='2015-03-25'),
    ],
    "A12": [ C('clap', start='2012-04-30', end='2015-03-25'), ],
    "E81": [ C('dmtpax+secondary', start='2015-07-01', end='2016-10-22'), ],
    "N11": [ C('grimm110xopc', start='2021-07-05'), ],
    "N12": [ C('generic_size_distribution', start='2012-10-25'), ],
    "N61": [ C('tsi377xcpc', start='2011-02-18'), ],
    "S11": [ C('tsi3563nephelometer', start='2009-10-13'), ],
    "XM1": [ C('generic_met'), ],
})
