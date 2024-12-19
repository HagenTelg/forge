#!/usr/bin/env python3

import typing
import os
from forge.const import STATIONS as VALID_STATIONS
from forge.cpd3.legacy.raw.write import InstrumentTimeConversion as C
from forge.cpd3.legacy.instrument.thermo49 import Converter as Thermo49
from forge.cpd3.legacy.instrument.gml_met import Converter as GMLMet

STATION = os.path.basename(__file__).split('.', 1)[0].lower()
assert STATION in VALID_STATIONS


C.run(STATION, {
    "N71": [
        C('admagic200cpc', start='2021-06-14', end='2024-08-13T10:18:00Z'),
        C('admagic250cpc', start='2024-08-13T10:18:00Z'),
    ],
    "A11": [
        C('psap1w+secondary', start='2011-05-06', end='2015-08-17'),
        C('clap', start='2015-08-17'),
    ],
    "A12": [
        # CLAP A12 as the primary absorption
        C('clap', start='2011-05-06', end='2015-08-17'),
    ],
    "A81": [
        C('mageeae31', start='2003-08-05', end='2016-07-08'),
        C('mageeae33', start='2016-10-26', end='2018-01-11'),
    ],
    "A82": [ C('mageeae33+secondary', start='2014-10-30', end='2016-11-04'), ],
    "S11": [ C('tsi3563nephelometer', start='2011-05-06'), ],
    "S12": [ C('ecotechnephelometer+secondary', start='2024-07-24'), ],
    "G81": [
        C(Thermo49.with_instrument_override(serial_number="75577-6380"), end='2015-08-17'),
        C('thermo49', start='2015-08-17'),
    ],
    "G82": [C('thermo49+secondary', start='2022-05-09', end='2022-05-26'), ],
    "N11": [C('dmtccn', start='2011-05-06', end='2011-07-22'), ],
    "N12": [C('pmslasair', start='2011-05-06', end='2011-07-22'), ],
    "Q11": [ C('tsimfm', start='2016-10-26'), ],
    "XM1": [ C(GMLMet.with_variables({
        "1": "at 2m",
        "2": "at 10m",
        "3": "at 20m",
    }, {
        "1": "at 10m",
        "2": "at 16m",
    }), start='2005-08-12', end='2017-07-29'),
    # CR1000 is XM2 before 2017-07-29
    ],
})
