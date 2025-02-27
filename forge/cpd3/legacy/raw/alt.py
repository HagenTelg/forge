#!/usr/bin/env python3

import typing
import os
from forge.const import STATIONS as VALID_STATIONS
from forge.cpd3.legacy.raw.write import InstrumentTimeConversion as C
from forge.cpd3.legacy.instrument.grimm110xopc import Converter as Grimm110xOPC
from forge.cpd3.legacy.instrument.tsi377xcpc import Converter as TSI377xCPC

STATION = os.path.basename(__file__).split('.', 1)[0].lower()
assert STATION in VALID_STATIONS


C.run(STATION, {
    "A11": [
        C('psap1w', start='2004-03-18', end='2007-07-11'),
        C('psap3w', start='2007-07-11', end='2014-08-31'),
        C('psap3w+secondary', start='2014-08-31', end='2017-03-28'),
        C('clap', start='2017-03-28'),
    ],
    "A12": [
        C('psap1w+secondary', start='2007-07-11', end='2010-03-30'),
        C('clap', start='2014-08-31', end='2017-03-28'),
        C('clap+secondary', start='2018-01-12'),
    ],
    "A13": [ C('psap3w+secondary', start='2018-01-16'), ],
    "A81": [
        C('mageeae31', start='2008-04-27', end='2019-03-24'),
        C('mageeae31+secondary', start='2019-03-24', end='2023-11-28'),
    ],
    "A82": [ C('mageeae33', start='2019-03-24'), ],
    "E81": [ C('dmtpax+secondary', start='2012-03-29', end='2014-10-17'), ],
    "N11": [
        C(Grimm110xOPC, start='2011-02-25', end='2017-01-18'),
        C(Grimm110xOPC.with_instrument_override(serial_number="11A15096", model="11-A"), start='2017-01-18', end='2024-03-28'),
        C(Grimm110xOPC, start='2024-03-28'),
    ],
    "N12": [ C('generic_size_distribution', start='2011-03-15'), ],
    "N61": [
        C('tsi3010cpc', start='2004-03-18', end='2011-01-23'),
        C(TSI377xCPC.with_instrument_override(serial_number="71033080", model="3772"), start='2011-01-23', end='2023-06-26'),
        C(TSI377xCPC, start='2023-06-26'),
    ],
    "N62": [
        C(TSI377xCPC.with_instrument_override(serial_number="71033080", model="3772").with_added_tag("secondary"),
          start='2015-11-30', end='2020-08-14'),
        C(TSI377xCPC.with_instrument_override(serial_number="3772150501", model="3772").with_added_tag("secondary"),
          start='2020-08-14', end='2023-11-29'),
        C(TSI377xCPC.with_added_tag("secondary"), start='2023-11-29'),
    ],
    "S11": [ C('tsi3563nephelometer', start='2004-03-18'), ],
    "XM1": [ C('generic_met'), ],
})
