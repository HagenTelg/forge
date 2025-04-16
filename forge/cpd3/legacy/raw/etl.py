#!/usr/bin/env python3

import typing
import os
from forge.const import STATIONS as VALID_STATIONS
from forge.cpd3.legacy.raw.write import InstrumentTimeConversion as C
from forge.cpd3.legacy.instrument.generic_size_distribution import Converter as BaseSizeDistribution

STATION = os.path.basename(__file__).split('.', 1)[0].lower()
assert STATION in VALID_STATIONS


class SMPS(BaseSizeDistribution):
    def run(self) -> bool:
        if not super().run():
            return False

        g = self.root.groups["data"]
        times = g.variables["time"][...].data
        self.declare_system_flags(g, times, flags_map={
            "ContaminateFault": "data_contamination_smps_not_ok"
        })

        return True


C.run(STATION, {
    "A11": [
        C('psap1w', end='2011-06-20'),
        C('psap1w+secondary', start='2011-06-20', end='2016-07-19'),
        C('clap', start='2016-07-19'),
    ],
    "A12": [ C('clap', start='2011-06-20', end='2016-07-19'), ],
    "A81": [ C('mageeae31', start='2019-04-25'), ],
    "N11": [ C('grimm110xopc', start='2016-07-19'), ],
    "N12": [ C(SMPS, start='2013-05-09'), ],
    "N61": [ C('tsi377xcpc', start='2010-05-27'), ],
    "S11": [ C('tsi3563nephelometer'), ],
    "S12": [ C('tsi3563nephelometer+secondary', start='2023-04-18'), ],
    "XM1": [
        C('generic_met', start='2009-01-01', end='2018-04-07'),
        C('rmy86xxx', start='2019-04-24'),
    ],
})
