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
        C('psap1w', start='2009-10-13', end='2012-04-30'),
        C('psap1w+secondary', start='2012-04-30', end='2015-03-25'),
        C('clap', start='2015-03-25'),
    ],
    "A12": [ C('clap', start='2012-04-30', end='2015-03-25'), ],
    "E81": [ C('dmtpax+secondary', start='2015-07-01', end='2016-10-22'), ],
    "N11": [ C('grimm110xopc', start='2021-07-05'), ],
    "N12": [ C(SMPS, start='2012-10-25'), ],
    "N61": [ C('tsi377xcpc', start='2011-02-18'), ],
    "S11": [ C('tsi3563nephelometer', start='2009-10-13'), ],
    "XM1": [ C('generic_met'), ],
})
