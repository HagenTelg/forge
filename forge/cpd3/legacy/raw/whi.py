#!/usr/bin/env python3

import typing
import os
import numpy as np
from forge.const import STATIONS as VALID_STATIONS
from forge.cpd3.legacy.raw.write import InstrumentTimeConversion as C
from forge.cpd3.legacy.instrument.converter import InstrumentConverter

STATION = os.path.basename(__file__).split('.', 1)[0].lower()
assert STATION in VALID_STATIONS


class AirQuality(InstrumentConverter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._average_interval: typing.Optional[float] = None

    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"aerosol"}

    @property
    def average_interval(self) -> typing.Optional[float]:
        return self._average_interval

    def run(self) -> bool:
        system_flags_time = self.load_variable(f"F1?_{self.instrument_id}", convert=bool, dtype=np.bool_).time
        if system_flags_time.shape[0] == 0:
            return False
        self._average_interval = self.calculate_average_interval(system_flags_time)
        if not super().run():
            return False

        g, times = self.data_group([system_flags_time], fill_gaps=False)

        self.declare_system_flags(g, times, flags_map={
            "ContaminateAirQuality": "data_contamination_air_quality",
        })

    def analyze_flags_mapping_bug(
            self,
            variable: str = None,
            flags_map: typing.Dict[str, typing.Union[str, typing.Tuple[str, int]]] = None,
            bit_shift: int = 16,
            only_fixed_assignment: bool = False,
    ):
        return self.analyze_flags_mapping_bug(flags_map={
            "ContaminateAirQuality": "data_contamination_air_quality",
        }, only_fixed_assignment=only_fixed_assignment)


C.run(STATION, {
    "A11": [
        C('psap1w', start='2008-09-28', end='2010-06-27'),
        C('psap3w', start='2010-06-27', end='2016-04-24'),
        C('clap', start='2016-04-24'),
    ],
    "A12": [ C('clap+secondary', start='2012-07-10', end='2016-04-24'), ],
    "N61": [ C('tsi377xcpc', start='2008-09-28'), ],
    "S11": [ C('tsi3563nephelometer', start='2008-09-28'), ],
    "XAQ": [ C(AirQuality), ],
})
