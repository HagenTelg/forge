#!/usr/bin/env python3

import typing
import os
import numpy as np
from forge.const import STATIONS as VALID_STATIONS
from forge.cpd3.legacy.raw.write import InstrumentTimeConversion as C
from forge.cpd3.legacy.instrument.thermo49 import Converter as Thermo49
from forge.cpd3.legacy.instrument.converter import read_archive, Selection

STATION = os.path.basename(__file__).split('.', 1)[0].lower()
assert STATION in VALID_STATIONS


class Thermo49AlternateStation(Thermo49):
    CONVERT_ZERO = True

    def load_variable(
            self,
            variable: str,
            convert: typing.Callable[[typing.Any], typing.Any] = None,
            dtype: typing.Type = np.float64,
    ) -> "Thermo49.Data":
        return self.Data(*self.convert_loaded(read_archive([Selection(
            start=self.file_start,
            end=self.file_end,
            stations=["bao"],
            archives=[self.archive],
            variables=[variable],
            include_meta_archive=False,
            include_default_station=False,
            lacks_flavors=["cover", "stats"],
        )]), convert=convert, is_state=False, dtype=dtype, return_cut_size=True))

    def apply_instrument_metadata(self, *args, **kwargs):
        return None

    def apply_coverage(self, *args, **kwargs):
        return None


C.run(STATION, {
    "G81": [
        C(Thermo49AlternateStation),
    ],
})
