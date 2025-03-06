#!/usr/bin/env python3

import typing
import os
import forge.data.structure.variable as netcdf_var
import forge.data.structure.timeseries as netcdf_timeseries
from math import nan
from forge.const import STATIONS as VALID_STATIONS
from forge.cpd3.legacy.raw.write import InstrumentTimeConversion as C
from forge.cpd3.legacy.instrument.converter import InstrumentConverter
from forge.cpd3.legacy.instrument.brooks0254 import Converter as Brooks0254

STATION = os.path.basename(__file__).split('.', 1)[0].lower()
assert STATION in VALID_STATIONS


class TSI3010Legacy(InstrumentConverter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._average_interval: typing.Optional[float] = None

    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"aerosol", "cpc", "tsi3010cpc"}

    @property
    def instrument_type(self) -> typing.Optional[str]:
        return "tsi3010cpc"

    @property
    def average_interval(self) -> typing.Optional[float]:
        return self._average_interval

    def run(self) -> bool:
        data_N = self.load_variable(f"N_{self.instrument_id}")
        if data_N.time.shape[0] == 0:
            return False
        self._average_interval = self.calculate_average_interval(data_N.time)
        if not super().run():
            return False

        g, times = self.data_group([data_N])

        var_N = g.createVariable("number_concentration", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_number_concentration(var_N)
        netcdf_timeseries.variable_coordinates(g, var_N)
        var_N.variable_id = "N"
        var_N.coverage_content_type = "physicalMeasurement"
        var_N.cell_methods = "time: mean"
        self.apply_data(times, var_N, data_N)

        self.apply_cut_size(g, times, [
            (var_N, data_N),
        ])
        self.apply_coverage(g, times, f"N_{self.instrument_id}")

        self.apply_instrument_metadata(f"N_{self.instrument_id}", manufacturer="TSI", model="3010")

        return True


C.run(STATION, {
    "A11": [
        C('psap1w', end='2013-09-23T08:47:00Z'),
        C('clap', start='2013-09-23T08:47:00Z'),
    ],
    "A12": [ C('clap+secondary', start='2012-05-23', end='2013-09-24'), ],
    "N61": [ C(TSI3010Legacy, end='2012-04-16'), ],
    "S11": [ C('tsi3563nephelometer'), ],
    "XM1": [ C('generic_met'), ],
    "X1": [ C(Brooks0254.with_variables({
        "Q_Q11": {
            "units": "lpm",
            "C_format": "%6.2f",
            "long_name": "analyzer flow",
        },
    }, {
        "Q_Q12": {
            "units": "lpm",
            "C_format": "%6.2f",
            "long_name": "dilution flow",
        },
    }), start='2013-11-04'), ],
})
