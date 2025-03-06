#!/usr/bin/env python3

import typing
import os
from forge.const import STATIONS as VALID_STATIONS
from forge.cpd3.legacy.raw.write import InstrumentTimeConversion as C
from forge.cpd3.legacy.instrument.campbellcr1000gmd import Converter as CR1000
from forge.cpd3.legacy.instrument.lovepid import Converter as LovePID

STATION = os.path.basename(__file__).split('.', 1)[0].lower()
assert STATION in VALID_STATIONS


C.run(STATION, {
    "S12": [ C('tsi3563nephelometer'), ],
    "X1": [ C(CR1000.with_variables({}, {
        "T_V11": {
            "units": "degC",
            "C_format": "%5.1f",
            "long_name": "humidified nephelometer sensor temperature",
        },
        "U_V11": {
            "units": "%",
            "C_format": "%5.1f",
            "long_name": "humidified nephelometer sensor RH",
        },
        "T_V12": {
            "units": "degC",
            "C_format": "%5.1f",
            "long_name": "humidifier outlet temperature",
        },
        "T_V13": {
            "units": "degC",
            "C_format": "%5.1f",
            "long_name": "humidified nephelometer internal temperature",
        },
        "U_V13": {
            "units": "%",
            "C_format": "%5.1f",
            "long_name": "humidified nephelometer internal RH",
        },
    })) ],
    "X2": [ C(LovePID.with_variables({}, {
        "U_V12": {
            "units": "%",
            "C_format": "%5.1f",
            "long_name": "humidifier outlet RH",
        },
    })) ],
})
