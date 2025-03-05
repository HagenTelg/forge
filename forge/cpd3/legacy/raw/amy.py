#!/usr/bin/env python3

import typing
import os
from forge.const import STATIONS as VALID_STATIONS
from forge.cpd3.legacy.raw.write import InstrumentTimeConversion as C
from forge.cpd3.legacy.instrument.azonixumac1050 import Converter as UMAC
from forge.cpd3.legacy.instrument.lovepid import Converter as LovePID

STATION = os.path.basename(__file__).split('.', 1)[0].lower()
assert STATION in VALID_STATIONS


C.run(STATION, {
    "A11": [
        C('clap', end='2018-12-07'),
        C('clap', start='2021-04-14'),
    ],
    "N11": [ C('bmi1720cpc+secondary', start='2022-02-03', end='2022-02-08'), ],
    "N71": [
        C('bmi1720cpc', start='2018-11-09', end='2018-11-30'),
        C('bmi1720cpc', start='2021-05-20'),
    ],
    "Q11": [ C('tsimfm', start='2018-11-09', end='2018-11-11'), ],
    "S11": [ C('tsi3563nephelometer', start='2018-11-09'), ],
    "S12": [
        C('ecotechnephelometer+secondary', start='2018-12-06', end='2021-04-14'),
        C('ecotechnephelometer+secondary', start='2021-06-01', end='2022-02-05'),
        C('ecotechnephelometer+secondary', start='2025-03-01'),
    ],

    "X1": [ C(UMAC.with_variables({
        "Pd_P11": {
            "units": "hPa",
            "C_format": "%5.1f",
            "long_name": "impactor pressure drop",
        },
        "Pd_P12": {
            "units": "hPa",
            "C_format": "%5.1f",
            "long_name": "pump vacuum",
        },
        "T_V11": {
            "units": "degC",
            "C_format": "%5.1f",
            "long_name": "impactor box inlet temperature",
        },
    }, {
        "Pd_P01": {
            "units": "hPa",
            "C_format": "%5.1f",
            "long_name": "stack pitot tube",
        },
        "Q_Q71": {
            "units": "lpm",
            "C_format": "%6.2f",
            "long_name": "CPC flow",
        },
        "Q_Q72": {
            "units": "lpm",
            "C_format": "%6.2f",
            "long_name": "CPC drier flow",
        },
        "T_V51": {
            "units": "degC",
            "C_format": "%5.1f",
            "long_name": "splitter temperature",
        },
    }), start='2018-01-03') ],
    "X2": [ C(LovePID.with_variables({
        "Q_Q11": {
            "units": "lpm",
            "C_format": "%6.2f",
            "long_name": "analyzer flow",
        },
        "U_V11": {
            "units": "%",
            "C_format": "%5.1f",
            "long_name": "impactor box inlet RH",
        },
    }, {
        "U_V51": {
            "units": "%",
            "C_format": "%5.1f",
            "long_name": "splitter RH",
        },
    }), start='2018-01-03') ],
})
