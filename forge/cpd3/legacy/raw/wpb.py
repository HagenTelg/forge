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
    "A11": [ C('clap'), ],
    "A12": [ C('bmitap+secondary', start='2023-04-18', end='2023-09-07'), ],
    "A81": [ C('mageeae33'), ],
    "N71": [ C('admagic200cpc', start='2022-01-26'), ],
    "N72": [ C('admagic200cpc+secondary', start='2022-01-26', end='2022-06-07'), ],
    "N73": [ C('admagic200cpc+secondary', start='2022-01-26', end='2022-06-07'), ],
    "N74": [ C('admagic200cpc+secondary', start='2022-01-26', end='2022-02-05'), ],
    "S11": [ C('ecotechnephelometer'), ],
    "X1": [ C(CR1000.with_variables({
        "Pd_P11": {
            "units": "hPa",
            "C_format": "%5.1f",
            "long_name": "impactor pressure drop",
        },
        "T_V11": {
            "units": "degC",
            "C_format": "%5.1f",
            "long_name": "impactor box inlet temperature",
        },
    }, {
        "T_V21": {
            "units": "degC",
            "C_format": "%5.1f",
            "long_name": "second rack sensor",
        },
    })) ],
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
        "U_V21": {
            "units": "%",
            "C_format": "%5.1f",
            "long_name": "second rack sensor",
        },
    })) ],
    "XM1": [ C('vaisalawxt5xx'), ],
})
