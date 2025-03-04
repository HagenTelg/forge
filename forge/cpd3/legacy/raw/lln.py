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
        C('psap3w', start='2008-10-10', end='2023-12-05'),
        C('clap', start='2023-12-05'),
    ],
    "A12": [
        C('clap+secondary', start='2011-07-01', end='2017-04-19T12:45:00Z'),
        C('clap+secondary', start='2017-04-19T14:01:00Z', end='2023-12-06'),
    ],
    "A61": [ C('clap+secondary', start='2017-04-20', end='2017-04-22'), ],
    "A81": [
        C('mageeae31', end='2023-12-05'),
        C('mageeae33', start='2024-01-15'),
    ],
    "A82": [ C('mageeae33+secondary', start='2023-12-06'), ],
    "A91": [ C('clap+secondary', start='2017-04-21'), ],
    "M81": [ C('mageetca08', start='2020-05-01'), ],
    "N71": [
        C('tsi3010cpc', start='2008-10-10', end='2021-09-30'),
        C('admagic200cpc', start='2023-02-28'),
    ],
    "S11": [ C('tsi3563nephelometer', start='2008-10-10'), ],

    "X1": [
        C(UMAC.with_variables({}, {
            "T_V01": {
                "units": "degC",
                "C_format": "%5.1f",
                "long_name": "stack temperature",
            },
            "T_V51": {
                "units": "degC",
                "C_format": "%5.1f",
                "long_name": "splitter temperature",
            },
            "Pd_P01": {
                "units": "hPa",
                "C_format": "%5.1f",
                "long_name": "stack pitot tube",
            },
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
        }), start='2008-10-10', end='2010-11-03'),
        C(UMAC.with_variables({
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
            "T_V01": {
                "units": "degC",
                "C_format": "%5.1f",
                "long_name": "stack temperature",
            },
            "T_V51": {
                "units": "degC",
                "C_format": "%5.1f",
                "long_name": "splitter temperature",
            },
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
        }), start='2010-11-03'),
    ],
    "X2": [
        C(LovePID.with_variables({}, {
            "U_V51": {
                "units": "%",
                "C_format": "%5.1f",
                "long_name": "splitter RH",
            },
            "Q_Q11": {
                "units": "lpm",
                "C_format": "%6.2f",
                "long_name": "analyzer flow",
            },
        }), start='2008-10-10', end='2010-11-03'),
        C(LovePID.with_variables({
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
            "U_V01": {
                "units": "%",
                "C_format": "%5.1f",
                "long_name": "stack RH",
            },
        }), start='2010-11-03'),
    ],
})
