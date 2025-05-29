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
    "A11": [ C('psap3w'), ],
    "A12": [ C('bmitap+secondary', start='2019-06-21'), ],
    "N71": [ C('tsi3010cpc'), ],
    "S11": [ C('tsi3563nephelometer'), ],

    "X1": [
        C(UMAC.with_variables({}, {
            "T_V11": {
                "units": "degC",
                "C_format": "%5.1f",
                "long_name": "impactor box inlet temperature",
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
        }), end='2011-08-24T11:16:00Z'),
        C(UMAC.with_variables({
            "T_V11": {
                "units": "degC",
                "C_format": "%5.1f",
                "long_name": "impactor box inlet temperature",
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
        }, {
            "T_V51": {
                "units": "degC",
                "C_format": "%5.1f",
                "long_name": "splitter temperature",
            },
            "T_V02": {
                "units": "degC",
                "C_format": "%5.1f",
                "long_name": "room temperature",
            },
            "Pd_P01": {
                "units": "hPa",
                "C_format": "%5.1f",
                "long_name": "stack pitot tube",
            },
            "Q_Q61": {
                "units": "lpm",
                "C_format": "%6.2f",
                "long_name": "CPC flow",
            },
            "Q_Q62": {
                "units": "lpm",
                "C_format": "%6.2f",
                "long_name": "CPC drier flow",
            },
        }), start='2011-08-24T11:16:00Z'),
    ],
    "X2": [
        C(LovePID.with_variables({}, {
            "U_V51": {
                "units": "%",
                "C_format": "%5.1f",
                "long_name": "splitter RH",
            },
            "U_V11": {
                "units": "%",
                "C_format": "%5.1f",
                "long_name": "impactor box inlet RH",
            },
            "Q_Q11": {
                "units": "lpm",
                "C_format": "%6.2f",
                "long_name": "analyzer flow",
            },
        }), end='2011-08-24T11:16:00Z'),
        C(LovePID.with_variables({
            "U_V11": {
                "units": "%",
                "C_format": "%5.1f",
                "long_name": "impactor box inlet RH",
            },
            "Q_Q11": {
                "units": "lpm",
                "C_format": "%6.2f",
                "long_name": "analyzer flow",
            },
        }, {
            "U_V51": {
                "units": "%",
                "C_format": "%5.1f",
                "long_name": "splitter RH",
            },
        }), start='2011-08-24T11:16:00Z'),
    ],
})
