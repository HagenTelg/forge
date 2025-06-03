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
        C('psap3w', end='2016-04-11'),
        C('clap', start='2016-04-11'),
    ],
    "A12": [ C('clap+secondary', start='2011-07-20', end='2016-04-12'), ],
    "N71": [ C('tsi3781cpc', start='2008-02-21', end='2012-09-01'), ],
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
            "T_V01": {
                "units": "degC",
                "C_format": "%5.1f",
                "long_name": "pump box temperature",
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
        }), end='2010-12-01'),
        C(UMAC.with_variables({
            "T_V11": {
                "units": "degC",
                "C_format": "%5.1f",
                "long_name": "impactor box inlet temperature",
            },
            "U_V11": {
                "units": "%",
                "C_format": "%5.1f",
                "long_name": "impactor box inlet RH",
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
            "T_V01": {
                "units": "degC",
                "C_format": "%5.1f",
                "long_name": "pump box temperature",
            },
            "Pd_P01": {
                "units": "hPa",
                "C_format": "%5.1f",
                "long_name": "stack pitot tube",
            },
        }), start='2010-12-01', end='2012-02-08'),
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
            "T_V01": {
                "units": "degC",
                "C_format": "%5.1f",
                "long_name": "pump box temperature",
            },
            "Pd_P01": {
                "units": "hPa",
                "C_format": "%5.1f",
                "long_name": "stack pitot tube",
            },
        }), start='2012-02-08'),
    ],
    "X2": [
        C(LovePID.with_variables({}, {
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
            "U_V51": {
                "units": "%",
                "C_format": "%5.1f",
                "long_name": "splitter RH",
            },
        }), end='2010-12-01'),
        C(LovePID.with_variables({
            "Q_Q11": {
                "units": "lpm",
                "C_format": "%6.2f",
                "long_name": "analyzer flow",
            },
        }, {}), start='2010-12-01', end='2012-02-08'),
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
        }), start='2012-02-08'),
    ],
})
