#!/usr/bin/env python3

import typing
import os
from forge.const import STATIONS as VALID_STATIONS
from forge.cpd3.legacy.raw.write import InstrumentTimeConversion as C
from forge.cpd3.legacy.instrument.vaisalawmt700 import Converter as VasalaWMT700
from forge.cpd3.legacy.instrument.lovepid import Converter as LovePID
from forge.cpd3.legacy.instrument.azonixumac1050 import Converter as UMAC
from forge.cpd3.legacy.instrument.campbellcr1000gmd import Converter as CR1000

STATION = os.path.basename(__file__).split('.', 1)[0].lower()
assert STATION in VALID_STATIONS


C.run(STATION, {
    "A11": [ C('clap'), ],
    "A12": [ C('clap+secondary'), ],
    "N71": [ C('tsi3760cpc'), ],
    "N81": [ C('dmtbcp+secondary', end='2018-05-20'), ],
    "Q12": [ C('tsimfm'), ],
    "Q71": [ C('tsimfm'), ],
    "Q72": [ C('tsimfm'), ],
    "S11": [
        C('tsi3563nephelometer', end='2019-04-04'),
        C('ecotechnephelometer', start='2019-04-04', end='2019-06-20'),
    ],
    "S12": [ C('tsi3563nephelometer+secondary', end='2019-04-10'), ],
    "XM1": [
        C('vaisalawxt5xx', end='2019-03-30'),
        C('vaisalapwdx2', start='2021-03-30'),
    ],
    "XM2": [ C('vaisalapwdx2', end='2019-03-28'), ],
    "XM4": [ C(VasalaWMT700.with_instrument_override(serial_number="K4730203").with_added_tag("secondary"), start='2019-03-21', end='2019-03-28'), ],

    "X1": [
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
                "long_name": "room temperature",
            },
            "Pd_P01": {
                "units": "hPa",
                "C_format": "%5.1f",
                "long_name": "stack pitot tube",
            },
        }), end='2018-11-29'),
        C(CR1000.with_variables({
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
                "long_name": "room temperature",
            },
            "Pd_P01": {
                "units": "hPa",
                "C_format": "%5.1f",
                "long_name": "stack pitot tube",
            },
        }), start='2019-01-31'),
    ],
    "X2": [
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
            "U_V01": {
                "units": "%",
                "C_format": "%5.1f",
                "long_name": "room RH",
            },
        })),
    ],
})
