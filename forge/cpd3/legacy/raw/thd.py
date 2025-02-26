#!/usr/bin/env python3

import typing
import os
from forge.const import STATIONS as VALID_STATIONS
from forge.cpd3.legacy.raw.write import InstrumentTimeConversion as C
from forge.cpd3.legacy.instrument.thermo49 import Converter as Thermo49
from forge.cpd3.legacy.instrument.gml_met import Converter as GMLMet
from forge.cpd3.legacy.instrument.azonixumac1050 import Converter as UMAC
from forge.cpd3.legacy.instrument.lovepid import Converter as LovePID

STATION = os.path.basename(__file__).split('.', 1)[0].lower()
assert STATION in VALID_STATIONS


C.run(STATION, {
    "A11": [
        C('psap1w', start='2002-04-10', end='2005-10-07'),
        C('psap3w', start='2005-10-07', end='2013-06-10'),
        C('clap', start='2013-06-10', end='2017-06-02'),
    ],
    "A12": [
        C('psap1w+secondary', start='2005-10-07', end='2005-10-25'),
        C('clap+secondary', start='2011-11-25', end='2013-06-11'),
    ],
    "F21": [ C("filtercarousel", start='2002-04-10', end='2006-08-04'), ],
    "G81": [
        C(Thermo49.with_instrument_override(serial_number="323000000000552"), end='2016-10-24'),
        C('thermo49', start='2016-10-24'),
    ],
    "N71": [ C('tsi3760cpc', start='2002-04-10', end='2017-06-02'), ],
    "N72": [
        C('bmi1710cpc+secondary', start='2012-12-07', end='2016-02-19'),
        C('bmi1720cpc+secondary', start='2016-02-19', end='2017-06-02'),
    ],
    "Q71": [ C('tsimfm', start='2011-12-09', end='2017-06-02'), ],
    "Q72": [ C('tsimfm', start='2011-12-09', end='2017-06-02'), ],
    "S11": [ C('tsi3563nephelometer', start='2002-04-10', end='2017-06-02'), ],
    "S12": [ C('tsi3563nephelometer+secondary', start='2002-04-10', end='2017-06-02'), ],
    "XM1": [
        C(GMLMet.with_variables({
            "1": "at 2m",
            "2": "at 10m",
        }, {
            "1": "",
        }), start='2007-01-11', end='2020-10-22'),
    ],
    "X1": [
        C(UMAC.with_variables({}, {
            "Pd_P11": {
                "units": "hPa",
                "C_format": "%5.1f",
                "long_name": "impactor pressure drop",
            },
            "Pd_P01": {
                "units": "hPa",
                "C_format": "%5.1f",
                "long_name": "stack pitot tube",
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
            "T_V11": {
                "units": "degC",
                "C_format": "%5.1f",
                "long_name": "impactor box inlet temperature",
            },
            "T_V01": {
                "units": "degC",
                "C_format": "%5.1f",
                "long_name": "pump box temperature",
            },
            "T_V02": {
                "units": "degC",
                "C_format": "%5.1f",
                "long_name": "rack temperature",
            },
            "T_V51": {
                "units": "degC",
                "C_format": "%5.1f",
                "long_name": "splitter temperature",
            },
            "WS_X1": {
                "units": "m s-1",
                "C_format": "%4.1f",
                "long_name": "wind speed",
                "cell_methods": "time: WD_X1: vector_direction",
            },
            "WD_X1": {
                "units": "degree",
                "C_format": "%5.1f",
                "long_name": "wind direction from true north",
                "cell_methods": "time: mean WS_X1: vector_magnitude",
            },
        }), start='2002-04-10', end='2011-12-09'),
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
            "Pd_P01": {
                "units": "hPa",
                "C_format": "%5.1f",
                "long_name": "stack pitot tube",
            },
            "T_V02": {
                "units": "degC",
                "C_format": "%5.1f",
                "long_name": "rack temperature",
            },
            "T_V51": {
                "units": "degC",
                "C_format": "%5.1f",
                "long_name": "splitter temperature",
            },
        }), start='2011-12-09', end='2017-06-02'),
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
        }), start='2002-04-10', end='2009-10-26'),
        C(LovePID.with_variables({}, {
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
        }), start='2009-10-26', end='2011-12-09'),
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
            "U_V02": {
                "units": "%",
                "C_format": "%5.1f",
                "long_name": "rack RH",
            },
        }), start='2011-12-09', end='2017-06-02'),
    ],
    "X3": [ C(UMAC.with_variables({
        "T_V12": {
            "units": "degC",
            "C_format": "%5.1f",
            "long_name": "humidifier pre-heater temperature",
        },
        "T_V13": {
            "units": "degC",
            "C_format": "%5.1f",
            "long_name": "humidifier nephelometer inlet temperature",
        },
    }, {
        "Pd_P21": {
            "units": "hPa",
            "C_format": "%5.1f",
            "long_name": "pressure drop across filter position 1",
        },
        "Pd_P22": {
            "units": "hPa",
            "C_format": "%5.1f",
            "long_name": "pressure drop across filter position 2",
        },
        "Pd_P23": {
            "units": "hPa",
            "C_format": "%5.1f",
            "long_name": "pressure drop across filter position 3",
        },
        "Pd_P24": {
            "units": "hPa",
            "C_format": "%5.1f",
            "long_name": "pressure drop across filter position 4",
        },
        "Pd_P25": {
            "units": "hPa",
            "C_format": "%5.1f",
            "long_name": "pressure drop across filter position 5",
        },
        "Pd_P26": {
            "units": "hPa",
            "C_format": "%5.1f",
            "long_name": "pressure drop across filter position 6",
        },
        "Pd_P27": {
            "units": "hPa",
            "C_format": "%5.1f",
            "long_name": "pressure drop across filter position 7",
        },
        "Pd_P28": {
            "units": "hPa",
            "C_format": "%5.1f",
            "long_name": "pressure drop across filter position 8",
        },
        "T_V21": {
            "units": "degC",
            "C_format": "%5.1f",
            "long_name": "filter sample temperature",
        },
        "T_V22": {
            "units": "degC",
            "C_format": "%5.1f",
            "long_name": "filter rack temperature",
        },
    }).with_added_tag("secondary"), start='2002-04-10', end='2006-08-04') ],
    "X4": [ C(LovePID.with_variables({
        "U_V12": {
            "units": "%",
            "C_format": "%5.1f",
            "long_name": "humidifier pre-heater RH",
        },
        "U_V13": {
            "units": "%",
            "C_format": "%5.1f",
            "long_name": "humidifier nephelometer inlet RH",
        },
    }, {
        "Q_Q21": {
            "units": "lpm",
            "C_format": "%6.2f",
            "long_name": "filter sample flow",
        },
        "U_V21": {
            "units": "%",
            "C_format": "%5.1f",
            "long_name": "filter sample RH",
        },
    }).with_added_tag("secondary"), start='2002-04-10', end='2006-08-04') ],
})
