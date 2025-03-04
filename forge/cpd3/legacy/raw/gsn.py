#!/usr/bin/env python3

import typing
import os
import numpy as np
import forge.data.structure.timeseries as netcdf_timeseries
from forge.const import STATIONS as VALID_STATIONS
from forge.cpd3.legacy.raw.write import InstrumentTimeConversion as C
from forge.cpd3.legacy.instrument.converter import InstrumentConverter
from forge.cpd3.legacy.instrument.azonixumac1050 import Converter as UMAC
from forge.cpd3.legacy.instrument.lovepid import Converter as LovePID

STATION = os.path.basename(__file__).split('.', 1)[0].lower()
assert STATION in VALID_STATIONS


class LegacyFilterCarousel(InstrumentConverter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"aerosol"}

    @property
    def instrument_type(self) -> typing.Optional[str]:
        return "filtercarousel"

    @property
    def average_interval(self) -> typing.Optional[float]:
        return None

    def run(self) -> bool:
        data_Fn = self.load_state(f"Fn_{self.instrument_id}", dtype=np.uint64)
        if data_Fn.time.shape[0] == 0:
            return False
        if not super().run():
            return False

        g, times = self.state_group([data_Fn])

        var_Fn = g.createVariable("active_filter", "u8", ("time",), fill_value=False)
        netcdf_timeseries.variable_coordinates(g, var_Fn)
        var_Fn.variable_id = "Fn"
        var_Fn.coverage_content_type = "auxiliaryInformation"
        var_Fn.cell_methods = "time: point"
        var_Fn.long_name = "currently accumulating filter number or zero for the bypass"
        var_Fn.C_format = "%2llu"
        self.apply_data(times, var_Fn, data_Fn)

        return True


C.run(STATION, {
    "A11": [
        C('psap1w', start='2001-04-10', end='2002-02-08'),
        C('clap', start='2011-08-25'),
    ],
    "A21": [ C('mageeae31', start='2007-08-22'), ],
    "E21": [ C('dmtpax+secondary', start='2013-05-09', end='2015-10-14'), ],
    "F21": [ C(LegacyFilterCarousel, start='2001-02-23', end='2002-01-30'), ],
    "N61": [
        C('tsi3760cpc', end='2002-02-08'),
        C('tsi377xcpc', start='2008-08-08'),
    ],
    "S11": [
        C('tsi3563nephelometer', start='2001-04-10', end='2002-02-08'),
        C('tsi3563nephelometer', start='2007-12-27'),
    ],
    "S12": [
        C('tsi3563nephelometer+secondary', start='2001-04-10', end='2002-02-08'),
        C('tsi3563nephelometer+secondary', start='2023-08-22', end='2024-11-13'),
    ],

    "Q12": [ C('tsimfm', start='2011-08-25'), ],
    "Q61": [ C('tsimfm', start='2011-08-25', end='2015-10-14'), ],
    "X1": [
        C(UMAC.with_variables({
            "T_V11": {
                "units": "degC",
                "C_format": "%5.1f",
                "long_name": "impactor box inlet temperature",
            },
            "T_V12": {
                "units": "degC",
                "C_format": "%5.1f",
                "long_name": "humidifier pre-heater temperature",
            },
            "T_V13": {
                "units": "degC",
                "C_format": "%5.1f",
                "long_name": "humidified nephelometer inlet temperature",
            },
            "U_V11": {
                "units": "%",
                "C_format": "%5.1f",
                "long_name": "impactor box inlet RH",
            },
            "U_V12": {
                "units": "%",
                "C_format": "%5.1f",
                "long_name": "humidifier pre-heater RH",
            },
            "U_V13": {
                "units": "%",
                "C_format": "%5.1f",
                "long_name": "humidified nephelometer inlet RH",
            },
        }, {
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
        }), start='2001-04-10', end='2002-02-08'),
        C(UMAC.with_variables({
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
            "T_V21": {
                "units": "degC",
                "C_format": "%5.1f",
                "long_name": "line 2, aethalometer, cosmos, PAX, and ECOC temperature",
            },
            "T_V31": {
                "units": "degC",
                "C_format": "%5.1f",
                "long_name": "line 3, APS, SMPS temperature",
            },
            "Pd_P12": {
                "units": "hPa",
                "C_format": "%5.1f",
                "long_name": "pump vacuum",
            },
        }, {
            "Pd_P01": {
                "units": "hPa",
                "C_format": "%5.1f",
                "long_name": "stack pitot tube",
            },
        }), start='2011-08-25'),
    ],
    "X2": [
        C(UMAC.with_variables({}, {
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
        }).with_added_tag("secondary"), start='2001-04-10', end='2002-02-08'),
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
            "U_V21": {
                "units": "%",
                "C_format": "%5.1f",
                "long_name": "line 2, aethalometer, cosmos, PAX, and ECOC RH",
            },
            "U_V31": {
                "units": "%",
                "C_format": "%5.1f",
                "long_name": "line 3, APS, SMPS RH",
            },
        }, {}), start='2011-08-25'),
    ],
})
