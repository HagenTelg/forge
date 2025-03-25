#!/usr/bin/env python3

import typing
import os
import forge.data.structure.variable as netcdf_var
import forge.data.structure.timeseries as netcdf_timeseries
from math import nan
from forge.const import STATIONS as VALID_STATIONS
from forge.cpd3.legacy.raw.write import InstrumentTimeConversion as C
from forge.cpd3.legacy.instrument.azonixumac1050 import Converter as UMAC
from forge.cpd3.legacy.instrument.lovepid import Converter as LovePID
from forge.cpd3.legacy.instrument.generic_met import Converter as BaseMet

STATION = os.path.basename(__file__).split('.', 1)[0].lower()
assert STATION in VALID_STATIONS


class Met(BaseMet):
    def run(self) -> bool:
        if not super().run():
            return False

        data_WSx = self.load_variable(f"WSx1?_{self.instrument_id}")
        data_X = self.load_variable(f"X_{self.instrument_id}")

        g = self.root.groups["data"]
        times = g.variables["time"][...].data

        if data_WSx.time.shape[0] > 0:
            var_ZWSGust = g.createVariable("wind_speed_gust", "f8", ("time",), fill_value=nan)
            netcdf_timeseries.variable_coordinates(g, var_ZWSGust)
            var_ZWSGust.variable_id = f"ZWSGust1"
            var_ZWSGust.coverage_content_type = "physicalMeasurement"
            var_ZWSGust.cell_methods = "time: mean"
            var_ZWSGust.long_name = "averaged wind gust speed"
            var_ZWSGust.standard_name = "wind_speed_of_gust"
            var_ZWSGust.units = "m s-1"
            var_ZWSGust.C_format = "%4.1f"
            self.apply_data(times, var_ZWSGust, data_WSx)

        if data_X.time.shape[0] > 0:
            var_X = g.createVariable("ozone_mixing_ratio", "f8", ("time",), fill_value=nan)
            netcdf_var.variable_ozone(var_X)
            netcdf_timeseries.variable_coordinates(g, var_X)
            var_X.variable_id = "X"
            var_X.coverage_content_type = "physicalMeasurement"
            var_X.cell_methods = "time: mean"
            self.apply_data(times, var_X, data_X)

        return True


C.run(STATION, {
    "A11": [
        C('psap3w', start='2011-01-24', end='2011-06-25'),
        C('psap3w+secondary', start='2011-06-25', end='2018-06-08'),
        C('clap', start='2018-06-08'),
    ],
    "A12": [ C('clap', start='2011-06-25', end='2018-06-08'),],
    "A81": [ C('clap+secondary', start='2011-08-25', end='2016-10-06'), ],
    "E11": [ C('aerodynecaps', start='2010-11-14', end='2011-06-24'), ],
    "N11": [ C('grimm110xopc', start='2013-08-20', end='2013-11-09T'), ],
    "N71": [
        C('tsi3010cpc+secondary', start='2011-01-24', end='2011-08-10'),
        C('tsi3010cpc', start='2011-08-10'),
    ],
    "N72": [
        C('tsi302xcpc+secondary', start='2012-08-26', end='2016-10-06'),
        C('tsi3010cpc+secondary', start='2021-07-07', end='2021-07-08'),
    ],
    "N73": [ C('dmtccn', start='2012-08-26'), ],
    "N74": [ C('tsi377xcpc+secondary', start='2014-08-19'), ],
    "N81": [ C('tsi3010cpc', end='2011-08-10'), ],  # Primary CPC until removal
    "N82": [ C('tsi302xcpc+secondary', start='2001-02-01', end='2011-08-10'), ],
    "N83": [ C('tsi3010cpc+secondary', start='2012-08-25', end='2016-10-06'), ],
    "N84": [ C('bmi1710cpc+secondary', start='2012-08-26', end='2016-10-06'), ],
    "N85": [ C('tsi302xcpc+secondary', start='2014-08-07', end='2016-10-06'), ],
    "N86": [ C('tsi377xcpc+secondary', start='2014-08-19', end='2016-10-05'), ],
    "S11": [ C('tsi3563nephelometer', start='2011-01-24'), ],
    "S81": [ C('tsi3563nephelometer+secondary', start='2012-08-25', end='2016-10-06'), ],
    "Q83": [ C('tsimfm+secondary', start='2012-08-25', end='2016-10-06'),],
    "X1": [ C(UMAC.with_variables({
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
        "Pd_P12": {
            "units": "hPa",
            "C_format": "%5.1f",
            "long_name": "CPC pump vacuum",
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
        "T_V01": {
            "units": "degC",
            "C_format": "%5.1f",
            "long_name": "lab temperature",
        },
    }), start='2011-01-24') ],
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
        "U_V01": {
            "units": "%",
            "C_format": "%5.1f",
            "long_name": "lab RH",
        },
    }), start='2011-01-24') ],

    "X3": [ C(UMAC.with_variables({
        "Pd_P81": {
            "units": "hPa",
            "C_format": "%5.1f",
            "long_name": "GML impactor pressure drop",
        },
        "Pd_P82": {
            "units": "hPa",
            "C_format": "%5.1f",
            "long_name": "pump vacuum",
        },
        "T_V81": {
            "units": "degC",
            "C_format": "%5.1f",
            "long_name": "GML impactor box inlet temperature",
        },
    }, {
        "Pd_P01": {
            "units": "hPa",
            "C_format": "%5.1f",
            "long_name": "stack pitot tube",
        },
    }).with_added_tag("secondary"), start='2012-08-25', end='2016-10-06') ],
    "X4": [ C(LovePID.with_variables({
        "Q_Q81": {
            "units": "lpm",
            "C_format": "%6.2f",
            "long_name": "GML analyzer flow",
        },
        "U_V81": {
            "units": "%",
            "C_format": "%5.1f",
            "long_name": "GML impactor box inlet RH",
        },
    }, {}).with_added_tag("secondary"), start='2012-08-25', end='2016-10-06') ],
    "XM1": [ C(Met, start='1998-01-01', end='2019-08-30'),],
})
