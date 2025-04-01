#!/usr/bin/env python3

import typing
import os
import numpy as np
from math import nan
import forge.data.structure.variable as netcdf_var
import forge.data.structure.timeseries as netcdf_timeseries
from forge.const import STATIONS as VALID_STATIONS
from forge.cpd3.legacy.raw.write import InstrumentTimeConversion as C
from forge.cpd3.legacy.instrument.generic_met import Converter as BaseMet
from forge.cpd3.legacy.instrument.azonixumac1050 import Converter as UMAC
from forge.cpd3.legacy.instrument.lovepid import Converter as LovePID

STATION = os.path.basename(__file__).split('.', 1)[0].lower()
assert STATION in VALID_STATIONS


class Met(BaseMet):
    def run(self) -> bool:
        if not super().run():
            return False

        data_Tx = self.load_variable(f"Tx_{self.instrument_id}")

        g = self.root.groups["data"]
        times = g.variables["time"][...].data

        if data_Tx.time.shape[0] > 0:
            var_Tx = g.createVariable("sonic_temperature", "f8", ("time",), fill_value=nan)
            netcdf_var.variable_temperature(var_Tx)
            netcdf_timeseries.variable_coordinates(g, var_Tx)
            var_Tx.variable_id = "Tx"
            var_Tx.coverage_content_T1ype = "physicalMeasurement"
            var_Tx.cell_methods = "time: mean"
            var_Tx.long_name = "sonic sensor temperature"
            self.apply_data(times, var_Tx, data_Tx)

        return True


class CutSizeFixUMAC(UMAC):
    def apply_cut_size(
            self,
            g,
            group_times,
            variables,
            extra_sources = None,
    ) -> None:
        for i in range(len(variables)):
            var = variables[i]
            if var[0] is None:
                continue
            if not isinstance(var[1], UMAC.Data):
                continue
            if var[0].name == "T_V12":
                var[1].cut_size = np.empty((0,), dtype=np.float64)
        return super().apply_cut_size(g, group_times, variables, extra_sources)

    def data_group(
            self,
            variable_times,
            name: str = "data",
            fill_gaps: typing.Union[bool, float] = True,
    ):
        if name == "data":
            original_variables = variable_times
            variable_times = list(variable_times)
            for i in reversed(range(len(variable_times))):
                var = variable_times[i]
                if not isinstance(var, UMAC.Data):
                    continue
                if np.all(np.invert(np.isfinite(var.cut_size))):
                    del variable_times[i]
            if not variable_times:
                variable_times = original_variables
        return super().data_group(variable_times, name=name, fill_gaps=fill_gaps)


C.run(STATION, {
    "A11": [
        C('psap1w', end='2010-09-20'),
        C('psap3w', start='2010-09-20', end='2016-03-07'),
        C('clap', start='2016-03-07'),
    ],
    "A12": [ C('clap+secondary', start='2012-01-30', end='2016-03-08'), ],
    "A81": [
        C('mageeae31', start='2010-09-20', end='2013-02-27'),
        C('mageeae33', start='2022-06-17'),
    ],
    "N12": [ C('dmtccn', start='2023-08-25'), ],
    "N71": [
        C('tsi3760cpc', end='2016-08-17'),
        C('tsi3010cpc', start='2016-08-17'),
    ],
    "N72": [ C('tsi3783cpc+secondary', start='2012-03-19', end='2013-09-24'), ],
    "N73": [ C('tsi3760cpc+secondary', start='2013-08-19', end='2014-05-27'), ],
    "S11": [
        C('tsi3563nephelometer', end='2024-01-09'),
        C('ecotechnephelometer', start='2024-01-09'),
    ],
    "S12": [
        C('rrm903nephelometer+secondary', end='2012-05-09'),
        C('tsi3563nephelometer+secondary', start='2012-05-09', end='2024-01-09'),
        C('ecotechnephelometer+secondary', start='2024-01-09'),
    ],
    "S13": [ C('ecotechnephelometer+secondary', start='2023-09-30', end='2024-01-09'), ],
    "S14": [ C('ecotechnephelometer+secondary', start='2023-09-30', end='2024-01-09'), ],
    "Q13": [ C('tsimfm', start='2014-05-27', end='2018-03-15'), ],  # Dilution flow
    "XM1": [ C(Met, start='2010-10-04', end='2012-06-29'), ],
    "XM2": [ C('vaisalapwdx2', start='2011-04-27', end='2013-01-20'), ],

    "X1": [ C(UMAC.with_variables({
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
    }), end='2010-09-20'), C(UMAC.with_variables({
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
        "T_V12": {
            "units": "degC",
            "C_format": "%5.1f",
            "long_name": "humdified neph downstream temperature",
        },
        "Pd_P12": {
            "units": "hPa",
            "C_format": "%5.1f",
            "long_name": "pump vacuum",
        },
        "Q_Q12": {
            "units": "lpm",
            "C_format": "%6.2f",
            "long_name": "humdified neph flow",
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
    }), start='2010-09-20', end='2017-06-05T18:04:00Z') , C(CutSizeFixUMAC.with_variables({
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
        "T_V12": {
            "units": "degC",
            "C_format": "%5.1f",
            "long_name": "humdified neph downstream temperature",
        },
        "Pd_P12": {
            "units": "hPa",
            "C_format": "%5.1f",
            "long_name": "pump vacuum",
        },
        "Q_Q12": {
            "units": "lpm",
            "C_format": "%6.2f",
            "long_name": "humdified neph flow",
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
    }), start='2017-06-05T18:04:00Z', end='2019-03-15T16:08:00Z'), C(UMAC.with_variables({
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
        "T_V12": {
            "units": "degC",
            "C_format": "%5.1f",
            "long_name": "humdified neph downstream temperature",
        },
        "Pd_P12": {
            "units": "hPa",
            "C_format": "%5.1f",
            "long_name": "pump vacuum",
        },
        "Q_Q12": {
            "units": "lpm",
            "C_format": "%6.2f",
            "long_name": "humdified neph flow",
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
    }), start='2019-03-15T16:08:00Z') ],
    "X2": [ C(LovePID.with_variables({
        "U_V11": {
            "units": "%",
            "C_format": "%5.1f",
            "long_name": "impactor box inlet RH",
        },
    }, {
        "Q_Q11": {
            "units": "lpm",
            "C_format": "%6.2f",
            "long_name": "analyzer flow",
        },
    }), end='2010-09-20'), C(LovePID.with_variables({
        "U_V11": {
            "units": "%",
            "C_format": "%5.1f",
            "long_name": "impactor box inlet RH",
        },
        "U_V12": {
            "units": "%",
            "C_format": "%5.1f",
            "long_name": "humdified neph downstream RH",
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
    }), start='2010-09-20', end='2024-07-19'), C(LovePID.with_variables({
        "U_V11": {
            "units": "%",
            "C_format": "%5.1f",
            "long_name": "impactor box inlet RH",
        },
        "U_V12": {
            "units": "%",
            "C_format": "%5.1f",
            "long_name": "humdified neph upstream RH",
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
    }), start='2024-07-19') ],
})
