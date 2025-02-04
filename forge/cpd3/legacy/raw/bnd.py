#!/usr/bin/env python3

import typing
import os
import forge.data.structure.variable as netcdf_var
import forge.data.structure.timeseries as netcdf_timeseries
import numpy as np
from math import nan
from forge.const import STATIONS as VALID_STATIONS
from forge.cpd3.legacy.raw.write import InstrumentTimeConversion as C
from forge.cpd3.legacy.instrument.converter import InstrumentConverter, WavelengthConverter
from forge.cpd3.legacy.instrument.azonixumac1050 import Converter as UMAC
from forge.cpd3.legacy.instrument.lovepid import Converter as LovePID

STATION = os.path.basename(__file__).split('.', 1)[0].lower()
assert STATION in VALID_STATIONS


class MRINeph(WavelengthConverter):
    WAVELENGTHS = [
        (550.0, "G"),
    ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._average_interval: typing.Optional[float] = None

    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"aerosol", "scattering", "mrinephelometer"}

    @property
    def instrument_type(self) -> typing.Optional[str]:
        return "mrinephelometer"

    @property
    def average_interval(self) -> typing.Optional[float]:
        return self._average_interval

    def run(self) -> bool:
        data_Bs = self.load_wavelength_variable("Bs")
        if not any([v.time.shape != 0 for v in data_Bs]):
            return False
        self._average_interval = self.calculate_average_interval(np.concatenate([v.time for v in data_Bs]))
        if not super().run():
            return False

        data_P = self.load_variable(f"P_{self.instrument_id}")
        data_T = self.load_variable(f"T_{self.instrument_id}")

        g, times = self.data_group(data_Bs, fill_gaps=False)
        data_system_flags, system_flags_bits = self.declare_system_flags(g, times)

        var_Bs = g.createVariable("scattering_coefficient", "f8", ("time", "wavelength"), fill_value=nan)
        netcdf_var.variable_total_scattering(var_Bs)
        netcdf_timeseries.variable_coordinates(g, var_Bs)
        var_Bs.variable_id = "Bs"
        var_Bs.coverage_content_type = "physicalMeasurement"
        var_Bs.cell_methods = "time: mean"
        self.apply_wavelength_data(times, var_Bs, data_Bs)

        var_P = g.createVariable("sample_pressure", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_air_pressure(var_P)
        netcdf_timeseries.variable_coordinates(g, var_P)
        var_P.variable_id = "P"
        var_P.coverage_content_type = "physicalMeasurement"
        var_P.cell_methods = "time: mean"
        var_P.long_name = "measurement cell pressure"
        self.apply_data(times, var_P, data_P)

        var_T = g.createVariable("sample_temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_air_temperature(var_T)
        netcdf_timeseries.variable_coordinates(g, var_T)
        var_T.variable_id = "T"
        var_T.coverage_content_type = "physicalMeasurement"
        var_T.cell_methods = "time: mean"
        var_T.long_name = "measurement cell temperature"
        self.apply_data(times, var_T, data_T)

        self.apply_cut_size(g, times, [
            (var_P, data_P),
            (var_T, data_T),
        ], [
            (var_Bs, data_Bs),
        ], extra_sources=[data_system_flags])
        selected_idx = 0
        for wlidx in range(len(self.WAVELENGTHS)):
            if data_Bs[wlidx].time.shape[0] > data_Bs[selected_idx].time.shape[0]:
                selected_idx = wlidx
        self.apply_coverage(g, times, f"Bs{self.WAVELENGTHS[selected_idx][1]}_{self.instrument_id}")

        self.apply_instrument_metadata(
            [f"Bs{code}_{self.instrument_id}" for _, code in self.WAVELENGTHS],
            manufacturer="MRI", model="1550"
        )
        return True


C.run(STATION, {
    "A11": [
        C('psap1w', start='1996-05-06', end='2006-02-22'),
        C('psap3w', start='2006-02-22', end='2012-03-25'),
        C('clap', start='2012-03-25'),
    ],
    "A12": [ C('clap+secondary', start='2010-12-02', end='2012-03-26'), ],
    "F21": [ C("filtercarousel", start='1994-07-14', end='2010-12-02'), ],
    "N61": [ C('tsi3760cpc', start='1994-07-14'), ],
    "N62": [
        C('bmi1710cpc+secondary', start='2012-10-12', end='2015-08-04'),
        C('bmi1720cpc+secondary', start='2015-09-29', end='2019-07-26'),
        C('tsi3760cpc+secondary', start='2022-08-02', end='2022-08-04'),
        C('admagic250cpc+secondary', start='2022-10-13'),
    ],
    "N11": [ C('tsi3760cpc+secondary', start='2019-11-19', end='2019-11-20'), ],
    "N12": [ C('tsi3760cpc+secondary', start='2019-11-19', end='2019-11-20'), ],
    "N51": [ C('tsi3760cpc+secondary', start='2019-11-19', end='2019-11-22'), ],
    "S11": [
        C(MRINeph, start='1994-07-14', end='1996-06-14'),
        C('tsi3563nephelometer', start='1996-06-14'),
    ],
    "S12": [
        C('tsi3563nephelometer+secondary', start='2022-08-02', end='2022-10-07'),
        C('tsi3563nephelometer+secondary', start='2024-05-23', end='2024-05-25'),
    ],

    "X1": [
        C(UMAC.with_variables({}, {
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
            "Pd_P61": {
                "units": "hPa",
                "C_format": "%5.1f",
                "long_name": "CPC vacuum",
            },
            "P_P01": {
                "units": "hPa",
                "C_format": "%6.1f",
                "long_name": "room pressure",
            },
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
            "T_V02": {
                "units": "degC",
                "C_format": "%5.1f",
                "long_name": "room temperature",
            },
            "Q_Q11": {
                "units": "lpm",
                "C_format": "%6.2f",
                "long_name": "analyzer flow",
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
            "Q_Q31": {
                "units": "lpm",
                "C_format": "%6.2f",
                "long_name": "splitter spare line flow",
            },
            "Q_Q41": {
                "units": "lpm",
                "C_format": "%6.2f",
                "long_name": "splitter spare line flow",
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
        }), start='1994-07-14', end='2001-05-07'),
        C(UMAC.with_variables({}, {
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
            "Pd_P61": {
                "units": "hPa",
                "C_format": "%5.1f",
                "long_name": "CPC vacuum",
            },
            "P_P01": {
                "units": "hPa",
                "C_format": "%6.1f",
                "long_name": "room pressure",
            },
            "T_V11": {
                "units": "degC",
                "C_format": "%5.1f",
                "long_name": "impactor box inlet temperature",
            },
            "T_V02": {
                "units": "degC",
                "C_format": "%5.1f",
                "long_name": "room temperature",
            },
            "T_V51": {
                "units": "degC",
                "C_format": "%5.1f",
                "long_name": "splitter temperature",
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
            "Q_Q31": {
                "units": "lpm",
                "C_format": "%6.2f",
                "long_name": "splitter spare line flow",
            },
            "Q_Q41": {
                "units": "lpm",
                "C_format": "%6.2f",
                "long_name": "splitter spare line flow",
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
        }), start='2001-05-07', end='2010-12-01'),
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
            "Pd_P01": {
                "units": "hPa",
                "C_format": "%5.1f",
                "long_name": "stack pitot tube",
            },
            "P_P01": {
                "units": "hPa",
                "C_format": "%6.1f",
                "long_name": "room pressure",
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
            "T_V02": {
                "units": "degC",
                "C_format": "%5.1f",
                "long_name": "room temperature",
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
        }), start='2010-12-01', end='2012-10-10'),
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
            "Pd_P01": {
                "units": "hPa",
                "C_format": "%5.1f",
                "long_name": "stack pitot tube",
            },
            "P_P01": {
                "units": "hPa",
                "C_format": "%6.1f",
                "long_name": "room pressure",
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
            "T_V01": {
                "units": "degC",
                "C_format": "%5.1f",
                "long_name": "room temperature",
            },
            "T_V02": {
                "units": "degC",
                "C_format": "%5.1f",
                "long_name": "room temperature",
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
        }), start='2012-10-10'),
    ],
    "X3": [
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
        }), start='2001-05-07', end='2012-12-07'),
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
            "U_V02": {
                "units": "%",
                "C_format": "%5.1f",
                "long_name": "room RH",
            },
        }), start='2012-12-07', end='2024-10-07'),
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
            "T_V21": {
                "units": "degC",
                "C_format": "%5.1f",
                "long_name": "filter sample temperature",
            },
            "T_V01": {
            "units": "degC",
            "C_format": "%5.1f",
            "long_name": "pump box temperature",
        },
        }).with_added_tag("secondary"), start='2010-12-01', end='2012-10-10'),

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
            "U_V02": {
                "units": "%",
                "C_format": "%5.1f",
                "long_name": "room RH",
            },
        }), start='2024-10-07'),
    ],

    "X4": [
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
            "T_V01": {
                "units": "degC",
                "C_format": "%5.1f",
                "long_name": "pump box temperature",
            },
            "Q_Q21": {
                "units": "lpm",
                "C_format": "%6.2f",
                "long_name": "filter sample flow",
            },
        }).with_added_tag("secondary"), start='1994-07-14', end='2001-05-07'),
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
            "T_V01": {
                "units": "degC",
                "C_format": "%5.1f",
                "long_name": "pump box temperature",
            },
        }).with_added_tag("secondary"), start='2001-05-07', end='2010-12-01'),
    ],
    "X5": [
        C(LovePID.with_variables({}, {
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
            "U_V51": {
                "units": "%",
                "C_format": "%5.1f",
                "long_name": "splitter RH",
            },
        }).with_added_tag("secondary"), start='2001-05-07', end='2012-10-10'),
    ],
})
