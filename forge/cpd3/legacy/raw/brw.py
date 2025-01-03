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
from forge.cpd3.legacy.instrument.campbellcr1000gmd import Converter as CR1000
from forge.cpd3.legacy.instrument.lovepid import Converter as LovePID
from forge.cpd3.legacy.instrument.gml_met import Converter as GMLMet
from forge.cpd3.legacy.instrument.generic_met import Converter as BaseMet

STATION = os.path.basename(__file__).split('.', 1)[0].lower()
assert STATION in VALID_STATIONS


class GERichCounter(InstrumentConverter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._average_interval: typing.Optional[float] = None

    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"aerosol", "cpc", "gerichcpc"}

    @property
    def instrument_type(self) -> typing.Optional[str]:
        return "gerichcpc"

    @property
    def average_interval(self) -> typing.Optional[float]:
        return self._average_interval

    def run(self) -> bool:
        data_N = self.load_variable(f"N_{self.instrument_id}")
        if data_N.time.shape[0] == 0:
            return False
        self._average_interval = self.calculate_average_interval(data_N.time)
        if not super().run():
            return False

        g, times = self.data_group([data_N])

        var_N = g.createVariable("number_concentration", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_number_concentration(var_N)
        netcdf_timeseries.variable_coordinates(g, var_N)
        var_N.variable_id = "N"
        var_N.coverage_content_type = "physicalMeasurement"
        var_N.cell_methods = "time: mean"
        self.apply_data(times, var_N, data_N)

        self.apply_cut_size(g, times, [
            (var_N, data_N),
        ])
        self.apply_coverage(g, times, f"N_{self.instrument_id}")

        self.apply_instrument_metadata(f"N_{self.instrument_id}", manufacturer="GE", model="Rich")

        return True


class MRINeph(WavelengthConverter):
    WAVELENGTHS = [
        (450.0, "B"),
        (550.0, "G"),
        (700.0, "R"),
        (850.0, "Q"),
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

        g, times = self.data_group(data_Bs, fill_gaps=False)
        data_system_flags, system_flags_bits = self.declare_system_flags(g, times)

        var_Bs = g.createVariable("scattering_coefficient", "f8", ("time", "wavelength"), fill_value=nan)
        netcdf_var.variable_total_scattering(var_Bs)
        netcdf_timeseries.variable_coordinates(g, var_Bs)
        var_Bs.variable_id = "Bs"
        var_Bs.coverage_content_type = "physicalMeasurement"
        var_Bs.cell_methods = "time: mean"
        self.apply_wavelength_data(times, var_Bs, data_Bs)

        self.apply_cut_size(g, times, [
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
            manufacturer="MRI", model="4-W"
        )
        return True


class LegacyAE8(WavelengthConverter):
    WAVELENGTHS = [
        (880.0, "1"),
    ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._average_interval: typing.Optional[float] = None

    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"aerosol", "aethalometer", "absorption", "mageeae8"}

    @property
    def instrument_type(self) -> typing.Optional[str]:
        return "mageeae8"

    @property
    def average_interval(self) -> typing.Optional[float]:
        return self._average_interval

    def run(self) -> bool:
        data_Ba = self.load_wavelength_variable("Ba")
        if not any([v.time.shape != 0 for v in data_Ba]):
            return False
        self._average_interval = self.calculate_average_interval(np.concatenate([v.time for v in data_Ba]))
        if not super().run():
            return False

        g, times = self.data_group(data_Ba, fill_gaps=False)
        data_system_flags, system_flags_bits = self.declare_system_flags(g, times)

        var_Ba = g.createVariable("light_absorption", "f8", ("time", "wavelength"), fill_value=nan)
        netcdf_var.variable_absorption(var_Ba, is_stp=True)
        netcdf_timeseries.variable_coordinates(g, var_Ba)
        var_Ba.variable_id = "Ba"
        var_Ba.coverage_content_type = "physicalMeasurement"
        var_Ba.cell_methods = "time: mean"
        self.apply_wavelength_data(times, var_Ba, data_Ba)

        self.apply_cut_size(g, times, [
        ], [
            (var_Ba, data_Ba),
        ], extra_sources=[data_system_flags])
        selected_idx = 0
        for wlidx in range(len(self.WAVELENGTHS)):
            if data_Ba[wlidx].time.shape[0] > data_Ba[selected_idx].time.shape[0]:
                selected_idx = wlidx
        self.apply_coverage(g, times,f"Ba{self.WAVELENGTHS[selected_idx][1]}_{self.instrument_id}")

        self.apply_instrument_metadata(
            [f"Ba{code}_{self.instrument_id}" for _, code in self.WAVELENGTHS],
            manufacturer="Magee", model="AE8"
        )
        return True


class LegacyThermo49(InstrumentConverter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._average_interval: typing.Optional[float] = None

    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"ozone", "thermo49"}

    @property
    def instrument_type(self) -> typing.Optional[str]:
        return "thermo49"

    @property
    def average_interval(self) -> typing.Optional[float]:
        return self._average_interval

    def run(self) -> bool:
        data_X = self.load_variable(f"X_{self.instrument_id}")
        if data_X.time.shape[0] == 0:
            return False
        self._average_interval = self.calculate_average_interval(data_X.time)
        if not super().run():
            return False

        g, times = self.data_group([data_X])
        self.declare_system_flags(g, times)

        var_X = g.createVariable("ozone_mixing_ratio", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_ozone(var_X)
        netcdf_timeseries.variable_coordinates(g, var_X)
        var_X.variable_id = "X"
        var_X.coverage_content_type = "physicalMeasurement"
        var_X.cell_methods = "time: mean"
        self.apply_data(times, var_X, data_X)

        self.apply_coverage(g, times, f"X_{self.instrument_id}")
        self.apply_instrument_metadata(f"X_{self.instrument_id}", manufacturer="Thermo", generic_model="49")
        return True


class RealtimeMet(BaseMet):
    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"met", "aerosol", "realtimewind", "secondary"}

    @property
    def instrument_type(self) -> typing.Optional[str]:
        return "realtimewind"


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
        data_Fn = self.load_variable(f"Fn_{self.instrument_id}", dtype=np.uint64)
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
        C('psap1w', start='1997-10-06', end='2006-08-12'),
        C('psap3w', start='2006-08-12', end='2014-08-16'),
        C('clap', start='2014-08-16'),
    ],
    "A12": [ C('clap+secondary', start='2011-08-30', end='2014-08-22'), ],
    "A81": [
        C(LegacyAE8, start='1988-03-24', end='2001-12-10'),
        C('mageeae31', start='2010-02-18', end='2016-08-18'),
        C('mageeae33', start='2016-08-18'),
    ],
    "A82": [
        C('mageeae33+secondary', start='2014-08-15', end='2016-12-03'),
    ],
    "G81": [
        C(LegacyThermo49, start='1976-05-06', end='2003-01-02'),
        C('thermo49', start='2003-12-17', end='2020-11-05T18:34:00Z'),
        C('thermo49iq', start='2020-11-05T18:34:00Z'),
    ],
    "G82": [ C('thermo49+secondary', start='2020-11-05', end='2022-01-21'), ],
    "N12": [
        C('dmtccn', start='2022-06-07'),
    ],
    "N21": [
        C('dmtccn', start='2006-08-12', end='2012-03-23'),
        C('dmtccn', start='2012-06-01', end='2012-12-22'),
    ],
    "N61": [
        C(GERichCounter, end='1991-03-31'),
        C('tsi3760cpc', start='1991-03-31', end='2007-09-10'),
        C('tsi3010cpc', start='2007-09-10', end='2024-08-06T03:37:00Z'),
        C('admagic250cpc', start='2024-08-09T18:26:00Z'),
    ],
    "N62": [ C('admagic250cpc+secondary', start='2022-09-26T19:50:00Z', end='2024-08-09T18:00:00Z'), ],
    "N81": [ C('tsi3010cpc+secondary', start='2019-09-25', end='2019-09-29'), ],
    "N91": [ C('tsi3010cpc+secondary', start='2020-10-22', end='2021-09-03'), ],
    "N92": [ C('tsi3010cpc+secondary', start='2021-08-29', end='2021-08-31'), ],
    "Q61": [
        C('tsimfm', start='2020-10-22'),
    ],
    "Q62": [
        C('tsimfm', start='2020-10-22'),
    ],
    "S11": [
        C(MRINeph, start='1976-05-06', end='1997-10-06'),
        C('tsi3563nephelometer', start='1997-10-06'),
    ],
    "S12": [ C('tsi3563nephelometer+secondary', start='2006-08-12', end='2011-08-31'), ],
    "S91": [ C('tsi3563nephelometer+secondary', start='2020-10-22', end='2021-09-01'), ],

    "X1": [
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
            "T_V12": {
                "units": "degC",
                "C_format": "%5.1f",
                "long_name": "humidified neph exhaust temperature",
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
        }), start='1997-10-06', end='2020-10-22'),
        C(CR1000.with_variables({
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
            "Pd_P12": {
                "units": "hPa",
                "C_format": "%5.1f",
                "long_name": "pump vac",
            },
        }, {
            "Pd_P01": {
                "units": "hPa",
                "C_format": "%5.1f",
                "long_name": "stack pitot tube",
            },
            "T_V51": {
                "units": "degC",
                "C_format": "%5.1f",
                "long_name": "splitter temperature",
            },
        }), start='2020-10-22')
    ],
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
        "U_V12": {
            "units": "%",
            "C_format": "%5.1f",
            "long_name": "humdified neph exhaust RH",
        },
    }, {
        "U_V51": {
            "units": "%",
            "C_format": "%5.1f",
            "long_name": "splitter RH",
        },
    }), start='1997-10-06') ],

    "X3": [ C(UMAC.with_variables({}, {
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
    }).with_added_tag("secondary"), start='1997-10-06') ],
    "X4": [ C(LovePID.with_variables({}, {
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
        "U_V22": {
            "units": "%",
            "C_format": "%5.1f",
            "long_name": "filter carousel RH",
        },
    }).with_added_tag("secondary"), start='1997-10-06') ],

    "X5": [ C(UMAC.with_variables({}, {
        "Pd_P31": {
            "units": "hPa",
            "C_format": "%5.1f",
            "long_name": "pressure drop across filter position 1",
        },
        "Pd_P32": {
            "units": "hPa",
            "C_format": "%5.1f",
            "long_name": "pressure drop across filter position 2",
        },
        "Pd_P33": {
            "units": "hPa",
            "C_format": "%5.1f",
            "long_name": "pressure drop across filter position 3",
        },
        "Pd_P34": {
            "units": "hPa",
            "C_format": "%5.1f",
            "long_name": "pressure drop across filter position 4",
        },
        "Pd_P35": {
            "units": "hPa",
            "C_format": "%5.1f",
            "long_name": "pressure drop across filter position 5",
        },
        "Pd_P36": {
            "units": "hPa",
            "C_format": "%5.1f",
            "long_name": "pressure drop across filter position 6",
        },
        "Pd_P37": {
            "units": "hPa",
            "C_format": "%5.1f",
            "long_name": "pressure drop across filter position 7",
        },
        "Pd_P38": {
            "units": "hPa",
            "C_format": "%5.1f",
            "long_name": "pressure drop across filter position 8",
        },
        "T_V31": {
            "units": "degC",
            "C_format": "%5.1f",
            "long_name": "filter sample temperature",
        },
        "T_V32": {
            "units": "degC",
            "C_format": "%5.1f",
            "long_name": "filter carousel temperature",
        },
    }).with_added_tag("secondary"), start='2008-01-15') ],
    "X6": [ C(LovePID.with_variables({}, {
        "Q_Q31": {
            "units": "lpm",
            "C_format": "%6.2f",
            "long_name": "filter sample flow",
        },
        "U_V31": {
            "units": "%",
            "C_format": "%5.1f",
            "long_name": "filter sample RH",
        },
        "U_V32": {
            "units": "%",
            "C_format": "%5.1f",
            "long_name": "filter carousel RH",
        },
    }).with_added_tag("secondary"), start='2008-01-15') ],

    "X91": [ C(LovePID.with_variables({
        "Q_Q91": {
            "units": "lpm",
            "C_format": "%6.2f",
            "long_name": "analyzer flow",
        },
        "U_V91": {
            "units": "%",
            "C_format": "%5.1f",
            "long_name": "impactor box inlet RH",
        },
    }, {}).with_added_tag("secondary"), start='2020-10-22', end='2021-08-31') ],
    "X92": [
        C(UMAC.with_variables({
            "Pd_P91": {
                "units": "hPa",
                "C_format": "%5.1f",
                "long_name": "impactor pressure drop",
            },
            "T_V91": {
                "units": "degC",
                "C_format": "%5.1f",
                "long_name": "impactor box inlet temperature",
            },
            "Pd_P92": {
                "units": "hPa",
                "C_format": "%5.1f",
                "long_name": "pump vacuum",
            },
        }, {
            "Pd_P02": {
                "units": "hPa",
                "C_format": "%5.1f",
                "long_name": "stack pitot tube",
            },
            "Q_Q92": {
                "units": "lpm",
                "C_format": "%6.2f",
                "long_name": "CPC flow",
            },
        }).with_added_tag("secondary"),  start='2020-10-22', end='2021-08-31'),
    ],

    "XM1": [
        C(GMLMet.with_variables({
            "1": "at 3m",
        }, {
            "1": "",
        }), start='1973-01-01', end='1994-04-13'),
        C(GMLMet.with_variables({
            "1": "at 3m",
            "2": "at 15.7m",
        }, {
            "1": "",
        }), start='1994-04-13', end='2007-08-19'),
        C(GMLMet.with_variables({
            "1": "at 3m",
            "2": "at 15.7m",
        }, {
            "1": "at 10m",
        }), start='2007-08-19', end='2017-09-24'),
        # CR1000 is XM3 before 2017-09-24
    ],
    "XM2": [ C(RealtimeMet, start='2012-01-01'), ],

    "F21": [
        C(LegacyFilterCarousel, start='1997-10-10', end='2016-08-18'),
        C("filtercarousel", start='2016-08-18'),
    ],
    "F31": [
        C(LegacyFilterCarousel, start='2008-01-15', end='2010-03-05'),
        C("filtercarousel", start='2022-06-07'),
    ],
})
