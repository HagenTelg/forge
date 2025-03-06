#!/usr/bin/env python3

import typing
import os
import numpy as np
import forge.data.structure.variable as netcdf_var
import forge.data.structure.timeseries as netcdf_timeseries
from math import nan
from forge.const import STATIONS as VALID_STATIONS
from forge.cpd3.legacy.raw.write import InstrumentTimeConversion as C
from forge.cpd3.legacy.instrument.converter import InstrumentConverter, WavelengthConverter
from forge.cpd3.legacy.instrument.campbellcr1000gmd import Converter as CR1000
from forge.cpd3.legacy.instrument.lovepid import Converter as LovePID
from forge.data.structure.stp import standard_temperature, standard_pressure

STATION = os.path.basename(__file__).split('.', 1)[0].lower()
assert STATION in VALID_STATIONS


class LegacyMAAP(WavelengthConverter):
    WAVELENGTHS = [
        (670.0, "R"),
    ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._average_interval: typing.Optional[float] = None

    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"aerosol", "absorption", "thermomaap"}

    @property
    def instrument_type(self) -> typing.Optional[str]:
        return "thermomaap"

    @property
    def average_interval(self) -> typing.Optional[float]:
        return self._average_interval

    def run(self) -> bool:
        data_Bac = self.load_wavelength_variable("Ba")
        if not any([v.time.shape[0] != 0 for v in data_Bac]):
            return False
        self._average_interval = self.calculate_average_interval(np.concatenate([v.time for v in data_Bac]))
        if not super().run():
            return False

        data_T1 = self.load_variable(f"T_{self.instrument_id}")

        g, times = self.data_group(data_Bac)
        standard_temperature(g)
        standard_pressure(g)
        self.declare_system_flags(g, times)

        var_Bac = g.createVariable("light_absorption", "f8", ("time", "wavelength"), fill_value=nan)
        netcdf_var.variable_absorption(var_Bac, is_stp=True)
        netcdf_timeseries.variable_coordinates(g, var_Bac)
        var_Bac.variable_id = "Bac"
        var_Bac.coverage_content_type = "physicalMeasurement"
        var_Bac.cell_methods = "time: mean"
        var_Bac.ancillary_variables = "standard_temperature standard_pressure"
        self.apply_wavelength_data(times, var_Bac, data_Bac)

        var_T1 = g.createVariable("sample_temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_air_temperature(var_T1)
        netcdf_timeseries.variable_coordinates(g, var_T1)
        var_T1.variable_id = "T1"
        var_T1.coverage_content_T1ype = "physicalMeasurement"
        var_T1.cell_methods = "time: mean"
        self.apply_data(times, var_T1, data_T1)

        self.apply_cut_size(g, times, [
            (var_T1, data_T1),
        ], [
            (var_Bac, data_Bac),
        ])
        selected_idx = 0
        for wlidx in range(len(self.WAVELENGTHS)):
            if data_Bac[wlidx].time.shape[0] > data_Bac[selected_idx].time.shape[0]:
                selected_idx = wlidx
        self.apply_coverage(g, times, f"Ba{self.WAVELENGTHS[selected_idx][1]}_{self.instrument_id}")

        self.apply_instrument_metadata(
            [f"Bac{code}_{self.instrument_id}" for _, code in self.WAVELENGTHS],
            manufacturer="Thermo", model="MAAP",
        )

        return True


class LegacyTSI3010(InstrumentConverter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._average_interval: typing.Optional[float] = None

    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"aerosol", "cpc", "tsi377xcpc"}

    @property
    def instrument_type(self) -> typing.Optional[str]:
        return "tsi377xcpc"

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

        self.apply_instrument_metadata(f"N_{self.instrument_id}", manufacturer="TSI", model="3772")

        return True


C.run(STATION, {
    "A11": [
        C(LegacyMAAP, end='2014-01-01'),
        C('mageeae33', start='2018-01-31', end='2018-02-01'),
        C('thermomaap', start='2019-02-27', end='2019-03-15'),
    ],
    "A12": [
        C('mageeae33', start='2019-02-27', end='2019-03-07T10:03:00Z'),
        C('thermomaap', start='2019-03-07T10:03:00Z', end='2019-03-15'),
    ],
    "A21": [ C('thermomaap', start='2017-01-23'), ],
    "A81": [ C('mageeae33', start='2017-03-13'), ],
    "N61": [ C(LegacyTSI3010, start='2013-01-01', end='2013-12-18'), ],
    "S11": [
        C('ecotechnephelometer', start='2010-01-01', end='2020-09-07'),
        C('ecotechnephelometer', start='2020-09-27'),
    ],
    "S12": [ C('ecotechnephelometer+secondary', start='2017-01-24', end='2018-07-24'), ],
    "X1": [ C(CR1000.with_variables({}, {
        "T_V11": {
            "units": "degC",
            "C_format": "%5.1f",
            "long_name": "reference nephelometer exhaust temperature",
        },
        "U_V11": {
            "units": "%",
            "C_format": "%5.1f",
            "long_name": "reference nephelometer exhaust RH",
        },
        "T_V12": {
            "units": "degC",
            "C_format": "%5.1f",
            "long_name": "humidifier inlet temperature",
        },
        "U_V12": {
            "units": "%",
            "C_format": "%5.1f",
            "long_name": "humidifier inlet temperature RH",
        },
        "T_V13": {
            "units": "degC",
            "C_format": "%5.1f",
            "long_name": "humidifier outlet temperature",
        },
        "T_V14": {
            "units": "degC",
            "C_format": "%5.1f",
            "long_name": "humidified nephelometer exhaust temperature",
        },
        "U_V14": {
            "units": "%",
            "C_format": "%5.1f",
            "long_name": "humidified nephelometer exhaust RH",
        },
    })) ],
    "X2": [ C(LovePID.with_variables({}, {
        "U_V13": {
            "units": "%",
            "C_format": "%5.1f",
            "long_name": "humidifier outlet RH",
        },
    })) ],
})
