#!/usr/bin/env python3

import typing
import os
import forge.data.structure.variable as netcdf_var
import forge.data.structure.timeseries as netcdf_timeseries
import numpy as np
from math import nan
from forge.const import STATIONS as VALID_STATIONS
from forge.cpd3.legacy.instrument.converter import WavelengthConverter
from forge.cpd3.legacy.raw.write import InstrumentTimeConversion as C
from forge.data.structure.stp import standard_temperature, standard_pressure

STATION = os.path.basename(__file__).split('.', 1)[0].lower()
assert STATION in VALID_STATIONS


class LegacyPSAP(WavelengthConverter):
    WAVELENGTHS = [
        (525.0, "G"),
    ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._average_interval: typing.Optional[float] = None

    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"aerosol", "absorption", "psap1w"}

    @property
    def instrument_type(self) -> typing.Optional[str]:
        return "psap1w"

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
        standard_temperature(g)
        standard_pressure(g)
        data_system_flags, system_flags_bits = self.declare_system_flags(g, times)

        var_Ba = g.createVariable("light_absorption", "f8", ("time", "wavelength"), fill_value=nan)
        netcdf_var.variable_absorption(var_Ba, is_stp=True)
        netcdf_timeseries.variable_coordinates(g, var_Ba)
        var_Ba.variable_id = "Ba"
        var_Ba.coverage_content_type = "physicalMeasurement"
        var_Ba.cell_methods = "time: mean"
        var_Ba.ancillary_variables = "standard_temperature standard_pressure"
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
            manufacturer="Radiance Research", model="PSAP-CUSTOM"
        )
        return True


class LegacyAE31(WavelengthConverter):
    WAVELENGTHS = [
        (370.0, "1"),
        (470.0, "2"),
        (520.0, "3"),
        (590.0, "4"),
        (660.0, "5"),
        (880.0, "6"),
        (950.0, "7"),
    ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._average_interval: typing.Optional[float] = None

    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"aerosol", "aethalometer", "absorption", "mageeae31"}

    @property
    def instrument_type(self) -> typing.Optional[str]:
        return "mageeae31"

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
        standard_temperature(g)
        standard_pressure(g)
        data_system_flags, system_flags_bits = self.declare_system_flags(g, times)

        var_Ba = g.createVariable("light_absorption", "f8", ("time", "wavelength"), fill_value=nan)
        netcdf_var.variable_absorption(var_Ba, is_stp=True)
        netcdf_timeseries.variable_coordinates(g, var_Ba)
        var_Ba.variable_id = "Ba"
        var_Ba.coverage_content_type = "physicalMeasurement"
        var_Ba.cell_methods = "time: mean"
        var_Ba.ancillary_variables = "standard_temperature standard_pressure"
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
            manufacturer="Magee", generic_model="AE31",
        )
        return True


C.run(STATION, {
    "A11": [
        C(LegacyPSAP, end='2015-01-01'),
        C(LegacyAE31, start='2015-01-01', end='2016-01-01'),
    ],
    "A21": [ C('thermomaap', start='2018-04-21', end='2018-04-21'), ],
    "A31": [ C('thermomaap', start='2018-04-21'), ],
    "S11": [ C('tsi3563nephelometer', start='2010-01-01', end='2018-04-21'), ],
    "S12": [ C('ecotechnephelometer+secondary', start='2018-04-06', end='2018-04-22'), ],
    "S13": [ C('ecotechnephelometer+secondary', start='2018-04-21', end='2018-04-22'), ],
    "S41": [
        C('tsi3563nephelometer+secondary', start='2013-09-18', end='2018-04-21'),
        C('tsi3563nephelometer', start='2018-04-21'),
    ],
})
