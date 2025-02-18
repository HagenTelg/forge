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
from forge.cpd3.legacy.instrument.generic_met import Converter as BaseMet
from forge.cpd3.legacy.instrument.generic_size_distribution import Converter as BaseSizeDistribution
from forge.cpd3.legacy.instrument.thermo49 import Converter as Thermo49
from forge.cpd3.legacy.instrument.gml_met import Converter as GMLMet

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


C.run(STATION, {
    "G81": [
        C(Thermo49.with_instrument_override(serial_number="1217853289") , end='2016-12-16'),
        C('thermo49', start='2016-12-16', end='2018-01-09'),
        C('tech2b205', start='2018-01-09', end='2023-03-31'),
        C('thermo49iq', start='2023-03-31'),
    ],
    "N71": [
        C(GERichCounter, start='1977-07-25', end='1995-01-13'),
        C('tsi3760cpc', start='1995-01-13', end='2004-01-27'),
        C('tsi3010cpc', start='2004-01-27', end='2018-03-31'),
    ],
    "S11": [
        #C(MRINeph, start='1977-07-25', end='1997-10-06'),
        #C('tsi3563nephelometer', start='1997-10-06', end='2000-12-10'),
        C(MRINeph, start='1977-07-25', end='1991-03-30'),
    ],
    "XM1": [
        C(GMLMet.with_variables({
            "1": "at 2m",
            "2": "at 20m",
        }, {
            "1": "",
        }), start='1976-01-01', end='2018-01-18'),
    ],
})
