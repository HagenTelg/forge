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
from forge.cpd3.legacy.instrument.generic_size_distribution import Converter as BaseSizeDistribution
from forge.cpd3.legacy.instrument.gml_met import Converter as GMLMet

STATION = os.path.basename(__file__).split('.', 1)[0].lower()
assert STATION in VALID_STATIONS


class LegacyAE16(WavelengthConverter):
    WAVELENGTHS = [
        (880.0, "1"),
    ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._average_interval: typing.Optional[float] = None

    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"aerosol", "aethalometer", "absorption", "mageeae16"}

    @property
    def instrument_type(self) -> typing.Optional[str]:
        return "mageeae16"

    @property
    def average_interval(self) -> typing.Optional[float]:
        return self._average_interval

    def run(self) -> bool:
        data_Ba = self.load_wavelength_variable("Ba")
        data_X = self.load_wavelength_variable("X")
        if not any([v.time.shape != 0 for v in data_Ba]) and not any([v.time.shape != 0 for v in data_X]):
            return False
        self._average_interval = self.calculate_average_interval(np.concatenate([v.time for v in data_Ba] + [v.time for v in data_Ba]))
        if not super().run():
            return False

        data_Ir = self.load_wavelength_variable("Ir")
        data_If = self.load_wavelength_variable("If")
        data_Ip = self.load_wavelength_variable("Ip")
        data_Q = self.load_variable(f"Q_{self.instrument_id}")

        g, times = self.data_group(data_Ba + data_X, fill_gaps=False)
        data_system_flags, system_flags_bits = self.declare_system_flags(g, times)

        if any([v.time.shape != 0 for v in data_Ba]):
            var_Ba = g.createVariable("light_absorption", "f8", ("time", "wavelength"), fill_value=nan)
            netcdf_var.variable_absorption(var_Ba, is_stp=False)
            netcdf_timeseries.variable_coordinates(g, var_Ba)
            var_Ba.variable_id = "Ba"
            var_Ba.coverage_content_type = "physicalMeasurement"
            var_Ba.cell_methods = "time: mean"
            self.apply_wavelength_data(times, var_Ba, data_Ba)
        else:
            var_Ba = None

        if any([v.time.shape != 0 for v in data_X]):
            var_X = g.createVariable("equivalent_black_carbon", "f8", ("time", "wavelength"), fill_value=nan)
            netcdf_var.variable_ebc(var_X)
            netcdf_timeseries.variable_coordinates(g, var_X)
            var_X.variable_id = "X"
            var_X.coverage_content_type = "physicalMeasurement"
            var_X.cell_methods = "time: mean"
            self.apply_wavelength_data(times, var_X, data_X)
        else:
            var_X = None

        if any([v.time.shape != 0 for v in data_Ir]):
            var_Ir = g.createVariable("transmittance", "f8", ("time", "wavelength"), fill_value=nan)
            netcdf_var.variable_transmittance(var_Ir)
            netcdf_timeseries.variable_coordinates(g, var_Ir)
            var_Ir.variable_id = "Ir"
            var_Ir.coverage_content_type = "physicalMeasurement"
            var_Ir.cell_methods = "time: last"
            self.apply_wavelength_data(times, var_Ir, data_Ir)
        else:
            var_Ir = None

        if any([v.time.shape != 0 for v in data_If]):
            var_If = g.createVariable("reference_intensity", "f8", ("time", "wavelength"), fill_value=nan)
            netcdf_timeseries.variable_coordinates(g, var_If)
            var_If.variable_id = "If"
            var_If.coverage_content_type = "physicalMeasurement"
            var_If.cell_methods = "time: mean"
            var_If.long_name = "sensing beam signal"
            var_If.C_format = "%7.4f"
            self.apply_wavelength_data(times, var_If, data_If)
        else:
            var_If = None

        if any([v.time.shape != 0 for v in data_Ip]):
            var_Ip = g.createVariable("sample_intensity", "f8", ("time", "wavelength"), fill_value=nan)
            netcdf_timeseries.variable_coordinates(g, var_Ip)
            var_Ip.variable_id = "Ip"
            var_Ip.coverage_content_type = "physicalMeasurement"
            var_Ip.cell_methods = "time: mean"
            var_Ip.long_name = "reference beam signal"
            var_Ip.C_format = "%7.4f"
            self.apply_wavelength_data(times, var_Ip, data_Ip)
        else:
            var_Ip = None

        if data_Q.time.shape != 0:
            var_Q = g.createVariable("sample_flow", "f8", ("time",), fill_value=nan)
            netcdf_var.variable_sample_flow(var_Q)
            netcdf_timeseries.variable_coordinates(g, var_Q)
            var_Q.variable_id = "Q"
            var_Q.coverage_content_Qype = "physicalMeasurement"
            var_Q.cell_methods = "time: mean"
            var_Q.C_format = "%6.3f"
            var_Q.ancillary_variables = "standard_temperature standard_pressure"
            self.apply_data(times, var_Q, data_Q)
        else:
            var_Q = None

        self.apply_cut_size(g, times, [
            (var_Q, data_Q),
        ], [
            (var_X, data_X),
            (var_Ba, data_Ba),
            (var_Ir, data_Ir),
            (var_If, data_If),
            (var_Ip, data_Ip),
        ])

        self.apply_instrument_metadata(
            [f"Ba{code}_{self.instrument_id}" for _, code in self.WAVELENGTHS] +
            [f"X{code}_{self.instrument_id}" for _, code in self.WAVELENGTHS],
            manufacturer="Magee", model="AE16"
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
        self.apply_instrument_metadata(f"X_{self.instrument_id}", manufacturer="Thermo", generic_model="49c")
        return True


class DMPS(BaseSizeDistribution):
    @property
    def average_interval(self) -> typing.Optional[float]:
        return None

    def add_other_data(self, times, g) -> None:
        data_P1 = self.load_variable(f"P1_{self.instrument_id}")
        data_P2 = self.load_variable(f"P2_{self.instrument_id}")
        data_Q1 = self.load_variable(f"Q1_{self.instrument_id}")
        data_Q2 = self.load_variable(f"Q2_{self.instrument_id}")
        data_T1 = self.load_variable(f"T1_{self.instrument_id}")
        data_T2 = self.load_variable(f"T2_{self.instrument_id}")
        data_V = self.load_array_variable(f"V_{self.instrument_id}")
        data_ZNb = self.load_array_variable(f"ZNb_{self.instrument_id}")
        data_N_raw = self.load_variable(f"N_N12")

        var_P1 = g.createVariable("sample_pressure", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_air_pressure(var_P1)
        netcdf_timeseries.variable_coordinates(g, var_P1)
        var_P1.variable_id = "P1"
        var_P1.coverage_content_P1ype = "physicalMeasurement"
        var_P1.cell_methods = "time: mean"
        var_P1.long_name = "aerosol pressure"
        self.apply_data(times, var_P1, data_P1)

        var_P2 = g.createVariable("sheath_pressure", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_pressure(var_P2)
        netcdf_timeseries.variable_coordinates(g, var_P2)
        var_P2.variable_id = "P2"
        var_P2.coverage_content_P2ype = "physicalMeasurement"
        var_P2.cell_methods = "time: mean"
        var_P2.long_name = "sheath pressure"
        self.apply_data(times, var_P2, data_P2)

        var_Q1 = g.createVariable("sample_flow", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_sample_flow(var_Q1)
        netcdf_timeseries.variable_coordinates(g, var_Q1)
        var_Q1.variable_id = "Q1"
        var_Q1.coverage_content_Q1ype = "physicalMeasurement"
        var_Q1.cell_methods = "time: mean"
        var_Q1.long_name = "aerosol flow"
        self.apply_data(times, var_Q1, data_Q1)

        var_Q2 = g.createVariable("sheath_flow", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_flow(var_Q2)
        netcdf_timeseries.variable_coordinates(g, var_Q2)
        var_Q2.variable_id = "Q2"
        var_Q2.coverage_content_Q2ype = "physicalMeasurement"
        var_Q2.cell_methods = "time: mean"
        var_Q2.long_name = "sheath flow"
        self.apply_data(times, var_Q2, data_Q2)

        var_T1 = g.createVariable("sample_temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_air_temperature(var_T1)
        netcdf_timeseries.variable_coordinates(g, var_T1)
        var_T1.variable_id = "T1"
        var_T1.coverage_content_T1ype = "physicalMeasurement"
        var_T1.cell_methods = "time: mean"
        var_T1.long_name = "aerosol temperature"
        self.apply_data(times, var_T1, data_T1)

        var_T2 = g.createVariable("sheath_temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_temperature(var_T2)
        netcdf_timeseries.variable_coordinates(g, var_T2)
        var_T2.variable_id = "T2"
        var_T2.coverage_content_T2ype = "physicalMeasurement"
        var_T2.cell_methods = "time: mean"
        var_T2.long_name = "sheath temperature"
        self.apply_data(times, var_T2, data_T2)

        if data_V.time.shape[0] > 0:
            var_V = g.createVariable("dma_voltage", "f8", ("time", "diameter"), fill_value=nan)
            var_V.variable_id = "V"
            var_V.coverage_content_type = "physicalMeasurement"
            var_V.cell_methods = "time: mean"
            var_V.long_name = "DMA voltage"
            var_V.units = "V"
            var_V.C_format = "%5.0f"
            n_diameters = g.dimensions["diameter"].size
            n_add = n_diameters - data_V.value.shape[1]
            if n_add > 0:
                value_V = np.pad(data_V.value, ((0, 0), (0, n_add)), mode='constant', constant_values=nan)
            else:
                value_V = data_V.value
            self.apply_data(times, var_V, data_V.time, value_V)

        if data_ZNb.time.shape[0] > 0:
            var_ZNb = g.createVariable("noninverted_number_distribution", "f8", ("time", "diameter"), fill_value=nan)
            var_ZNb.variable_id = "ZNb"
            var_ZNb.coverage_content_ZNbype = "physicalMeasurement"
            var_ZNb.cell_methods = "time: mean"
            var_ZNb.long_name = "non-inverted binned number concentration (dN)"
            var_ZNb.units = "cm-3"
            var_ZNb.C_format = "%7.1f"
            n_diameters = g.dimensions["diameter"].size
            n_add = n_diameters - data_ZNb.value.shape[1]
            if n_add > 0:
                value_ZNb = np.pad(data_ZNb.value, ((0, 0), (0, n_add)), mode='constant', constant_values=nan)
            else:
                value_ZNb = data_ZNb.value
            self.apply_data(times, var_ZNb, data_ZNb.time, value_ZNb)

        var_N_raw = g.createVariable("raw_number_concentration", "f8", ("time",), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_N_raw)
        var_N_raw.variable_id = "N_N12"
        var_N_raw.coverage_content_N_rawype = "physicalMeasurement"
        var_N_raw.cell_methods = "time: mean"
        var_N_raw.long_name = "DMPS raw CPC concentration"
        var_N_raw.units = "cm-3"
        var_N_raw.C_format = "%7.1f"
        self.apply_data(times, var_N_raw, data_N_raw)


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

    def analyze_flags_mapping_bug(
            self,
            variable: str = None,
            flags_map: typing.Dict[str, typing.Union[str, typing.Tuple[str, int]]] = None,
            bit_shift: int = 16,
    ) -> None:
        return None


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
    "A11": [ C('clap', start='2017-12-08'), ],
    "A81": [
        C(LegacyAE16, start='1987-01-01', end='1997-11-15'),
        C('mageeae31', start='2006-02-07', end='2013-02-02'),
        C('mageeae31+secondary', start='2013-02-02', end='2017-12-07'),
    ],
    "A82": [
        C('mageeae33', start='2013-02-02'),
    ],
    "G81": [
        C(LegacyThermo49, start='1975-01-01', end='2014-12-27'),
        C('thermo49', start='2014-12-27'),
    ],
    "N11": [C(DMPS, start='2023-01-23'),],
    "N31": [
        C(GERichCounter, end='1989-01-01'),
        C('tsi3760cpc', start='1989-01-01', end='2007-11-11'),
        C('tsi3760cpc+secondary', start='2007-11-11', end='2009-02-07'),
    ],
    "N41": [ C('tsi3760cpc', start='2007-11-11'), ],
    "N42": [
        C('tsi3781cpc+secondary', start='2008-01-04', end='2013-01-22'),
        C('bmi1710cpc+secondary', start='2013-01-22', end='2015-11-19'),
        C('bmi1720cpc+secondary', start='2015-11-19', end='2018-08-04'),
        C('admagic250cpc+secondary', start='2023-01-23'),
    ],
    "N43": [ C('tsi3010cpc+secondary', start='2023-04-06', end='2023-06-07'), ],
    "S11": [
        C(MRINeph, start='1979-01-12', end='2002-12-07'),
        C('tsi3563nephelometer', start='2002-12-07'),
    ],
    "S12": [ C('tsi3563nephelometer', start='2023-06-17', end='2023-06-18'), ],
    "XM1": [
        C(GMLMet.with_variables({
        }, {
            "1": "",
        }), start='1975-02-21', end='1976-01-01'),
        C(GMLMet.with_variables({
            "1": "at 2m",
        }, {
            "1": "",
        }), start='1976-01-01', end='1994-01-18'),
        C(GMLMet.with_variables({
            "1": "at 3m",
            "2": "at 13m",
            "3": "at 21.9m",
        }, {
            "1": "",
        }), start='1994-01-18', end='2007-11-14'),
        C(GMLMet.with_variables({
            "1": "at 3m",
            "2": "at 13m",
            "3": "at 21.9m",
        }, {
            "1": "at 10m",
            "2": "at 2m",
            "3": "at 20m",
        }), start='2007-11-14', end='2018-01-01'),
        # CR1000 is XM2 before 2018-01-01
    ],
})
