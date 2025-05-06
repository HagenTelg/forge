#!/usr/bin/env python3

import typing
import os
import forge.data.structure.variable as netcdf_var
import forge.data.structure.timeseries as netcdf_timeseries
import numpy as np
from math import nan
from forge.const import STATIONS as VALID_STATIONS
from forge.data.merge.timealign import incoming_before
from forge.cpd3.legacy.raw.write import InstrumentTimeConversion as C
from forge.cpd3.legacy.instrument.converter import InstrumentConverter, WavelengthConverter
from forge.cpd3.legacy.instrument.azonixumac1050 import Converter as UMAC
from forge.cpd3.legacy.instrument.lovepid import Converter as LovePID
from forge.cpd3.legacy.instrument.gml_met import Converter as GMLMet
from forge.cpd3.legacy.instrument.tsi3563nephelometer import Converter as TSINeph

STATION = os.path.basename(__file__).split('.', 1)[0].lower()
assert STATION in VALID_STATIONS


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


class MRINephSecondary(MRINeph):
    WAVELENGTHS = [
        (467.0, "B"),
        (550.0, "G"),
        (700.0, "R"),
    ]

    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"aerosol", "scattering", "mrinephelometer", "secondary"}


class MsENeph(WavelengthConverter):
    WAVELENGTHS = [
        (467.0, "B"),
        (550.0, "G"),
        (700.0, "R"),
    ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._average_interval: typing.Optional[float] = None

    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"aerosol", "scattering", "msenephelometer"}

    @property
    def instrument_type(self) -> typing.Optional[str]:
        return "msenephelometer"

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

        data_Bsw = self.load_wavelength_state("Bsw")
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

        split_monitor = self.calculate_split_monitor(data_T.time)
        if not split_monitor:
            mon_g = g
            mon_times = times
        elif data_T.time.shape[0] > 0 or data_T.time.shape[0] > 0:
            mon_g, mon_times = self.data_group([data_T], name='status', fill_gaps=False)
        else:
            mon_g, mon_times = None, None
            split_monitor = True

        if mon_g is not None:
            var_P = mon_g.createVariable("sample_pressure", "f8", ("time",), fill_value=nan)
            netcdf_var.variable_air_pressure(var_P)
            netcdf_timeseries.variable_coordinates(mon_g, var_P)
            var_P.variable_id = "P"
            var_P.coverage_content_type = "physicalMeasurement"
            var_P.cell_methods = "time: mean"
            var_P.long_name = "measurement cell pressure"
            self.apply_data(mon_times, var_P, data_P)

            var_T = mon_g.createVariable("sample_temperature", "f8", ("time",), fill_value=nan)
            netcdf_var.variable_air_temperature(var_T)
            netcdf_timeseries.variable_coordinates(mon_g, var_T)
            var_T.variable_id = "T"
            var_T.coverage_content_type = "physicalMeasurement"
            var_T.cell_methods = "time: mean"
            var_T.long_name = "measurement cell temperature"
            self.apply_data(mon_times, var_T, data_T)
        else:
            var_P = None
            var_T = None

        if not split_monitor:
            self.apply_cut_size(g, times, [
                (var_P, data_P),
                (var_T, data_T),
            ], [
                (var_Bs, data_Bs),
            ], extra_sources=[data_system_flags])
        else:
            self.apply_cut_size(g, times, [
            ], [
                (var_Bs, data_Bs),
            ], extra_sources=[data_system_flags])

        g, times = self.state_group(data_Bsw, name="zero")

        var_Bsw = g.createVariable("wall_scattering_coefficient", "f8", ("time", "wavelength"), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_Bsw)
        netcdf_var.variable_wall_total_scattering(var_Bsw)
        var_Bsw.variable_id = "Bsw"
        var_Bsw.coverage_content_type = "physicalMeasurement"
        var_Bsw.cell_methods = "time: point"
        self.apply_wavelength_state(times, var_Bsw, data_Bsw)

        self.apply_instrument_metadata(
            [f"Bs{code}_{self.instrument_id}" for _, code in self.WAVELENGTHS],
            manufacturer="MsE", model="3W-02"
        )
        return True


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
            only_fixed_assignment: bool = False,
    ) -> None:
        return None


class CPD3NephZeroFix(TSINeph):
    """
    From a raw mode edit:

    # Originally CPD3 didn't correctly reset the accumulators for the T/P during zeros, so we copy the last
    reported T/P into the zero T/P instead (low variablity means this is good enough).

    This has no effect on the actual background numbers (the reported Rayleigh scattering is already STP adjusted),
    we just need them for recalculating the Rayleigh scattering for EBAS submissions.
    """
    def run(self) -> bool:
        if not super().run():
            return False

        data = self.root.groups.get('data')
        zero = self.root.groups.get('zero')
        if data is None or zero is None:
            return True

        sample_temperature = data.variables.get('sample_temperature')
        sample_pressure = data.variables.get('sample_pressure')
        if sample_temperature is None or sample_pressure is None:
            return True

        zero_temperature = zero.variables.get('zero_temperature')
        zero_pressure = zero.variables.get('zero_pressure')
        if zero_temperature is None or zero_pressure is None:
            return True

        zero_times = data.variables['time']
        data_times = data.variables['time']
        if zero_times.shape[0] == 0 or data_times.shape[0] == 0:
            return True

        set_indices = incoming_before(zero_times[:].data, data_times[:].data)
        zero_temperature[:] = sample_temperature[:].data[set_indices]
        zero_pressure[:] = sample_pressure[:].data[set_indices]

        return True


C.run(STATION, {
    "A11": [
        C('psap1w', start='2000-04-29', end='2006-09-05'),
        C('psap3w', start='2006-09-05', end='2013-07-18'),
        C('clap', start='2013-07-18'),
    ],
    "A12": [ C('clap+secondary', start='2011-12-12', end='2013-07-19'), ],
    "S11": [
        C(MRINeph, start='1974-01-01', end='1994-04-21'),
        C(MsENeph, start='1994-04-21', end='2000-04-07'),
        C('tsi3563nephelometer', start='2000-04-07', end='2015-05-07T19:45:00Z'),
        C(CPD3NephZeroFix, start='2015-05-07T19:45:00Z', end='2015-09-02T20:00:00Z'),
        C('tsi3563nephelometer', start='2015-09-02T20:00:00Z'),
    ],
    "S12": [ C(MRINephSecondary, start='1994-04-21', end='1998-08-03'), ],
    "S81": [
        C(MsENeph.with_added_tag("secondary"), start='2000-04-02', end='2001-04-01'),
        C('purpleair', start='2019-03-06', end='2020-06-05'),
    ],
    "N61": [
        C(GERichCounter, start='1974-01-01', end='1988-06-01'),
        C('tsi3760cpc', start='1988-06-01'),
    ],
    "N62": [
        C('bmi1710cpc+secondary', start='2012-11-26T15:23:00Z', end='2015-12-09T14:53:00Z'),
        C('bmi1720cpc+secondary', start='2015-12-09T14:53:00Z', end='2020-02-18T23:06:00Z'),
        C('tsi3760cpc+secondary', start='2020-02-18T23:06:00Z', end='2020-02-22T05:46:00Z'),
        C('admagic200cpc+secondary', start='2022-03-14', end='2022-10-14T23:58:00Z'),
        C('admagic250cpc+secondary', start='2022-10-14T23:58:00Z'),
    ],
    "N63": [ C('admagic200cpc+secondary', start='2022-03-14', end='2022-03-18'), ],
    "N81": [ C('tsi3760cpc+secondary', start='2000-04-02', end='2001-04-01'), ],
    "A81": [
        C('mageeae31', start='1990-04-19', end='2017-10-04'),  # Also handles AE15/AE16 data
        C('mageeae33', start='2017-10-04'),
    ],
    "A82": [ C('mageeae33+secondary', start='2014-06-24', end='2017-10-21'), ],
    "G81": [
        C('thermo49', start='1973-09-01', end='2023-05-11T13:11:00Z'),
        C('thermo49iq', start='2023-05-11T13:11:00Z'),
    ],
    "G82": [
        C('thermo49iq+secondary', start='2022-10-28', end='2022-11-30'),
        C('thermo49iq+secondary', start='2023-03-29', end='2023-05-14'),
    ],
    "Q61": [ C('tsimfm', start='2015-05-07'), ],
    "Q62": [ C('tsimfm', start='2015-05-07'), ],
    "X1": [ C(UMAC.with_variables({}, {
        "Q_Q11": {
            "units": "lpm",
            "C_format": "%6.2f",
            "long_name": "analyzer flow",
        },
    }), start='1994-10-01', end='2000-04-07'), C(UMAC.with_variables({}, {
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
        "T_V11": {
            "units": "degC",
            "C_format": "%5.1f",
            "long_name": "impactor box inlet temperature",
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
        "T_V51": {
            "units": "degC",
            "C_format": "%5.1f",
            "long_name": "splitter temperature",
        },
        "T_V01": {
            "units": "degC",
            "C_format": "%5.1f",
            "long_name": "stack temperature",
        },
        "T_V02": {
            "units": "degC",
            "C_format": "%5.1f",
            "long_name": "pump box temperature",
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
    }), start='2000-04-07', end='2011-03-15'), C(UMAC.with_variables({
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
    }, {
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
        "T_V51": {
            "units": "degC",
            "C_format": "%5.1f",
            "long_name": "splitter temperature",
        },
        "T_V01": {
            "units": "degC",
            "C_format": "%5.1f",
            "long_name": "stack temperature",
        },
        "T_V02": {
            "units": "degC",
            "C_format": "%5.1f",
            "long_name": "pump box temperature",
        },
        "T_V03": {
            "units": "degC",
            "C_format": "%5.1f",
            "long_name": "rack temperature",
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
    }), start='2011-03-15', end='2015-05-07'), C(UMAC.with_variables({
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
    }, {
        "Pd_P12": {
            "units": "hPa",
            "C_format": "%5.1f",
            "long_name": "pump vacuum",
        },
        "T_V51": {
            "units": "degC",
            "C_format": "%5.1f",
            "long_name": "splitter temperature",
        },
        "T_V01": {
            "units": "degC",
            "C_format": "%5.1f",
            "long_name": "stack temperature",
        },
        "T_V02": {
            "units": "degC",
            "C_format": "%5.1f",
            "long_name": "pump box temperature",
        },
        "T_V03": {
            "units": "degC",
            "C_format": "%5.1f",
            "long_name": "rack temperature",
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
    }), start='2015-05-07') ],
    "X2": [ C(LovePID.with_variables({}, {
        "Q_Q11": {
            "units": "lpm",
            "C_format": "%6.2f",
            "long_name": "analyzer flow",
        },
        "U_V51": {
            "units": "%",
            "C_format": "%5.1f",
            "long_name": "splitter RH",
        },
    }), start='2000-04-07', end='2011-03-15'), C(LovePID.with_variables({
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
        "U_V01": {
            "units": "%",
            "C_format": "%5.1f",
            "long_name": "stack RH",
        },
    }), start='2011-03-15') ],
    "XM1": [ C(GMLMet.with_variables({
        "1": "at 2m",
    }, {
        "1": "at 10m",
    }), start='1977-01-01', end='1993-10-30'), C(GMLMet.with_variables({
        "1": "at 2m",
        "2": "at 9m",
        "3": "at 37m",
    }, {
        "1": "at 10m",
        "2": "at 8.5m",
        "3": "at 38m",
    }), start='1993-10-30', end='2007-02-15'), C(GMLMet.with_variables({
        "1": "at 2m",
        "2": "at 9m",
        "3": "at 37m",
    }, {
        # WS/WD invalid, get them from the aerosol data
        "2": "at 8.5m",
        "3": "at 38m",
    }), start='2007-02-15', end='2007-03-09'), C(GMLMet.with_variables({
        "1": "at 2m",
        "2": "at 9m",
        "3": "at 37m",
    }, {
        "1": "at 10m",
        "2": "at 8.5m",
        "3": "at 38m",
    }), start='2007-03-09', end='2018-05-17'),
    # CR1000 is XM2 before 2018-06-27
    ],
})

