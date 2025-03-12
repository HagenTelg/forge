#!/usr/bin/env python3

import typing
import os
import forge.data.structure.variable as netcdf_var
import forge.data.structure.timeseries as netcdf_timeseries
import numpy as np
from math import nan
from forge.const import STATIONS as VALID_STATIONS
from forge.data.structure.stp import standard_temperature, standard_pressure
from forge.cpd3.convert.instrument.lookup import instrument_data
from forge.cpd3.legacy.raw.write import InstrumentTimeConversion as BaseConverter
from forge.cpd3.legacy.instrument.converter import WavelengthConverter, InstrumentConverter, read_archive, Selection
from forge.cpd3.legacy.instrument.thermo49 import Converter as Thermo49
from forge.cpd3.legacy.instrument.azonixumac1050 import Converter as UMAC
from forge.cpd3.legacy.instrument.lovepid import Converter as LovePID

STATION = os.path.basename(__file__).split('.', 1)[0].lower()
assert STATION in VALID_STATIONS


class LegacyTSI3563(WavelengthConverter):
    WAVELENGTHS = [
        (450.0, "B"),
        (550.0, "G"),
        (700.0, "R"),
    ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._average_interval: typing.Optional[float] = None

    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"aerosol", "scattering", "tsi3563nephelometer"}

    @property
    def instrument_type(self) -> typing.Optional[str]:
        return "tsi3563nephelometer"

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

        data_Bbs = self.load_wavelength_variable("Bbs")
        data_P = self.load_variable(f"P_{self.instrument_id}")
        data_T = self.load_variable(f"T_{self.instrument_id}")
        data_Tu = self.load_variable(f"Tu_{self.instrument_id}")
        data_U = self.load_variable(f"U_{self.instrument_id}")

        g, times = self.data_group(data_Bs, fill_gaps=False)
        data_system_flags, system_flags_bits = self.declare_system_flags(g, times)
        standard_temperature(g)
        standard_pressure(g)

        var_Bs = g.createVariable("scattering_coefficient", "f8", ("time", "wavelength"), fill_value=nan)
        netcdf_var.variable_total_scattering(var_Bs)
        netcdf_timeseries.variable_coordinates(g, var_Bs)
        var_Bs.variable_id = "Bs"
        var_Bs.coverage_content_type = "physicalMeasurement"
        var_Bs.cell_methods = "time: mean"
        var_Bs.ancillary_variables = "standard_temperature standard_pressure"
        self.apply_wavelength_data(times, var_Bs, data_Bs)

        if any([v.time.shape != 0 for v in data_Bbs]):
            var_Bbs = g.createVariable("backscattering_coefficient", "f8", ("time", "wavelength"), fill_value=nan)
            netcdf_var.variable_back_scattering(var_Bbs)
            netcdf_timeseries.variable_coordinates(g, var_Bbs)
            var_Bbs.variable_id = "Bbs"
            var_Bbs.coverage_content_type = "physicalMeasurement"
            var_Bbs.cell_methods = "time: mean"
            var_Bbs.ancillary_variables = "standard_temperature standard_pressure"
            self.apply_wavelength_data(times, var_Bbs, data_Bbs)
        else:
            var_Bbs = None

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

        var_Tu = g.createVariable("inlet_temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_temperature(var_Tu)
        netcdf_timeseries.variable_coordinates(g, var_Tu)
        var_Tu.variable_id = "Tu"
        var_Tu.coverage_content_type = "physicalMeasurement"
        var_Tu.cell_methods = "time: mean"
        var_Tu.long_name = "inlet temperature"
        self.apply_data(times, var_Tu, data_Tu)

        var_U = g.createVariable("sample_humidity", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_air_rh(var_U)
        netcdf_timeseries.variable_coordinates(g, var_U)
        var_U.variable_id = "U"
        var_U.coverage_content_type = "physicalMeasurement"
        var_U.cell_methods = "time: mean"
        var_U.long_name = "measurement cell relative humidity"
        self.apply_data(times, var_U, data_U)

        self.apply_cut_size(g, times, [
            (var_P, data_P),
            (var_T, data_T),
            (var_Tu, data_Tu),
            (var_U, data_U),
        ], [
            (var_Bs, data_Bs),
            (var_Bbs, data_Bbs),
        ], extra_sources=[data_system_flags])
        selected_idx = 0
        for wlidx in range(len(self.WAVELENGTHS)):
            if data_Bs[wlidx].time.shape[0] > data_Bs[selected_idx].time.shape[0]:
                selected_idx = wlidx
        self.apply_coverage(g, times, f"Bs{self.WAVELENGTHS[selected_idx][1]}_{self.instrument_id}")

        self.apply_instrument_metadata(
            [f"Bs{code}_{self.instrument_id}" for _, code in self.WAVELENGTHS],
            manufacturer="TSI", model="3563"
        )
        return True


class LegacyRRNeph(WavelengthConverter):
    WAVELENGTHS = [
        (535.0, "G"),
    ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._average_interval: typing.Optional[float] = None

    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"aerosol", "scattering", "rrm903", "secondary"}

    @property
    def instrument_type(self) -> typing.Optional[str]:
        return "rrm903"

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
        standard_temperature(g)
        standard_pressure(g)

        var_Bs = g.createVariable("scattering_coefficient", "f8", ("time", "wavelength"), fill_value=nan)
        netcdf_var.variable_total_scattering(var_Bs)
        netcdf_timeseries.variable_coordinates(g, var_Bs)
        var_Bs.variable_id = "Bs"
        var_Bs.coverage_content_type = "physicalMeasurement"
        var_Bs.cell_methods = "time: mean"
        var_Bs.ancillary_variables = "standard_temperature standard_pressure"
        self.apply_wavelength_data(times, var_Bs, data_Bs)

        self.apply_cut_size(g, times, [], [
            (var_Bs, data_Bs),
        ], extra_sources=[data_system_flags])
        selected_idx = 0
        for wlidx in range(len(self.WAVELENGTHS)):
            if data_Bs[wlidx].time.shape[0] > data_Bs[selected_idx].time.shape[0]:
                selected_idx = wlidx
        self.apply_coverage(g, times, f"Bs{self.WAVELENGTHS[selected_idx][1]}_{self.instrument_id}")

        self.apply_instrument_metadata(
            [f"Bs{code}_{self.instrument_id}" for _, code in self.WAVELENGTHS],
            manufacturer="Radiance Research", model="M903"
        )
        return True


class LegacyPSAP3W(WavelengthConverter):
    WAVELENGTHS = [
        (467.0, "B"),
        (530.0, "G"),
        (660.0, "R"),
    ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._average_interval: typing.Optional[float] = None

    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"aerosol", "absorption", "psap3w"}

    @property
    def instrument_type(self) -> typing.Optional[str]:
        return "psap3w"

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
            manufacturer="Radiance Research", model="PSAP-3W"
        )
        return True


class LegacyBMITAP(WavelengthConverter):
    WAVELENGTHS = [
        (467.0, "B"),
        (528.0, "G"),
        (652.0, "R"),
    ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._average_interval: typing.Optional[float] = None

    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"aerosol", "absorption", "bmitap"}

    @property
    def instrument_type(self) -> typing.Optional[str]:
        return "bmitap"

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

        data_Ir = self.load_wavelength_variable("Ir")
        data_Ip = self.load_wavelength_variable("Ip")
        data_If = self.load_wavelength_variable("If")
        data_Q = self.load_variable(f"Q_{self.instrument_id}")
        data_T1 = self.load_variable(f"T1_{self.instrument_id}")

        def convert_fn(v: typing.Any) -> typing.Optional[int]:
            try:
                v = int(v)
                if v < 0 or v > 8:
                    return None
                return v
            except (TypeError, ValueError):
                return None

        data_Fn = self.load_state(f"Fn_{self.instrument_id}", dtype=np.uint64, convert=convert_fn)

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

        if any([v.time.shape != 0 for v in data_Ir]):
            var_Ir = g.createVariable("transmittance", "f8", ("time", "wavelength"), fill_value=nan)
            netcdf_var.variable_transmittance(var_Ir)
            netcdf_timeseries.variable_coordinates(g, var_Ir)
            var_Ir.variable_id = "Ir"
            var_Ir.coverage_content_type = "physicalMeasurement"
            var_Ir.cell_methods = "time: last"
            var_Ir.long_name = "transmittance fraction of light through the filter relative to the amount before sampling on spot one"
            self.apply_wavelength_data(times, var_Ir, data_Ir)
        else:
            var_Ir = None

        if any([v.time.shape != 0 for v in data_Ip]):
            var_Ip = g.createVariable("sample_intensity", "f8", ("time", "wavelength"), fill_value=nan)
            netcdf_timeseries.variable_coordinates(g, var_Ip)
            var_Ip.variable_id = "Ip"
            var_Ip.coverage_content_type = "physicalMeasurement"
            var_Ip.cell_methods = "time: mean"
            var_Ip.long_name = "active spot sample intensity"
            var_Ip.C_format = "%10.2f"
            self.apply_wavelength_data(times, var_Ip, data_Ip)
        else:
            var_Ip = None

        if any([v.time.shape != 0 for v in data_If]):
            var_If = g.createVariable("reference_intensity", "f8", ("time", "wavelength"), fill_value=nan)
            netcdf_timeseries.variable_coordinates(g, var_If)
            var_If.variable_id = "If"
            var_If.coverage_content_type = "physicalMeasurement"
            var_If.cell_methods = "time: mean"
            var_If.long_name = "active spot sample intensity"
            var_If.C_format = "%10.2f"
            self.apply_wavelength_data(times, var_If, data_If)
        else:
            var_If = None

        if data_Q.time.shape != 0:
            var_Q = g.createVariable("sample_flow", "f8", ("time",), fill_value=nan)
            netcdf_var.variable_sample_flow(var_Q)
            netcdf_timeseries.variable_coordinates(g, var_Q)
            var_Q.variable_id = "Q"
            var_Q.coverage_content_Qype = "physicalMeasurement"
            var_Q.cell_methods = "time: mean"
            var_Q.C_format = "%6.3f"
            self.apply_data(times, var_Q, data_Q)
        else:
            var_Q = None

        if data_T1.time.shape != 0:
            var_T1 = g.createVariable("sample_temperature", "f8", ("time",), fill_value=nan)
            netcdf_var.variable_air_temperature(var_T1)
            netcdf_timeseries.variable_coordinates(g, var_T1)
            var_T1.variable_id = "T1"
            var_T1.coverage_content_T1ype = "physicalMeasurement"
            var_T1.cell_methods = "time: mean"
            self.apply_data(times, var_T1, data_T1)
        else:
            var_T1 = None

        self.apply_cut_size(g, times, [
            (var_Q, data_Q),
            (var_T1, data_T1),
        ], [
            (var_Ba, data_Ba),
            (var_Ir, data_Ir),
            (var_Ip, data_Ip),
            (var_If, data_If),
        ], extra_sources=[data_system_flags])
        selected_idx = 0
        for wlidx in range(len(self.WAVELENGTHS)):
            if data_Ba[wlidx].time.shape[0] > data_Ba[selected_idx].time.shape[0]:
                selected_idx = wlidx
        self.apply_coverage(g, times,f"Ba{self.WAVELENGTHS[selected_idx][1]}_{self.instrument_id}")

        if data_Fn.time.shape[0] != 0:
            g, times = self.state_group([data_Fn])

            var_Fn = g.createVariable("spot_number", "u8", ("time",), fill_value=False)
            netcdf_timeseries.variable_coordinates(g, var_Fn)
            var_Fn.variable_id = "Fn"
            var_Fn.coverage_content_type = "auxiliaryInformation"
            var_Fn.cell_methods = "time: point"
            var_Fn.long_name = "active spot number"
            self.apply_state(times, var_Fn, data_Fn)

        self.apply_instrument_metadata(
            [f"Ba{code}_{self.instrument_id}" for _, code in self.WAVELENGTHS],
            manufacturer="BMI", model="TAP"
        )
        return True


class LegacyGrimm(InstrumentConverter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._average_interval: typing.Optional[float] = None

    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"aerosol", "size", "opc", "grimm110xopc"}

    @property
    def instrument_type(self) -> typing.Optional[str]:
        return "grimm110xopc"

    @property
    def average_interval(self) -> typing.Optional[float]:
        return self._average_interval

    def run(self) -> bool:
        data_X: typing.Dict[float, InstrumentConverter.Data] = dict()
        for diameter, flavor in (
                (1.0, "pm1"),
                (2.5, "pm25"),
                (10.0, "pm10"),
        ):
            data = self.Data(*self.convert_loaded(read_archive([Selection(
                start=self.file_start,
                end=self.file_end,
                stations=[self.station],
                archives=[self.archive],
                variables=[f"X_{self.instrument_id}"],
                include_meta_archive=False,
                include_default_station=False,
                lacks_flavors=["cover", "stats"],
                has_flavors=[flavor],
            )])))
            if data.time.shape[0] == 0:
                continue
            data_X[diameter] = data

        if not data_X:
            return False
        self._average_interval = self.calculate_average_interval(np.concatenate([v.time for v in data_X.values()]))
        if not super().run():
            return False

        g, times = self.data_group(list(data_X.values()), fill_gaps=False)
        self.declare_system_flags(g, times)

        mass_diameters = sorted(data_X.keys())
        g.createDimension("mass_diameter", len(mass_diameters))
        var_mass_diameter = g.createVariable("mass_diameter", "f8", ("mass_diameter",), fill_value=nan)
        var_mass_diameter.coverage_content_type = "coordinate"
        var_mass_diameter.long_name = "particle mass upper particle diameter threshold"
        var_mass_diameter.units = "um"
        var_mass_diameter.C_format = "%4.1f"
        var_mass_diameter[:] = mass_diameters

        var_X = g.createVariable("mass_concentration", "f8", ("time", "mass_diameter"), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_X)
        var_X.variable_id = "X"
        var_X.coverage_content_type = "physicalMeasurement"
        var_X.cell_methods = "time: mean"
        var_X.long_name = "calculated mass concentration of particles"
        var_X.units = "ug m-3"
        var_X.C_format = "%6.1f"
        for idx in range(len(mass_diameters)):
            data = data_X[mass_diameters[idx]]
            self.apply_data(times, var_X, data.time, data.value, (idx,))

        self.apply_coverage(g, times, f"X_{self.instrument_id}")

        self.apply_instrument_metadata(f"X_{self.instrument_id}", manufacturer="Grimm", generic_model="1.10x")
        return True


class LegacyAuxCPC(InstrumentConverter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._average_interval: typing.Optional[float] = None

    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"aerosol", "cpc", "secondary"}

    @property
    def average_interval(self) -> typing.Optional[float]:
        return self._average_interval

    def run(self) -> bool:
        data_N = self.load_variable(f"N_{self.instrument_id}")
        if not data_N:
            return False
        self._average_interval = self.calculate_average_interval(data_N.time)
        if not super().run():
            return False

        g, times = self.data_group([data_N], fill_gaps=False)

        var_N = g.createVariable("number_concentration", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_number_concentration(var_N)
        netcdf_timeseries.variable_coordinates(g, var_N)
        var_N.variable_id = "N"
        var_N.coverage_content_type = "physicalMeasurement"
        var_N.cell_methods = "time: mean"
        self.apply_data(times, var_N, data_N)

        self.apply_coverage(g, times, f"N_{self.instrument_id}")

        return True

    def analyze_flags_mapping_bug(
            self,
            variable: str = None,
            flags_map: typing.Dict[str, typing.Union[str, typing.Tuple[str, int]]] = None,
            bit_shift: int = 16,
    ) -> None:
        return None


class Met(InstrumentConverter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._average_interval: typing.Optional[float] = None

    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"aerosol", "met"}

    @property
    def average_interval(self) -> typing.Optional[float]:
        return self._average_interval

    def run(self) -> bool:
        data_WS = self.load_variable(f"WS1?_{self.instrument_id}")
        if data_WS.time.shape[0] == 0:
            return False
        self._average_interval = self.calculate_average_interval(data_WS.time)
        if not super().run():
            return False

        data_WSx = self.load_variable(f"WSx1?_{self.instrument_id}")
        data_WD = self.load_variable(f"WD1?_{self.instrument_id}")
        data_T1 = self.load_variable(f"T1?_{self.instrument_id}")
        data_T2 = self.load_variable(f"T2_{self.instrument_id}")
        data_U1 = self.load_variable(f"U1?_{self.instrument_id}")
        data_U2 = self.load_variable(f"U2_{self.instrument_id}")
        data_P = self.load_variable(f"P1?_{self.instrument_id}")
        data_T_V01 = self.load_variable("T_V01")
        data_T_V02 = self.load_variable("T_V02")
        data_T_V03 = self.load_variable("T_V03")
        data_Tx1 = self.load_variable(f"Tx1_{self.instrument_id}")
        data_Tx2 = self.load_variable(f"Tx2_{self.instrument_id}")
        data_Q_Q81 = self.load_variable(f"Q_Q81")
        data_X_G82 = self.load_variable(f"X_G82")
        data_X_G83 = self.load_variable(f"X_G83")
        data_X1_G72 = self.load_variable(f"X1_G72")
        data_X2_G72 = self.load_variable(f"X2_G72")

        g, times = self.data_group([data_WS, data_WD, data_T1, data_P, data_U1])

        if data_WS.time.shape[0] > 0:
            var_WS = g.createVariable("wind_speed", "f8", ("time",), fill_value=nan)
            netcdf_var.variable_wind_speed(var_WS)
            netcdf_timeseries.variable_coordinates(g, var_WS)
            var_WS.variable_id = "WS1"
            var_WS.coverage_content_type = "physicalMeasurement"
            var_WS.cell_methods = "time: mean wind_direction: vector_direction"
            self.apply_data(times, var_WS, data_WS)

        if data_WD.time.shape[0] > 0:
            var_WD = g.createVariable("wind_direction", "f8", ("time",), fill_value=nan)
            netcdf_var.variable_wind_direction(var_WD)
            netcdf_timeseries.variable_coordinates(g, var_WD)
            var_WD.variable_id = "WD1"
            var_WD.coverage_content_type = "physicalMeasurement"
            var_WD.cell_methods = "time: mean wind_speed: vector_magnitude"
            self.apply_data(times, var_WD, data_WD)

        if data_WSx.time.shape[0] > 0:
            var_WSx = g.createVariable("wind_gust_speed", "f8", ("time",), fill_value=nan)
            netcdf_timeseries.variable_coordinates(g, var_WSx)
            var_WSx.variable_id = "ZWS1Gust"
            var_WSx.coverage_content_type = "physicalMeasurement"
            var_WSx.long_name = "averaged wind gust speed"
            var_WSx.standard_name = "wind_speed_of_gust"
            var_WSx.units = "m s-1"
            var_WSx.C_format = "%4.1f"
            self.apply_data(times, var_WSx, data_WSx)

        if data_P.time.shape[0] > 0:
            var_P = g.createVariable("ambient_pressure", "f8", ("time",), fill_value=nan)
            netcdf_var.variable_air_pressure(var_P)
            netcdf_timeseries.variable_coordinates(g, var_P)
            var_P.variable_id = "P"
            var_P.coverage_content_Pype = "physicalMeasurement"
            var_P.cell_methods = "time: mean"
            var_P.long_name = "ambient pressure"
            self.apply_data(times, var_P, data_P)

        if data_T1.time.shape[0] > 0:
            var_T1 = g.createVariable("ambient_temperature", "f8", ("time",), fill_value=nan)
            netcdf_var.variable_air_temperature(var_T1)
            netcdf_timeseries.variable_coordinates(g, var_T1)
            var_T1.variable_id = "T1"
            var_T1.coverage_content_type = "physicalMeasurement"
            var_T1.cell_methods = "time: mean"
            var_T1.long_name = "ambient air temperature"
            self.apply_data(times, var_T1, data_T1)

        if data_U1.time.shape[0] > 0:
            var_U1 = g.createVariable("ambient_humidity", "f8", ("time",), fill_value=nan)
            netcdf_var.variable_air_rh(var_U1)
            netcdf_timeseries.variable_coordinates(g, var_U1)
            var_U1.variable_id = "U1"
            var_U1.coverage_content_type = "physicalMeasurement"
            var_U1.cell_methods = "time: mean"
            var_U1.long_name = "ambient air humidity"
            self.apply_data(times, var_U1, data_U1)
            
        if data_T2.time.shape[0] > 0:
            var_T2 = g.createVariable("sheltered_temperature", "f8", ("time",), fill_value=nan)
            netcdf_var.variable_temperature(var_T2)
            netcdf_timeseries.variable_coordinates(g, var_T2)
            var_T2.variable_id = "T2"
            var_T2.coverage_content_type = "physicalMeasurement"
            var_T2.cell_methods = "time: mean"
            var_T2.long_name = "sheltered air temperature"
            self.apply_data(times, var_T2, data_T2)
            
        if data_U2.time.shape[0] > 0:
            var_U2 = g.createVariable("sheltered_humidity", "f8", ("time",), fill_value=nan)
            netcdf_var.variable_rh(var_U2)
            netcdf_timeseries.variable_coordinates(g, var_U2)
            var_U2.variable_id = "U2"
            var_U2.coverage_content_type = "physicalMeasurement"
            var_U2.cell_methods = "time: mean"
            var_U2.long_name = "sheltered air humidity"
            self.apply_data(times, var_U2, data_U2)
            
        if data_T_V01.time.shape[0] > 0:
            var_T_V01 = g.createVariable("room_air_temperature", "f8", ("time",), fill_value=nan)
            netcdf_var.variable_temperature(var_T_V01)
            netcdf_timeseries.variable_coordinates(g, var_T_V01)
            var_T_V01.variable_id = "T_V01"
            var_T_V01.coverage_content_type = "physicalMeasurement"
            var_T_V01.cell_methods = "time: mean"
            var_T_V01.long_name = "room air temperature"
            self.apply_data(times, var_T_V01, data_T_V01)
            
        if data_T_V02.time.shape[0] > 0:
            var_T_V02 = g.createVariable("panel_temperature", "f8", ("time",), fill_value=nan)
            netcdf_var.variable_temperature(var_T_V02)
            netcdf_timeseries.variable_coordinates(g, var_T_V02)
            var_T_V02.variable_id = "T_V02"
            var_T_V02.coverage_content_type = "physicalMeasurement"
            var_T_V02.cell_methods = "time: mean"
            var_T_V02.long_name = "CR1000 panel temperature"
            self.apply_data(times, var_T_V02, data_T_V02)
            
        if data_T_V03.time.shape[0] > 0:
            var_T_V03 = g.createVariable("room_2_temperature", "f8", ("time",), fill_value=nan)
            netcdf_var.variable_temperature(var_T_V03)
            netcdf_timeseries.variable_coordinates(g, var_T_V03)
            var_T_V03.variable_id = "T_V03"
            var_T_V03.coverage_content_type = "physicalMeasurement"
            var_T_V03.cell_methods = "time: mean"
            var_T_V03.long_name = "thermocouple in room 2"
            self.apply_data(times, var_T_V03, data_T_V03)
            
        if data_Tx1.time.shape[0] > 0:
            var_Tx1 = g.createVariable("thermodenuder_section_1_temperature", "f8", ("time",), fill_value=nan)
            netcdf_var.variable_temperature(var_Tx1)
            netcdf_timeseries.variable_coordinates(g, var_Tx1)
            var_Tx1.variable_id = "Tx1"
            var_Tx1.coverage_content_type = "physicalMeasurement"
            var_Tx1.cell_methods = "time: mean"
            var_Tx1.long_name = "thermodenuder section 1"
            self.apply_data(times, var_Tx1, data_Tx1)
            
        if data_Tx2.time.shape[0] > 0:
            var_Tx2 = g.createVariable("thermodenuder_section_2_temperature", "f8", ("time",), fill_value=nan)
            netcdf_var.variable_temperature(var_Tx2)
            netcdf_timeseries.variable_coordinates(g, var_Tx2)
            var_Tx2.variable_id = "Tx2"
            var_Tx2.coverage_content_type = "physicalMeasurement"
            var_Tx2.cell_methods = "time: mean"
            var_Tx2.long_name = "thermodenuder section 2"
            self.apply_data(times, var_Tx2, data_Tx2)
            
        if data_Q_Q81.time.shape[0] > 0:
            var_Q_Q81 = g.createVariable("gas_inlet_flow", "f8", ("time",), fill_value=nan)
            netcdf_var.variable_flow(var_Q_Q81)
            netcdf_timeseries.variable_coordinates(g, var_Q_Q81)
            var_Q_Q81.variable_id = "Q_Q81"
            var_Q_Q81.coverage_content_type = "physicalMeasurement"
            var_Q_Q81.cell_methods = "time: mean"
            var_Q_Q81.long_name = "gas inlet flow"
            self.apply_data(times, var_Q_Q81, data_Q_Q81)
            
        if data_X_G82.time.shape[0] > 0:
            var_X_G82 = g.createVariable("ozone_mixing_ratio_ecotech", "f8", ("time",), fill_value=nan)
            netcdf_var.variable_ozone(var_X_G82)
            netcdf_timeseries.variable_coordinates(g, var_X_G82)
            var_X_G82.variable_id = "X_G82"
            var_X_G82.coverage_content_type = "physicalMeasurement"
            var_X_G82.cell_methods = "time: mean"
            var_X_G82.long_name = "fraction concentration of ozone measured by Ecotech instrument"
            self.apply_data(times, var_X_G82, data_X_G82)
            
        if data_X_G83.time.shape[0] > 0:
            var_X_G83 = g.createVariable("ozone_mixing_ratio_2btech", "f8", ("time",), fill_value=nan)
            netcdf_var.variable_ozone(var_X_G83)
            netcdf_timeseries.variable_coordinates(g, var_X_G83)
            var_X_G83.variable_id = "X_G83"
            var_X_G83.coverage_content_type = "physicalMeasurement"
            var_X_G83.cell_methods = "time: mean"
            var_X_G83.long_name = "fraction concentration of ozone measured by 2B Tech instrument"
            self.apply_data(times, var_X_G83, data_X_G83)
            
        if data_X1_G72.time.shape[0] > 0:
            var_X1_G72 = g.createVariable("nitrogen_monoxide_mixing_ratio", "f8", ("time",), fill_value=nan)
            netcdf_var.variable_no(var_X1_G72)
            netcdf_timeseries.variable_coordinates(g, var_X1_G72)
            var_X1_G72.variable_id = "X1_G72"
            var_X1_G72.coverage_content_type = "physicalMeasurement"
            var_X1_G72.cell_methods = "time: mean"
            self.apply_data(times, var_X1_G72, data_X1_G72)
            
        if data_X2_G72.time.shape[0] > 0:
            var_X2_G72 = g.createVariable("nitrogen_oxide_mixing_ratio", "f8", ("time",), fill_value=nan)
            netcdf_timeseries.variable_coordinates(g, var_X2_G72)
            var_X2_G72.variable_id = "X2_G72"
            var_X2_G72.coverage_content_type = "physicalMeasurement"
            var_X2_G72.cell_methods = "time: mean"
            var_X2_G72.long_name = "fractional concentration of NOy"
            var_X2_G72.units = "1e-9"
            var_X2_G72.C_format = "%9.2f"
            self.apply_data(times, var_X2_G72, data_X2_G72)

        return True

    def analyze_flags_mapping_bug(
            self,
            variable: str = None,
            flags_map: typing.Dict[str, typing.Union[str, typing.Tuple[str, int]]] = None,
            bit_shift: int = 16,
    ) -> None:
        return None


class CCGG(InstrumentConverter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._average_interval: typing.Optional[float] = None

    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"ccgg"}

    @property
    def average_interval(self) -> typing.Optional[float]:
        return self._average_interval

    def run(self) -> bool:
        data_X1 = self.load_variable(f"X1_{self.instrument_id}")
        data_X2 = self.load_variable(f"X2_{self.instrument_id}")
        if data_X1.time.shape[0] == 0 and data_X2.time.shape[0] == 0:
            return False
        self._average_interval = self.calculate_average_interval(np.concatenate((data_X1.time, data_X2.time)))
        if not super().run():
            return False

        g, times = self.data_group([data_X1, data_X2])

        var_X1 = g.createVariable("carbon_monoxide_mixing_ratio", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_co(var_X1)
        netcdf_timeseries.variable_coordinates(g, var_X1)
        var_X1.variable_id = "X1"
        var_X1.coverage_content_type = "physicalMeasurement"
        var_X1.cell_methods = "time: mean"
        self.apply_data(times, var_X1, data_X1)

        var_X2 = g.createVariable("carbon_dioxide_mixing_ratio", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_co2(var_X2)
        netcdf_timeseries.variable_coordinates(g, var_X2)
        var_X2.variable_id = "X2"
        var_X2.coverage_content_type = "physicalMeasurement"
        var_X2.cell_methods = "time: mean"
        data_X2.value *= 1E-3  # CPD3 stored in ppb
        self.apply_data(times, var_X2, data_X2)

        return True

    def analyze_flags_mapping_bug(
            self,
            variable: str = None,
            flags_map: typing.Dict[str, typing.Union[str, typing.Tuple[str, int]]] = None,
            bit_shift: int = 16,
    ) -> None:
        return None


class C(BaseConverter):
    @classmethod
    def thermodenuder(cls, converter: typing.Union[str, typing.Type[InstrumentConverter]],
                      start: typing.Optional[typing.Union[float, str]] = None,
                      end: typing.Optional[typing.Union[float, str]] = None):
        seg = cls(converter, start, end)

        class WithTDFlags(seg.converter):
            def declare_system_flags(
                    self,
                    g,
                    group_times: np.ndarray,
                    variable: str = None,
                    flags_map: typing.Dict[str, typing.Union[str, typing.Tuple[str, int]]] = None,
                    bit_shift: int = 16,
            ) -> typing.Tuple["InstrumentConverter.Data", typing.Dict[str, int]]:
                if flags_map is None:
                    flags_map = self.default_flags_map(bit_shift)
                flags_map["ContaminateThermodenuder"] = "data_contamination_thermodenuder"
                return super().declare_system_flags(g, group_times, variable=variable, flags_map=flags_map)

            def analyze_flags_mapping_bug(
                    self,
                    variable: str = None,
                    flags_map: typing.Dict[str, typing.Union[str, typing.Tuple[str, int]]] = None,
                    bit_shift: int = 16,
            ) -> bool:
                if flags_map is None:
                    flags_map = self.default_flags_map(bit_shift)
                flags_map["ContaminateThermodenuder"] = "data_contamination_thermodenuder"
                return super().analyze_flags_mapping_bug(variable=variable, flags_map=flags_map)


        seg.converter = WithTDFlags
        return seg


C.run(STATION, {
    "A11": [
        C(LegacyPSAP3W, start='2008-01-01', end='2014-01-01'),
        C(LegacyPSAP3W, start='2015-01-01', end='2017-01-01'),
        C(LegacyBMITAP, start='2017-01-01', end='2018-04-10'),
        C.thermodenuder('clap', start='2018-04-10', end='2019-09-25'),
        C('clap', start='2019-09-25', end='2024-05-10'),
        C('bmitap', start='2024-05-10'),
    ],
    "A12": [
        C.thermodenuder('bmitap+secondary', start='2018-04-09', end='2019-09-25'),
        C('bmitap+secondary', start='2019-09-25', end='2022-10-06'),
    ],
    "S11": [
        C(LegacyTSI3563, start='2010-01-01', end='2018-04-10'),
        C.thermodenuder('tsi3563nephelometer', start='2018-04-10', end='2019-09-25'),
        C('tsi3563nephelometer', start='2019-09-25'),
    ],
    "S12": [
        C(LegacyRRNeph, start='2005-01-01', end='2011-01-01'),
        C('ecotechnephelometer+secondary', start='2024-05-09'),
    ],
    "N11": [
        C(LegacyGrimm, start='2017-01-01', end='2018-04-10'),
        C.thermodenuder('grimm110xopc', start='2018-04-11', end='2019-09-25'),  # Invalid initial data
        C('grimm110xopc', start='2019-09-25'),
    ],
    "N71": [
        C.thermodenuder('tsi3010cpc', start='2018-04-10', end='2019-09-25'),
        C('tsi3010cpc', start='2019-09-25'),
    ],
    "N72": [ C('tsi3010cpc+secondary', start='2021-07-20', end='2021-07-22'), ],
    "N81": [ C(LegacyAuxCPC, start='2005-01-01', end='2005-10-01'), ],
    "Q12": [ C('tsimfm', start='2019-07-10', end='2021-07-21'), ],
    "Q71": [
        C.thermodenuder('tsimfm', start='2018-04-10', end='2019-09-25'),
        C('tsimfm', start='2019-09-25'),
    ],
    "Q72": [
        C.thermodenuder('tsimfm', start='2018-04-10', end='2019-09-25'),
        C('tsimfm', start='2019-09-25'),
    ],
    "XM1": [ C(Met, start='2004-02-22'), ],
    "G81": [ C(Thermo49.with_instrument_override(serial_number="32300000000551"), start='2018-04-10'), ],
    "G71": [ C(CCGG, start='2004-02-22'), ],
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
            "long_name": "pump vacuum",
        },
    }), start='2018-04-10', end='2024-05-09') ],
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
    }), start='2018-04-10', end='2024-05-09') ],
})
