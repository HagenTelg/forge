import typing
from os.path import split

import numpy as np
import forge.data.structure.variable as netcdf_var
import forge.data.structure.timeseries as netcdf_timeseries
from math import nan
from .converter import WavelengthConverter
from forge.data.structure.stp import standard_temperature, standard_pressure


class Converter(WavelengthConverter):
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

    @property
    def split_monitor(self) -> typing.Optional[bool]:
        return None

    def _declare_parameters(self, parameters: typing.Dict[str, typing.Any]) -> None:
        if not isinstance(parameters, dict):
            return
        if not parameters:
            return

        g = self.root.createGroup("parameters")
        self.declare_wavelength(g)

        for name, long_name, units, C_format in (
                ("SMZ", "zero mode: 0=manual only, 1-24=autozero with average of last N zeros", None, "%2llu"),
                ("SP", "lamp power", "W", "%2llu"),
                ("STA", "averaging time", "s", "%4llu"),
                ("STB", "blanking time", "s", "%3llu"),
                ("STP", "autozero interval", "s", "%5llu"),
                ("STZ", "zero measurement length", "s", "%4llu"),
                ("B", "blower power (0-255)", None, "%3llu"),
                ("H", "heater enable", None, "%1llu"),
                ("SMB", "backscatter shutter enable", None, "%1llu"),
        ):
            value = parameters.get(name)
            if value is None:
                continue

            var = g.createVariable(name, "u8", (), fill_value=False)
            var.coverage_content_type = "referenceInformation"
            var.long_name = long_name
            if units:
                var.units = units
            var.C_format = C_format
            var[:] = int(value)

        for name, long_name, units, C_format in (
                ("SV", "photomultiplier tube voltage", "V", "%4.0f"),
        ):
            values = np.full((len(self.WAVELENGTHS), ), nan, dtype=np.float64)
            for wlidx in range(len(self.WAVELENGTHS)):
                _, code = self.WAVELENGTHS[wlidx]
                values[wlidx] = float(parameters.get(name + code, nan))
            if not np.any(np.isfinite(values)):
                continue

            var = g.createVariable(name, "f8", ("wavelength",), fill_value=False)
            var.coverage_content_type = "referenceInformation"
            var.long_name = long_name
            if units:
                var.units = units
            var.C_format = C_format
            var[:] = values

        for name, long_name, units, C_format in (
                ("K1", "photomultiplier tube dead time", "ps", "%5.0f"),
                ("K2", "total scattering calibration", "m-1", "%.3e"),
                ("K3", "air Rayleigh scattering", "m-1", "%.3e"),
                ("K4", "backscattering Rayleigh contribution fraction", None, "%5.3f"),
        ):
            values = np.full((len(self.WAVELENGTHS), ), nan, dtype=np.float64)
            for wlidx in range(len(self.WAVELENGTHS)):
                _, code = self.WAVELENGTHS[wlidx]
                basep = parameters.get("SK" + code)
                if not basep:
                    continue
                values[wlidx] = float(basep.get(name, nan))
            if not np.any(np.isfinite(values)):
                continue

            var = g.createVariable(name, "f8", ("wavelength",), fill_value=False)
            var.coverage_content_type = "referenceInformation"
            var.long_name = long_name
            if units:
                var.units = units
            var.C_format = C_format
            if name == "K3":
                standard_temperature(g)
                standard_pressure(g)
                var.ancillary_variables = "standard_temperature standard_pressure"
            var[:] = values

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
        data_Uu = self.load_variable(f"Uu_{self.instrument_id}")
        data_Vl = self.load_variable(f"Vl_{self.instrument_id}")
        data_Al = self.load_variable(f"Al_{self.instrument_id}")

        data_Cs = self.load_wavelength_variable("Cs")
        data_Cbs = self.load_wavelength_variable("Cbs")
        data_Cf = self.load_wavelength_variable("Cf")
        data_Cd = self.load_wavelength_variable("Cd")
        data_Cbd = self.load_wavelength_variable("Cbd")

        mode = self.load_state(f"F2_{self.instrument_id}", dtype=str)

        data_Tw = self.load_state(f"Tw_{self.instrument_id}")
        data_Pw = self.load_state(f"Pw_{self.instrument_id}")
        data_Bsw = self.load_wavelength_state("Bsw")
        data_Bbsw = self.load_wavelength_state("Bbsw")

        data_PCTc = self.load_wavelength_state("PCTc")
        data_PCTbc = self.load_wavelength_state("PCTbc")
        data_Cc = self.load_wavelength_state("Cc")
        data_Cbc = self.load_wavelength_state("Cbc")

        parameters = self.load_state(f"ZPARAMETERS_{self.instrument_id}", dtype=dict)

        system_flags_time = self.load_variable(f"F1?_{self.instrument_id}", convert=bool, dtype=np.bool_).time

        g, times = self.data_group(data_Bs + [system_flags_time], fill_gaps=False)
        data_system_flags, system_flags_bits = self.declare_system_flags(g, times)

        var_Bs = g.createVariable("scattering_coefficient", "f8", ("time", "wavelength"), fill_value=nan)
        netcdf_var.variable_total_scattering(var_Bs)
        netcdf_timeseries.variable_coordinates(g, var_Bs)
        var_Bs.variable_id = "Bs"
        var_Bs.coverage_content_type = "physicalMeasurement"
        var_Bs.cell_methods = "time: mean"
        self.apply_wavelength_data(times, var_Bs, data_Bs)

        if any([v.time.shape != 0 for v in data_Bbs]):
            var_Bbs = g.createVariable("backscattering_coefficient", "f8", ("time", "wavelength"), fill_value=nan)
            netcdf_var.variable_back_scattering(var_Bbs)
            netcdf_timeseries.variable_coordinates(g, var_Bbs)
            var_Bbs.variable_id = "Bbs"
            var_Bbs.coverage_content_type = "physicalMeasurement"
            var_Bbs.cell_methods = "time: mean"
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

        var_Uu = g.createVariable("inlet_humidity", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_rh(var_Uu)
        netcdf_timeseries.variable_coordinates(g, var_Uu)
        var_Uu.variable_id = "Uu"
        var_Uu.coverage_content_type = "referenceInformation"
        var_Uu.cell_methods = "time: mean"
        var_Uu.long_name = "calculated inlet humidity"
        self.apply_data(times, var_Uu, data_Uu)

        if any([v.time.shape != 0 for v in data_Cs]):
            var_Cs = g.createVariable("scattering_counts", "f8", ("time", "wavelength"), fill_value=nan)
            netcdf_timeseries.variable_coordinates(g, var_Cs)
            var_Cs.variable_id = "Cs"
            var_Cs.coverage_content_type = "physicalMeasurement"
            var_Cs.cell_methods = "time: mean"
            var_Cs.long_name = "total scattering photon count rate"
            var_Cs.units = "Hz"
            var_Cs.C_format = "%7.0f"
            self.apply_wavelength_data(times, var_Cs, data_Cs)
        else:
            var_Cs = None

        if any([v.time.shape != 0 for v in data_Cbs]):
            var_Cbs = g.createVariable("backscattering_counts", "f8", ("time", "wavelength"), fill_value=nan)
            netcdf_timeseries.variable_coordinates(g, var_Cbs)
            var_Cbs.variable_id = "Cbs"
            var_Cbs.coverage_content_type = "physicalMeasurement"
            var_Cbs.cell_methods = "time: mean"
            var_Cbs.long_name = "backwards hemispheric scattering photon count rate"
            var_Cbs.units = "Hz"
            var_Cbs.C_format = "%7.0f"
            self.apply_wavelength_data(times, var_Cbs, data_Cbs)
        else:
            var_Cbs = None

        if any([v.time.shape != 0 for v in data_Cd]):
            var_Cd = g.createVariable("scattering_dark_counts", "f8", ("time", "wavelength"), fill_value=nan)
            netcdf_timeseries.variable_coordinates(g, var_Cd)
            var_Cd.variable_id = "Cs"
            var_Cd.coverage_content_type = "physicalMeasurement"
            var_Cd.cell_methods = "time: mean"
            var_Cd.long_name = "total scattering dark count rate"
            var_Cd.units = "Hz"
            var_Cd.C_format = "%7.0f"
            self.apply_wavelength_data(times, var_Cd, data_Cd)
        else:
            var_Cd = None

        if any([v.time.shape != 0 for v in data_Cbd]):
            var_Cbd = g.createVariable("backscattering_dark_counts", "f8", ("time", "wavelength"), fill_value=nan)
            netcdf_timeseries.variable_coordinates(g, var_Cbd)
            var_Cbd.variable_id = "Cbs"
            var_Cbd.coverage_content_type = "physicalMeasurement"
            var_Cbd.cell_methods = "time: mean"
            var_Cbd.long_name = "backwards hemispheric scattering dark count rate"
            var_Cbd.units = "Hz"
            var_Cbd.C_format = "%7.0f"
            self.apply_wavelength_data(times, var_Cbd, data_Cbd)
        else:
            var_Cbd = None

        split_monitor = self.split_monitor
        if split_monitor is None:
            split_monitor = self.calculate_split_monitor(data_Vl.time)
        if not split_monitor:
            mon_g = g
            mon_times = times
        elif data_Vl.time.shape[0] > 0 or data_Al.time.shape[0] > 0 or any([v.time.shape != 0 for v in data_Cs]):
            mon_g, mon_times = self.data_group([data_Al], name='status', fill_gaps=False)
        else:
            mon_g, mon_times = None, None
            split_monitor = True

        if mon_g is not None:
            var_Vl = mon_g.createVariable("lamp_voltage", "f8", ("time",), fill_value=nan)
            netcdf_timeseries.variable_coordinates(mon_g, var_Vl)
            var_Vl.variable_id = "Vl"
            var_Vl.coverage_content_type = "physicalMeasurement"
            var_Vl.cell_methods = "time: mean"
            var_Vl.long_name = "lamp supply voltage"
            var_Vl.units = "V"
            var_Vl.C_format = "%4.1f"
            self.apply_data(mon_times, var_Vl, data_Vl)

            var_Al = mon_g.createVariable("lamp_current", "f8", ("time",), fill_value=nan)
            netcdf_timeseries.variable_coordinates(mon_g, var_Al)
            var_Al.variable_id = "Al"
            var_Al.coverage_content_type = "physicalMeasurement"
            var_Al.cell_methods = "time: mean"
            var_Al.long_name = "lamp current"
            var_Al.units = "A"
            var_Al.C_format = "%4.1f"
            self.apply_data(mon_times, var_Al, data_Al)

            if any([v.time.shape != 0 for v in data_Cf]):
                var_Cf = mon_g.createVariable("reference_counts", "f8", ("time", "wavelength"), fill_value=nan)
                netcdf_timeseries.variable_coordinates(mon_g, var_Cf)
                var_Cf.variable_id = "Cf"
                var_Cf.coverage_content_type = "physicalMeasurement"
                var_Cf.cell_methods = "time: mean"
                var_Cf.long_name = "reference shutter photon count rate"
                var_Cf.units = "Hz"
                var_Cf.C_format = "%7.0f"
                self.apply_wavelength_data(mon_times, var_Cf, data_Cf)
            else:
                var_Cf = None

        if not split_monitor:
            self.apply_cut_size(g, times, [
                (var_P, data_P),
                (var_T, data_T),
                (var_Tu, data_Tu),
                (var_U, data_U),
                (var_Uu, data_Uu),
                (var_Vl, data_Vl),
                (var_Al, data_Al),
            ], [
                (var_Bs, data_Bs),
                (var_Bbs, data_Bbs),
                (var_Cs, data_Cs),
                (var_Cbs, data_Cbs),
                (var_Cf, data_Cf),
                (var_Cd, data_Cd),
                (var_Cbd, data_Cbd),
            ], extra_sources=[data_system_flags])
        else:
            self.apply_cut_size(g, times, [
                (var_P, data_P),
                (var_T, data_T),
                (var_Tu, data_Tu),
                (var_U, data_U),
                (var_Uu, data_Uu),
            ], [
                (var_Bs, data_Bs),
                (var_Bbs, data_Bbs),
                (var_Cs, data_Cs),
                (var_Cbs, data_Cbs),
                (var_Cd, data_Cd),
                (var_Cbd, data_Cbd),
            ], extra_sources=[data_system_flags])
        selected_idx = 0
        for wlidx in range(len(self.WAVELENGTHS)):
            if data_Bs[wlidx].time.shape[0] > data_Bs[selected_idx].time.shape[0]:
                selected_idx = wlidx
        self.apply_coverage(g, times,f"Bs{self.WAVELENGTHS[selected_idx][1]}_{self.instrument_id}")

        g, times = self.state_group([mode], wavelength=False)

        var_mode = g.createVariable("mode", str, ("time",), fill_value=False)
        netcdf_timeseries.variable_coordinates(g, var_mode)
        var_mode.coverage_content_type = "auxiliaryInformation"
        var_mode.cell_methods = "time: point"
        var_mode.long_name = "instrument mode string"
        self.apply_state(times, var_mode, mode)

        sampling_t = g.createEnumType(np.uint8, "sampling_t", {
            'Normal': 0,
            'Zero': 1,
            'Blank': 2,
            'Spancheck': 3,
        })
        var_sampling = g.createVariable("sampling", sampling_t, ("time",), fill_value=False)
        netcdf_timeseries.variable_coordinates(g, var_sampling)
        var_sampling.coverage_content_type = "auxiliaryInformation"
        var_sampling.cell_methods = "time: point"
        var_sampling.long_name = "sampling mode"
        sampling_data: typing.List[int] = []
        spancheck_bit = system_flags_bits.get('spancheck', 0)
        idx_flag: int = 0
        for idx_mode in range(mode.time.shape[0]):
            if spancheck_bit:
                while idx_flag < data_system_flags.time.shape[0]-1 and int(data_system_flags.time[idx_flag+1]) < int(mode.time[idx_mode]):
                    idx_flag += 1
                if idx_flag < data_system_flags.time.shape[0] and (int(data_system_flags.value[idx_flag]) & spancheck_bit) != 0:
                    sampling_data.append(3)
                    continue

            v = str(mode.value[idx_mode])
            if v.startswith('Z'):
                sampling_data.append(1)
            elif v.startswith('B'):
                sampling_data.append(2)
            else:
                sampling_data.append(0)
        self.apply_state(times, var_sampling, mode.time, np.array(sampling_data, dtype=np.uint8))

        g, times = self.state_group(data_Bsw, name="zero")

        var_Tw = g.createVariable("zero_temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_temperature(var_Tw)
        netcdf_timeseries.variable_coordinates(g, var_Tw)
        var_Tw.variable_id = "Tw"
        var_Tw.coverage_content_type = "physicalMeasurement"
        var_Tw.cell_methods = "time: point"
        var_Tw.long_name = "measurement cell temperature during the zero"
        self.apply_state(times, var_Tw, data_Tw)

        var_Pw = g.createVariable("zero_pressure", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_pressure(var_Pw)
        netcdf_timeseries.variable_coordinates(g, var_Pw)
        var_Pw.variable_id = "Tw"
        var_Pw.coverage_content_type = "physicalMeasurement"
        var_Pw.cell_methods = "time: point"
        var_Pw.long_name = "measurement cell pressure during the zero"
        self.apply_state(times, var_Pw, data_Pw)

        var_Bsw = g.createVariable("wall_scattering_coefficient", "f8", ("time", "wavelength"), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_Bsw)
        netcdf_var.variable_wall_total_scattering(var_Bsw)
        var_Bsw.variable_id = "Bsw"
        var_Bsw.coverage_content_type = "physicalMeasurement"
        var_Bsw.cell_methods = "time: point"
        self.apply_wavelength_state(times, var_Bsw, data_Bsw)

        var_Bbsw = g.createVariable("wall_backscattering_coefficient", "f8", ("time", "wavelength"), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_Bbsw)
        netcdf_var.variable_wall_back_scattering(var_Bbsw)
        var_Bbsw.variable_id = "Bbsw"
        var_Bbsw.coverage_content_type = "physicalMeasurement"
        var_Bbsw.cell_methods = "time: point"
        self.apply_wavelength_state(times, var_Bbsw, data_Bbsw)

        g, times = self.state_group(data_PCTc, name="spancheck")
        standard_temperature(g)
        standard_pressure(g)

        var_PCTc = g.createVariable("scattering_percent_error", "f8", ("time", "wavelength"), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_PCTc)
        var_PCTc.variable_id = "PCTc"
        var_PCTc.coverage_content_type = "physicalMeasurement"
        var_PCTc.cell_methods = "time: point"
        var_PCTc.long_name = "spancheck total light scattering percent error"
        var_PCTc.units = "%"
        var_PCTc.C_format = "%6.2f"
        var_PCTc.ancillary_variables = "standard_temperature standard_pressure"
        self.apply_wavelength_state(times, var_PCTc, data_PCTc)

        var_PCTbc = g.createVariable("backscattering_percent_error", "f8", ("time", "wavelength"), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_PCTbc)
        var_PCTbc.variable_id = "PCTbc"
        var_PCTbc.coverage_content_type = "physicalMeasurement"
        var_PCTbc.cell_methods = "time: point"
        var_PCTbc.long_name = "spancheck backwards hemispheric light scattering percent error"
        var_PCTbc.units = "%"
        var_PCTbc.C_format = "%6.2f"
        var_PCTbc.ancillary_variables = "standard_temperature standard_pressure"
        self.apply_wavelength_state(times, var_PCTbc, data_PCTbc)

        var_Cc = g.createVariable("scattering_sensitivity_factor", "f8", ("time", "wavelength"), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_Cc)
        var_Cc.variable_id = "Cc"
        var_Cc.coverage_content_type = "physicalMeasurement"
        var_Cc.cell_methods = "time: point"
        var_Cc.long_name = "total photon count rate attributable to Rayleigh scattering by air at STP"
        var_Cc.units = "Hz"
        var_Cc.C_format = "%7.1f"
        var_Cc.ancillary_variables = "standard_temperature standard_pressure"
        self.apply_wavelength_state(times, var_Cc, data_Cc)

        var_Cbc = g.createVariable("backscattering_sensitivity_factor", "f8", ("time", "wavelength"), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_Cbc)
        var_Cbc.variable_id = "Cbc"
        var_Cbc.coverage_content_type = "physicalMeasurement"
        var_Cbc.cell_methods = "time: point"
        var_Cbc.long_name = "total photon count rate attributable to Rayleigh scattering by air at STP"
        var_Cbc.units = "Hz"
        var_Cbc.C_format = "%7.1f"
        var_Cbc.ancillary_variables = "standard_temperature standard_pressure"
        self.apply_wavelength_state(times, var_Cbc, data_Cbc)

        if parameters.value.shape[0] > 0:
            self._declare_parameters(dict(parameters.value[-1]))

        self.apply_instrument_metadata(
            [f"Bs{code}_{self.instrument_id}" for _, code in self.WAVELENGTHS],
            manufacturer="TSI", model="3563"
        )
        return True
