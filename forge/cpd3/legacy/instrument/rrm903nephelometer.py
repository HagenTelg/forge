import typing
import numpy as np
import forge.data.structure.variable as netcdf_var
import forge.data.structure.timeseries as netcdf_timeseries
from math import nan
from .converter import WavelengthConverter, read_archive, Selection, variant
from forge.data.structure.stp import standard_temperature, standard_pressure


class Converter(WavelengthConverter):
    WAVELENGTHS = [
        (530.0, "G"),
    ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._average_interval: typing.Optional[float] = None

    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"aerosol", "scattering", "rrm903nephelometer"}

    @property
    def instrument_type(self) -> typing.Optional[str]:
        return "rrm903nephelometer"

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
                ("Bsr", "Rayleigh scattering coefficient", "Mm-1", "%7.2f"),
                ("ZBsZeroRatio", "zero offset subtraction as a factor of the calibrator extinction coefficient", None, "%7.5f"),
                ("ZBsSpanRatio", "span gas scattering coefficient ratio", None, "%7.5f"),
        ):
            values = np.full((len(self.WAVELENGTHS),), nan, dtype=np.float64)
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
                ("V", "photomultiplier tube voltage", "V", "%4.0f"),
        ):
            value = parameters.get(name)
            if value is None:
                continue

            var = g.createVariable(name, "f8", (), fill_value=False)
            var.coverage_content_type = "referenceInformation"
            var.long_name = long_name
            if units:
                var.units = units
            var.C_format = C_format
            var[:] = float(value)

    def run(self) -> bool:
        data_Bs = self.load_wavelength_variable("Bs")
        if not any([v.time.shape[0] != 0 for v in data_Bs]):
            return False
        self._average_interval = self.calculate_average_interval(np.concatenate([v.time for v in data_Bs]))
        if not super().run():
            return False

        data_P = self.load_variable(f"P_{self.instrument_id}")
        data_T = self.load_variable(f"T_{self.instrument_id}")
        data_Tu = self.load_variable(f"Tu_{self.instrument_id}")
        data_U = self.load_variable(f"U_{self.instrument_id}")
        data_Uu = self.load_variable(f"Uu_{self.instrument_id}")

        data_Cs = self.load_wavelength_variable("Cs")
        data_Cf = self.load_wavelength_variable("Cf")
        if not any([v.time.shape[0] != 0 for v in data_Cf]):
            data_Cf = self.load_wavelength_variable("Cr")
        data_Bsf = self.load_wavelength_variable("Bsf")
        data_Cd = self.load_variable(f"Cd_{self.instrument_id}")
        data_Cp = self.load_wavelength_variable(f"Cp")
        data_Cpf = self.load_wavelength_variable(f"Cpf")

        data_Tz = self.load_state(f"Tz_{self.instrument_id}")
        data_Pz = self.load_state(f"Pz_{self.instrument_id}")
        data_Bsz = self.load_wavelength_state("Bsz")

        data_PCTc = self.load_wavelength_state("PCTc")
        data_Cc = self.load_wavelength_state("Cc")

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

        if data_Tu.time.shape[0] > 0:
            var_Tu = g.createVariable("inlet_temperature", "f8", ("time",), fill_value=nan)
            netcdf_var.variable_temperature(var_Tu)
            netcdf_timeseries.variable_coordinates(g, var_Tu)
            var_Tu.variable_id = "Tu"
            var_Tu.coverage_content_type = "physicalMeasurement"
            var_Tu.cell_methods = "time: mean"
            var_Tu.long_name = "inlet temperature"
            self.apply_data(times, var_Tu, data_Tu)
        else:
            var_Tu = None

        var_U = g.createVariable("sample_humidity", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_air_rh(var_U)
        netcdf_timeseries.variable_coordinates(g, var_U)
        var_U.variable_id = "U"
        var_U.coverage_content_type = "physicalMeasurement"
        var_U.cell_methods = "time: mean"
        var_U.long_name = "measurement cell relative humidity"
        self.apply_data(times, var_U, data_U)

        if data_Uu.time.shape[0] > 0:
            var_Uu = g.createVariable("inlet_humidity", "f8", ("time",), fill_value=nan)
            netcdf_var.variable_rh(var_Uu)
            netcdf_timeseries.variable_coordinates(g, var_Uu)
            var_Uu.variable_id = "Uu"
            var_Uu.coverage_content_type = "referenceInformation"
            var_Uu.cell_methods = "time: mean"
            var_Uu.long_name = "inlet humidity"
            self.apply_data(times, var_Uu, data_Uu)
        else:
            var_Uu = None

        var_Cd = g.createVariable("dark_counts", "f8", ("time",), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_Cd)
        var_Cd.variable_id = "Cd"
        var_Cd.coverage_content_type = "physicalMeasurement"
        var_Cd.cell_methods = "time: mean"
        var_Cd.long_name = "dark counts"
        var_Cd.C_format = "%5.0f"
        self.apply_data(times, var_Cd, data_Cd)

        if any([v.time.shape[0] != 0 for v in data_Cs]):
            var_Cs = g.createVariable("scattering_counts", "f8", ("time", "wavelength"), fill_value=nan)
            netcdf_timeseries.variable_coordinates(g, var_Cs)
            var_Cs.variable_id = "Cs"
            var_Cs.coverage_content_type = "physicalMeasurement"
            var_Cs.cell_methods = "time: mean"
            var_Cs.long_name = "total scattering counts"
            var_Cs.C_format = "%5.0f"
            self.apply_wavelength_data(times, var_Cs, data_Cs)
        else:
            var_Cs = None

        if any([v.time.shape[0] != 0 for v in data_Cs]):
            var_Cp = g.createVariable("sample_pmt_output", "f8", ("time", "wavelength"), fill_value=nan)
            netcdf_timeseries.variable_coordinates(g, var_Cp)
            var_Cp.variable_id = "Cp"
            var_Cp.coverage_content_type = "physicalMeasurement"
            var_Cp.cell_methods = "time: mean"
            var_Cp.long_name = "sample raw PMT output with dark current removed"
            var_Cp.C_format = "%5.0f"
            self.apply_wavelength_data(times, var_Cp, data_Cp)
        else:
            var_Cp = None

        if any([v.time.shape[0] != 0 for v in data_Cs]):
            var_Cpf = g.createVariable("calibrator_pmt_output", "f8", ("time", "wavelength"), fill_value=nan)
            netcdf_timeseries.variable_coordinates(g, var_Cpf)
            var_Cpf.variable_id = "Cp"
            var_Cpf.coverage_content_type = "physicalMeasurement"
            var_Cpf.cell_methods = "time: mean"
            var_Cpf.long_name = "calibrator raw PMT output with dark current removed"
            var_Cpf.C_format = "%5.0f"
            self.apply_wavelength_data(times, var_Cpf, data_Cpf)
        else:
            var_Cpf = None

        if any([v.time.shape[0] != 0 for v in data_Cs]):
            var_Bsf = g.createVariable("calibrator_scattering_coefficient", "f8", ("time", "wavelength"), fill_value=nan)
            netcdf_timeseries.variable_coordinates(g, var_Bsf)
            var_Bsf.variable_id = "Bsf"
            var_Bsf.coverage_content_type = "physicalMeasurement"
            var_Bsf.cell_methods = "time: mean"
            var_Bsf.long_name = "calibrator light scattering coefficient"
            var_Bsf.units = "Mm-1"
            var_Bsf.C_format = "%7.2f"
            self.apply_wavelength_data(times, var_Bsf, data_Bsf)
        else:
            var_Bsf = None

        split_monitor = self.split_monitor
        if split_monitor is None:
            split_monitor = self.calculate_split_monitor(np.concatenate([v.time for v in data_Cf]))
        if not split_monitor:
            mon_g = g
            mon_times = times
        elif any([v.time.shape[0] != 0 for v in data_Cf]):
            mon_g, mon_times = self.data_group(data_Cf, name='status', fill_gaps=False)
        else:
            mon_g, mon_times = None, None
            split_monitor = True

        if mon_g is not None:
            if any([v.time.shape[0] != 0 for v in data_Cf]):
                var_Cf = mon_g.createVariable("reference_counts", "f8", ("time", "wavelength"), fill_value=nan)
                netcdf_timeseries.variable_coordinates(mon_g, var_Cf)
                var_Cf.variable_id = "Cf"
                var_Cf.coverage_content_type = "physicalMeasurement"
                var_Cf.cell_methods = "time: mean"
                var_Cf.long_name = "reference shutter counts"
                var_Cf.C_format = "%5.0f"
                self.apply_wavelength_data(mon_times, var_Cf, data_Cf)
            else:
                var_Cf = None
        else:
            var_Cf = None

        if not split_monitor:
            self.apply_cut_size(g, times, [
                (var_P, data_P),
                (var_T, data_T),
                (var_Tu, data_Tu),
                (var_U, data_U),
                (var_Uu, data_Uu),
                (var_Cd, data_Cd),
            ], [
                (var_Bs, data_Bs),
                (var_Bsf, data_Bsf),
                (var_Cs, data_Cs),
                (var_Cf, data_Cf),
                (var_Cp, data_Cp),
                (var_Cpf, data_Cpf),
            ], extra_sources=[data_system_flags])
        else:
            self.apply_cut_size(g, times, [
                (var_P, data_P),
                (var_T, data_T),
                (var_Tu, data_Tu),
                (var_U, data_U),
                (var_Cd, data_Cd),
            ], [
                (var_Bs, data_Bs),
                (var_Bsf, data_Bsf),
                (var_Cs, data_Cs),
                (var_Cp, data_Cp),
                (var_Cpf, data_Cpf),
            ], extra_sources=[data_system_flags])
        selected_idx = 0
        for wlidx in range(len(self.WAVELENGTHS)):
            if data_Bs[wlidx].time.shape[0] > data_Bs[selected_idx].time.shape[0]:
                selected_idx = wlidx
        self.apply_coverage(g, times, f"Bs{self.WAVELENGTHS[selected_idx][1]}_{self.instrument_id}")

        if any([v.time.shape[0] != 0 for v in data_Bsz]):
            g, times = self.state_group(data_Bsz, name="zero")

            if data_Tz.time.shape[0] > 0:
                var_Tz = g.createVariable("zero_temperature", "f8", ("time",), fill_value=nan)
                netcdf_var.variable_temperature(var_Tz)
                netcdf_timeseries.variable_coordinates(g, var_Tz)
                var_Tz.variable_id = "Tz"
                var_Tz.coverage_content_type = "physicalMeasurement"
                var_Tz.cell_methods = "time: point"
                var_Tz.long_name = "sample temperature during the zero"
                self.apply_state(times, var_Tz, data_Tz)

            if data_Pz.time.shape[0] > 0:
                var_Pz = g.createVariable("zero_pressure", "f8", ("time",), fill_value=nan)
                netcdf_var.variable_pressure(var_Pz)
                netcdf_timeseries.variable_coordinates(g, var_Pz)
                var_Pz.variable_id = "Tz"
                var_Pz.coverage_content_type = "physicalMeasurement"
                var_Pz.cell_methods = "time: point"
                var_Pz.long_name = "sample pressure during the zero"
                self.apply_state(times, var_Pz, data_Pz)

            var_Bsz = g.createVariable("zero_scattering_coefficient", "f8", ("time", "wavelength"), fill_value=nan)
            netcdf_timeseries.variable_coordinates(g, var_Bsz)
            var_Bsz.variable_id = "Bsz"
            var_Bsz.coverage_content_type = "physicalMeasurement"
            var_Bsz.cell_methods = "time: point"
            var_Bsz.long_name = "light scattering coefficient during the zero measurement"
            var_Bsz.units = "Mm-1"
            var_Bsz.C_format = "%7.2f"
            self.apply_wavelength_state(times, var_Bsz, data_Bsz)

        if any([v.time.shape[0] != 0 for v in data_PCTc]):
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

        if parameters.value.shape[0] > 0:
            self._declare_parameters(dict(parameters.value[-1]))

        self.apply_instrument_metadata(
            [f"Bs{code}_{self.instrument_id}" for _, code in self.WAVELENGTHS],
            manufacturer="Radiance Research", model="M903"
        )
        return True
