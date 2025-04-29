import typing
import numpy as np
import forge.data.structure.variable as netcdf_var
import forge.data.structure.timeseries as netcdf_timeseries
from math import nan, isfinite
from forge.data.structure.stp import standard_temperature, standard_pressure
from .converter import WavelengthConverter, read_archive, Selection, variant


class Converter(WavelengthConverter):
    @property
    def WAVELENGTHS(self) -> typing.List[typing.Tuple[float, str]]:
        if self._wavelengths is None:
            selected_wavelength = None
            selected_wavelength_code = None
            for identity, value, _ in read_archive([Selection(
                    start=self.file_start,
                    end=self.file_end,
                    stations=[self.station],
                    archives=[self.archive + "_meta"],
                    variables=["B[sa][BGRQ0-9]*_" + self.instrument_id],
                    include_meta_archive=False,
                    include_default_station=False,
            )]):
                if not isinstance(value, variant.Metadata):
                    continue
                wavelength = value.get("Wavelength")
                if wavelength is None:
                    continue
                try:
                    wavelength = float(wavelength)
                except (ValueError, TypeError):
                    continue
                if not isfinite(wavelength) or wavelength <= 0.0:
                    continue
                selected_wavelength = wavelength

                try:
                    prefix, _ = identity.variable.split("_")
                    if len(prefix) <= 2:
                        raise ValueError
                    selected_wavelength_code = prefix[2:]
                except ValueError:
                    selected_wavelength_code = None

            if selected_wavelength is None:
                self._wavelengths = [ (532.0, "R"), ]
            elif selected_wavelength_code is not None:
                self._wavelengths = [ (selected_wavelength, selected_wavelength_code), ]
            else:
                if 400 <= selected_wavelength < 500:
                    self._wavelengths = [ (selected_wavelength, "B"), ]
                elif 500 <= selected_wavelength < 600:
                    self._wavelengths = [ (selected_wavelength, "G"), ]
                elif 600 <= selected_wavelength < 750:
                    self._wavelengths = [ (selected_wavelength, "R"), ]
                elif 750 <= selected_wavelength < 900:
                    self._wavelengths = [ (selected_wavelength, "Q"), ]
                else:
                    self._wavelengths = [(selected_wavelength, "1"), ]
        return self._wavelengths

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._average_interval: typing.Optional[float] = None
        self._wavelengths = None

    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"aerosol", "scattering", "absorption", "dmtpax"}

    @property
    def instrument_type(self) -> typing.Optional[str]:
        return "dmtpax"

    @property
    def average_interval(self) -> typing.Optional[float]:
        return self._average_interval

    @property
    def split_monitor(self) -> typing.Optional[bool]:
        return None

    def run(self) -> bool:
        data_Ba = self.load_wavelength_variable("Ba")
        data_Bs = self.load_wavelength_variable("Bs")
        if not any([v.time.shape[0] != 0 for v in data_Bs]) and not any([v.time.shape[0] != 0 for v in data_Ba]):
            return False
        self._average_interval = self.calculate_average_interval(np.concatenate([v.time for v in data_Ba] + [v.time for v in data_Bs]))
        if not super().run():
            return False

        data_Bag = self.load_wavelength_variable("Ba", "g")
        data_ZBaPhase = self.load_wavelength_variable("ZBa", "Phase")
        data_ZIMic = self.load_wavelength_variable("ZIMic")
        data_ZIBs = self.load_wavelength_variable("ZIBs")
        data_P1 = self.load_variable(f"P1_{self.instrument_id}")
        data_P2 = self.load_variable(f"P2_{self.instrument_id}")
        data_Pu = self.load_variable(f"Pu_{self.instrument_id}")
        data_T1 = self.load_variable(f"T1_{self.instrument_id}")
        data_T2 = self.load_variable(f"T2_{self.instrument_id}")
        data_U = self.load_variable(f"U_{self.instrument_id}")
        data_C = self.load_variable(f"C_{self.instrument_id}")
        data_A1 = self.load_variable(f"A1_{self.instrument_id}")
        data_A2 = self.load_variable(f"A2_{self.instrument_id}")
        data_VA = self.load_variable(f"VA_{self.instrument_id}")
        data_ZLaserPhase = self.load_variable(f"ZLaserPhase_{self.instrument_id}")
        data_ZMicPressure = self.load_variable(f"ZMicPressure_{self.instrument_id}")
        data_ZQ = self.load_variable(f"ZQ_{self.instrument_id}")

        data_Tz = self.load_state(f"Tz_{self.instrument_id}")
        data_Pz = self.load_state(f"Pz_{self.instrument_id}")
        data_Bsz = self.load_wavelength_state("Bsz")
        data_Baz = self.load_wavelength_state("Baz")
        data_ZBazPhase = self.load_wavelength_state("ZBaz", "Phase")

        data_PCTc = self.load_wavelength_state("PCTc")

        #mode_number = self.load_state(f"F2_{self.instrument_id}", dtype=str)

        system_flags_time = self.load_variable(f"F1?_{self.instrument_id}", convert=bool, dtype=np.bool_).time

        g, times = self.data_group(data_Ba + data_Bs + [system_flags_time], fill_gaps=False)
        data_system_flags, system_flags_bits = self.declare_system_flags(g, times)

        var_Bs = g.createVariable("scattering_coefficient", "f8", ("time", "wavelength"), fill_value=nan)
        netcdf_var.variable_extinction(var_Bs, is_stp=False)
        netcdf_timeseries.variable_coordinates(g, var_Bs)
        var_Bs.variable_id = "Bs"
        var_Bs.coverage_content_type = "physicalMeasurement"
        var_Bs.cell_methods = "time: mean"
        self.apply_wavelength_data(times, var_Bs, data_Bs)

        var_Ba = g.createVariable("light_absorption", "f8", ("time", "wavelength"), fill_value=nan)
        netcdf_var.variable_absorption(var_Ba, is_stp=False)
        netcdf_timeseries.variable_coordinates(g, var_Ba)
        var_Ba.variable_id = "Ba"
        var_Ba.coverage_content_type = "physicalMeasurement"
        var_Ba.cell_methods = "time: mean"
        self.apply_wavelength_data(times, var_Ba, data_Ba)

        var_Bag = g.createVariable("light_absorption_noise", "f8", ("time", "wavelength"), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_Bag)
        var_Bag.variable_id = "Bag"
        var_Bag.coverage_content_type = "physicalMeasurement"
        var_Bag.cell_methods = "time: mean"
        var_Bag.long_name = "light absorption coefficient noise estimate"
        var_Bag.units = "Mm-1"
        var_Bag.C_format = "%7.2f"
        self.apply_wavelength_data(times, var_Bag, data_Bag)

        var_ZBaPhase = g.createVariable("light_absorption_phase", "f8", ("time", "wavelength"), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_ZBaPhase)
        var_ZBaPhase.variable_id = "ZBaPhase"
        var_ZBaPhase.coverage_content_type = "physicalMeasurement"
        var_ZBaPhase.cell_methods = "time: mean"
        var_ZBaPhase.long_name = "light absorption vector phase angle to signal"
        var_ZBaPhase.units = "degrees"
        var_ZBaPhase.C_format = "%6.2f"
        self.apply_wavelength_data(times, var_ZBaPhase, data_ZBaPhase)

        var_ZIMic = g.createVariable("microphone_intensity", "f8", ("time", "wavelength"), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_ZIMic)
        var_ZIMic.variable_id = "ZIMic"
        var_ZIMic.coverage_content_type = "physicalMeasurement"
        var_ZIMic.cell_methods = "time: mean"
        var_ZIMic.long_name = "microphone raw intensity"
        var_ZIMic.C_format = "%9.6f"
        self.apply_wavelength_data(times, var_ZIMic, data_ZIMic)

        var_ZIBs = g.createVariable("scattering_intensity", "f8", ("time", "wavelength"), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_ZIBs)
        var_ZIBs.variable_id = "ZIBs"
        var_ZIBs.coverage_content_type = "physicalMeasurement"
        var_ZIBs.cell_methods = "time: mean"
        var_ZIBs.long_name = "scattering raw intensity"
        var_ZIBs.C_format = "%9.6f"
        self.apply_wavelength_data(times, var_ZIBs, data_ZIBs)

        var_P1 = g.createVariable("sample_pressure", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_air_pressure(var_P1)
        netcdf_timeseries.variable_coordinates(g, var_P1)
        var_P1.variable_id = "P1"
        var_P1.coverage_content_type = "physicalMeasurement"
        var_P1.cell_methods = "time: mean"
        var_P1.long_name = "sample pressure"
        self.apply_data(times, var_P1, data_P1)

        var_P2 = g.createVariable("vacuum_pressure", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_pressure(var_P2)
        netcdf_timeseries.variable_coordinates(g, var_P2)
        var_P2.variable_id = "P2"
        var_P2.coverage_content_type = "physicalMeasurement"
        var_P2.cell_methods = "time: mean"
        var_P2.long_name = "vacuum pressure"
        self.apply_data(times, var_P2, data_P2)

        var_Pu = g.createVariable("inlet_pressure", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_pressure(var_Pu)
        netcdf_timeseries.variable_coordinates(g, var_Pu)
        var_Pu.variable_id = "Pu"
        var_Pu.coverage_content_type = "physicalMeasurement"
        var_Pu.cell_methods = "time: mean"
        var_Pu.long_name = "vacuum pressure"
        self.apply_data(times, var_Pu, data_Pu)

        var_T1 = g.createVariable("sample_temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_air_temperature(var_T1)
        netcdf_timeseries.variable_coordinates(g, var_T1)
        var_T1.variable_id = "T1"
        var_T1.coverage_content_type = "physicalMeasurement"
        var_T1.cell_methods = "time: mean"
        var_T1.long_name = "sample temperature"
        self.apply_data(times, var_T1, data_T1)

        var_T2 = g.createVariable("laser_temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_temperature(var_T2)
        netcdf_timeseries.variable_coordinates(g, var_T2)
        var_T2.variable_id = "T2"
        var_T2.coverage_content_type = "physicalMeasurement"
        var_T2.cell_methods = "time: mean"
        var_T2.long_name = "laser temperature"
        self.apply_data(times, var_T2, data_T2)

        var_U = g.createVariable("sample_humidity", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_air_rh(var_U)
        netcdf_timeseries.variable_coordinates(g, var_U)
        var_U.variable_id = "U"
        var_U.coverage_content_type = "physicalMeasurement"
        var_U.cell_methods = "time: mean"
        var_U.long_name = "sample relative humidity"
        self.apply_data(times, var_U, data_U)

        var_C = g.createVariable("resonant_frequency", "f8", ("time",), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_C)
        var_C.variable_id = "C"
        var_C.coverage_content_type = "physicalMeasurement"
        var_C.cell_methods = "time: mean"
        var_C.long_name = "resonant frequency"
        var_C.units = "Hz"
        var_C.C_format = "%9.2f"
        self.apply_data(times, var_C, data_C)

        var_A1 = g.createVariable("laser_current", "f8", ("time",), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_A1)
        var_A1.variable_id = "A1"
        var_A1.coverage_content_type = "physicalMeasurement"
        var_A1.cell_methods = "time: mean"
        var_A1.long_name = "laser current"
        var_A1.units = "A"
        var_A1.C_format = "%7.4f"
        self.apply_data(times, var_A1, data_A1)

        var_A2 = g.createVariable("photodiode_current", "f8", ("time",), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_A2)
        var_A2.variable_id = "A2"
        var_A2.coverage_content_type = "physicalMeasurement"
        var_A2.cell_methods = "time: mean"
        var_A2.long_name = "photodiode current"
        var_A2.units = "A"
        var_A2.C_format = "%7.4f"
        self.apply_data(times, var_A2, data_A2)

        var_VA = g.createVariable("laser_power", "f8", ("time",), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_VA)
        var_VA.variable_id = "VA"
        var_VA.coverage_content_type = "physicalMeasurement"
        var_VA.cell_methods = "time: mean"
        var_VA.long_name = "laser power"
        var_VA.units = "W"
        var_VA.C_format = "%7.4f"
        self.apply_data(times, var_VA, data_VA)

        var_ZLaserPhase = g.createVariable("laser_phase", "f8", ("time",), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_ZLaserPhase)
        var_ZLaserPhase.variable_id = "ZLaserPhase"
        var_ZLaserPhase.coverage_content_type = "physicalMeasurement"
        var_ZLaserPhase.cell_methods = "time: mean"
        var_ZLaserPhase.long_name = "laser relative to microphone"
        var_ZLaserPhase.units = "degrees"
        var_ZLaserPhase.C_format = "%7.3f"
        self.apply_data(times, var_ZLaserPhase, data_ZLaserPhase)

        var_ZMicPressure = g.createVariable("microphone_pressure", "f8", ("time",), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_ZMicPressure)
        var_ZMicPressure.variable_id = "ZMicPressure"
        var_ZMicPressure.coverage_content_type = "physicalMeasurement"
        var_ZMicPressure.cell_methods = "time: mean"
        var_ZMicPressure.long_name = "microphone pressure at resonant frequency"
        var_ZMicPressure.units = "dB"
        var_ZMicPressure.C_format = "%7.3f"
        self.apply_data(times, var_ZMicPressure, data_ZMicPressure)

        var_ZQ = g.createVariable("resonator_q_factor", "f8", ("time",), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_ZQ)
        var_ZQ.variable_id = "ZQ"
        var_ZQ.coverage_content_type = "physicalMeasurement"
        var_ZQ.cell_methods = "time: mean"
        var_ZQ.long_name = "resonator Q factor"
        var_ZQ.C_format = "%8.4f"
        self.apply_data(times, var_ZQ, data_ZQ)

        self.apply_cut_size(g, times, [
            (var_P1, data_P1),
            (var_P2, data_P2),
            (var_Pu, data_Pu),
            (var_T1, data_T1),
            (var_T2, data_T2),
            (var_U, data_U),
            (var_C, data_C),
            (var_A1, data_A1),
            (var_A2, data_A2),
            (var_VA, data_VA),
            (var_ZLaserPhase, data_ZLaserPhase),
            (var_ZMicPressure, data_ZMicPressure),
            (var_ZQ, data_ZQ),
        ], [
            (var_Ba, data_Ba),
            (var_Bs, data_Bs),
            (var_Bag, data_Bag),
            (var_ZBaPhase, data_ZBaPhase),
            (var_ZIMic, data_ZIMic),
            (var_ZIBs, data_ZIBs),
        ], extra_sources=[data_system_flags])

        selected_idx_Bs = 0
        for wlidx in range(len(self.WAVELENGTHS)):
            if data_Bs[wlidx].time.shape[0] > data_Bs[selected_idx_Bs].time.shape[0]:
                selected_idx_Bs = wlidx
        selected_idx_Ba = 0
        for wlidx in range(len(self.WAVELENGTHS)):
            if data_Ba[wlidx].time.shape[0] > data_Ba[selected_idx_Ba].time.shape[0]:
                selected_idx_Ba = wlidx
        if data_Bs[selected_idx_Bs].time.shape[0] > data_Ba[selected_idx_Ba].time.shape[0]:
            self.apply_coverage(g, times,f"Bs{self.WAVELENGTHS[selected_idx_Bs][1]}_{self.instrument_id}")
        else:
            self.apply_coverage(g, times, f"Ba{self.WAVELENGTHS[selected_idx_Ba][1]}_{self.instrument_id}")

        if any([v.time.shape[0] != 0 for v in data_Bsz]) or any([v.time.shape[0] != 0 for v in data_Baz]):
            g, times = self.state_group(data_Bsz + data_Baz, name="zero")

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

            var_Baz = g.createVariable("zero_light_absorption", "f8", ("time", "wavelength"), fill_value=nan)
            netcdf_timeseries.variable_coordinates(g, var_Baz)
            var_Baz.variable_id = "Baz"
            var_Baz.coverage_content_type = "physicalMeasurement"
            var_Baz.cell_methods = "time: point"
            var_Baz.long_name = "light absorption coefficient during the zero measurement"
            var_Baz.units = "Mm-1"
            var_Baz.C_format = "%7.2f"
            self.apply_wavelength_state(times, var_Baz, data_Baz)

            var_ZBazPhase = g.createVariable("zero_light_absorption_phase", "f8", ("time", "wavelength"), fill_value=nan)
            netcdf_timeseries.variable_coordinates(g, var_ZBazPhase)
            var_ZBazPhase.variable_id = "BazPhase"
            var_ZBazPhase.coverage_content_type = "physicalMeasurement"
            var_ZBazPhase.cell_methods = "time: point"
            var_ZBazPhase.long_name = "light absorption vector phase angle during the zero measurement"
            var_ZBazPhase.units = "degrees"
            var_ZBazPhase.C_format = "%7.3f"
            self.apply_wavelength_state(times, var_ZBazPhase, data_ZBazPhase)

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

        self.apply_instrument_metadata(
            [f"Bs{code}_{self.instrument_id}" for _, code in self.WAVELENGTHS] +
            [f"Ba{code}_{self.instrument_id}" for _, code in self.WAVELENGTHS],
            manufacturer="DMT", model="PAX"
        )
        return True
