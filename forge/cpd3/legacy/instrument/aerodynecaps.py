import typing
import numpy as np
import forge.data.structure.variable as netcdf_var
import forge.data.structure.timeseries as netcdf_timeseries
from math import nan, isfinite
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
                    variables=["Be[BGRQ0-9]*_" + self.instrument_id],
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
                self._wavelengths = [ (656.0, "R"), ]
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
        return {"aerosol", "extinction", "aerodynecaps"}

    @property
    def instrument_type(self) -> typing.Optional[str]:
        return "aerodynecaps"

    @property
    def average_interval(self) -> typing.Optional[float]:
        return self._average_interval

    @property
    def split_monitor(self) -> typing.Optional[bool]:
        return None

    def run(self) -> bool:
        data_Be = self.load_wavelength_variable("Be")
        if not any([v.time.shape != 0 for v in data_Be]):
            return False
        self._average_interval = self.calculate_average_interval(np.concatenate([v.time for v in data_Be]))
        if not super().run():
            return False

        data_ZBel = self.load_wavelength_variable("ZBel")
        data_T = self.load_variable(f"T_{self.instrument_id}")
        data_P = self.load_variable(f"P_{self.instrument_id}")
        data_V = self.load_variable(f"V_{self.instrument_id}")

        #status = self.load_state(f"F2_{self.instrument_id}", dtype=str)

        data_Tz = self.load_state(f"Tz_{self.instrument_id}")
        data_Pz = self.load_state(f"Pz_{self.instrument_id}")
        data_Bez = self.load_wavelength_state("Bez")

        system_flags_time = self.load_variable(f"F1?_{self.instrument_id}", convert=bool, dtype=np.bool_).time

        g, times = self.data_group(data_Be + [system_flags_time], fill_gaps=False)
        data_system_flags, system_flags_bits = self.declare_system_flags(g, times)

        var_Be = g.createVariable("scattering_coefficient", "f8", ("time", "wavelength"), fill_value=nan)
        netcdf_var.variable_extinction(var_Be, is_stp=False, is_dried=False)
        netcdf_timeseries.variable_coordinates(g, var_Be)
        var_Be.variable_id = "Be"
        var_Be.coverage_content_type = "physicalMeasurement"
        var_Be.cell_methods = "time: mean"
        self.apply_wavelength_data(times, var_Be, data_Be)

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

        split_monitor = self.split_monitor
        if split_monitor is None:
            split_monitor = self.calculate_split_monitor(data_V.time)
        if not split_monitor:
            mon_g = g
            mon_times = times
        elif data_V.time.shape[0] > 0 or any([v.time.shape != 0 for v in data_ZBel]):
            mon_g, mon_times = self.data_group([data_V] + data_ZBel, name='status', fill_gaps=False)
        else:
            mon_g, mon_times = None, None
            split_monitor = True

        if mon_g is not None:
            if data_V.time.shape[0] > 0:
                var_V = mon_g.createVariable("light_signal", "f8", ("time",), fill_value=nan)
                netcdf_timeseries.variable_coordinates(mon_g, var_V)
                var_V.variable_id = "V"
                var_V.coverage_content_type = "physicalMeasurement"
                var_V.cell_methods = "time: mean"
                var_V.long_name = "lamp supply voltage"
                var_V.units = "mV"
                var_V.C_format = "%6.0f"
                self.apply_data(mon_times, var_V, data_V)
            else:
                var_V = None

            if any([v.time.shape != 0 for v in data_ZBel]):
                var_ZBel = mon_g.createVariable("loss", "f8", ("time", "wavelength"), fill_value=nan)
                netcdf_timeseries.variable_coordinates(mon_g, var_ZBel)
                var_ZBel.variable_id = "ZBel"
                var_ZBel.coverage_content_type = "physicalMeasurement"
                var_ZBel.cell_methods = "time: mean"
                var_ZBel.long_name = "loss measurement"
                var_ZBel.units = "Mm-1"
                var_ZBel.C_format = "%7.2f"
                self.apply_wavelength_data(mon_times, var_ZBel, data_ZBel)
            else:
                var_V = None
                var_ZBel = None
        else:
            var_V = None
            var_ZBel = None

        if not split_monitor:
            self.apply_cut_size(g, times, [
                (var_P, data_P),
                (var_T, data_T),
                (var_V, data_V),
            ], [
                (var_Be, data_Be),
                (var_ZBel, data_ZBel),
            ], extra_sources=[data_system_flags])
        else:
            self.apply_cut_size(g, times, [
                (var_P, data_P),
                (var_T, data_T),
            ], [
                (var_Be, data_Be),
            ], extra_sources=[data_system_flags])
        selected_idx = 0
        for wlidx in range(len(self.WAVELENGTHS)):
            if data_Be[wlidx].time.shape[0] > data_Be[selected_idx].time.shape[0]:
                selected_idx = wlidx
        self.apply_coverage(g, times,f"Be{self.WAVELENGTHS[selected_idx][1]}_{self.instrument_id}")

        if any([v.time.shape != 0 for v in data_Bez]):
            g, times = self.state_group(data_Bez, name="zero")

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

            var_Bez = g.createVariable("zero_extinction_coefficient", "f8", ("time", "wavelength"), fill_value=nan)
            netcdf_timeseries.variable_coordinates(g, var_Bez)
            var_Bez.variable_id = "Bsw"
            var_Bez.coverage_content_type = "physicalMeasurement"
            var_Bez.cell_methods = "time: point"
            var_Bez.long_name = "light extinction coefficient during the zero measurement"
            var_Bez.units = "Mm-1"
            var_Bez.C_format = "%7.2f"
            self.apply_wavelength_state(times, var_Bez, data_Bez)

        self.apply_instrument_metadata(
            [f"Be{code}_{self.instrument_id}" for _, code in self.WAVELENGTHS],
            manufacturer="Aerodyne", model="CAPS"
        )
        return True
