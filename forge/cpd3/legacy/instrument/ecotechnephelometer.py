import typing
import numpy as np
import forge.data.structure.variable as netcdf_var
import forge.data.structure.timeseries as netcdf_timeseries
from math import nan
from .converter import WavelengthConverter, read_archive, Selection, variant
from forge.data.structure.stp import standard_temperature, standard_pressure


class Converter(WavelengthConverter):
    WAVELENGTHS = [
        (450.0, "B"),
        (525.0, "G"),
        (635.0, "R"),
    ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._average_interval: typing.Optional[float] = None

    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"aerosol", "scattering", "ecotechnephelometer"}

    @property
    def instrument_type(self) -> typing.Optional[str]:
        return "ecotechnephelometer"

    @property
    def average_interval(self) -> typing.Optional[float]:
        return self._average_interval

    def _declare_parameters(self, parameters: typing.Dict[str, typing.Any]) -> None:
        if not isinstance(parameters, dict):
            return
        if not parameters:
            return
        lines = parameters.get("Lines")
        if not lines:
            return

        g = self.root.createGroup("parameters")

        var_parameters = g.createVariable("instrument_parameters", str, (), fill_value=False)
        var_parameters.coverage_content_type = "referenceInformation"
        var_parameters.long_name = "instrument response to the EE command"
        var_parameters[0] = "\n".join(list(lines))

    def _instrument_stp(self, g, var: str) -> str:
        stp_t: typing.Optional[float] = None
        stp_p: typing.Optional[float] = None
        for _, value, _ in read_archive([Selection(
                start=self.file_start,
                end=self.file_end,
                stations=[self.station],
                archives=[self.archive + "_meta"],
                variables=[var],
                include_meta_archive=False,
                include_default_station=False,
        )]):
            if not isinstance(value, variant.Metadata):
                continue
            stp_t = value.get("ReportT")
            stp_p = value.get("ReportP")
        a_vars = list()
        if stp_t is not None:
            standard_temperature(g, stp_t)
            a_vars.append("standard_temperature")
        if stp_p is not None:
            standard_pressure(g, stp_p)
            a_vars.append("standard_pressure")
        return " ".join(a_vars)

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
        data_Tx = self.load_variable(f"Tx_{self.instrument_id}")
        data_U = self.load_variable(f"U_{self.instrument_id}")

        data_Cs = self.load_wavelength_variable("Cs")
        data_Cbs = self.load_wavelength_variable("Cbs")
        data_Cf = self.load_wavelength_variable("Cf")
        data_Cd = self.load_variable("Cd")

        data_Tw = self.load_state(f"Tw_{self.instrument_id}")
        data_Pw = self.load_state(f"Pw_{self.instrument_id}")
        data_Bsw = self.load_wavelength_state("Bsw")
        data_Bbsw = self.load_wavelength_state("Bbsw")

        data_PCTc = self.load_wavelength_state("PCTc")
        data_PCTbc = self.load_wavelength_state("PCTbc")
        data_Cc = self.load_wavelength_state("Cc")
        data_Cbc = self.load_wavelength_state("Cbc")

        data_Bn = self.load_array_state(f"Bn_{self.instrument_id}")
        data_Bsn = self.load_wavelength_variable("Bsn")
        data_Bsnw = self.load_wavelength_variable("Bsnw")
        data_PCTnc = self.load_wavelength_variable("PCTnc")

        parameters = self.load_state(f"ZEE_{self.instrument_id}", dtype=dict)

        system_flags_time = self.load_variable(f"F1?_{self.instrument_id}", convert=bool, dtype=np.bool_).time

        g, times = self.data_group(var_Bs + [system_flags_time], fill_gaps=False)
        selected_idx = 0
        for wlidx in range(len(self.WAVELENGTHS)):
            if data_Bs[wlidx].time.shape[0] > data_Bs[selected_idx].time.shape[0]:
                selected_idx = wlidx
        stp_vars = self._instrument_stp(g, f"Bs{self.WAVELENGTHS[selected_idx][1]}_{self.instrument_id}")
        data_system_flags, system_flags_bits = self.declare_system_flags(g, times)

        var_Bs = g.createVariable("scattering_coefficient", "f8", ("time", "wavelength"), fill_value=nan)
        netcdf_var.variable_total_scattering(var_Bs)
        netcdf_timeseries.variable_coordinates(g, var_Bs)
        var_Bs.variable_id = "Bs"
        var_Bs.coverage_content_type = "physicalMeasurement"
        var_Bs.cell_methods = "time: mean"
        if stp_vars:
            var_Bs.ancillary_variables = stp_vars
        self.apply_wavelength_data(times, var_Bs, data_Bs)

        var_Bbs = g.createVariable("backscattering_coefficient", "f8", ("time", "wavelength"), fill_value=nan)
        netcdf_var.variable_back_scattering(var_Bbs)
        netcdf_timeseries.variable_coordinates(g, var_Bbs)
        var_Bbs.variable_id = "Bbs"
        var_Bbs.coverage_content_type = "physicalMeasurement"
        var_Bbs.cell_methods = "time: mean"
        if stp_vars:
            var_Bbs.ancillary_variables = stp_vars
        self.apply_wavelength_data(times, var_Bbs, data_Bbs)

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

        var_Tx = g.createVariable("cell_temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_temperature(var_Tx)
        netcdf_timeseries.variable_coordinates(g, var_Tx)
        var_Tx.variable_id = "Tx"
        var_Tx.coverage_content_type = "physicalMeasurement"
        var_Tx.cell_methods = "time: mean"
        var_Tx.long_name = "cell enclosure temperature"
        self.apply_data(times, var_Tx, data_Tx)

        var_U = g.createVariable("sample_humidity", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_air_rh(var_U)
        netcdf_timeseries.variable_coordinates(g, var_U)
        var_U.variable_id = "U"
        var_U.coverage_content_type = "physicalMeasurement"
        var_U.cell_methods = "time: mean"
        var_U.long_name = "measurement cell relative humidity"
        self.apply_data(times, var_U, data_U)

        var_Cd = g.createVariable("dark_counts", "f8", ("time",), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_Cd)
        var_Cd.variable_id = "Cd"
        var_Cd.coverage_content_type = "physicalMeasurement"
        var_Cd.cell_methods = "time: mean"
        var_Cd.long_name = "dark count rate"
        var_Cd.units = "Hz"
        var_Cd.C_format = "%7.0f"
        self.apply_data(times, var_Cd, data_Cd)

        var_Cs = g.createVariable("scattering_counts", "f8", ("time", "wavelength"), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_Cs)
        var_Cs.variable_id = "Cs"
        var_Cs.coverage_content_type = "physicalMeasurement"
        var_Cs.cell_methods = "time: mean"
        var_Cs.long_name = "total scattering photon count rate"
        var_Cs.units = "Hz"
        var_Cs.C_format = "%7.0f"
        self.apply_wavelength_data(times, var_Cs, data_Cs)

        var_Cbs = g.createVariable("backscattering_counts", "f8", ("time", "wavelength"), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_Cbs)
        var_Cbs.variable_id = "Cbs"
        var_Cbs.coverage_content_type = "physicalMeasurement"
        var_Cbs.cell_methods = "time: mean"
        var_Cbs.long_name = "backwards hemispheric scattering photon count rate"
        var_Cbs.units = "Hz"
        var_Cbs.C_format = "%7.0f"
        self.apply_wavelength_data(times, var_Cbs, data_Cbs)

        var_Cf = g.createVariable("reference_counts", "f8", ("time", "wavelength"), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_Cf)
        var_Cf.variable_id = "Cf"
        var_Cf.coverage_content_type = "physicalMeasurement"
        var_Cf.cell_methods = "time: mean"
        var_Cf.long_name = "reference shutter photon count rate"
        var_Cf.units = "Hz"
        var_Cf.C_format = "%7.0f"
        self.apply_wavelength_data(times, var_Cf, data_Cf)

        self.apply_cut_size(g, times, [
            (var_P, data_P),
            (var_T, data_T),
            (var_Tx, data_Tx),
            (var_U, data_U),
            (var_Cd, data_Cd),
        ], [
            (var_Bs, data_Bs),
            (var_Bbs, data_Bbs),
            (var_Cs, data_Cs),
            (var_Cbs, data_Cbs),
            (var_Cf, data_Cf),
        ], extra_sources=[data_system_flags])
        self.apply_coverage(g, times,f"Bs{self.WAVELENGTHS[selected_idx][1]}_{self.instrument_id}")

        g, times = self.state_group(data_Bsw, name="zero")
        selected_idx = 0
        for wlidx in range(len(self.WAVELENGTHS)):
            if data_Bsw[wlidx].time.shape[0] > data_Bsw[selected_idx].time.shape[0]:
                selected_idx = wlidx
        stp_vars = self._instrument_stp(g, f"Bsw{self.WAVELENGTHS[selected_idx][1]}_{self.instrument_id}")

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
        if stp_vars:
            var_Bsw.ancillary_variables = stp_vars
        self.apply_wavelength_state(times, var_Bsw, data_Bsw)

        var_Bbsw = g.createVariable("wall_backscattering_coefficient", "f8", ("time", "wavelength"), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_Bbsw)
        netcdf_var.variable_wall_back_scattering(var_Bbsw)
        var_Bbsw.variable_id = "Bbsw"
        var_Bbsw.coverage_content_type = "physicalMeasurement"
        var_Bbsw.cell_methods = "time: point"
        if stp_vars:
            var_Bbsw.ancillary_variables = stp_vars
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

        if any([v.time.shape != 0 for v in data_Bsn]) and data_Bn.value.shape[0] > 0 and data_Bn.value.shape[1] > 0:
            angles = data_Bn.value[-1,:]

            def declare_angle_dimension(g):
                g.createDimension("angle", angles.shape[0])
                var_angle = g.createVariable("angle", "f8", ("angle",), fill_value=nan)
                var_angle.variable_id = "Bn"
                var_angle.coverage_content_type = "coordinate"
                var_angle.long_name = "polar scattering start angle (zero is total scattering)"
                var_angle.units = "degrees"
                var_angle.C_format = "%2.0f"
                var_angle[:] = angles

            g, times = self.data_group(data_Bsn + [system_flags_time], fill_gaps=False)
            selected_idx = 0
            for wlidx in range(len(self.WAVELENGTHS)):
                if data_Bsn[wlidx].time.shape[0] > data_Bsn[selected_idx].time.shape[0]:
                    selected_idx = wlidx
            stp_vars = self._instrument_stp(g, f"Bsn{self.WAVELENGTHS[selected_idx][1]}_{self.instrument_id}")
            declare_angle_dimension(g)

            var_Bsn = g.createVariable("polar_scattering_coefficient", "f8", ("time", "angle", "wavelength"), fill_value=nan)
            netcdf_var.variable_total_scattering(var_Bsn)
            netcdf_timeseries.variable_coordinates(g, var_Bsn)
            var_Bsn.variable_id = "Bsn"
            var_Bsn.coverage_content_type = "physicalMeasurement"
            var_Bsn.cell_methods = "time: mean"
            if stp_vars:
                var_Bsn.ancillary_variables = stp_vars
            for wlidx in range(len(self.WAVELENGTHS)):
                self.apply_data(
                    times, var_Bsn, data_Bsn[wlidx].time, data_Bsn[wlidx].value, (slice(None), wlidx,),
                )

            g, times = self.state_group(data_Bsw, name="polar_zero")
            selected_idx = 0
            for wlidx in range(len(self.WAVELENGTHS)):
                if data_Bsnw[wlidx].time.shape[0] > data_Bsnw[selected_idx].time.shape[0]:
                    selected_idx = wlidx
            stp_vars = self._instrument_stp(g, f"Bsnw{self.WAVELENGTHS[selected_idx][1]}_{self.instrument_id}")
            declare_angle_dimension(g)

            var_Tw = g.createVariable("zero_temperature", "f8", ("time",), fill_value=nan)
            netcdf_var.variable_temperature(var_Tw)
            netcdf_timeseries.variable_coordinates(g, var_Tw)
            var_Tw.coverage_content_type = "physicalMeasurement"
            var_Tw.cell_methods = "time: point"
            var_Tw.long_name = "measurement cell temperature during the zero"
            self.apply_state(times, var_Tw, data_Tw)

            var_Pw = g.createVariable("zero_pressure", "f8", ("time",), fill_value=nan)
            netcdf_var.variable_pressure(var_Pw)
            netcdf_timeseries.variable_coordinates(g, var_Pw)
            var_Pw.coverage_content_type = "physicalMeasurement"
            var_Pw.cell_methods = "time: point"
            var_Pw.long_name = "measurement cell pressure during the zero"
            self.apply_state(times, var_Pw, data_Pw)

            var_Bsnw = g.createVariable("polar_wall_scattering_coefficient", "f8", ("time", "angle", "wavelength"), fill_value=nan)
            netcdf_timeseries.variable_coordinates(g, var_Bsnw)
            var_Bsnw.variable_id = "Bsnw"
            var_Bsnw.coverage_content_type = "physicalMeasurement"
            var_Bsnw.cell_methods = "time: point"
            var_Bsnw.long_name = "polar light scattering coefficient from wall signal"
            var_Bsnw.units = "Mm-1"
            var_Bsnw.C_format = "%7.2f"
            if stp_vars:
                var_Bsnw.ancillary_variables = stp_vars
            for wlidx in range(len(self.WAVELENGTHS)):
                self.apply_state(
                    times, var_Bsnw, data_Bsnw[wlidx].time, data_Bsnw[wlidx].value, (slice(None), wlidx,),
                )

            g, times = self.state_group(data_PCTnc, name="polar_spancheck")
            standard_temperature(g)
            standard_pressure(g)
            declare_angle_dimension(g)

            var_PCTnc = g.createVariable("polar_scattering_percent_error", "f8", ("time", "angle", "wavelength"), fill_value=nan)
            netcdf_timeseries.variable_coordinates(g, var_PCTnc)
            var_PCTnc.variable_id = "PCTnc"
            var_PCTnc.coverage_content_type = "physicalMeasurement"
            var_PCTnc.cell_methods = "time: point"
            var_PCTnc.long_name = "spancheck polar light scattering percent error"
            var_PCTnc.units = "%"
            var_PCTnc.C_format = "%6.2f"
            var_PCTnc.ancillary_variables = "standard_temperature standard_pressure"
            for wlidx in range(len(self.WAVELENGTHS)):
                self.apply_state(
                    times, var_PCTnc, data_PCTnc[wlidx].time, data_PCTnc[wlidx].value, (slice(None), wlidx,),
                )

        if parameters.value.shape[0] > 0:
            self._declare_parameters(dict(parameters.value[-1]))

        self.apply_instrument_metadata(
            [f"Bs{code}_{self.instrument_id}" for _, code in self.WAVELENGTHS],
            manufacturer="Ecotech", model="Aurora"
        )
        return True
