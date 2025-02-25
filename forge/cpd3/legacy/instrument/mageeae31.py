import typing
import numpy as np
import forge.data.structure.variable as netcdf_var
import forge.data.structure.timeseries as netcdf_timeseries
from math import nan, isfinite
from .converter import WavelengthConverter
from forge.data.structure.stp import standard_temperature, standard_pressure


class Converter(WavelengthConverter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._average_interval: typing.Optional[float] = None
        self._wavelengths = [
            (370.0, "1"),
            (470.0, "2"),
            (520.0, "3"),
            (590.0, "4"),
            (660.0, "5"),
            (880.0, "6"),
            (950.0, "7"),
        ]

    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"aerosol", "aethalometer", "absorption", "mageeae31"}

    @property
    def instrument_type(self) -> typing.Optional[str]:
        return "mageeae31"

    @property
    def average_interval(self) -> typing.Optional[float]:
        return self._average_interval

    @property
    def WAVELENGTHS(self):
        return self._wavelengths

    def _declare_parameters(self, parameters: typing.Dict[str, typing.Any]) -> None:
        if not isinstance(parameters, dict):
            return
        if not parameters:
            return

        g = self.root.createGroup("parameters")

        var_parameters = g.createVariable("instrument_parameters", str, (), fill_value=False)
        var_parameters.coverage_content_type = "referenceInformation"
        var_parameters.long_name = "instrument response to the $AE33:SG command, representing the raw parameters values from the instrument configuration without formatting context"
        var_parameters[0] = str(parameters.get("Frame", ""))

        data_sigma = parameters.get("Sigma")
        if data_sigma:
            data_sigma = [float(v) for v in data_sigma]
            g.createDimension("mass_absorption_efficiency", len(data_sigma))
            var_sigma = g.createVariable("mass_absorption_efficiency", "f8", ("mass_absorption_efficiency",), fill_value=False)
            var_sigma.coverage_content_type = "referenceInformation"
            var_sigma.long_name = "the efficiency factor used to convert absorption coefficients into an equivalent black carbon"
            var_sigma.units = "m2 g"
            var_sigma.C_format = "%5.2f"
            var_sigma[:] = data_sigma

    def run(self) -> bool:
        if self.load_variable(f"X7_{self.instrument_id}").time.shape[0] > 0:
            self._wavelengths = [
                (370.0, "1"),
                (470.0, "2"),
                (520.0, "3"),
                (590.0, "4"),
                (660.0, "5"),
                (880.0, "6"),
                (950.0, "7"),
            ]
        elif self.load_variable(f"X2_{self.instrument_id}").time.shape[0] > 0:
            self._wavelengths = [
                (880.0, "1"),
                (370.0, "2"),
            ]
        else:
            self._wavelengths = [
                (880.0, "1"),
            ]

        data_X = self.load_wavelength_variable("X")
        if not any([v.time.shape[0] != 0 for v in data_X]):
            return False
        self._average_interval = self.calculate_average_interval(np.concatenate([v.time for v in data_X]))
        if not super().run():
            return False

        instrument_sample_t: float = nan
        instrument_sample_p: float = nan
        mean_ratio: float = nan
        spot_area: float = nan
        abs_efficiency: typing.Dict[float, float] = dict()

        def process_meta(meta) -> typing.Dict[str, typing.Tuple[str, str]]:
            nonlocal instrument_sample_t
            nonlocal instrument_sample_p
            nonlocal mean_ratio
            nonlocal spot_area
            wl = meta.get("Wavelength")
            processing = meta.get("Processing")
            if processing and isinstance(processing, list):
                processing = processing[0]
                if isinstance(processing, dict):
                    processing = processing.get("Parameters", dict())
                    instrument_sample_t = float(processing.get("SampleT", instrument_sample_t))
                    instrument_sample_p = float(processing.get("SampleP", instrument_sample_p))
                    mean_ratio = float(processing.get("MeanRatio", mean_ratio))
                    spot_area = float(processing.get("SpotSize", spot_area))

                    e = processing.get("Efficiency")
                    if e and wl:
                        abs_efficiency[float(wl)] = float(e)
            return dict()

        self.apply_instrument_metadata(
            [f"X{code}_{self.instrument_id}" for _, code in self.WAVELENGTHS] +
            [f"Ba{code}_{self.instrument_id}" for _, code in self.WAVELENGTHS],
            manufacturer="Magee", generic_model="AE31",
            extra=process_meta,
        )

        data_Ba = self.load_wavelength_variable("Ba")
        data_Ir = self.load_wavelength_variable("Ir")
        data_If = self.load_wavelength_variable("If")
        data_Ip = self.load_wavelength_variable("Ip")
        data_Q = self.load_variable(f"Q_{self.instrument_id}")

        g, times = self.data_group(data_X)
        standard_temperature(g)
        standard_pressure(g)
        self.declare_system_flags(g, times)

        var_X = g.createVariable("equivalent_black_carbon", "f8", ("time", "wavelength"), fill_value=nan)
        netcdf_var.variable_ebc(var_X)
        netcdf_timeseries.variable_coordinates(g, var_X)
        var_X.variable_id = "X"
        var_X.coverage_content_type = "physicalMeasurement"
        var_X.cell_methods = "time: mean"
        var_X.ancillary_variables = "standard_temperature standard_pressure"
        self.apply_wavelength_data(times, var_X, data_X)

        var_Ba = g.createVariable("light_absorption", "f8", ("time", "wavelength"), fill_value=nan)
        netcdf_var.variable_absorption(var_Ba, is_stp=True)
        netcdf_timeseries.variable_coordinates(g, var_Ba)
        var_Ba.variable_id = "Ba"
        var_Ba.coverage_content_type = "physicalMeasurement"
        var_Ba.cell_methods = "time: mean"
        var_Ba.ancillary_variables = "standard_temperature standard_pressure"
        self.apply_wavelength_data(times, var_Ba, data_Ba)

        var_Ir = g.createVariable("transmittance", "f8", ("time", "wavelength"), fill_value=nan)
        netcdf_var.variable_transmittance(var_Ir)
        netcdf_timeseries.variable_coordinates(g, var_Ir)
        var_Ir.variable_id = "Ir"
        var_Ir.coverage_content_type = "physicalMeasurement"
        var_Ir.cell_methods = "time: last"
        self.apply_wavelength_data(times, var_Ir, data_Ir)

        var_If = g.createVariable("reference_intensity", "f8", ("time", "wavelength"), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_If)
        var_If.variable_id = "If"
        var_If.coverage_content_type = "physicalMeasurement"
        var_If.cell_methods = "time: mean"
        var_If.long_name = "sensing beam signal"
        var_If.C_format = "%7.4f"
        self.apply_wavelength_data(times, var_If, data_If)

        var_Ip = g.createVariable("sample_intensity", "f8", ("time", "wavelength"), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_Ip)
        var_Ip.variable_id = "Ip"
        var_Ip.coverage_content_type = "physicalMeasurement"
        var_Ip.cell_methods = "time: mean"
        var_Ip.long_name = "reference beam signal"
        var_Ip.C_format = "%7.4f"
        self.apply_wavelength_data(times, var_Ip, data_Ip)

        var_Q = g.createVariable("sample_flow", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_sample_flow(var_Q)
        netcdf_timeseries.variable_coordinates(g, var_Q)
        var_Q.variable_id = "Q"
        var_Q.coverage_content_Qype = "physicalMeasurement"
        var_Q.cell_methods = "time: mean"
        var_Q.C_format = "%6.3f"
        var_Q.ancillary_variables = "standard_temperature standard_pressure"
        self.apply_data(times, var_Q, data_Q)

        self.apply_cut_size(g, times, [
            (var_Q, data_Q),
        ], [
            (var_X, data_X),
            (var_Ba, data_Ba),
            (var_Ir, data_Ir),
            (var_If, data_If),
            (var_Ip, data_Ip),
        ])
        selected_idx = 0
        for wlidx in range(len(self.WAVELENGTHS)):
            if data_X[wlidx].time.shape[0] > data_X[selected_idx].time.shape[0]:
                selected_idx = wlidx
        self.apply_coverage(g, times, f"X{self.WAVELENGTHS[selected_idx][1]}_{self.instrument_id}")

        if abs_efficiency or isfinite(spot_area) or isfinite(instrument_sample_t) or isfinite(instrument_sample_p) or isfinite(mean_ratio):
            g = self.root.createGroup("parameters")

            if abs_efficiency:
                data_sigma = [float(abs_efficiency.get(wl, nan)) for wl, _ in self.WAVELENGTHS]
                g.createDimension("mass_absorption_efficiency", len(data_sigma))
                var_sigma = g.createVariable("mass_absorption_efficiency", "f8", ("mass_absorption_efficiency",),
                                             fill_value=nan)
                var_sigma.coverage_content_type = "referenceInformation"
                var_sigma.long_name = "the efficiency factor used to convert absorption coefficients into an equivalent black carbon"
                var_sigma.units = "m2 g"
                var_sigma.C_format = "%5.2f"
                var_sigma[:] = data_sigma

            if isfinite(spot_area):
                var_spot_area = g.createVariable("spot_area", "f8", (), fill_value=False)
                var_spot_area.coverage_content_type = "referenceInformation"
                var_spot_area.long_name = "sampling spot area"
                var_spot_area.C_format = "%5.2f"
                var_spot_area.units = "mm2"
                var_spot_area[0] = spot_area

            if isfinite(instrument_sample_t):
                var_instrument_standard_temperature = g.createVariable("instrument_standard_temperature", "f8", (), fill_value=False)
                var_instrument_standard_temperature.coverage_content_type = "referenceInformation"
                var_instrument_standard_temperature.long_name = "standard temperature of instrument data"
                var_instrument_standard_temperature.C_format = "%5.2f"
                var_instrument_standard_temperature.units = "degC"
                var_instrument_standard_temperature[0] = instrument_sample_t

            if isfinite(instrument_sample_p):
                var_instrument_standard_pressure = g.createVariable("instrument_standard_pressure", "f8", (), fill_value=False)
                var_instrument_standard_pressure.coverage_content_type = "referenceInformation"
                var_instrument_standard_pressure.long_name = "standard pressure of instrument data"
                var_instrument_standard_pressure.C_format = "%7.2f"
                var_instrument_standard_pressure.units = "hPa"
                var_instrument_standard_pressure[0] = instrument_sample_p

            if isfinite(mean_ratio):
                var_mean_ratio = g.createVariable("mean_ratio", "f8", (), fill_value=False)
                var_mean_ratio.coverage_content_type = "referenceInformation"
                var_mean_ratio.long_name = "instrument mean ratio correction factor"
                var_mean_ratio.C_format = "%5.3f"
                var_mean_ratio[0] = mean_ratio

        return True
