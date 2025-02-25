import typing
import numpy as np
import forge.data.structure.variable as netcdf_var
import forge.data.structure.timeseries as netcdf_timeseries
from math import nan
from .converter import WavelengthConverter
from forge.data.structure.stp import standard_temperature, standard_pressure


class Converter(WavelengthConverter):
    WAVELENGTHS = [
        (370.0, "1"),
        (470.0, "2"),
        (520.0, "3"),
        (590.0, "4"),
        (660.0, "5"),
        (880.0, "6"),
        (950.0, "7"),
    ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._average_interval: typing.Optional[float] = None

    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"aerosol", "aethalometer", "absorption", "mageeae33"}

    @property
    def instrument_type(self) -> typing.Optional[str]:
        return "mageeae33"

    @property
    def average_interval(self) -> typing.Optional[float]:
        return self._average_interval

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
        data_X = self.load_wavelength_variable("X")
        if not any([v.time.shape[0] != 0 for v in data_X]):
            return False
        self._average_interval = self.calculate_average_interval(np.concatenate([v.time for v in data_X]))
        if not super().run():
            return False

        data_Bac = self.load_wavelength_variable("Bac")
        data_Ba = self.load_wavelength_variable("Ba")
        data_Bas = self.load_wavelength_variable("Bas")
        data_Ir = self.load_wavelength_variable("Ir")
        data_Irs = self.load_wavelength_variable("Irs")
        data_If = self.load_wavelength_variable("If")
        data_Ip = self.load_wavelength_variable("Ip")
        data_Ips = self.load_wavelength_variable("Ips")
        data_ZFACTOR = self.load_wavelength_variable("ZFACTOR")
        data_Q1 = self.load_variable(f"Q1_{self.instrument_id}")
        data_Q2 = self.load_variable(f"Q2_{self.instrument_id}")
        data_T1 = self.load_variable(f"T1_{self.instrument_id}")
        data_T2 = self.load_variable(f"T2_{self.instrument_id}")
        data_T3 = self.load_variable(f"T3_{self.instrument_id}")

        data_Fn = self.load_state(f"Fn_{self.instrument_id}", dtype=np.uint64)
        spot = self.load_state(f"ZSPOT_{self.instrument_id}", dtype=dict)

        parameters = self.load_state(f"ZPARAMETERS_{self.instrument_id}", dtype=dict)

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

        var_Bac = g.createVariable("light_absorption", "f8", ("time", "wavelength"), fill_value=nan)
        netcdf_var.variable_absorption(var_Bac, is_stp=True)
        netcdf_timeseries.variable_coordinates(g, var_Bac)
        var_Bac.variable_id = "Bac"
        var_Bac.coverage_content_type = "physicalMeasurement"
        var_Bac.cell_methods = "time: mean"
        var_Bac.ancillary_variables = "standard_temperature standard_pressure"
        self.apply_wavelength_data(times, var_Bac, data_Bac)

        var_Ba = g.createVariable("spot_one_light_absorption", "f8", ("time", "wavelength"), fill_value=nan)
        netcdf_var.variable_absorption(var_Ba, is_stp=True)
        netcdf_timeseries.variable_coordinates(g, var_Ba)
        del var_Ba.standard_name
        var_Ba.variable_id = "Ba"
        var_Ba.coverage_content_type = "physicalMeasurement"
        var_Ba.cell_methods = "time: mean"
        var_Ba.long_name = "uncorrected light absorption coefficient at STP on spot one"
        var_Ba.ancillary_variables = "standard_temperature standard_pressure"
        self.apply_wavelength_data(times, var_Ba, data_Ba)

        var_Bas = g.createVariable("spot_two_light_absorption", "f8", ("time", "wavelength"), fill_value=nan)
        netcdf_var.variable_absorption(var_Bas, is_stp=True)
        netcdf_timeseries.variable_coordinates(g, var_Bas)
        del var_Bas.standard_name
        var_Bas.variable_id = "Bas"
        var_Bas.coverage_content_type = "physicalMeasurement"
        var_Bas.cell_methods = "time: mean"
        var_Bas.long_name = "uncorrected light absorption coefficient at STP on spot two"
        var_Bas.ancillary_variables = "standard_temperature standard_pressure"
        self.apply_wavelength_data(times, var_Bas, data_Bas)

        var_Ir = g.createVariable("spot_one_transmittance", "f8", ("time", "wavelength"), fill_value=nan)
        netcdf_var.variable_transmittance(var_Ir)
        netcdf_timeseries.variable_coordinates(g, var_Ir)
        var_Ir.variable_id = "Ir"
        var_Ir.coverage_content_type = "physicalMeasurement"
        var_Ir.cell_methods = "time: last"
        var_Ir.long_name = "transmittance fraction of light through the filter relative to the amount before sampling on spot one"
        self.apply_wavelength_data(times, var_Ir, data_Ir)

        var_Irs = g.createVariable("spot_two_transmittance", "f8", ("time", "wavelength"), fill_value=nan)
        netcdf_var.variable_transmittance(var_Irs)
        netcdf_timeseries.variable_coordinates(g, var_Irs)
        var_Irs.variable_id = "Irs"
        var_Irs.coverage_content_type = "physicalMeasurement"
        var_Irs.cell_methods = "time: last"
        var_Irs.long_name = "transmittance fraction of light through the filter relative to the amount before sampling on spot two"
        self.apply_wavelength_data(times, var_Irs, data_Irs)

        var_If = g.createVariable("reference_intensity", "f8", ("time", "wavelength"), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_If)
        var_If.variable_id = "If"
        var_If.coverage_content_type = "physicalMeasurement"
        var_If.cell_methods = "time: mean"
        var_If.long_name = "reference detector signal"
        var_If.C_format = "%6.0f"
        self.apply_wavelength_data(times, var_If, data_If)

        var_Ip = g.createVariable("spot_one_sample_intensity", "f8", ("time", "wavelength"), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_Ip)
        var_Ip.variable_id = "Ip"
        var_Ip.coverage_content_type = "physicalMeasurement"
        var_Ip.cell_methods = "time: mean"
        var_Ip.long_name = "sample detector signal on spot one"
        var_Ip.C_format = "%6.0f"
        self.apply_wavelength_data(times, var_Ip, data_Ip)

        var_Ips = g.createVariable("spot_two_sample_intensity", "f8", ("time", "wavelength"), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_Ips)
        var_Ips.variable_id = "Ips"
        var_Ips.coverage_content_type = "physicalMeasurement"
        var_Ips.cell_methods = "time: mean"
        var_Ips.long_name = "sample detector signal on spot two"
        var_Ips.C_format = "%6.0f"
        self.apply_wavelength_data(times, var_Ips, data_Ips)

        var_factor = g.createVariable("correction_factor", "f8", ("time", "wavelength"), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_factor)
        var_factor.variable_id = "ZFACTOR"
        var_factor.coverage_content_type = "physicalMeasurement"
        var_factor.cell_methods = "time: mean"
        var_factor.long_name = "correction factor applied to calculate the final EBC"
        var_factor.C_format = "%9.6f"
        self.apply_wavelength_data(times, var_factor, data_ZFACTOR)

        var_Q1 = g.createVariable("spot_one_flow", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_flow(var_Q1)
        netcdf_timeseries.variable_coordinates(g, var_Q1)
        var_Q1.variable_id = "Q1"
        var_Q1.coverage_content_Q1ype = "physicalMeasurement"
        var_Q1.cell_methods = "time: mean"
        var_Q1.long_name = "sample flow through spot one"
        var_Q1.C_format = "%7.3f"
        self.apply_data(times, var_Q1, data_Q1)

        var_Q2 = g.createVariable("spot_two_flow", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_flow(var_Q2)
        netcdf_timeseries.variable_coordinates(g, var_Q2)
        var_Q2.variable_id = "Q2"
        var_Q2.coverage_content_Q2ype = "physicalMeasurement"
        var_Q2.cell_methods = "time: mean"
        var_Q2.long_name = "sample flow through spot two"
        var_Q2.C_format = "%7.3f"
        self.apply_data(times, var_Q2, data_Q2)

        var_T1 = g.createVariable("controller_temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_temperature(var_T1)
        netcdf_timeseries.variable_coordinates(g, var_T1)
        var_T1.variable_id = "T1"
        var_T1.coverage_content_T1ype = "physicalMeasurement"
        var_T1.cell_methods = "time: mean"
        var_T1.long_name = "controller board temperature"
        self.apply_data(times, var_T1, data_T1)

        var_T2 = g.createVariable("supply_temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_temperature(var_T2)
        netcdf_timeseries.variable_coordinates(g, var_T2)
        var_T2.variable_id = "T2"
        var_T2.coverage_content_T2ype = "physicalMeasurement"
        var_T2.cell_methods = "time: mean"
        var_T2.long_name = "power supply board temperature"
        self.apply_data(times, var_T2, data_T2)

        var_T3 = g.createVariable("led_temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_temperature(var_T3)
        netcdf_timeseries.variable_coordinates(g, var_T3)
        var_T3.variable_id = "T3"
        var_T3.coverage_content_T3ype = "physicalMeasurement"
        var_T3.cell_methods = "time: mean"
        var_T3.long_name = "LED board temperature"
        self.apply_data(times, var_T3, data_T3)

        self.apply_cut_size(g, times, [
            (var_Q1, data_Q1),
            (var_Q2, data_Q2),
            (var_T1, data_T1),
            (var_T2, data_T2),
            (var_T3, data_T3),
        ], [
            (var_X, data_X),
            (var_Bac, data_Bac),
            (var_Ba, data_Ba),
            (var_Bas, data_Bas),
            (var_Ir, data_Ir),
            (var_Irs, data_Irs),
            (var_If, data_If),
            (var_Ip, data_Ip),
            (var_Ips, data_Ips),
            (var_factor, data_ZFACTOR),
        ])
        selected_idx = 0
        for wlidx in range(len(self.WAVELENGTHS)):
            if data_X[wlidx].time.shape[0] > data_X[selected_idx].time.shape[0]:
                selected_idx = wlidx
        self.apply_coverage(g, times, f"X{self.WAVELENGTHS[selected_idx][1]}_{self.instrument_id}")

        g, times = self.state_group([data_Fn, spot])

        var_Fn = g.createVariable("tape_advance", "u8", ("time",), fill_value=False)
        netcdf_timeseries.variable_coordinates(g, var_Fn)
        var_Fn.variable_id = "Fn"
        var_Fn.coverage_content_type = "auxiliaryInformation"
        var_Fn.cell_methods = "time: point"
        var_Fn.long_name = "tape advance count"
        self.apply_state(times, var_Fn, data_Fn)

        var_In = g.createVariable("spot_one_normalization", "f8", ("time", "wavelength"), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_In)
        var_In.variable_id = "In"
        var_In.coverage_content_type = "physicalMeasurement"
        var_In.cell_methods = "time: point"
        var_In.long_name = "sample/reference intensity at spot one sampling start"
        var_In.units = "1"
        var_In.C_format = "%9.7f"

        var_Ins = g.createVariable("spot_two_normalization", "f8", ("time", "wavelength"), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_Ins)
        var_Ins.variable_id = "Ins"
        var_Ins.coverage_content_type = "physicalMeasurement"
        var_Ins.cell_methods = "time: point"
        var_Ins.long_name = "sample/reference intensity at spot two sampling start"
        var_Ins.units = "1"
        var_Ins.C_format = "%9.7f"

        data_In = list()
        data_Ins = list()
        for spot_data in spot.value:
            spot_one = list(spot_data.get("In1", []))
            if len(spot_one) < len(self.WAVELENGTHS):
                spot_one += [nan] * (len(self.WAVELENGTHS) - len(spot_one))
            spot_one = spot_one[:len(self.WAVELENGTHS)]
            data_In.append(spot_one)

            spot_two = list(spot_data.get("In2", []))
            if len(spot_two) < len(self.WAVELENGTHS):
                spot_two += [nan] * (len(self.WAVELENGTHS) - len(spot_two))
            spot_two = spot_two[:len(self.WAVELENGTHS)]
            data_Ins.append(spot_two)
        data_In = np.array(data_In, dtype=np.float64)
        data_Ins = np.array(data_Ins, dtype=np.float64)
        for wlidx in range(len(self.WAVELENGTHS)):
            if len(data_In.shape) == 2 and data_In.shape[0] > 0 and data_In.shape[1] > 0:
                self.apply_state(times, var_In, spot.time, data_In[:,wlidx], (wlidx,))
            if len(data_Ins.shape) == 2 and data_Ins.shape[0] > 0 and data_Ins.shape[1] > 0:
                self.apply_state(times, var_Ins, spot.time, data_Ins[:,wlidx], (wlidx,))

        if parameters.value.shape[0] > 0:
            self._declare_parameters(dict(parameters.value[-1]))

        self.apply_instrument_metadata(
            [f"X{code}_{self.instrument_id}" for _, code in self.WAVELENGTHS],
            manufacturer="Magee", model="AE33"
        )
        return True
