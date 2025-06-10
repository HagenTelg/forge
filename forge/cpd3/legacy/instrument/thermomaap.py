import typing
import numpy as np
import forge.data.structure.variable as netcdf_var
import forge.data.structure.timeseries as netcdf_timeseries
from math import nan
from .converter import WavelengthConverter, read_archive, Selection
from forge.data.structure.stp import standard_temperature, standard_pressure


class Converter(WavelengthConverter):
    WAVELENGTHS = [
        (670.0, "R"),
    ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._average_interval: typing.Optional[float] = None

    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"aerosol", "absorption", "thermomaap"}

    @property
    def instrument_type(self) -> typing.Optional[str]:
        return "thermomaap"

    @property
    def average_interval(self) -> typing.Optional[float]:
        return self._average_interval

    def _declare_parameters(self, parameters: typing.Optional[typing.List[str]], data_sigma: typing.Optional[float]) -> None:
        g = self.root.createGroup("parameters")

        if parameters:
            var_parameters = g.createVariable("instrument_parameters", str, (), fill_value=False)
            var_parameters.coverage_content_type = "referenceInformation"
            var_parameters.long_name = "instrument format 8 record"
            var_parameters[0] = "\n".join(parameters)

        if data_sigma:
            var_sigma = g.createVariable("mass_absorption_efficiency", "f8", (), fill_value=False)
            var_sigma.coverage_content_type = "referenceInformation"
            var_sigma.long_name = "the efficiency factor used to convert absorption coefficients into an equivalent black carbon"
            var_sigma.units = "m2 g"
            var_sigma.C_format = "%5.2f"
            var_sigma[0] = data_sigma

    def run(self) -> bool:
        data_X = self.load_wavelength_variable("X")
        if not any([v.time.shape[0] != 0 for v in data_X]):
            return False
        self._average_interval = self.calculate_average_interval(np.concatenate([v.time for v in data_X]))
        if not super().run():
            return False

        data_Bac = self.load_wavelength_variable("Bac")
        data_Ba = self.load_wavelength_variable("Ba")
        data_Ir = self.load_wavelength_variable("Ir")
        data_If = self.load_wavelength_variable("If")
        data_Ip = self.load_wavelength_variable("Ip")
        data_Is1 = self.load_wavelength_variable("Is1")
        if not any([v.time.shape[0] != 0 for v in data_Is1]):
            data_Is1 = [self.load_variable(f"Is1_{self.instrument_id}")]
        data_Is2 = self.load_wavelength_variable("Is2")
        if not any([v.time.shape[0] != 0 for v in data_Is2]):
            data_Is2 = [self.load_variable(f"Is2_{self.instrument_id}")]
        data_ZSSA = self.load_wavelength_variable("ZSSA")
        data_Q = self.load_variable(f"Q_{self.instrument_id}")
        data_Ld = self.load_variable(f"Ld_{self.instrument_id}")
        data_T1 = self.load_variable(f"T1_{self.instrument_id}")
        data_T2 = self.load_variable(f"T2_{self.instrument_id}")
        data_T3 = self.load_variable(f"T3_{self.instrument_id}")
        data_P = self.load_variable(f"P_{self.instrument_id}")
        data_Pd1 = self.load_variable(f"Pd1_{self.instrument_id}")
        data_Pd2 = self.load_variable(f"Pd2_{self.instrument_id}")

        parameters = self.load_state(f"ZPARAMETERS_{self.instrument_id}", dtype=list)

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

        var_Ba = g.createVariable("uncorrected_light_absorption", "f8", ("time", "wavelength"), fill_value=nan)
        netcdf_var.variable_absorption(var_Ba, is_stp=True)
        netcdf_timeseries.variable_coordinates(g, var_Ba)
        var_Ba.variable_id = "Ba"
        var_Ba.coverage_content_type = "physicalMeasurement"
        var_Ba.cell_methods = "time: mean"
        var_Ba.ancillary_variables = "standard_temperature standard_pressure"
        var_Ba.long_name = "uncorrected light absorption coefficient at STP"
        del var_Ba.standard_name
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
        var_If.long_name = "reference detector signal"
        var_If.C_format = "%7.2f"
        self.apply_wavelength_data(times, var_If, data_If)

        var_Ip = g.createVariable("sample_intensity", "f8", ("time", "wavelength"), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_Ip)
        var_Ip.variable_id = "Ip"
        var_Ip.coverage_content_type = "physicalMeasurement"
        var_Ip.cell_methods = "time: mean"
        var_Ip.long_name = "forward detector signal"
        var_Ip.C_format = "%7.2f"
        self.apply_wavelength_data(times, var_Ip, data_Ip)

        var_Is1 = g.createVariable("backscatter_135_intensity", "f8", ("time", "wavelength"), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_Is1)
        var_Is1.variable_id = "Is1"
        var_Is1.coverage_content_type = "physicalMeasurement"
        var_Is1.cell_methods = "time: mean"
        var_Is1.long_name = "135 degree backscatter detector signal"
        var_Is1.C_format = "%7.2f"
        self.apply_wavelength_data(times, var_Is1, data_Is1)

        var_Is2 = g.createVariable("backscatter_165_intensity", "f8", ("time", "wavelength"), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_Is2)
        var_Is2.variable_id = "Is2"
        var_Is2.coverage_content_type = "physicalMeasurement"
        var_Is2.cell_methods = "time: mean"
        var_Is2.long_name = "165 degree backscatter detector signal"
        var_Is2.C_format = "%7.2f"
        self.apply_wavelength_data(times, var_Is2, data_Is2)

        var_ZSSA = g.createVariable("single_scattering_albedo", "f8", ("time", "wavelength"), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_ZSSA)
        var_ZSSA.variable_id = "ZSSA"
        var_ZSSA.coverage_content_type = "physicalMeasurement"
        var_ZSSA.cell_methods = "time: mean"
        var_ZSSA.long_name = "model result scattering / extinction of the aerosol-filter layer"
        var_ZSSA.C_format = "%8.6f"
        self.apply_wavelength_data(times, var_ZSSA, data_ZSSA)

        var_Q = g.createVariable("sample_flow", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_sample_flow(var_Q)
        netcdf_timeseries.variable_coordinates(g, var_Q)
        var_Q.variable_id = "Q"
        var_Q.coverage_content_type = "physicalMeasurement"
        var_Q.cell_methods = "time: mean"
        var_Q.C_format = "%6.3f"
        var_Q.ancillary_variables = "standard_temperature standard_pressure"
        self.apply_data(times, var_Q, data_Q)

        var_T1 = g.createVariable("sample_temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_air_temperature(var_T1)
        netcdf_timeseries.variable_coordinates(g, var_T1)
        var_T1.variable_id = "T1"
        var_T1.coverage_content_T1ype = "physicalMeasurement"
        var_T1.cell_methods = "time: mean"
        self.apply_data(times, var_T1, data_T1)

        var_T2 = g.createVariable("measurement_head_temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_temperature(var_T2)
        netcdf_timeseries.variable_coordinates(g, var_T2)
        var_T2.variable_id = "T2"
        var_T2.coverage_content_T2ype = "physicalMeasurement"
        var_T2.cell_methods = "time: mean"
        var_T2.long_name = "measuring head temperature"
        self.apply_data(times, var_T2, data_T2)

        var_T3 = g.createVariable("system_temperature", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_temperature(var_T3)
        netcdf_timeseries.variable_coordinates(g, var_T3)
        var_T3.variable_id = "T3"
        var_T3.coverage_content_T3ype = "physicalMeasurement"
        var_T3.cell_methods = "time: mean"
        var_T3.long_name = "system temperature"
        self.apply_data(times, var_T3, data_T3)

        var_P = g.createVariable("sample_pressure", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_air_pressure(var_P)
        netcdf_timeseries.variable_coordinates(g, var_P)
        var_P.variable_id = "P"
        var_P.coverage_content_type = "physicalMeasurement"
        var_P.cell_methods = "time: mean"
        var_P.long_name = "sample pressure"
        self.apply_data(times, var_P, data_P)

        var_Pd1 = g.createVariable("orifice_pressure_drop", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_delta_pressure(var_Pd1)
        netcdf_timeseries.variable_coordinates(g, var_Pd1)
        var_Pd1.variable_id = "Pd1"
        var_Pd1.coverage_content_Pd1ype = "physicalMeasurement"
        var_Pd1.cell_methods = "time: mean"
        var_Pd1.long_name = "pressure drop from ambient to orifice face"
        var_Pd1.C_format = "%7.2f"
        self.apply_data(times, var_Pd1, data_Pd1)

        var_Pd2 = g.createVariable("vacuum_pressure_drop", "f8", ("time",), fill_value=nan)
        netcdf_var.variable_delta_pressure(var_Pd2)
        netcdf_timeseries.variable_coordinates(g, var_Pd2)
        var_Pd2.variable_id = "Pd2"
        var_Pd2.coverage_content_Pd2ype = "physicalMeasurement"
        var_Pd2.cell_methods = "time: mean"
        var_Pd2.long_name = "vacuum pressure pump drop across orifice"
        var_Pd2.C_format = "%7.2f"
        self.apply_data(times, var_Pd2, data_Pd2)

        var_Ld = g.createVariable("path_length_change", "f8", ("time",), fill_value=nan)
        netcdf_timeseries.variable_coordinates(g, var_Ld)
        var_Ld.variable_id = "Ld"
        var_Ld.coverage_content_type = "physicalMeasurement"
        var_Ld.cell_methods = "time: sum"
        var_Ld.long_name = "change in path sample path length (flow/area)"
        var_Ld.units = "m"
        var_Ld.C_format = "%7.4f"
        var_Ld.ancillary_variables = "standard_temperature standard_pressure"
        data_L_start = self.Data(*self.convert_loaded(read_archive([Selection(
            start=self.file_start,
            end=self.file_end,
            stations=[self.station],
            archives=[self.archive],
            variables=[f"L_{self.instrument_id}"],
            include_meta_archive=False,
            include_default_station=False,
            lacks_flavors=["cover", "stats", "end"],
        )]), is_state=False, dtype=np.float64, return_cut_size=True))
        data_L_end = self.Data(*self.convert_loaded(read_archive([Selection(
            start=self.file_start,
            end=self.file_end,
            stations=[self.station],
            archives=[self.archive],
            variables=[f"L_{self.instrument_id}"],
            include_meta_archive=False,
            include_default_station=False,
            has_flavors=["end"],
            lacks_flavors=["cover", "stats"],
        )]), is_state=False, dtype=np.float64))
        if data_L_start.time.shape[0] > 0:
            calc_Ld = np.full(data_L_start.time.shape, nan, dtype=np.float64)
            if data_L_end.time.shape[0] > 0:
                out_begin = np.searchsorted(data_L_start.time, data_L_end.time[0], side="left")
                out_end = np.searchsorted(data_L_start.time, data_L_end.time[-1], side="right")
                if (out_end - out_begin) == data_L_end.time.shape[0]:
                    calc_Ld[out_begin:out_end] = data_L_end.value - data_L_start.value[out_begin:out_end]

            if data_L_start.time.shape[0] > 1:
                diff_Ld = data_L_start.value[1:] - data_L_start.value[:-1]
                diff_Ld = np.concatenate((
                    [diff_Ld[0]],
                    diff_Ld
                ))
                calc_dest = np.invert(np.isfinite(calc_Ld))
                calc_Ld[calc_dest] = diff_Ld[calc_dest]

            self.apply_data(times, var_Ld, data_L_start.time, calc_Ld)
        self.apply_data(times, var_Ld, data_Ld)

        self.apply_cut_size(g, times, [
            (var_Q, data_Q),
            (var_Ld, data_Ld),
            (var_T1, data_T1),
            (var_T2, data_T2),
            (var_T3, data_T3),
            (var_P, data_P),
            (var_Pd1, data_Pd1),
            (var_Pd2, data_Pd2),
        ], [
            (var_X, data_X),
            (var_Ba, data_Ba),
            (var_Bac, data_Bac),
            (var_Ir, data_Ir),
            (var_If, data_If),
            (var_Ip, data_Ip),
            (var_Is1, data_Is1),
            (var_Is2, data_Is2),
            (var_ZSSA, data_ZSSA),
        ])
        selected_idx = 0
        for wlidx in range(len(self.WAVELENGTHS)):
            if data_X[wlidx].time.shape[0] > data_X[selected_idx].time.shape[0]:
                selected_idx = wlidx
        self.apply_coverage(g, times, f"X{self.WAVELENGTHS[selected_idx][1]}_{self.instrument_id}")


        abs_efficiency: typing.Optional[float] = None

        def process_meta(meta) -> typing.Dict[str, typing.Tuple[str, str]]:
            nonlocal abs_efficiency
            processing = meta.get("Processing")
            if processing and isinstance(processing, list):
                processing = processing[0]
                if isinstance(processing, dict):
                    processing = processing.get("Parameters", dict())

                    e = processing.get("Efficiency")
                    if e:
                        abs_efficiency = float(e)
            return dict()

        self.apply_instrument_metadata(
            [f"X{code}_{self.instrument_id}" for _, code in self.WAVELENGTHS] +
            [f"Bac{code}_{self.instrument_id}" for _, code in self.WAVELENGTHS],
            manufacturer="Thermo", model="MAAP",
            extra=process_meta,
        )

        if parameters.value.shape[0] > 0:
            self._declare_parameters(list(parameters.value[-1]), abs_efficiency)
        elif abs_efficiency:
            self._declare_parameters(None, abs_efficiency)

        return True
