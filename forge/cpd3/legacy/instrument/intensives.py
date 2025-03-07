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
        (550.0, "G"),
        (700.0, "R"),
    ]
    ANGSTROM = [
        "BG",
        "BR",
        "GR",
    ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._average_interval: typing.Optional[float] = None

    @property
    def archive(self) -> str:
        return "clean"

    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"aerosol", "intensives"}

    @property
    def average_interval(self) -> typing.Optional[float]:
        return self._average_interval

    def load_angstrom_variable(self, prefix: str) -> typing.List["WavelengthConverter.Data"]:
        result: typing.List[WavelengthConverter.Data] = list()
        for code in self.ANGSTROM:
            result.append(self.load_variable(
                f"{prefix}{code}_{self.instrument_id}",
            ))
        return result

    def run(self) -> bool:
        data_Bs = self.load_wavelength_variable("Bs")
        data_Ba = self.load_wavelength_variable("Ba")
        data_Be = self.load_wavelength_variable("Be")
        if not any([v.time.shape[0] != 0 for v in data_Bs]) and not any([v.time.shape[0] != 0 for v in data_Ba]) and not any([v.time.shape[0] != 0 for v in data_Be]):
            return False
        self._average_interval = self.calculate_average_interval(np.concatenate([v.time for v in data_Bs] + [v.time for v in data_Ba] + [v.time for v in data_Be]))
        if not super().run():
            return False

        data_Bbs = self.load_wavelength_variable("Bbs")
        data_N = self.load_variable(f"N_{self.instrument_id}")
        data_ZBfr = self.load_wavelength_variable("ZBfr")
        data_ZG = self.load_wavelength_variable("ZG")
        data_ZSSA = self.load_wavelength_variable("ZSSA")
        data_ZRFE = self.load_wavelength_variable("ZRFE")
        data_ZAngBs = self.load_angstrom_variable("ZAngBs")
        data_ZAngBa = self.load_angstrom_variable("ZAngBa")

        system_flags_time = self.load_variable(f"F1?_{self.instrument_id}", convert=bool, dtype=np.bool_).time

        g, times = self.data_group(data_Bs + data_Ba + data_Be + [system_flags_time], fill_gaps=False)
        standard_temperature(g)
        standard_pressure(g)
        data_system_flags, system_flags_bits = self.declare_system_flags(g, times)

        if any([v.time.shape[0] != 0 for v in data_Bs]):
            var_Bs = g.createVariable("scattering_coefficient", "f8", ("time", "wavelength"), fill_value=nan)
            netcdf_var.variable_total_scattering(var_Bs, is_stp=True)
            netcdf_timeseries.variable_coordinates(g, var_Bs)
            var_Bs.variable_id = "Bs"
            var_Bs.coverage_content_type = "physicalMeasurement"
            var_Bs.cell_methods = "time: mean"
            var_Bs.ancillary_variables = "standard_temperature standard_pressure"
            self.apply_wavelength_data(times, var_Bs, data_Bs)
        else:
            var_Bs = None

        if any([v.time.shape[0] != 0 for v in data_Bbs]):
            var_Bbs = g.createVariable("backscattering_coefficient", "f8", ("time", "wavelength"), fill_value=nan)
            netcdf_var.variable_back_scattering(var_Bbs, is_stp=True)
            netcdf_timeseries.variable_coordinates(g, var_Bbs)
            var_Bbs.variable_id = "Bbs"
            var_Bbs.coverage_content_type = "physicalMeasurement"
            var_Bbs.cell_methods = "time: mean"
            var_Bbs.ancillary_variables = "standard_temperature standard_pressure"
            self.apply_wavelength_data(times, var_Bbs, data_Bbs)
        else:
            var_Bbs = None

        if any([v.time.shape[0] != 0 for v in data_Ba]):
            var_Ba = g.createVariable("light_absorption", "f8", ("time", "wavelength"), fill_value=nan)
            netcdf_var.variable_absorption(var_Ba, is_stp=True)
            netcdf_timeseries.variable_coordinates(g, var_Ba)
            var_Ba.variable_id = "Ba"
            var_Ba.coverage_content_type = "physicalMeasurement"
            var_Ba.cell_methods = "time: mean"
            var_Ba.ancillary_variables = "standard_temperature standard_pressure"
            self.apply_wavelength_data(times, var_Ba, data_Ba)
        else:
            var_Ba = None

        if any([v.time.shape[0] != 0 for v in data_Be]):
            var_Be = g.createVariable("light_extinction", "f8", ("time", "wavelength"), fill_value=nan)
            netcdf_var.variable_extinction(var_Be, is_stp=True)
            netcdf_timeseries.variable_coordinates(g, var_Be)
            var_Be.variable_id = "Be"
            var_Be.coverage_content_type = "physicalMeasurement"
            var_Be.cell_methods = "time: mean"
            var_Be.ancillary_variables = "standard_temperature standard_pressure"
            self.apply_wavelength_data(times, var_Be, data_Be)
        else:
            var_Be = None

        if data_N.time.shape[0] != 0:
            var_N = g.createVariable("number_concentration", "f8", ("time",), fill_value=nan)
            netcdf_var.variable_number_concentration(var_N, is_stp=True)
            netcdf_timeseries.variable_coordinates(g, var_N)
            var_N.variable_id = "N"
            var_N.coverage_content_type = "physicalMeasurement"
            var_N.cell_methods = "time: mean"
            var_N.ancillary_variables = "standard_temperature standard_pressure"
            self.apply_data(times, var_N, data_N)
        else:
            var_N = None

        if any([v.time.shape[0] != 0 for v in data_ZBfr]):
            var_ZBfr = g.createVariable("backscatter_fraction", "f8", ("time", "wavelength"), fill_value=nan)
            netcdf_timeseries.variable_coordinates(g, var_ZBfr)
            var_ZBfr.variable_id = "ZBfr"
            var_ZBfr.coverage_content_type = "physicalMeasurement"
            var_ZBfr.cell_methods = "time: mean"
            var_ZBfr.long_name = "ratio of backwards hemispheric light scattering to total light scattering"
            var_ZBfr.standard_name = "backscattering_ratio"
            var_ZBfr.units = "1"
            var_ZBfr.C_format = "%6.3f"
            var_ZBfr.ancillary_variables = "standard_temperature standard_pressure"
            self.apply_wavelength_data(times, var_ZBfr, data_ZBfr)
        else:
            var_ZBfr = None

        if any([v.time.shape[0] != 0 for v in data_ZG]):
            var_ZG = g.createVariable("asymmetry_parameter", "f8", ("time", "wavelength"), fill_value=nan)
            netcdf_timeseries.variable_coordinates(g, var_ZG)
            var_ZG.variable_id = "ZG"
            var_ZG.coverage_content_type = "physicalMeasurement"
            var_ZG.cell_methods = "time: mean"
            var_ZG.long_name = "scattering asymmetry parameter derived from the backscatter ratio"
            var_ZG.units = "1"
            var_ZG.C_format = "%6.3f"
            var_ZG.ancillary_variables = "standard_temperature standard_pressure"
            self.apply_wavelength_data(times, var_ZG, data_ZG)
        else:
            var_ZG = None

        if any([v.time.shape[0] != 0 for v in data_ZSSA]):
            var_ZSSA = g.createVariable("single_scattering_albedo", "f8", ("time", "wavelength"), fill_value=nan)
            netcdf_timeseries.variable_coordinates(g, var_ZSSA)
            var_ZSSA.variable_id = "ZSSA"
            var_ZSSA.coverage_content_type = "physicalMeasurement"
            var_ZSSA.cell_methods = "time: mean"
            var_ZSSA.long_name = "single scattering albedo (ratio of scattering to extinction)"
            var_ZSSA.units = "1"
            var_ZSSA.C_format = "%6.3f"
            var_ZSSA.ancillary_variables = "standard_temperature standard_pressure"
            self.apply_wavelength_data(times, var_ZSSA, data_ZSSA)
        else:
            var_ZSSA = None

        if any([v.time.shape[0] != 0 for v in data_ZRFE]):
            var_ZRFE = g.createVariable("radiative_forcing_efficiency", "f8", ("time", "wavelength"), fill_value=nan)
            netcdf_timeseries.variable_coordinates(g, var_ZRFE)
            var_ZRFE.variable_id = "ZRFE"
            var_ZRFE.coverage_content_type = "physicalMeasurement"
            var_ZRFE.cell_methods = "time: mean"
            var_ZRFE.long_name = "derived radiative forcing efficiency"
            var_ZRFE.C_format = "%7.2f"
            var_ZRFE.variable_id = "ZRFE"
            var_ZRFE.ancillary_variables = "standard_temperature standard_pressure"
            self.apply_wavelength_data(times, var_ZRFE, data_ZRFE)
        else:
            var_ZRFE = None

        if any([v.time.shape[0] != 0 for v in data_ZAngBs]):
            var_ZAngBs = g.createVariable("scattering_angstrom_exponent", "f8", ("time", "wavelength"), fill_value=nan)
            netcdf_timeseries.variable_coordinates(g, var_ZAngBs)
            var_ZAngBs.variable_id = "ZAngBs"
            var_ZAngBs.coverage_content_type = "physicalMeasurement"
            var_ZAngBs.cell_methods = "time: mean"
            var_ZAngBs.long_name = "scattering Ångström exponent from adjacent wavelengths"
            var_ZAngBs.units = "1"
            var_ZAngBs.C_format = "%6.3f"
            var_ZAngBs.variable_id = "ZAngBs"
            var_ZAngBs.ancillary_variables = "standard_temperature standard_pressure"
            self.apply_wavelength_data(times, var_ZAngBs, data_ZAngBs)
        else:
            var_ZAngBs = None

        if any([v.time.shape[0] != 0 for v in data_ZAngBa]):
            var_ZAngBa = g.createVariable("absorption_angstrom_exponent", "f8", ("time", "wavelength"), fill_value=nan)
            netcdf_timeseries.variable_coordinates(g, var_ZAngBa)
            var_ZAngBa.variable_id = "ZAngBa"
            var_ZAngBa.coverage_content_type = "physicalMeasurement"
            var_ZAngBa.cell_methods = "time: mean"
            var_ZAngBa.long_name = "absorption Ångström exponent from adjacent wavelengths"
            var_ZAngBa.units = "1"
            var_ZAngBa.C_format = "%6.3f"
            var_ZAngBa.variable_id = "ZAngBa"
            var_ZAngBa.ancillary_variables = "standard_temperature standard_pressure"
            self.apply_wavelength_data(times, var_ZAngBa, data_ZAngBa)
        else:
            var_ZAngBa = None

        self.apply_cut_size(g, times, [], [
            (var_Bs, data_Bs),
            (var_Bbs, data_Bbs),
            (var_Ba, data_Ba),
            (var_Be, data_Be),
            (var_ZBfr, data_ZBfr),
            (var_ZG, data_ZG),
            (var_ZSSA, data_ZSSA),
            (var_ZRFE, data_ZRFE),
            (var_ZAngBs, data_ZAngBs),
            (var_ZAngBa, data_ZAngBa),
        ], extra_sources=[data_system_flags])

        return True
