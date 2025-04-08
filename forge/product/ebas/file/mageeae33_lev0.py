import typing
import numpy as np
from pathlib import Path
from math import isfinite, nan, inf
from forge.units import ZERO_C_IN_K
from forge.temp import WorkingDirectory
from forge.product.selection import InstrumentSelection
from .spectral import SpectralFile
from .aerosol_instrument import AerosolInstrument


class File(SpectralFile, AerosolInstrument):
    WAVELENGTH_BANDS: typing.List[typing.Tuple[float, float]] = [
        (-inf, 420.0),
        (420.0, 495.0),
        (495.0, 555.0),
        (555.0, 625.0),
        (625.0, 770.0),
        (770.0, 915.0),
        (915.0, inf),
    ]

    @property
    def instrument_selection(self) -> typing.Iterable[InstrumentSelection]:
        return [InstrumentSelection(
            instrument_type=["mageeae33"],
            exclude_tags=["secondary"],
        )]

    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"aerosol", "absorption", "aethalometer", "mageeae33"}

    @property
    def instrument_manufacturer(self) -> str:
        return "Magee"

    @property
    def instrument_model(self) -> str:
        return "AE33"

    @property
    def instrument_name(self) -> str:
        return f'Magee_AE33_{self.station.upper()}'

    @property
    def instrument_type(self) -> str:
        return 'filter_absorption_photometer'

    @property
    def file_metadata(self) -> typing.Dict[str, str]:
        r = super().file_metadata
        r.update(self.level0_metadata)
        r.update({
            'std_method': 'Single-angle_Correction=Drinovec2015',
            'method': f'{self.lab_code}_AE33',
            'unit': 'ug/m3',
            'comp_name': 'equivalent_black_carbon',
            'vol_std_temp': '273.15K',
            'vol_std_pressure': '1013.25hPa',
            'zero_negative': 'Zero/negative possible',
            'zero_negative_desc': 'Zero and neg. values may appear due to statistical variations at very low concentrations',
        })
        return r

    @staticmethod
    def _transmittance_to_atn(x: np.ndarray) -> np.ndarray:
        result = np.full_like(x, np.nan)
        valid = np.logical_and(np.isfinite(x), x > 0.0)
        result[valid] = np.log(x[valid]) * -100.0
        return result

    async def __call__(self, output_directory: Path) -> None:
        async with WorkingDirectory() as data_directory:
            data_directory = Path(data_directory)
            await self.fetch_instrument_files(self.instrument_selection, 'raw', data_directory)

            matrix = self.MatrixData(self)
            flags = matrix.flags()
            instrument = matrix.metadata_tracker()
            pressure = matrix.variable(
                comp_name="pressure",
                unit="hPa",
                location="instrument internal",
                matrix="instrument",
                title="p_int",
            )
            temperature = matrix.variable(
                comp_name="temperature",
                unit="K",
                location="instrument internal",
                matrix="instrument",
                title="T_int",
            )
            temperature_control_board = matrix.variable(
                comp_name="temperature",
                unit="K",
                location="control board",
                matrix="instrument",
                title="Tcntrl",
            )
            temperature_supply_board = matrix.variable(
                comp_name="temperature",
                unit="K",
                location="power supply board",
                matrix="instrument",
                title="Tsupply",
            )
            temperature_LED = matrix.variable(
                comp_name="temperature",
                unit="K",
                location="LED board",
                matrix="instrument",
                title="T_LED",
            )
            flow_rate_1 = matrix.variable(
                comp_name="flow_rate",
                unit="l/min",
                location="filter spot 1",
                matrix="instrument",
                title="flow1",
            )
            flow_rate_2 = matrix.variable(
                comp_name="flow_rate",
                unit="l/min",
                location="filter spot 2",
                matrix="instrument",
                title="flow2",
            )
            filter_number = matrix.variable(
                comp_name="filter_number",
                matrix="instrument",
                title="tpcnt",
            )
            reference_beam_signal = matrix.spectral_variable()
            sensing_beam_signal_1 = matrix.spectral_variable()
            sensing_beam_signal_2 = matrix.spectral_variable()
            equivalent_black_carbon_1 = matrix.spectral_variable()
            equivalent_black_carbon_2 = matrix.spectral_variable()
            equivalent_black_carbon_corrected = matrix.spectral_variable()
            filter_loading_compensation_parameter = matrix.spectral_variable()
            attenuation_coefficient_1 = matrix.spectral_variable()
            attenuation_coefficient_2 = matrix.spectral_variable()
            wavelength_efficiency = {round(wl): 6833.0 / wl for wl in [370.0, 470.0, 520.0, 590.0, 660.0, 880.0, 950.0]}
            async for nas, selector, root in matrix.iter_data_files(data_directory):
                flags[nas].integrate_file(root, selector)
                instrument[nas].integrate_file(root)
                for var in self.select_variable(
                        root,
                        {"standard_name": "air_temperature"},
                ):
                    temperature[nas].integrate_variable(
                        var, selector(var, require_cut_size_match=False),
                        converter=lambda x: x + ZERO_C_IN_K
                    )
                for var in self.select_variable(
                        root,
                        {"variable_name": "controller_temperature"},
                ):
                    temperature_control_board[nas].integrate_variable(
                        var, selector(var, require_cut_size_match=False),
                        converter=lambda x: x + ZERO_C_IN_K
                    )
                for var in self.select_variable(
                        root,
                        {"variable_name": "supply_temperature"},
                ):
                    temperature_supply_board[nas].integrate_variable(
                        var, selector(var, require_cut_size_match=False),
                        converter=lambda x: x + ZERO_C_IN_K
                    )
                for var in self.select_variable(
                        root,
                        {"variable_name": "led_temperature"},
                ):
                    temperature_LED[nas].integrate_variable(
                        var, selector(var, require_cut_size_match=False),
                        converter=lambda x: x + ZERO_C_IN_K
                    )
                for var in self.select_variable(
                        root,
                        {"standard_name": "air_pressure"},
                ):
                    pressure[nas].integrate_variable(var, selector(var, require_cut_size_match=False))
                for var in self.select_variable(
                        root,
                        {"variable_name": "spot_one_flow"},
                ):
                    flow_rate_1[nas].integrate_variable(var, selector(var, require_cut_size_match=False))
                for var in self.select_variable(
                        root,
                        {"variable_name": "spot_two_flow"},
                ):
                    flow_rate_2[nas].integrate_variable(var, selector(var, require_cut_size_match=False))
                for var in self.select_variable(
                        root,
                        {"variable_name": "tape_advance"},
                ):
                    filter_number[nas].integrate_variable(var, selector(var, require_cut_size_match=False))

                for var in self.select_variable(
                        root,
                        {"variable_name": "equivalent_black_carbon"},
                ):
                    equivalent_black_carbon_corrected[nas].integrate_variable(
                        var, selector(var),
                    )
                for var in self.select_variable(
                        root,
                        {"variable_name": "reference_intensity"},
                ):
                    reference_beam_signal[nas].integrate_variable(
                        var, selector(var),
                    )
                for var in self.select_variable(
                        root,
                        {"variable_name": "spot_one_sample_intensity"},
                ):
                    sensing_beam_signal_1[nas].integrate_variable(
                        var, selector(var),
                    )
                for var in self.select_variable(
                        root,
                        {"variable_name": "spot_two_sample_intensity"},
                ):
                    sensing_beam_signal_2[nas].integrate_variable(
                        var, selector(var),
                    )
                for var in self.select_variable(
                        root,
                        {"variable_name": "spot_one_transmittance"},
                ):
                    attenuation_coefficient_1[nas].integrate_variable(
                        var, selector(var),
                        converter=self._transmittance_to_atn,
                    )
                for var in self.select_variable(
                        root,
                        {"variable_name": "spot_two_transmittance"},
                ):
                    attenuation_coefficient_2[nas].integrate_variable(
                        var, selector(var),
                        converter=self._transmittance_to_atn,
                    )
                for var in self.select_variable(
                        root,
                        {"variable_name": "correction_factor"},
                ):
                    filter_loading_compensation_parameter[nas].integrate_variable(
                        var, selector(var),
                    )

                wavelengths = [370.0, 470.0, 520.0, 590.0, 660.0, 880.0, 950.0]
                data_group = root.groups.get("data")
                if data_group is not None:
                    wavelength_var = data_group.variables.get("wavelength")
                    if wavelength_var is not None:
                        wavelengths = wavelength_var[:].data.tolist()

                wavelength_efficiency = {round(wl): 6833.0 / wl for wl in wavelengths if isfinite(wl)}
                parameters_group = root.groups.get("parameters")
                if parameters_group is not None:
                    efficiency_var = parameters_group.variables.get("mass_absorption_efficiency")
                    if efficiency_var is not None:
                        values = efficiency_var[:].data.tolist()
                        for wlidx in range(len(wavelengths)):
                            wavelength = wavelengths[wlidx]
                            if not isfinite(wavelength):
                                continue
                            wavelength = round(wavelength)
                            if wlidx < len(values) and isfinite(values[wlidx]):
                                wavelength_efficiency[wavelength] = values[wlidx]

                def absorption_to_ebc(wavelength: float) -> typing.Optional[typing.Callable[[np.ndarray], np.ndarray]]:
                    efficiency = wavelength_efficiency.get(wavelength, nan)
                    if not isfinite(efficiency) or efficiency <= 0.0:
                        return lambda x: np.full_like(x, nan)
                    def converter(x: np.ndarray) -> np.ndarray:
                        return x / efficiency
                    return converter

                for var in self.select_variable(
                        root,
                        {"variable_name": "spot_one_light_absorption"},
                ):
                    equivalent_black_carbon_1[nas].integrate_variable(
                        var, selector(var),
                        wavelength_converter=absorption_to_ebc,
                    )
                for var in self.select_variable(
                        root,
                        {"variable_name": "spot_two_light_absorption"},
                ):
                    equivalent_black_carbon_2[nas].integrate_variable(
                        var, selector(var),
                        wavelength_converter=absorption_to_ebc,
                    )

        for var in reference_beam_signal:
            var.apply_metadata(
                title='ref_{wavelength}',
                comp_name='reference_beam_signal',
            )
        for var in sensing_beam_signal_1:
            var.apply_metadata(
                title='sens1_{wavelength}',
                comp_name='sensing_beam_signal',
                location="filter spot 1",
            )
        for var in sensing_beam_signal_2:
            var.apply_metadata(
                title='sens2_{wavelength}',
                comp_name='sensing_beam_signal',
                location="filter spot 2",
            )
        for var in equivalent_black_carbon_1:
            var.apply_metadata(
                title='EBC1_{wavelength}',
                comp_name='equivalent_black_carbon',
                unit='ug/m3',
                location="filter spot 1",
                filter_area="0.785",
                detection_limit=[0.03, "ug/m3"],
                detection_limit_desc="Adapted from manufacturer specification",
                uncertainty=[100.0, '%'],
                multi_scattering_corr_fact=1.57,
            )
            efficiency = wavelength_efficiency.get(var.wavelength, nan)
            if isfinite(efficiency):
                var.metadata.mass_abs_cross_section = f"{efficiency:.2f}"
        for var in equivalent_black_carbon_2:
            var.apply_metadata(
                title='EBC2_{wavelength}',
                comp_name='equivalent_black_carbon',
                unit='ug/m3',
                location="filter spot 2",
                filter_area="0.785",
                detection_limit=[0.03, "ug/m3"],
                detection_limit_desc="Adapted from manufacturer specification",
                uncertainty=[100.0, '%'],
                multi_scattering_corr_fact=1.57,
            )
            efficiency = wavelength_efficiency.get(var.wavelength, nan)
            if isfinite(efficiency):
                var.metadata.mass_abs_cross_section = f"{efficiency:.2f}"
        for var in equivalent_black_carbon_corrected:
            var.apply_metadata(
                title='EBC_{wavelength}',
                comp_name='equivalent_black_carbon',
                unit='ug/m3',
                detection_limit=[0.03, "ug/m3"],
                detection_limit_desc="Adapted from manufacturer specification",
                uncertainty=[20.0, '%'],
                multi_scattering_corr_fact=1.57,
            )
            efficiency = wavelength_efficiency.get(var.wavelength, nan)
            if isfinite(efficiency):
                var.metadata.mass_abs_cross_section = f"{efficiency:.2f}"

        for nas in matrix:
            instrument[nas].set_serial_number(nas)
            self.apply_inlet(nas)
            await self.assemble_file(
                nas, output_directory,
                equivalent_black_carbon_corrected[nas],
                optional=[pressure[nas], temperature[nas], temperature_control_board[nas],
                          temperature_supply_board[nas], temperature_LED[nas], filter_number[nas],
                          flow_rate_1[nas], flow_rate_2[nas]] +
                         list(reference_beam_signal[nas]) + list(filter_loading_compensation_parameter[nas]) +
                         list(sensing_beam_signal_1[nas]) + list(sensing_beam_signal_2[nas]) +
                         list(equivalent_black_carbon_1[nas]) + list(equivalent_black_carbon_2[nas]) +
                         list(attenuation_coefficient_1[nas]) + list(attenuation_coefficient_2[nas]),
                flags=flags[nas],
            )
