import typing
from pathlib import Path
from math import inf, isfinite
from forge.units import ZERO_C_IN_K
from forge.temp import WorkingDirectory
from forge.product.selection import InstrumentSelection
from .spectral import SpectralFile
from .aerosol_instrument import AerosolInstrument


class File(SpectralFile, AerosolInstrument):
    WAVELENGTH_BANDS: typing.List[typing.Tuple[float, float]] = [(-inf, inf),]

    @property
    def instrument_selection(self) -> typing.Iterable[InstrumentSelection]:
        return [InstrumentSelection(
            instrument_type=["thermomaap"],
            exclude_tags=["secondary"],
        )]

    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"aerosol", "absorption", "thermomaap"}

    @property
    def instrument_manufacturer(self) -> str:
        return "Thermo"

    @property
    def instrument_model(self) -> str:
        return "5012"

    @property
    def instrument_name(self) -> str:
        return f'Thermo_5012_{self.station.upper()}'

    @property
    def instrument_type(self) -> str:
        return 'filter_absorption_photometer'

    @property
    def file_metadata(self) -> typing.Dict[str, str]:
        r = super().file_metadata
        r.update(self.level0_metadata)
        r.update({
            'std_method': 'Multi-angle_Correction=Petzold2004',
            'method': f'{self.lab_code}_MAAP_5012',
            'unit': 'ug/m3',
            'comp_name': 'equivalent_black_carbon',
            'vol_std_temp': '273.15K',
            'vol_std_pressure': '1013.25hPa',
            'zero_negative': 'Zero/negative possible',
            'zero_negative_desc': 'Zero and neg. values may appear due to statistical variations at very low concentrations',
            'detection_limit': [0.1, "ug/m3"],
            'detection_limit_desc': "Determined by instrument noise characteristics, no detection limit flag used",
        })
        return r

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
                matrix="instrument",
                title="pres_amb",
            )
            inlet_temperature = matrix.variable(
                comp_name="temperature",
                unit="K",
                matrix="instrument",
                title="temp_int",
            )
            internal_temperature = matrix.variable(
                comp_name="temperature",
                unit="K",
                matrix="instrument",
                title="temp_int",
            )
            humidity = matrix.variable(
                comp_name="relative_humidity",
                unit="%",
                matrix="instrument",
                title="rh_inl",
            )
            flow_rate = matrix.variable(
                comp_name="flow_rate",
                unit="l/min",
                matrix="instrument",
                title="flow_stp",
            )
            equivalent_black_carbon = matrix.spectral_variable()
            ebc_efficiency = 6.6
            async for nas, selector, root in matrix.iter_data_files(data_directory):
                flags[nas].integrate_file(root, selector)
                instrument[nas].integrate_file(root)
                for var in self.select_variable(
                        root,
                        {"standard_name": "air_temperature"},
                ):
                    internal_temperature[nas].integrate_variable(
                        var, selector(var, require_cut_size_match=False),
                        converter=lambda x: x + ZERO_C_IN_K
                    )
                for var in self.select_variable(
                        root,
                        {"variable_name": "inlet_temperature"},
                ):
                    inlet_temperature[nas].integrate_variable(
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
                        {"standard_name": "relative_humidity"},
                ):
                    humidity[nas].integrate_variable(var, selector(var, require_cut_size_match=False))
                for var in self.select_variable(
                        root,
                        {"variable_name": "sample_flow"},
                ):
                    flow_rate[nas].integrate_variable(var, selector(var, require_cut_size_match=False))

                for var in self.select_variable(
                        root,
                        {"variable_name": "equivalent_black_carbon"},
                ):
                    equivalent_black_carbon[nas].integrate_variable(
                        var, selector(var),
                    )

                parameters_group = root.groups.get("parameters")
                if parameters_group is not None:
                    efficiency_var = parameters_group.variables.get("mass_absorption_efficiency")
                    if efficiency_var is not None:
                        values = efficiency_var[:].data.tolist()
                        if len(values) > 0 and isfinite(values[0]):
                            ebc_efficiency = values[0]

        for var in equivalent_black_carbon:
            var.apply_metadata(
                title='ebc_conc{wavelength}',
                comp_name='equivalent_black_carbon',
                uncertainty=[6.0, '%'],
                uncertainty_desc='typical value of unit-to-unit variability',
                mass_abs_cross_section=ebc_efficiency,
                unit='ug/m3',
            )

        for var in pressure:
            var.add_characteristic('Location', 'instrument internal', self.instrument_type, var.metadata.comp_name, '0')
        for var in inlet_temperature:
            var.add_characteristic('Location', 'instrument inlet', self.instrument_type, var.metadata.comp_name, '0')
        for var in internal_temperature:
            var.add_characteristic('Location', 'instrument internal', self.instrument_type, var.metadata.comp_name, '0')
        for var in humidity:
            var.add_characteristic('Location', 'instrument outlet', self.instrument_type, var.metadata.comp_name, '0')
        for var in flow_rate:
            var.add_characteristic('Location', 'sample line', self.instrument_type, var.metadata.comp_name, '0')

        for nas in matrix:
            instrument[nas].set_serial_number(nas)
            self.apply_inlet(nas)
            await self.assemble_file(
                nas, output_directory,
                equivalent_black_carbon[nas],
                optional=[pressure[nas], inlet_temperature[nas], internal_temperature[nas],
                          humidity[nas], flow_rate[nas]],
                flags=flags[nas],
            )
