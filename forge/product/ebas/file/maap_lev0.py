import typing
from pathlib import Path
from forge.units import ZERO_C_IN_K
from forge.temp import WorkingDirectory
from forge.product.selection import InstrumentSelection
from .spectral import SpectralFile
from .aerosol_instrument import AerosolInstrument


class File(SpectralFile, AerosolInstrument):
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
            'method': f'{self.lab_code}_MAAP',
            'unit': 'ug/m3',
            'comp_name': 'equivalent_black_carbon',
            'vol_std_temp': '273.15K',
            'vol_std_pressure': '1013.25hPa',
            'zero_negative': 'Zero/negative possible',
            'zero_negative_desc': 'Zero and neg. values may appear due to statistical variations at very low concentrations',
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
            humidity = matrix.variable(
                comp_name="relative_humidity",
                unit="%",
                location="instrument internal",
                matrix="instrument",
                title="RH",
            )
            flow_rate = matrix.variable(
                comp_name="flow_rate",
                unit="l/min",
                location="sample line",
                matrix="instrument",
                title="flow",
            )
            ebc = matrix.spectral_variable()
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
                    ebc[nas].integrate_variable(
                        var, selector(var),
                    )

        for var in ebc:
            var.apply_metadata(
                title='BCconc{wavelength}',
                comp_name='equivalent_black_carbon',
                uncertainty='6%',
                unit='ug/m3',
            )

        for nas in matrix:
            instrument[nas].set_serial_number(nas)
            self.apply_inlet(nas)
            await self.assemble_file(
                nas, output_directory,
                ebc[nas],
                optional=[pressure[nas], temperature[nas], humidity[nas], flow_rate[nas]],
                flags=flags[nas],
            )
