import typing
import asyncio
from pathlib import Path
from forge.units import ZERO_C_IN_K
from forge.temp import WorkingDirectory
from forge.product.selection import InstrumentSelection
from . import EBASFile
from .aerosol_instrument import AerosolInstrument


class Level0File(EBASFile, AerosolInstrument):
    @property
    def instrument_selection(self) -> typing.Iterable[InstrumentSelection]:
        return [InstrumentSelection(
            require_tags=["cpc"],
            exclude_tags=["secondary"],
        )]

    @property
    def instrument_type(self) -> str:
        return 'cpc'

    @property
    def file_metadata(self) -> typing.Dict[str, str]:
        r = super().file_metadata
        r.update(self.level0_metadata)
        r.update({
            'unit': '1/cm3',
            'comp_name': 'particle_number_concentration',
            'std_method': 'None',
            'method': f'{self.lab_code}_cpc_ref',
            'zero_negative': 'Zero possible',
            'zero_negative_desc': 'Zero values may appear due to statistical variations at very low concentrations',
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
            temperature_condensor = matrix.variable(
                comp_name="temperature",
                unit="K",
                location="CPC condensor",
                matrix="instrument",
                title="T_con",
            )
            temperature_saturator = matrix.variable(
                comp_name="temperature",
                unit="K",
                location="CPC saturator",
                matrix="instrument",
                title="T_con",
            )
            humidity = matrix.variable(
                comp_name="relative_humidity",
                unit="%",
                location="instrument internal",
                matrix="instrument",
                title="RH_int",
            )
            flow_rate = matrix.variable(
                comp_name="flow_rate",
                unit="l/min",
                location="sample line",
                matrix="instrument",
                title="act_flow",
            )
            cnc = matrix.variable(
                comp_name="particle_number_concentration",
                unit="1/cm3",
                statistics='arithmetic mean',
                title="conc",
            )
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
                        {"variable_name": "condenser_temperature"},
                ):
                    temperature_condensor[nas].integrate_variable(
                        var, selector(var, require_cut_size_match=False),
                        converter=lambda x: x + ZERO_C_IN_K
                    )
                for var in self.select_variable(
                        root,
                        {"variable_name": "saturator_temperature"},
                ):
                    temperature_saturator[nas].integrate_variable(
                        var, selector(var, require_cut_size_match=False),
                        converter=lambda x: x + ZERO_C_IN_K
                    )

                for var in self.select_variable(
                        root,
                        {"variable_name": "number_concentration"},
                        {"standard_name": "number_concentration_of_ambient_aerosol_particles_in_air"},
                ):
                    cnc[nas].integrate_variable(var, selector(var))

        for var in flow_rate:
            var.add_characteristic('Nominal/measured', 'measured', self.instrument_type, var.metadata.comp_name, '0')

        for nas in matrix:
            instrument[nas].set_serial_number(nas)
            self.apply_inlet(nas)
            await self.assemble_file(
                nas, output_directory,
                [cnc[nas]],
                optional=[pressure[nas], temperature[nas], humidity[nas], flow_rate[nas]],
                flags=flags[nas],
            )


class Level1File(EBASFile, AerosolInstrument):
    @property
    def instrument_selection(self) -> typing.Iterable[InstrumentSelection]:
        return [InstrumentSelection(
            require_tags=["cpc"],
            exclude_tags=["secondary"],
        )]

    @property
    def instrument_type(self) -> str:
        return 'cpc'

    @property
    def file_metadata(self) -> typing.Dict[str, str]:
        r = super().file_metadata
        r.update(self.level1_metadata)
        r.update({
            'unit': '1/cm3',
            'comp_name': 'particle_number_concentration',
            'std_method': 'None',
            'method': f'{self.lab_code}_cpc_ref',
            'vol_std_temp': '273.15K',
            'vol_std_pressure': '1013.25hPa',
            'zero_negative': 'Zero possible',
            'zero_negative_desc': 'Zero values may appear due to statistical variations at very low concentrations',
        })
        return r

    async def __call__(self, output_directory: Path) -> None:
        async with WorkingDirectory() as data_directory:
            data_directory = Path(data_directory)
            await self.fetch_instrument_files(self.instrument_selection, 'clean', data_directory)

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
                title="RH_int",
            )
            cnc = matrix.variable(
                comp_name="particle_number_concentration",
                unit="1/cm3",
                statistics='arithmetic mean',
                title="conc",
            )
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
                        {"variable_name": "number_concentration"},
                        {"standard_name": "number_concentration_of_ambient_aerosol_particles_in_air"},
                ):
                    cnc[nas].integrate_variable(var, selector(var))

        for nas in matrix:
            instrument[nas].set_serial_number(nas)
            self.apply_inlet(nas)
            await self.assemble_file(
                nas, output_directory,
                [cnc[nas]],
                optional=[pressure[nas], temperature[nas], humidity[nas]],
                flags=flags[nas],
            )


class Level2File(EBASFile, AerosolInstrument):
    @property
    def instrument_selection(self) -> typing.Iterable[InstrumentSelection]:
        return [InstrumentSelection(
            require_tags=["cpc"],
            exclude_tags=["secondary"],
        )]

    @property
    def instrument_type(self) -> str:
        return 'cpc'

    @property
    def file_metadata(self) -> typing.Dict[str, str]:
        r = super().file_metadata
        r.update(self.level2_metadata)
        r.update({
            'unit': '1/cm3',
            'comp_name': 'particle_number_concentration',
            'std_method': 'None',
            'method': f'{self.lab_code}_cpc_ref',
            'vol_std_temp': '273.15K',
            'vol_std_pressure': '1013.25hPa',
            'zero_negative': 'Zero possible',
            'zero_negative_desc': 'Zero values may appear due to statistical variations at very low concentrations',
        })
        return r

    async def __call__(self, output_directory: Path) -> None:
        async with WorkingDirectory() as data_directory:
            data_directory = Path(data_directory)
            await self.fetch_instrument_files(self.instrument_selection, 'avgh', data_directory)

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
                title="RH_int",
            )
            cnc = matrix.variable(
                comp_name="particle_number_concentration",
                unit="1/cm3",
                statistics='arithmetic mean',
                title="conc",
            )
            cnc_q16 = matrix.variable(
                comp_name="particle_number_concentration",
                unit="1/cm3",
                statistics='percentile:15.87',
                title="conc_16pc",
            )
            cnc_q84 = matrix.variable(
                comp_name="particle_number_concentration",
                unit="1/cm3",
                statistics='percentile:84.13',
                title="conc_84pc",
            )
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
                        {"variable_name": "number_concentration"},
                        {"standard_name": "number_concentration_of_ambient_aerosol_particles_in_air"},
                ):
                    cnc[nas].integrate_variable(var, selector(var))
                for var in self.select_variable(
                        root,
                        {"variable_name": "number_concentration"},
                        {"standard_name": "number_concentration_of_ambient_aerosol_particles_in_air"},
                        statistics="quantiles",
                ):
                    cnc_q16[nas].integrate_variable(
                        var, selector(var),
                        converter=self.quantile_converter(var, 0.1587),
                    )
                    cnc_q84[nas].integrate_variable(
                        var, selector(var),
                        converter=self.quantile_converter(var, 0.8413),
                    )

        for nas in matrix:
            instrument[nas].set_serial_number(nas)
            self.apply_inlet(nas)
            await self.assemble_file(
                nas, output_directory,
                [cnc[nas]],
                optional=[pressure[nas], temperature[nas], humidity[nas], cnc_q16[nas], cnc_q84[nas]],
                flags=flags[nas],
                fixed_interval_ms=60 * 60 * 1000,
            )
