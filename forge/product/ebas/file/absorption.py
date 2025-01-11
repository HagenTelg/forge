import typing
import asyncio
from pathlib import Path
from forge.units import ZERO_C_IN_K
from forge.temp import WorkingDirectory
from forge.product.selection import InstrumentSelection
from .spectral import SpectralFile
from .aerosol_instrument import AerosolInstrument


class Level0File(SpectralFile, AerosolInstrument):
    @property
    def instrument_selection(self) -> typing.Iterable[InstrumentSelection]:
        return [InstrumentSelection(
            require_tags=["absorption"],
            exclude_tags=["secondary", "aethalometer", "thermomaap"],
        )]

    @property
    def instrument_type(self) -> str:
        return 'filter_absorption_photometer'

    @property
    def file_metadata(self) -> typing.Dict[str, str]:
        r = super().file_metadata
        r.update(self.level0_metadata)
        r.update({
            'unit': '1/Mm',
            'comp_name': 'aerosol_absorption_coefficient',
            'hum_temp_ctrl': 'Heating to 40% RH, limit 40 deg. C',
            'std_method': 'None',
            'method': f'{self.lab_code}_abs_coef',
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
                title="RH_int",
            )
            flow_rate = matrix.variable(
                comp_name="flow_rate",
                unit="l/min",
                location="sample line",
                matrix="instrument",
                title="flow",
            )
            absorption = matrix.spectral_variable()
            transmittance = matrix.spectral_variable()
            sample_intensity = matrix.spectral_variable()
            reference_intensity = matrix.spectral_variable()
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
                        {"variable_name": "light_absorption"},
                        {"standard_name": "volume_absorption_coefficient_in_air_due_to_dried_aerosol_particles"},
                        {"standard_name": "volume_extinction_coefficient_in_air_due_to_ambient_aerosol_particles"},
                ):
                    absorption[nas].integrate_variable(
                        var, selector(var),
                    )
                for var in self.select_variable(
                        root,
                        {"variable_name": "transmittance"},
                ):
                    transmittance[nas].integrate_variable(
                        var, selector(var),
                    )
                for var in self.select_variable(
                        root,
                        {"variable_name": "sample_intensity"},
                ):
                    sample_intensity[nas].integrate_variable(
                        var, selector(var),
                    )
                for var in self.select_variable(
                        root,
                        {"variable_name": "reference_intensity"},
                ):
                    reference_intensity[nas].integrate_variable(
                        var, selector(var),
                    )

        for var in absorption:
            var.apply_metadata(
                title='abs{wavelength}',
                comp_name='aerosol_absorption_coefficient',
                unit='1/Mm',
            )
        for var in transmittance:
            var.apply_metadata(
                title='Trans{wavelength}',
                comp_name='transmittance',
                unit='no unit',
            )
        for var in sample_intensity:
            var.apply_metadata(
                title='SampInt{wavelength}',
                comp_name='sample_intensity',
                unit='no unit',
            )
        for var in reference_intensity:
            var.apply_metadata(
                title='RefInt{wavelength}',
                comp_name='reference_intensity',
                unit='no unit',
            )

        for nas in matrix:
            instrument[nas].set_serial_number(nas)
            self.apply_inlet(nas)
            await self.assemble_file(
                nas, output_directory,
                absorption[nas],
                optional=[pressure[nas], temperature[nas], humidity[nas], flow_rate[nas]] +
                    list(transmittance[nas]) + list(sample_intensity[nas]) + list(reference_intensity[nas]),
                flags=flags[nas],
            )


class Level1File(SpectralFile, AerosolInstrument):
    @property
    def instrument_selection(self) -> typing.Iterable[InstrumentSelection]:
        return [InstrumentSelection(
            require_tags=["absorption"],
            exclude_tags=["secondary", "aethalometer", "thermomaap"],
        )]

    @property
    def instrument_type(self) -> str:
        return 'filter_absorption_photometer'

    @property
    def file_metadata(self) -> typing.Dict[str, str]:
        r = super().file_metadata
        r.update(self.level1_metadata)
        r.update({
            'unit': '1/Mm',
            'comp_name': 'aerosol_absorption_coefficient',
            'hum_temp_ctrl': 'Heating to 40% RH, limit 40 deg. C',
            'std_method': 'Single-angle_Correction=Bond1999_Ogren2010',
            'method': f'{self.lab_code}_abs_coef',
            'vol_std_temp': '273.15K',
            'vol_std_pressure': '1013.25hPa',
            'zero_negative': 'Zero/negative possible',
            'zero_negative_desc': 'Zero and neg. values may appear due to statistical variations at very low concentrations',
            'comment': 'Standard Bond et al. 1999 values for K1 and K2 used at all wavelengths',
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
            absorption = matrix.spectral_variable()
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
                        {"variable_name": "light_absorption"},
                        {"standard_name": "volume_absorption_coefficient_in_air_due_to_dried_aerosol_particles"},
                        {"standard_name": "volume_extinction_coefficient_in_air_due_to_ambient_aerosol_particles"},
                ):
                    absorption[nas].integrate_variable(
                        var, selector(var),
                    )

        for var in absorption:
            var.apply_metadata(
                title='abs{wavelength}',
                comp_name='aerosol_absorption_coefficient',
                unit='1/Mm',
            )

        for nas in matrix:
            instrument[nas].set_serial_number(nas)
            self.apply_inlet(nas)
            await self.assemble_file(
                nas, output_directory,
                absorption[nas],
                optional=[pressure[nas], temperature[nas], humidity[nas]],
                flags=flags[nas],
            )


class Level2File(SpectralFile, AerosolInstrument):
    @property
    def instrument_selection(self) -> typing.Iterable[InstrumentSelection]:
        return [InstrumentSelection(
            require_tags=["absorption"],
            exclude_tags=["secondary", "aethalometer", "thermomaap"],
        )]

    @property
    def instrument_type(self) -> str:
        return 'filter_absorption_photometer'

    @property
    def file_metadata(self) -> typing.Dict[str, str]:
        r = super().file_metadata
        r.update(self.level2_metadata)
        r.update({
            'unit': '1/Mm',
            'comp_name': 'aerosol_absorption_coefficient',
            'hum_temp_ctrl': 'Heating to 40% RH, limit 40 deg. C',
            'std_method': 'Single-angle_Correction=Bond1999_Ogren2010',
            'method': f'{self.lab_code}_abs_coef',
            'vol_std_temp': '273.15K',
            'vol_std_pressure': '1013.25hPa',
            'zero_negative': 'Zero/negative possible',
            'zero_negative_desc': 'Zero and neg. values may appear due to statistical variations at very low concentrations',
            'comment': 'Standard Bond et al. 1999 values for K1 and K2 used at all wavelengths',
        })
        return r

    @property
    def limit_absorption(self) -> typing.Tuple[typing.Optional[float], typing.Optional[float]]:
        return -0.5, None

    @property
    def limit_absorption_q16(self) -> typing.Tuple[typing.Optional[float], typing.Optional[float]]:
        return -5.0, None

    @property
    def limit_absorption_q84(self) -> typing.Tuple[typing.Optional[float], typing.Optional[float]]:
        return -0.5, None
    
    @classmethod
    def with_limits(
            cls, 
            absorption: typing.Tuple[typing.Optional[float], typing.Optional[float]] = None,
            absorption_q16: typing.Tuple[typing.Optional[float], typing.Optional[float]] = None,
            absorption_q84: typing.Tuple[typing.Optional[float], typing.Optional[float]] = None,
    ) -> typing.Type["Level2File"]:
        class Result(cls):
            @property
            def limit_absorption(self) -> typing.Tuple[typing.Optional[float], typing.Optional[float]]:
                if absorption is not None:
                    return absorption
                return super().limit_absorption
            
            @property
            def limit_absorption_q16(self) -> typing.Tuple[typing.Optional[float], typing.Optional[float]]:
                if absorption_q16 is not None:
                    return absorption_q16
                return super().limit_absorption_q16
            
            @property
            def limit_absorption_q84(self) -> typing.Tuple[typing.Optional[float], typing.Optional[float]]:
                if absorption_q84 is not None:
                    return absorption_q84
                return super().limit_absorption_q84

        return Result

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
            absorption = matrix.spectral_variable()
            absorption_q16 = matrix.spectral_variable()
            absorption_q84 = matrix.spectral_variable()
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
                        {"variable_name": "light_absorption"},
                        {"standard_name": "volume_absorption_coefficient_in_air_due_to_dried_aerosol_particles"},
                        {"standard_name": "volume_extinction_coefficient_in_air_due_to_ambient_aerosol_particles"},
                ):
                    absorption[nas].integrate_variable(
                        var, selector(var),
                        converter=self.limit_converter(self.limit_absorption),
                    )
                for var in self.select_variable(
                        root,
                        {"variable_name": "light_absorption"},
                        {"standard_name": "volume_absorption_coefficient_in_air_due_to_dried_aerosol_particles"},
                        {"standard_name": "volume_extinction_coefficient_in_air_due_to_ambient_aerosol_particles"},
                        statistics="quantiles",
                ):
                    absorption_q16[nas].integrate_variable(
                        var, selector(var),
                        converter=self.limit_converter(
                            self.limit_absorption_q16,
                            self.quantile_converter(var, 0.1587)
                        )
                    )
                    absorption_q84[nas].integrate_variable(
                        var, selector(var),
                        converter=self.limit_converter(
                            self.limit_absorption_q84,
                            self.quantile_converter(var, 0.8413)
                        )
                    )

        for var in absorption:
            var.apply_metadata(
                title='abs{wavelength}',
                comp_name='aerosol_absorption_coefficient',
                unit='1/Mm',
                statistics='arithmetic mean',
            )
        for var in absorption_q16:
            var.apply_metadata(
                title='abs{wavelength}pc16',
                comp_name='aerosol_absorption_coefficient',
                unit='1/Mm',
                statistics='percentile:15.87',
            )
        for var in absorption_q84:
            var.apply_metadata(
                title='abs{wavelength}pc84',
                comp_name='aerosol_absorption_coefficient',
                unit='1/Mm',
                statistics='percentile:84.13',
            )

        for nas in matrix:
            instrument[nas].set_serial_number(nas)
            self.apply_inlet(nas)
            await self.assemble_file(
                nas, output_directory,
                absorption[nas],
                optional=[pressure[nas], temperature[nas], humidity[nas]] +
                    list(absorption_q16[nas]) + list(absorption_q84[nas]),
                flags=flags[nas],
                fixed_interval_ms=60 * 60 * 1000,
            )
