import typing
import asyncio
import numpy as np
from pathlib import Path
from forge.units import ZERO_C_IN_K, ONE_ATM_IN_HPA
from forge.temp import WorkingDirectory
from forge.product.selection import InstrumentSelection
from forge.rayleigh import rayleigh_scattering
from .spectral import SpectralFile
from .aerosol_instrument import AerosolInstrument


class Level0File(SpectralFile, AerosolInstrument):
    @property
    def instrument_selection(self) -> typing.Iterable[InstrumentSelection]:
        return [InstrumentSelection(
            require_tags=["scattering"],
            exclude_tags=["secondary"],
        )]

    @property
    def instrument_type(self) -> str:
        return 'nephelometer'

    @property
    def file_metadata(self) -> typing.Dict[str, str]:
        r = super().file_metadata
        r.update(self.level0_metadata)
        r.update({
            'unit': '1/Mm',
            'comp_name': 'aerosol_light_scattering_coefficient',
            'hum_temp_ctrl': 'Heating to 40% RH, limit 40 deg. C',
            'std_method': 'cal-gas=CO2+AIR_truncation-correction=none',
            'method': f'{self.lab_code}_scat_coef',
            'zero_negative': 'Zero/negative possible',
            'zero_negative_desc': 'Zero and neg. values may appear due to statistical variations at very low concentrations',
        })
        return r

    async def __call__(self, output_directory: Path) -> None:
        def rayleigh_calculator(wavelength: float, angle: float = 0) -> typing.Callable[[np.ndarray, np.ndarray], np.ndarray]:
            rayleigh = rayleigh_scattering(wavelength, angle)

            def calculate(t: np.ndarray, p: np.ndarray) -> np.ndarray:
                density = (p / ONE_ATM_IN_HPA) * (ZERO_C_IN_K / (t + ZERO_C_IN_K))
                return density * rayleigh

            return calculate

        def convert_zero_scattering(wavelength: float) -> typing.Callable[[np.ndarray], np.ndarray]:
            r = rayleigh_calculator(wavelength)

            def convert(v: np.ndarray) -> np.ndarray:
                return v[..., 0] +  r(v[..., 1], v[..., 2])

            return convert

        def convert_zero_backscattering(wavelength: float) -> typing.Callable[[np.ndarray], np.ndarray]:
            r = rayleigh_calculator(wavelength, 90.0)

            def convert(v: np.ndarray) -> np.ndarray:
                return v[..., 0] + r(v[..., 1], v[..., 2])

            return convert

        def convert_rayleigh(wavelength: float) -> typing.Callable[[np.ndarray], np.ndarray]:
            r = rayleigh_calculator(wavelength)

            def convert(v: np.ndarray) -> np.ndarray:
                return r(v[..., 1], v[..., 2])

            return convert

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
                title="p_int",
            )
            temperature_outlet = matrix.variable(
                comp_name="temperature",
                unit="K",
                matrix="instrument",
                title="T_out",
            )
            temperature_inlet = matrix.variable(
                comp_name="temperature",
                unit="K",
                matrix="instrument",
                title="T_int",
            )
            humidity_outlet = matrix.variable(
                comp_name="relative_humidity",
                unit="%",
                matrix="instrument",
                title="RH_out",
            )
            humidity_inlet = matrix.variable(
                comp_name="relative_humidity",
                unit="%",
                matrix="instrument",
                title="RH_int",
            )
            lamp_current = matrix.variable(
                comp_name="electric_current",
                unit="A",
                matrix="instrument",
                title="lamp_c",
            )
            lamp_voltage = matrix.variable(
                comp_name="electric_tension",
                unit="V",
                matrix="instrument",
                title="lamp_v",
            )
            scattering = matrix.spectral_variable()
            backscattering = matrix.spectral_variable()
            scattering_zero = matrix.spectral_variable()
            backscattering_zero = matrix.spectral_variable()
            rayleigh_zero = matrix.spectral_variable()
            async for nas, selector, root in matrix.iter_data_files(data_directory):
                flags[nas].integrate_file(root, selector)
                instrument[nas].integrate_file(root)
                for var in self.select_variable(
                        root,
                        {"standard_name": "air_temperature"},
                ):
                    temperature_outlet[nas].integrate_variable(
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
                    humidity_outlet[nas].integrate_variable(var, selector(var, require_cut_size_match=False))
                for var in self.select_variable(
                        root,
                        {"variable_name": "inlet_temperature"},
                ):
                    temperature_inlet[nas].integrate_variable(
                        var, selector(var, require_cut_size_match=False),
                        converter=lambda x: x + ZERO_C_IN_K
                    )
                for var in self.select_variable(
                        root,
                        {"variable_name": "inlet_humidity"},
                ):
                    humidity_inlet[nas].integrate_variable(var, selector(var, require_cut_size_match=False))
                for var in self.select_variable(
                        root,
                        {"variable_name": "lamp_current"},
                ):
                    lamp_current[nas].integrate_variable(var, selector(var, require_cut_size_match=False))
                for var in self.select_variable(
                        root,
                        {"variable_name": "lamp_voltage"},
                ):
                    lamp_voltage[nas].integrate_variable(var, selector(var, require_cut_size_match=False))

                for var in self.select_variable(
                        root,
                        {"variable_name": "scattering_coefficient"},
                        {"standard_name": "volume_scattering_coefficient_in_air_due_to_dried_aerosol_particles"},
                ):
                    scattering[nas].integrate_variable(
                        var, selector(var),
                    )
                for var in self.select_variable(
                        root,
                        {"variable_name": "backscattering_coefficient"},
                        {"standard_name": "volume_backwards_scattering_coefficient_in_air_due_to_dried_aerosol_particles"},
                ):
                    backscattering[nas].integrate_variable(
                        var, selector(var),
                    )
                for var in self.select_variable(
                        root,
                        {"variable_name": "wall_scattering_coefficient"},
                ):
                    zero_temperature = var.group().variables.get("zero_temperature")
                    zero_pressure = var.group().variables.get("zero_pressure")
                    if zero_temperature is None or zero_pressure is None:
                        continue

                    scattering_zero[nas].integrate_variable(
                        var, selector(var, require_cut_size_match=False),
                        extra_vars=[zero_temperature, zero_pressure],
                        wavelength_converter=convert_zero_scattering,
                    )
                    rayleigh_zero[nas].integrate_variable(
                        var, selector(var, require_cut_size_match=False),
                        extra_vars=[zero_temperature, zero_pressure],
                        wavelength_converter=convert_rayleigh,
                    )

                for var in self.select_variable(
                        root,
                        {"variable_name": "wall_backscattering_coefficient"},
                ):
                    zero_temperature = var.group().variables.get("zero_temperature")
                    zero_pressure = var.group().variables.get("zero_pressure")
                    if zero_temperature is None or zero_pressure is None:
                        continue

                    backscattering_zero[nas].integrate_variable(
                        var, selector(var, require_cut_size_match=False),
                        extra_vars=[zero_temperature, zero_pressure],
                        wavelength_converter=convert_zero_backscattering,
                    )

        for var in scattering:
            var.apply_metadata(
                title='sc{wavelength}',
                comp_name='aerosol_light_scattering_coefficient',
                unit='1/Mm',
            )
        for var in backscattering:
            var.apply_metadata(
                title='bsc{wavelength}',
                comp_name='aerosol_light_backscattering_coefficient',
                unit='1/Mm',
            )
        for var in scattering_zero:
            var.apply_metadata(
                title='sc{wavelength}z',
                comp_name='aerosol_light_scattering_coefficient_zero_measurement',
                unit='1/Mm',
            )
        for var in backscattering_zero:
            var.apply_metadata(
                title='bsc{wavelength}z',
                comp_name='aerosol_light_backscattering_coefficient_zero_measurement',
                unit='1/Mm',
            )
        for var in rayleigh_zero:
            var.apply_metadata(
                title='sc{wavelength}r',
                comp_name='aerosol_light_rayleighscattering_coefficient_zero_measurement',
                unit='1/Mm',
            )

        for var in pressure:
            var.add_characteristic('Location', 'instrument internal', self.instrument_type, var.metadata.comp_name, '0')
        for var in temperature_outlet:
            var.add_characteristic('Location', 'instrument outlet', self.instrument_type, var.metadata.comp_name, '0')
        for var in temperature_inlet:
            var.add_characteristic('Location', 'instrument inlet', self.instrument_type, var.metadata.comp_name, '0')
        for var in humidity_outlet:
            var.add_characteristic('Location', 'instrument outlet', self.instrument_type, var.metadata.comp_name, '0')
        for var in humidity_inlet:
            var.add_characteristic('Location', 'instrument inlet', self.instrument_type, var.metadata.comp_name, '0')
        for var in lamp_current:
            var.add_characteristic('Location', 'lamp supply', self.instrument_type, var.metadata.comp_name, '0')
        for var in lamp_voltage:
            var.add_characteristic('Location', 'lamp supply', self.instrument_type, var.metadata.comp_name, '0')

        for nas in matrix:
            instrument[nas].set_serial_number(nas)
            self.apply_inlet(nas)
            await self.assemble_file(
                nas, output_directory,
                list(scattering[nas]) + list(backscattering[nas]),
                optional=[pressure[nas], temperature_outlet[nas], temperature_inlet[nas],
                          humidity_outlet[nas], humidity_inlet[nas], lamp_current[nas], lamp_voltage[nas]] +
                    list(scattering_zero[nas]) + list(backscattering_zero[nas]) + list(rayleigh_zero[nas]),
                flags=flags[nas],
            )


class Level1File(SpectralFile, AerosolInstrument):
    @property
    def instrument_selection(self) -> typing.Iterable[InstrumentSelection]:
        return [InstrumentSelection(
            require_tags=["scattering"],
            exclude_tags=["secondary"],
        )]

    @property
    def instrument_type(self) -> str:
        return 'nephelometer'

    @property
    def file_metadata(self) -> typing.Dict[str, str]:
        r = super().file_metadata
        r.update(self.level1_metadata)
        r.update({
            'unit': '1/Mm',
            'comp_name': 'aerosol_light_scattering_coefficient',
            'hum_temp_ctrl': 'Heating to 40% RH, limit 40 deg. C',
            'std_method': 'cal-gas=CO2+AIR_truncation-correction=Anderson1998',
            'method': f'{self.lab_code}_scat_coef',
            'vol_std_temp': '273.15K',
            'vol_std_pressure': '1013.25hPa',
            'zero_negative': 'Zero/negative possible',
            'zero_negative_desc': 'Zero and neg. values may appear due to statistical variations at very low concentrations',
            'comment': 'Standard Anderson & Ogren 1998 values used for truncation correction',
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
                matrix="instrument",
                title="p_int",
            )
            temperature = matrix.variable(
                comp_name="temperature",
                unit="K",
                matrix="instrument",
                title="T_int",
            )
            humidity = matrix.variable(
                comp_name="relative_humidity",
                unit="%",
                matrix="instrument",
                title="RH_int",
            )
            scattering = matrix.spectral_variable()
            backscattering = matrix.spectral_variable()
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
                        {"variable_name": "scattering_coefficient"},
                        {"standard_name": "volume_scattering_coefficient_in_air_due_to_dried_aerosol_particles"},
                ):
                    scattering[nas].integrate_variable(
                        var, selector(var),
                    )
                for var in self.select_variable(
                        root,
                        {"variable_name": "backscattering_coefficient"},
                        {"standard_name": "volume_backwards_scattering_coefficient_in_air_due_to_dried_aerosol_particles"},
                ):
                    backscattering[nas].integrate_variable(
                        var, selector(var),
                    )

        for var in scattering:
            var.apply_metadata(
                title='sc{wavelength}',
                comp_name='aerosol_light_scattering_coefficient',
                unit='1/Mm',
            )
        for var in backscattering:
            var.apply_metadata(
                title='bsc{wavelength}',
                comp_name='aerosol_light_backscattering_coefficient',
                unit='1/Mm',
            )

        for var in pressure:
            var.add_characteristic('Location', 'instrument internal', self.instrument_type, var.metadata.comp_name, '1')
        for var in temperature:
            var.add_characteristic('Location', 'instrument internal', self.instrument_type, var.metadata.comp_name, '1')
        for var in humidity:
            var.add_characteristic('Location', 'instrument internal', self.instrument_type, var.metadata.comp_name, '1')

        for nas in matrix:
            instrument[nas].set_serial_number(nas)
            self.apply_inlet(nas)
            await self.assemble_file(
                nas, output_directory,
                list(scattering[nas]) + list(backscattering[nas]),
                optional=[pressure[nas], temperature[nas], humidity[nas]],
                flags=flags[nas],
            )


class Level2File(SpectralFile, AerosolInstrument):
    @property
    def instrument_selection(self) -> typing.Iterable[InstrumentSelection]:
        return [InstrumentSelection(
            require_tags=["scattering"],
            exclude_tags=["secondary"],
        )]

    @property
    def instrument_type(self) -> str:
        return 'nephelometer'

    @property
    def file_metadata(self) -> typing.Dict[str, str]:
        r = super().file_metadata
        r.update(self.level2_metadata)
        r.update({
            'unit': '1/Mm',
            'comp_name': 'aerosol_light_scattering_coefficient',
            'hum_temp_ctrl': 'Heating to 40% RH, limit 40 deg. C',
            'std_method': 'cal-gas=CO2+AIR_truncation-correction=Anderson1998',
            'method': f'{self.lab_code}_scat_coef',
            'vol_std_temp': '273.15K',
            'vol_std_pressure': '1013.25hPa',
            'zero_negative': 'Zero/negative possible',
            'zero_negative_desc': 'Zero and neg. values may appear due to statistical variations at very low concentrations',
            'comment': 'Standard Anderson & Ogren 1998 values used for truncation correction',
        })
        return r

    @property
    def limit_scattering(self) -> typing.Tuple[typing.Optional[float], typing.Optional[float]]:
        return -0.1, None
        # return -0.1, 500
    
    @property
    def limit_scattering_q16(self) -> typing.Tuple[typing.Optional[float], typing.Optional[float]]:
        return -1.0, None
        # return -1.0, 500

    @property
    def limit_scattering_q84(self) -> typing.Tuple[typing.Optional[float], typing.Optional[float]]:
        return -0.1, None
        # return -0.1, 500

    @property
    def limit_backscattering(self) -> typing.Tuple[typing.Optional[float], typing.Optional[float]]:
        return -1.0, None
        # return -1.0, 30
    
    @property
    def limit_backscattering_q16(self) -> typing.Tuple[typing.Optional[float], typing.Optional[float]]:
        return -1.5, None
        # return -1.5, 30

    @property
    def limit_backscattering_q84(self) -> typing.Tuple[typing.Optional[float], typing.Optional[float]]:
        return -0.1, None
        # return -1.0, 40

    @property
    def limit_scattering_fine(self) -> typing.Optional[typing.Tuple[typing.Optional[float], typing.Optional[float]]]:
        return None

    @property
    def limit_scattering_q16_fine(self) -> typing.Optional[typing.Tuple[typing.Optional[float], typing.Optional[float]]]:
        return None

    @property
    def limit_scattering_q84_fine(self) -> typing.Optional[typing.Tuple[typing.Optional[float], typing.Optional[float]]]:
        return None

    @property
    def limit_backscattering_fine(self) -> typing.Optional[typing.Tuple[typing.Optional[float], typing.Optional[float]]]:
        return None

    @property
    def limit_backscattering_q16_fine(self) -> typing.Optional[typing.Tuple[typing.Optional[float], typing.Optional[float]]]:
        return None

    @property
    def limit_backscattering_q84_fine(self) -> typing.Optional[typing.Tuple[typing.Optional[float], typing.Optional[float]]]:
        return None
    
    @classmethod
    def with_limits(
            cls, 
            scattering: typing.Tuple[typing.Optional[float], typing.Optional[float]] = None,
            scattering_q16: typing.Tuple[typing.Optional[float], typing.Optional[float]] = None,
            scattering_q84: typing.Tuple[typing.Optional[float], typing.Optional[float]] = None,
            backscattering: typing.Tuple[typing.Optional[float], typing.Optional[float]] = None,
            backscattering_q16: typing.Tuple[typing.Optional[float], typing.Optional[float]] = None,
            backscattering_q84: typing.Tuple[typing.Optional[float], typing.Optional[float]] = None,
    ) -> typing.Type["Level2File"]:
        class Result(cls):
            @property
            def limit_scattering(self) -> typing.Tuple[typing.Optional[float], typing.Optional[float]]:
                if scattering is not None:
                    return scattering
                return super().limit_scattering
            
            @property
            def limit_scattering_q16(self) -> typing.Tuple[typing.Optional[float], typing.Optional[float]]:
                if scattering_q16 is not None:
                    return scattering_q16
                return super().limit_scattering_q16
            
            @property
            def limit_scattering_q84(self) -> typing.Tuple[typing.Optional[float], typing.Optional[float]]:
                if scattering_q84 is not None:
                    return scattering_q84
                return super().limit_scattering_q84
            
            @property
            def limit_backscattering(self) -> typing.Tuple[typing.Optional[float], typing.Optional[float]]:
                if backscattering is not None:
                    return backscattering
                return super().limit_backscattering
            
            @property
            def limit_backscattering_q16(self) -> typing.Tuple[typing.Optional[float], typing.Optional[float]]:
                if backscattering_q16 is not None:
                    return backscattering_q16
                return super().limit_backscattering_q16
            
            @property
            def limit_backscattering_q84(self) -> typing.Tuple[typing.Optional[float], typing.Optional[float]]:
                if backscattering_q84 is not None:
                    return backscattering_q84
                return super().limit_backscattering_q84

        return Result

    @classmethod
    def with_limits_fine(
            cls,
            scattering: typing.Tuple[typing.Optional[float], typing.Optional[float]] = None,
            scattering_fine: typing.Tuple[typing.Optional[float], typing.Optional[float]] = None,
            scattering_q16: typing.Tuple[typing.Optional[float], typing.Optional[float]] = None,
            scattering_q16_fine: typing.Tuple[typing.Optional[float], typing.Optional[float]] = None,
            scattering_q84: typing.Tuple[typing.Optional[float], typing.Optional[float]] = None,
            scattering_q84_fine: typing.Tuple[typing.Optional[float], typing.Optional[float]] = None,
            backscattering: typing.Tuple[typing.Optional[float], typing.Optional[float]] = None,
            backscattering_fine: typing.Tuple[typing.Optional[float], typing.Optional[float]] = None,
            backscattering_q16: typing.Tuple[typing.Optional[float], typing.Optional[float]] = None,
            backscattering_q16_fine: typing.Tuple[typing.Optional[float], typing.Optional[float]] = None,
            backscattering_q84: typing.Tuple[typing.Optional[float], typing.Optional[float]] = None,
            backscattering_q84_fine: typing.Tuple[typing.Optional[float], typing.Optional[float]] = None,
    ) -> typing.Type["Level2File"]:
        class Result(cls.with_limits(
            scattering, scattering_q16, scattering_q84,
            backscattering, backscattering_q16, backscattering_q84
        )):
            @property
            def limit_scattering_fine(self) -> typing.Optional[typing.Tuple[typing.Optional[float], typing.Optional[float]]]:
                if scattering_fine is not None:
                    return scattering_fine
                return super().limit_scattering_fine

            @property
            def limit_scattering_q16_fine(self) -> typing.Optional[typing.Tuple[typing.Optional[float], typing.Optional[float]]]:
                if scattering_q16_fine is not None:
                    return scattering_q16_fine
                return super().limit_scattering_q16_fine

            @property
            def limit_scattering_q84_fine(self) -> typing.Optional[typing.Tuple[typing.Optional[float], typing.Optional[float]]]:
                if scattering_q84_fine is not None:
                    return scattering_q84_fine
                return super().limit_scattering_q84_fine

            @property
            def limit_backscattering_fine(self) -> typing.Optional[typing.Tuple[typing.Optional[float], typing.Optional[float]]]:
                if backscattering_fine is not None:
                    return backscattering_fine
                return super().limit_backscattering_fine

            @property
            def limit_backscattering_q16_fine(self) -> typing.Optional[typing.Tuple[typing.Optional[float], typing.Optional[float]]]:
                if backscattering_q16_fine is not None:
                    return backscattering_q16_fine
                return super().limit_backscattering_q16_fine

            @property
            def limit_backscattering_q84_fine(self) -> typing.Optional[typing.Tuple[typing.Optional[float], typing.Optional[float]]]:
                if backscattering_q84_fine is not None:
                    return backscattering_q84_fine
                return super().limit_backscattering_q84_fine

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
                matrix="instrument",
                title="p_int",
            )
            temperature = matrix.variable(
                comp_name="temperature",
                unit="K",
                matrix="instrument",
                title="T_int",
            )
            humidity = matrix.variable(
                comp_name="relative_humidity",
                unit="%",
                matrix="instrument",
                title="RH_int",
            )
            scattering = matrix.spectral_variable()
            scattering_q16 = matrix.spectral_variable()
            scattering_q84 = matrix.spectral_variable()
            backscattering = matrix.spectral_variable()
            backscattering_q16 = matrix.spectral_variable()
            backscattering_q84 = matrix.spectral_variable()
            async for nas, selector, root in matrix.iter_data_files(data_directory):
                def choose_limit(base, fine):
                    if fine is not None and selector.cut_size < 10.0:
                        return fine
                    return base

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
                        {"variable_name": "scattering_coefficient"},
                        {"standard_name": "volume_scattering_coefficient_in_air_due_to_dried_aerosol_particles"},
                ):
                    scattering[nas].integrate_variable(
                        var, selector(var),
                        converter=self.limit_converter(
                            choose_limit(self.limit_scattering, self.limit_scattering_fine),
                        )
                    )
                for var in self.select_variable(
                        root,
                        {"variable_name": "scattering_coefficient"},
                        {"standard_name": "volume_scattering_coefficient_in_air_due_to_dried_aerosol_particles"},
                        statistics="quantiles",
                ):
                    scattering_q16[nas].integrate_variable(
                        var, selector(var),
                        converter=self.limit_converter(
                            choose_limit(self.limit_scattering_q16, self.limit_scattering_q16_fine),
                            self.quantile_converter(var, 0.1587)
                        )
                    )
                    scattering_q84[nas].integrate_variable(
                        var, selector(var),
                        converter=self.limit_converter(
                            choose_limit(self.limit_scattering_q84, self.limit_scattering_q84_fine),
                            self.quantile_converter(var, 0.8413)
                        )
                    )
                for var in self.select_variable(
                        root,
                        {"variable_name": "backscattering_coefficient"},
                        {"standard_name": "volume_backwards_scattering_coefficient_in_air_due_to_dried_aerosol_particles"},
                ):
                    backscattering[nas].integrate_variable(
                        var, selector(var),
                        converter=self.limit_converter(
                            choose_limit(self.limit_backscattering, self.limit_backscattering_fine),
                        )
                    )
                for var in self.select_variable(
                        root,
                        {"variable_name": "backscattering_coefficient"},
                        {"standard_name": "volume_backwards_scattering_coefficient_in_air_due_to_dried_aerosol_particles"},
                        statistics="quantiles",
                ):
                    backscattering_q16[nas].integrate_variable(
                        var, selector(var),
                        converter=self.limit_converter(
                            choose_limit(self.limit_backscattering_q16, self.limit_backscattering_q16_fine),
                            self.quantile_converter(var, 0.1587)
                        )
                    )
                    backscattering_q84[nas].integrate_variable(
                        var, selector(var),
                        converter=self.limit_converter(
                            choose_limit(self.limit_backscattering_q84, self.limit_backscattering_q84_fine),
                            self.quantile_converter(var, 0.8413)
                        )
                    )

        for var in scattering:
            var.apply_metadata(
                title='sc{wavelength}',
                comp_name='aerosol_light_scattering_coefficient',
                unit='1/Mm',
                statistics='arithmetic mean',
            )
        for var in scattering_q16:
            var.apply_metadata(
                title='sc{wavelength}pc16',
                comp_name='aerosol_light_scattering_coefficient',
                unit='1/Mm',
                statistics='percentile:15.87',
            )
        for var in scattering_q84:
            var.apply_metadata(
                title='sc{wavelength}pc84',
                comp_name='aerosol_light_scattering_coefficient',
                unit='1/Mm',
                statistics='percentile:84.13',
            )
        for var in backscattering:
            var.apply_metadata(
                title='bsc{wavelength}',
                comp_name='aerosol_light_backscattering_coefficient',
                unit='1/Mm',
                statistics='arithmetic mean',
            )
        for var in backscattering_q16:
            var.apply_metadata(
                title='bsc{wavelength}pc16',
                comp_name='aerosol_light_backscattering_coefficient',
                unit='1/Mm',
                statistics='percentile:15.87',
            )
        for var in backscattering_q84:
            var.apply_metadata(
                title='bsc{wavelength}pc84',
                comp_name='aerosol_light_backscattering_coefficient',
                unit='1/Mm',
                statistics='percentile:84.13',
            )

        for var in pressure:
            var.add_characteristic('Location', 'instrument internal', self.instrument_type, var.metadata.comp_name, '2')
        for var in temperature:
            var.add_characteristic('Location', 'instrument internal', self.instrument_type, var.metadata.comp_name, '2')
        for var in humidity:
            var.add_characteristic('Location', 'instrument internal', self.instrument_type, var.metadata.comp_name, '2')

        for nas in matrix:
            instrument[nas].set_serial_number(nas)
            self.apply_inlet(nas)
            await self.assemble_file(
                nas, output_directory,
                list(scattering[nas]) + list(backscattering[nas]),
                optional=[pressure[nas], temperature[nas], humidity[nas]] +
                    list(scattering_q16[nas]) + list(scattering_q84[nas]) +
                    list(backscattering_q16[nas]) + list(backscattering_q84[nas]),
                flags=flags[nas],
                fixed_interval_ms=60 * 60 * 1000,
            )
