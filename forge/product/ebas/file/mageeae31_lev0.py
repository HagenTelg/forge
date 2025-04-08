import typing
import numpy as np
from pathlib import Path
from math import inf
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
            instrument_type=["mageeae31"],
            exclude_tags=["secondary"],
        )]

    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"aerosol", "absorption", "aethalometer", "mageeae31"}

    @property
    def instrument_manufacturer(self) -> str:
        return "Magee"

    @property
    def instrument_model(self) -> str:
        return "AE31"

    @property
    def instrument_name(self) -> str:
        return f'Magee_AE31_{self.station.upper()}'

    @property
    def instrument_type(self) -> str:
        return 'filter_absorption_photometer'

    @property
    def file_metadata(self) -> typing.Dict[str, str]:
        r = super().file_metadata
        r.update(self.level0_metadata)
        r.update({
            'method': f'{self.lab_code}_AE31',
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
            sensing_beam_signal = matrix.spectral_variable()
            reference_beam_signal = matrix.spectral_variable()
            attenuation_coefficient = matrix.spectral_variable()
            equivalent_black_carbon = matrix.spectral_variable()
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
                    equivalent_black_carbon[nas].integrate_variable(
                        var, selector(var),
                    )
                for var in self.select_variable(
                        root,
                        {"variable_name": "sample_intensity"},
                ):
                    sensing_beam_signal[nas].integrate_variable(
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
                        {"variable_name": "transmittance"},
                ):
                    attenuation_coefficient[nas].integrate_variable(
                        var, selector(var),
                        converter=self._transmittance_to_atn,
                    )

        for var in sensing_beam_signal:
            var.apply_metadata(
                title='s_bm{wavelength}',
                comp_name='sensing_beam_signal',
            )
        for var in reference_beam_signal:
            var.apply_metadata(
                title='r_bm{wavelength}',
                comp_name='reference_beam_signal',
            )
        for var in attenuation_coefficient:
            var.apply_metadata(
                title='att{wavelength}',
                comp_name='attenuation_coefficient',
            )
        for var in equivalent_black_carbon:
            var.apply_metadata(
                title='EBC{wavelength}',
                comp_name='equivalent_black_carbon',
                uncertainty=[100.0, '%'],
                unit='ug/m3',
            )

        for nas in matrix:
            instrument[nas].set_serial_number(nas)
            self.apply_inlet(nas)
            await self.assemble_file(
                nas, output_directory,
                equivalent_black_carbon[nas],
                optional=[pressure[nas], temperature[nas], humidity[nas], flow_rate[nas]] +
                         list(sensing_beam_signal[nas]) + list(reference_beam_signal[nas]) +
                         list(attenuation_coefficient[nas]),
                flags=flags[nas],
            )
