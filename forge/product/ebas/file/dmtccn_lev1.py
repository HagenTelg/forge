import typing
import asyncio
from pathlib import Path
from forge.units import ZERO_C_IN_K
from forge.temp import WorkingDirectory
from forge.product.selection import InstrumentSelection
from . import EBASFile
from .aerosol_instrument import AerosolInstrument


class Level1File(EBASFile, AerosolInstrument):
    @property
    def instrument_selection(self) -> typing.Iterable[InstrumentSelection]:
        return [InstrumentSelection(
            instrument_type=["dmtccn"],
            exclude_tags=["secondary"],
        )]

    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"aerosol", "absorption", "dmtcc"}

    @property
    def instrument_manufacturer(self) -> str:
        return "DMT"

    @property
    def instrument_model(self) -> str:
        return "CCN-100"

    @property
    def instrument_name(self) -> str:
        return f'DMT_CCN-100_{self.station.upper()}'

    @property
    def instrument_type(self) -> str:
        return 'CCNC'

    @property
    def file_metadata(self) -> typing.Dict[str, str]:
        r = super().file_metadata
        r.update(self.level1_metadata)
        r.update({
            'method': f'{self.lab_code}_CCNC',
            'unit': '1/cm3',
            'comp_name': 'cloud_condensation_nuclei_number_concentration',
            'vol_std_temp': 'instrument internal',
            'vol_std_pressure': 'instrument internal',
            'zero_negative': 'Zero possible',
            'zero_negative_desc': 'Zero values may appear due to statistical variations at very low concentrations',
            'detection_limit': [0.007, "1/cm3"],
            'detection_limit_desc': "Determined only by instrument counting statistics and flow rate",
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
            supersaturation = matrix.variable(
                comp_name="supersaturation",
                unit="%",
                matrix="instrument",
                title="SS",
            )
            ccnc = matrix.variable(
                comp_name="cloud_condensation_nuclei_number_concentration,",
                unit="1/cm3",
                detection_limit=[0.007, "1/cm3"],
                detection_limit_desc="Determined only by instrument counting statistics and flow rate",
                title="CCN",
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
                        {"variable_name": "supersaturation_setting"},
                ):
                    supersaturation[nas].integrate_variable(var, selector(var))
                for var in self.select_variable(
                        root,
                        {"variable_name": "number_concentration"},
                ):
                    ccnc[nas].integrate_variable(var, selector(var))

        for nas in matrix:
            instrument[nas].set_serial_number(nas)
            self.apply_inlet(nas)
            await self.assemble_file(
                nas, output_directory,
                [ccnc[nas], supersaturation[nas]],
                optional=[pressure[nas], temperature[nas]],
                flags=flags[nas],
            )