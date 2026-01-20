import typing
from math import inf
from .absorption import Level1File, InstrumentSelection


class File(Level1File):
    WAVELENGTH_BANDS: typing.List[typing.Tuple[float, float]] = [(-inf, inf), ]

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
    def file_metadata(self) -> typing.Dict[str, str]:
        r = super().file_metadata
        r.update({
            'std_method': 'Multi-angle_Correction=Petzold2004',
            'comment': None,
            'method': f'{self.lab_code}_MAAP_5012',
        })
        return r
