import typing
from math import inf
from .absorption import Level1File, InstrumentSelection


class File(Level1File):
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
    def file_metadata(self) -> typing.Dict[str, str]:
        r = super().file_metadata
        r.update({
            'std_method': None,
            'method': f'{self.lab_code}_AE31',
        })
        return r
