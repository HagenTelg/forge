import typing
from math import inf
from .absorption import Level2File, InstrumentSelection


class File(Level2File):
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
    def file_metadata(self) -> typing.Dict[str, str]:
        r = super().file_metadata
        r.update({
            'std_method': 'Single-angle_Correction=Drinovec2015',
            'method': f'{self.lab_code}_AE33',
        })
        return r
