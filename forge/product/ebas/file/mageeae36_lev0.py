import typing
from forge.product.selection import InstrumentSelection
from .mageeae33_lev0 import File as AE33File


class File(AE33File):
    @property
    def instrument_selection(self) -> typing.Iterable[InstrumentSelection]:
        return [InstrumentSelection(
            instrument_type=["mageeae36"],
            exclude_tags=["secondary"],
        )]

    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"aerosol", "absorption", "aethalometer", "mageeae36"}

    @property
    def instrument_model(self) -> str:
        return "AE36"

    @property
    def instrument_name(self) -> str:
        return f'Magee_AE36_{self.station.upper()}'

    @property
    def file_metadata(self) -> typing.Dict[str, str]:
        r = super().file_metadata
        r.update({
            'method': f'{self.lab_code}_AE36',
        })
        return r