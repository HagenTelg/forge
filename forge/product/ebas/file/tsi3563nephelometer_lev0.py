import typing
from .scattering import Level0File


class File(Level0File):
    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"aerosol", "scattering", "tsi3563nephelometer"}

    @property
    def instrument_manufacturer(self) -> str:
        return "TSI"

    @property
    def instrument_model(self) -> str:
        return "3563"

    @property
    def instrument_name(self) -> str:
        return f'TSI_3563_{self.station.upper()}'
