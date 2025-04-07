import typing
from .cpc import Level1File


class File(Level1File):
    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"aerosol", "cpc"}

    @property
    def instrument_manufacturer(self) -> str:
        return "GE"

    @property
    def instrument_model(self) -> str:
        return "Rich"

    @property
    def instrument_name(self) -> str:
        return f'GE_Rich_{self.station.upper()}'
