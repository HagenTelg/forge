import typing
from .cpc import Level2File


class File(Level2File):
    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"aerosol", "cpc", "tsi377xcpc"}

    @property
    def instrument_manufacturer(self) -> str:
        return "TSI"

    @property
    def instrument_model(self) -> str:
        return "3776"

    @property
    def instrument_name(self) -> str:
        return f'TSI_3776_{self.station.upper()}'
