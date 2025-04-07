import typing
from .cpc import Level2File


class File(Level2File):
    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"aerosol", "cpc", "tsi3010cpc"}

    @property
    def instrument_manufacturer(self) -> str:
        return "TSI"

    @property
    def instrument_model(self) -> str:
        return "3010"

    @property
    def instrument_name(self) -> str:
        return f'TSI_3010_{self.station.upper()}'
