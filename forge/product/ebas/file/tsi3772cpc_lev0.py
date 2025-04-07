import typing
from .cpc import Level0File


class File(Level0File):
    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"aerosol", "cpc", "tsi377xcpc"}

    @property
    def instrument_manufacturer(self) -> str:
        return "TSI"

    @property
    def instrument_model(self) -> str:
        return "3772"

    @property
    def instrument_name(self) -> str:
        return f'TSI_3772_{self.station.upper()}'
