import typing
from .cpc import Level1File


class File(Level1File):
    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"aerosol", "cpc", "tsi3781cpc"}

    @property
    def instrument_manufacturer(self) -> str:
        return "TSI"

    @property
    def instrument_model(self) -> str:
        return "3781"

    @property
    def instrument_name(self) -> str:
        return f'TSI_3781_{self.station.upper()}'
