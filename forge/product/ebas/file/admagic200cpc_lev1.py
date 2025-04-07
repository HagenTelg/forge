import typing
from .cpc import Level1File


class File(Level1File):
    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"aerosol", "cpc", "admagic200cpc"}

    @property
    def instrument_manufacturer(self) -> str:
        return "Aerosol Dynamics"

    @property
    def instrument_model(self) -> str:
        return "MAGIC 200"

    @property
    def instrument_name(self) -> str:
        return f'AerosolDynamics_Magic200_{self.station.upper()}'
