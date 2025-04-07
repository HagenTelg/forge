import typing
from .cpc import Level0File


class File(Level0File):
    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"aerosol", "cpc", "admagic250cpc"}

    @property
    def instrument_manufacturer(self) -> str:
        return "Aerosol Dynamics"

    @property
    def instrument_model(self) -> str:
        return "MAGIC 250"

    @property
    def instrument_name(self) -> str:
        return f'AerosolDynamics_Magic250_{self.station.upper()}'
