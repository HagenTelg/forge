import typing
from .absorption import Level2File


class File(Level2File):
    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"aerosol", "absorption", "psap3w"}

    @property
    def instrument_manufacturer(self) -> str:
        return "Radiance-Research"

    @property
    def instrument_model(self) -> str:
        return "PSAP-3W"

    @property
    def instrument_name(self) -> str:
        return f'RadianceResearch_PSAP-3W_{self.station.upper()}'
