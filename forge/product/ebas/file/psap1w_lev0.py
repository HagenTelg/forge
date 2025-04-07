import typing
from .absorption import Level0File


class File(Level0File):
    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"aerosol", "absorption", "psap1w"}

    @property
    def instrument_manufacturer(self) -> str:
        return "Radiance-Research"

    @property
    def instrument_model(self) -> str:
        return "PSAP-1W"

    @property
    def instrument_name(self) -> str:
        return f'RadianceResearch_PSAP-1W_{self.station.upper()}'
