import typing
from .absorption import Level1File


class File(Level1File):
    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"aerosol", "absorption", "clap"}

    @property
    def instrument_manufacturer(self) -> str:
        return "NOAA/GML"

    @property
    def instrument_model(self) -> str:
        return "CLAP-3W"

    @property
    def instrument_name(self) -> str:
        return f'GMD_CLAP-3W_{self.station.upper()}'
