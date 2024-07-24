import typing
from .absorption import Level2File


class File(Level2File):
    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"aerosol", "absorption", "clap"}

    @property
    def instrument_manufacturer(self) -> str:
        return "NOAA/ESRL/GMD"

    @property
    def instrument_model(self) -> str:
        return "CLAP-10"

    @property
    def instrument_name(self) -> str:
        return f'GMD_CLAP-3W_{self.station.upper()}'
