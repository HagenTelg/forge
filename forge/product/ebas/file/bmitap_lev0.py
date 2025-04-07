import typing
from .absorption import Level0File


class File(Level0File):
    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"aerosol", "absorption", "bmitap"}

    @property
    def instrument_manufacturer(self) -> str:
        return "BMI"

    @property
    def instrument_model(self) -> str:
        return "TAP"

    @property
    def instrument_name(self) -> str:
        return f'BMI_TAP_{self.station.upper()}'
