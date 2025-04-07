import typing
from .cpc import Level0File


class File(Level0File):
    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"aerosol", "cpc", "bmi1720cpc"}

    @property
    def instrument_manufacturer(self) -> str:
        return "BMI"

    @property
    def instrument_model(self) -> str:
        return "1720"

    @property
    def instrument_name(self) -> str:
        return f'BMI_1720_{self.station.upper()}'
