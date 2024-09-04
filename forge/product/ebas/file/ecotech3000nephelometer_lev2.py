import typing
from .scattering import Level2File


class File(Level2File):
    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"aerosol", "scattering", "ecotechnephelometer"}

    @property
    def instrument_manufacturer(self) -> str:
        return "Ecotech"

    @property
    def instrument_model(self) -> str:
        return "3000"

    @property
    def instrument_name(self) -> str:
        return f'Ecotech_3000_{self.station.upper()}'

    @property
    def file_metadata(self) -> typing.Dict[str, str]:
        r = super().file_metadata
        r.update({
            'std_method': 'cal-gas=CO2+AIR_truncation-correction=Mueller2011',
            'comment': 'Standard Mueller 2011 values used for truncation correction',
        })
        return r
