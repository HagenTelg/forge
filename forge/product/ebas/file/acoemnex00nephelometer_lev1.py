import typing
from .scattering import Level1File


class File(Level1File):
    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"aerosol", "scattering", "acoemnex00nephelometer"}

    @property
    def instrument_manufacturer(self) -> str:
        return "acoem"

    @property
    def instrument_model(self) -> str:
        return "Aurora NE-300"

    @property
    def instrument_name(self) -> str:
        return f'NE300_{self.station.upper()}'

    @property
    def file_metadata(self) -> typing.Dict[str, str]:
        r = super().file_metadata
        r.update({
            'std_method': 'cal-gas=CO2+AIR_truncation-correction=Mueller2011',
            'comment': 'Standard Mueller 2011 values used for truncation correction',
        })
        return r