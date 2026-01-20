import typing
from .scattering import Level0File


class File(Level0File):
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
            'vol_std_temp': '273.15K',
            'vol_std_pressure': '1013.25hPa',
        })
        return r
