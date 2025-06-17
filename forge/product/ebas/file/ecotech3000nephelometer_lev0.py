import typing
from .scattering import Level0File


class File(Level0File):
    @property
    def tags(self) -> typing.Optional[typing.Set[str]]:
        return {"aerosol", "scattering", "ecotechnephelometer"}

    @property
    def instrument_manufacturer(self) -> str:
        return "Ecotech"

    @property
    def instrument_model(self) -> str:
        return "Aurora3000"

    @property
    def instrument_name(self) -> str:
        return f'Ecotech_Aurora3000_{self.station.upper()}'

    @property
    def file_metadata(self) -> typing.Dict[str, str]:
        r = super().file_metadata
        r.update({
            'vol_std_temp': '273.15K',
            'vol_std_pressure': '1013.25hPa',
        })
        return r
