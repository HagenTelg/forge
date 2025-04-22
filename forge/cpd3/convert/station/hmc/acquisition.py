import typing
import netCDF4
from forge.cpd3.identity import Identity
from ..default.acquisition import convert as default_convert


def convert(station: str, root: netCDF4.Dataset) -> typing.List[typing.Tuple[Identity, typing.Any]]:
    if root.instrument == 'bmitap':
        from forge.cpd3.convert.instrument.clap.acquisition import Converter as BaseConverter

        class Converter(BaseConverter):
            def record_converter(self, group: netCDF4.Group):
                converter = super().record_converter(group)
                if converter is not None:
                    if len(converter.wavelength_suffix) == 3:
                        converter.wavelength_suffix = ["B", "G", "R"]
                return converter

        return Converter(station, root).convert()

    return default_convert(station, root)
