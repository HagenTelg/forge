import typing
import netCDF4
from forge.cpd3.identity import Identity


def convert(station: str, root: netCDF4.Dataset) -> typing.List[typing.Tuple[Identity, typing.Any]]:
    from .converter import Converter
    return Converter(station, root).convert()
