import typing
import netCDF4
from forge.cpd3.identity import Identity
from forge.cpd3.convert.instrument.lookup import instrument_data


def convert(station: str, root: netCDF4.Dataset) -> typing.List[typing.Tuple[Identity, typing.Any]]:
    return instrument_data(root.instrument, 'acquisition', 'convert')(station, root)
