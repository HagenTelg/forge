import typing
import netCDF4
from forge.cpd3.identity import Identity
from forge.cpd3.convert.instrument.lookup import instrument_data


def convert(station: str, root: netCDF4.Dataset) -> typing.List[typing.Tuple[Identity, typing.Any]]:
    tags = getattr(root, 'forge_tags', None)
    if tags == 'eventlog':
        from . eventlog import convert_event_log
        return convert_event_log(station, root)

    return instrument_data(root.instrument, 'acquisition', 'convert')(station, root)
