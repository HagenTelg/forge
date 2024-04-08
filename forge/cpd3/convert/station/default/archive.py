import typing
import netCDF4
from forge.cpd3.identity import Identity
from forge.cpd3.convert.instrument.lookup import instrument_data
from forge.cpd3.archive.selection import FileMatch


def convert(station: str, archive: str, root: netCDF4.Dataset, matchers: typing.List[FileMatch],
            output: typing.List[typing.Tuple[Identity, typing.Any]]) -> None:
    tags = getattr(root, 'forge_tags', None)
    if tags == 'eventlog':
        raise NotImplementedError
    elif tags == 'passed':
        raise NotImplementedError

    instrument = getattr(root, 'instrument', None)
    if not instrument:
        instrument = 'default'

    instrument_data(instrument, 'archive', 'convert')(station, archive, root, matchers, output)


def event_log(station: str, root: netCDF4.Dataset) -> typing.List[typing.Tuple[Identity, typing.Any]]:
    from .eventlog import convert_event_log
    return convert_event_log(station, root)


def data_passed(station: str, root: netCDF4.Dataset) -> typing.List[typing.Tuple[Identity, typing.Any, float]]:
    from .passed import convert_data_passed
    return convert_data_passed(station, root)
