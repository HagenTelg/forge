import typing
from importlib import import_module


def station_data(station: str, package: str, data: typing.Optional[str] = None):
    try:
        result = import_module('.' + package, 'forge.cpd3.convert.station.' + station)
        if data:
            result = getattr(result, data)
    except (ModuleNotFoundError, AttributeError):
        result = import_module('.' + package, 'forge.cpd3.convert.station.default')
        if data:
            result = getattr(result, data)
    return result
