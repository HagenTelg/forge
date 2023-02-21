import typing
from importlib import import_module


def station_data(station: str, package: str, data: typing.Optional[str] = None):
    try:
        result = import_module('.' + package, 'forge.vis.station.' + station)
        if data:
            result = getattr(result, data)
    except (ModuleNotFoundError, AttributeError):
        result = default_data(package, data)
    return result


def default_data(package: str, data: typing.Optional[str] = None):
    result = import_module('.' + package, 'forge.vis.station.default')
    if data:
        result = getattr(result, data)
    return result
