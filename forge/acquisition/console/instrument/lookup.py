import typing
from importlib import import_module


def instrument_data(instrument: str, package: str, data: typing.Optional[str] = None):
    try:
        result = import_module('.' + package, 'forge.acquisition.console.instrument.' + instrument)
        if data:
            result = getattr(result, data)
    except (ModuleNotFoundError, AttributeError):
        result = import_module('.' + package, 'forge.acquisition.console.instrument.default')
        if data:
            result = getattr(result, data)
    return result
