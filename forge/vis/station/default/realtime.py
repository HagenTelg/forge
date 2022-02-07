import typing
from forge.vis.realtime import Translator
from forge.vis import CONFIGURATION


def visible(station: str, mode_name: typing.Optional[str] = None) -> bool:
    enable = CONFIGURATION.get('REALTIME.VISIBLE', CONFIGURATION.get('ACQUISITION.VISIBLE', False))
    if isinstance(enable, bool):
        return enable
    return station in enable


def translator(station: str) -> typing.Optional[Translator]:
    from forge.vis.station.cpd3 import realtime_translator
    return realtime_translator
