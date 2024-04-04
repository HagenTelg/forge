import typing
from forge.vis.realtime import Translator
from forge.vis import CONFIGURATION


def visible(station: str, mode_name: typing.Optional[str] = None) -> bool:
    enable = CONFIGURATION.get('REALTIME.VISIBLE', CONFIGURATION.get('ACQUISITION.VISIBLE', False))
    if isinstance(enable, bool):
        return enable
    return station in enable


_default_translator = None


def translator(station: str) -> typing.Optional[Translator]:
    from ..cpd3 import use_cpd3
    if use_cpd3(station):
        from forge.vis.station.cpd3 import realtime_translator
        return realtime_translator
    else:
        global _default_translator
        if _default_translator is None:
            from forge.vis.realtime.archive import Translator as RealtimeTranslator
            from .data import data_records
            _default_translator = RealtimeTranslator(data_records)
        return _default_translator

