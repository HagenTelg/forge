import typing
from forge.vis.realtime import Translator
from forge.vis.realtime.archive import Translator as RealtimeTranslator
from .data import data_records


def visible(station: str, mode_name: typing.Optional[str] = None) -> bool:
    return True


station_translator = RealtimeTranslator(data_records)

def translator(station: str) -> typing.Optional[Translator]:
    return station_translator
