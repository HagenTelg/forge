import typing
from forge.vis.realtime import Translator
from ..cpd3 import RealtimeTranslator
from .data import station_profile_data


station_translator = RealtimeTranslator.assemble_translator(station_profile_data)


def translator(station: str) -> typing.Optional[Translator]:
    return station_translator
