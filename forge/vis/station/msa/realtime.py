import typing
from forge.vis.realtime import Translator
from ..cpd3 import use_cpd3


if use_cpd3("msa"):
    from ..cpd3 import RealtimeTranslator
    from .data import station_profile_data
    station_translator = RealtimeTranslator.assemble_translator(station_profile_data)

    def translator(station: str) -> typing.Optional[Translator]:
        return station_translator
else:
    from forge.vis.realtime.archive import Translator as RealtimeTranslator
    from .data import data_records
    station_translator = RealtimeTranslator(data_records)

    def translator(station: str) -> typing.Optional[Translator]:
        return station_translator
