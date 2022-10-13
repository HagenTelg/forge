import typing
from abc import ABC, abstractmethod
from forge.vis.station.lookup import station_data
from forge.acquisition.cutsize import CutSize
from . import Translator


class RealtimeTranslator(Translator):
    class OutputInterface(ABC):
        @abstractmethod
        def send_data(self, data_name: str,
                      record: typing.Dict[str, typing.Union[float, typing.List[float]]]) -> None:
            pass

        @abstractmethod
        def send_field(self, data_name: str, field: str, value: typing.Union[float, typing.List[float]]) -> None:
            pass

    class Instrument:
        def __init__(self, source: str, instrument_info: typing.Dict[str, typing.Any]):
            self.source = source
            self.info = instrument_info

        def update_information(self, instrument_info: typing.Dict[str, typing.Any]) -> None:
            self.info = instrument_info

        def translate_data(self, cutsize: CutSize.Size,
                           values: typing.Dict[str, typing.Union[float, typing.List[float]]],
                           output: "RealtimeTranslator.OutputInterface") -> None:
            pass

        def translate_message(self, record: str, message: typing.Any,
                              output: "RealtimeTranslator.OutputInterface") -> None:
            pass

    @staticmethod
    def instrument_translator(source: str, instrument_info: typing.Dict[str, typing.Any]) -> typing.Optional["RealtimeTranslator.Instrument"]:
        return RealtimeTranslator.Instrument(source, instrument_info)

    @staticmethod
    def translate_message(source: str, record: str, message: typing.Any,
                          output: "RealtimeTranslator.OutputInterface") -> None:
        pass


def get_translator(station: str) -> typing.Optional[Translator]:
    return station_data(station, 'realtime', 'translator')(station)
