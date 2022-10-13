import typing
from abc import ABC, abstractmethod
from . import Translator
from forge.vis.station.lookup import station_data


class AcquisitionTranslator(Translator):
    class OutputInterface(ABC):
        @abstractmethod
        def set_data(self, source: str, values: typing.Dict[str, typing.Any]) -> None:
            pass

        @abstractmethod
        def set_single_data(self, source: str, name: str, value: typing.Any) -> None:
            pass

        @abstractmethod
        def set_state(self, source: str, state: typing.Dict[str, typing.Any]) -> None:
            pass

    class Instrument:
        def __init__(self, source: str, instrument_info: typing.Dict[str, typing.Any]):
            self.source = source
            self.info = instrument_info

        def update_information(self, instrument_info: typing.Dict[str, typing.Any]) -> None:
            self.info = instrument_info

        def translate_data(self, message: typing.Dict[str, float],
                           output: "AcquisitionTranslator.OutputInterface") -> None:
            output.set_data(self.source, message)

        def translate_state(self, message: typing.Dict[str, typing.Any],
                            output: "AcquisitionTranslator.OutputInterface") -> None:
            output.set_state(self.source, message)

        def translate_message(self, record: str, message: typing.Any,
                              output: "AcquisitionTranslator.OutputInterface") -> None:
            if message is None:
                return
            output.set_single_data(self.source, record, message)

    @staticmethod
    def instrument_translator(source: str, instrument_info: typing.Dict[str, typing.Any]) -> typing.Optional["AcquisitionTranslator.Instrument"]:
        return AcquisitionTranslator.Instrument(source, instrument_info)

    @staticmethod
    def translate_message(source: str, record: str, message: typing.Any,
                          output: "AcquisitionTranslator.OutputInterface") -> None:
        if source == 'spancheck' and record != 'command':
            output.set_single_data('_spancheck', record, message)


def get_translator(station: str) -> typing.Optional[Translator]:
    return station_data(station, 'acquisition', 'translator')(station)
