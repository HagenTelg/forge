import typing
import asyncio
import logging
from forge.vis.realtime.controller.client import WriteData as RealtimeOutput
from forge.vis.realtime.translation import RealtimeTranslator
from ..cutsize import CutSize

_LOGGER = logging.getLogger(__name__)


class RealtimeTranslatorOutput:
    class _TranslatorOutput(RealtimeTranslator.OutputInterface):
        def __init__(self, translator: "RealtimeTranslatorOutput"):
            self.translator = translator
            self.queued_data: typing.Dict[str, typing.Dict[str, typing.Union[float, typing.List[float]]]] = dict()

        def send_data(self, data_name: str,
                      record: typing.Dict[str, typing.Union[float, typing.List[float]]]) -> None:
            existing = self.queued_data.get(data_name)
            if existing is None:
                existing = dict()
                self.queued_data[data_name] = existing

            existing.update(record)
            self.translator._queue_send_data()

        def send_field(self, data_name: str, field: str, value: typing.Union[float, typing.List[float]]) -> None:
            existing = self.queued_data.get(data_name)
            if existing is None:
                existing = dict()
                self.queued_data[data_name] = existing

            existing[field] = value
            self.translator._queue_send_data()

    def __init__(self, station: str, output: RealtimeOutput, translator: RealtimeTranslator):
        self.station = station
        self.output = output
        self.translator = translator

        self._translator_output = self._TranslatorOutput(self)
        self._instruments: typing.Dict[str, RealtimeTranslator.Instrument] = dict()

        self.data_consolidation_time: float = 0.5
        self._send_data_task: typing.Optional[asyncio.Task] = None

    async def _send_queued_data(self) -> None:
        await asyncio.sleep(self.data_consolidation_time)

        to_send = list(self._translator_output.queued_data.items())
        self._translator_output.queued_data.clear()
        self._send_data_task = None

        if not self.output:
            return

        for data_name, record in to_send:
            await self.output.send_data(self.station, data_name, record)

    def _queue_send_data(self) -> None:
        if self._send_data_task:
            return
        self._send_data_task = asyncio.get_event_loop().create_task(self._send_queued_data())

    async def start(self) -> None:
        pass

    async def shutdown(self) -> None:
        t = self._send_data_task
        self._send_data_task = None
        if t:
            try:
                t.cancel()
            except:
                pass
            try:
                await t
            except:
                pass

        try:
            self.output.writer.close()
        except OSError:
            pass
        self.output = None

    def incoming_message(self, source: str, record: str, message: typing.Any) -> None:
        if record == 'avg':
            self._handle_data(source, CutSize.Size.WHOLE, message)
            return
        elif record.startswith('avg.'):
            try:
                size = CutSize.Size.parse(record[4:])
            except (ValueError, TypeError):
                _LOGGER.warning(f"Invalid cut size from {source}: {record}")
                return
            self._handle_data(source, size, message)
            return
        elif record == 'instrument':
            self._handle_instrument(source, message)
            return

        instrument = self._instruments.get(source)
        if instrument:
            instrument.translate_message(record, message, self._translator_output)
        else:
            self.translator.translate_message(source, record, message, self._translator_output)

    def _handle_instrument(self, source: str, instrument_info: typing.Optional[typing.Dict[str, typing.Any]]) -> None:
        existing = self._instruments.get(source)
        if not instrument_info or not isinstance(instrument_info, dict):
            if existing is None:
                return
            del self._instruments[source]
            return

        if existing is None:
            existing = self.translator.instrument_translator(source, instrument_info)
            if not existing:
                return
            self._instruments[source] = existing
            return

        existing.update_information(instrument_info)

    def _handle_data(self, source: str, cutsize: CutSize.Size, values: typing.Dict[str, typing.Any]) -> None:
        if not values or not isinstance(values, dict):
            return
        instrument = self._instruments.get(source)
        if instrument is None:
            return
        instrument.translate_data(cutsize, values, self._translator_output)

