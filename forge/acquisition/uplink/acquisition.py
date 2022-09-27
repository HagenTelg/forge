import typing
import asyncio
import logging
import time
from forge.vis.acquisition.controller.client import Client as AcquisitionClient
from forge.vis.acquisition.translation import AcquisitionTranslator
from .bus import BusInterface, PersistenceLevel


_LOGGER = logging.getLogger(__name__)


class AcquisitionTranslatorClient(AcquisitionClient):
    class _Output(AcquisitionTranslator.OutputInterface):
        def __init__(self, translator: "AcquisitionTranslatorClient"):
            self.translator = translator
            self.queued_data: typing.Dict[str, typing.Dict[str, typing.Any]] = dict()

        def set_data(self, source: str, values: typing.Dict[str, typing.Any]) -> None:
            queued = self.queued_data.get(source)
            if queued is None:
                queued: typing.Dict[str, typing.Any] = dict()
                self.queued_data[source] = queued
            queued.update(values)
            self.translator._queue_send_data()

        def set_single_data(self, source: str, name: str, value: typing.Any) -> None:
            queued = self.queued_data.get(source)
            if queued is None:
                queued: typing.Dict[str, typing.Any] = dict()
                self.queued_data[source] = queued
            queued[name] = value
            self.translator._queue_send_data()

        def set_state(self, source: str, state: typing.Dict[str, typing.Any]) -> None:
            self.translator.send_instrument_state(source, state)

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
                 translator: AcquisitionTranslator, use_instantaneous: bool):
        super().__init__(reader, writer)
        self.translator = translator
        self.bus: typing.Optional[BusInterface] = None
        self.use_instantaneous = use_instantaneous
        self._run_task: typing.Optional[asyncio.Task] = None
        self._output = self._Output(self)
        self._instruments: typing.Dict[str, AcquisitionTranslator.Instrument] = dict()

        self.data_consolidation_time: float = 0.1
        self._send_data_task: typing.Optional[asyncio.Task] = None

    async def start(self) -> None:
        self._run_task = asyncio.ensure_future(self.run())

    async def shutdown(self) -> None:
        t = self._run_task
        self._run_task = None
        if t:
            try:
                t.cancel()
            except:
                pass
            try:
                await t
            except:
                pass

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

        self.bus = None

        try:
            self.writer.close()
        except OSError:
            pass

    async def _send_queued_data(self) -> None:
        await asyncio.sleep(self.data_consolidation_time)

        for source, values in self._output.queued_data.items():
            if not values:
                continue
            self.send_data(source, values)
            values.clear()
        self._send_data_task = None

    def _queue_send_data(self) -> None:
        if self._send_data_task:
            return
        self._send_data_task = asyncio.get_event_loop().create_task(self._send_queued_data())

    def incoming_message(self, source: str, record: str, message: typing.Any) -> None:
        if record == 'data':
            if self.use_instantaneous:
                self._handle_data(source, message)
            return
        elif record == 'avg' or record == 'noavg':
            if not self.use_instantaneous:
                self._handle_data(source, message)
            return
        elif record.startswith('avg.'):
            if not self.use_instantaneous:
                self._handle_data(source, message)
            return
        elif record == 'chat':
            self.send_chat(message['epoch_ms'], message['name'], message['text'])
            return
        elif record == 'event_log':
            data: typing.Dict[str, typing.Any] = {
                'message': message["message"],
                'epoch_ms': round(time.time() * 1000.0),
            }
            message_type = message.get('type')
            if message_type == 'info':
                pass
            elif message_type == 'communications_established':
                pass
            elif message_type == 'communications_lost':
                data['level'] = 'error'
            elif message_type == 'error':
                data['level'] = 'error'
            else:
                # source = message['author']
                return
            self.send_event_log(source, data)
            return
        elif record == 'state':
            if message:
                instrument = self._instruments.get(source)
                if instrument is not None:
                    instrument.translate_state(message, self._output)
                else:
                    self.translator.translate_message(source, record, message, self._output)
            return
        elif record == 'instrument':
            self._handle_instrument(source, message)
            return
        elif record == 'bypass_held':
            return
        elif record == 'bypass_user':
            return
        elif record == 'command':
            return
        elif record == 'restart_acquisition':
            return

        instrument = self._instruments.get(source)
        if instrument:
            instrument.translate_message(record, message, self._output)
            return

        self.translator.translate_message(source, record, message, self._output)

    def _handle_instrument(self, source: str, instrument_info: typing.Optional[typing.Dict[str, typing.Any]]) -> None:
        existing = self._instruments.get(source)
        if not instrument_info or not isinstance(instrument_info, dict):
            if existing is None:
                return
            del self._instruments[source]
            self.send_instrument_remove(source)
            self._output.queued_data.pop(source, None)
            return

        if existing is None:
            existing = self.translator.instrument_translator(source, instrument_info)
            if not existing:
                return
            self._instruments[source] = existing
            self.send_instrument_add(source, existing.info)
            return

        existing.update_information(instrument_info)
        self.send_instrument_update(source, existing.info)

    def _handle_data(self, source: str, values: typing.Dict[str, typing.Any]) -> None:
        if not values or not isinstance(values, dict):
            return
        instrument = self._instruments.get(source)
        if instrument is None:
            return
        instrument.translate_data(values, self._output)

    async def incoming_command(self, target: str, command: str, data: typing.Any) -> None:
        await self.bus.send_message(PersistenceLevel.DATA, 'command', {
            'target': target,
            'command': command,
            'data': data,
        })

    async def incoming_bypass(self, bypassed: bool) -> None:
        await self.bus.send_message(PersistenceLevel.SYSTEM, 'bypass_user', bypassed and 1 or 0)

    async def incoming_message_log(self, author: str, text: str, auxiliary: typing.Any) -> None:
        await self.bus.send_message(PersistenceLevel.DATA, 'event_log', {
            'type': 'user',
            'author': author,
            'message': text,
            'auxiliary': auxiliary,
        })

    async def incoming_restart(self) -> None:
        await self.bus.send_message(PersistenceLevel.SYSTEM, 'restart_acquisition', 1)

    async def incoming_chat(self, epoch_ms: int, name: str, text: str) -> None:
        await self.bus.send_message(PersistenceLevel.DATA, 'chat', {
            'epoch_ms': epoch_ms,
            'name': name,
            'text': text,
        })

