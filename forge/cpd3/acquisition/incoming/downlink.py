import typing
import asyncio
import logging
from abc import ABC, abstractmethod
from starlette.websockets import WebSocket
from forge.const import STATIONS
from forge.cpd3.identity import Name as RealtimeName
from forge.vis.station.cpd3 import RealtimeTranslator, AcquisitionTranslator
from forge.vis.realtime.translation import get_translator as get_realtime_translator
from forge.vis.realtime.controller.client import WriteData as RealtimeOutput
from forge.vis.acquisition.translation import get_translator as get_acquisition_translator
from forge.vis.acquisition.controller.client import Client as AcquisitionClient
from . import CONFIGURATION
from .socket import AcquisitionSocket


_LOGGER = logging.getLogger(__name__)


class _AcquisitionTranslatorClient(AcquisitionClient):
    class IncomingDataResult:
        def __init__(self):
            self.source_data: typing.Dict[str, typing.Dict[str, typing.Any]] = dict()
            self.updated_state: typing.Set["_AcquisitionTranslatorClient._ActiveInterface"] = set()

        def __getitem__(self, source: str) -> typing.Dict[str, typing.Any]:
            result = self.source_data.get(source)
            if result is None:
                result: typing.Dict[str, typing.Any] = dict()
                self.source_data[source] = result
            return result

    class _BaseDispatch(ABC):
        @abstractmethod
        def __call__(self, result: "_AcquisitionTranslatorClient.IncomingDataResult", value: typing.Any) -> None:
            pass

    class _DataDispatch(_BaseDispatch):
        class Target:
            def __init__(self, interface: "_AcquisitionTranslatorClient._ActiveInterface", field: str):
                self.interface = interface
                self.field = field

            def __call__(self, result: "_AcquisitionTranslatorClient.IncomingDataResult",
                         value: typing.Any) -> None:
                result[self.interface.source][self.field] = value

        class TranslatedTarget(Target):
            def __init__(self, interface: "_AcquisitionTranslatorClient._ActiveInterface", field: str,
                         translator: typing.Callable[[typing.Any], typing.Any]):
                super().__init__(interface, field)
                self.translator = translator

            def __call__(self, result: "_AcquisitionTranslatorClient.IncomingDataResult",
                         value: typing.Any) -> None:
                value = self.translator(value)
                super().__call__(result, value)

        def __init__(self):
            self.targets: typing.List[_AcquisitionTranslatorClient._DataDispatch.Target] = list()

        def __call__(self, result: "_AcquisitionTranslatorClient.IncomingDataResult",
                     value: typing.Any) -> None:
            for t in self.targets:
                t(result, value)

    class _NotificationWarningDispatch(_BaseDispatch):
        def __init__(self, interface: "_AcquisitionTranslatorClient._ActiveInterface",
                     source_key: typing.Hashable = None):
            super().__init__()
            self.interface = interface
            self.notifications: typing.Set[str] = set()
            if source_key is None:
                source_key = self
            self.source_key = source_key
            interface.notifications[self.source_key] = self.notifications

        def update_state(self, value: typing.Any) -> None:
            pass

        def __call__(self, result: "_AcquisitionTranslatorClient.IncomingDataResult",
                     value: typing.Any) -> None:
            prior_notifications = set(self.notifications)
            prior_warning = len(self.interface.warnings) != 0
            self.notifications.clear()
            self.interface.warnings.discard(self.source_key)

            self.update_state(value)

            if prior_notifications != self.notifications or prior_warning != (len(self.interface.warnings) != 0):
                result.updated_state.add(self.interface)

    class _ZStateDispatch(_NotificationWarningDispatch):
        def __init__(self, interface: "_AcquisitionTranslatorClient._ActiveInterface"):
            super().__init__(interface, source_key='ZSTATE')

        def update_state(self, value: typing.Any) -> None:
            if not isinstance(value, str):
                return

            for raw in value.split(','):
                translated = self.interface.active.interface.zstate_notifications.get(raw)
                if translated:
                    self.notifications.add(translated)
                if raw in self.interface.active.interface.zstate_set_warning:
                    self.interface.warnings.add(self.source_key)

    class _FlagsDispatch(_NotificationWarningDispatch):
        def __init__(self, interface: "_AcquisitionTranslatorClient._ActiveInterface"):
            super().__init__(interface, source_key='F1')

        def update_state(self, value: typing.Any) -> None:
            if value is None:
                return

            if isinstance(value, str):
                value = [value]
            try:
                for raw in value:
                    translated = self.interface.active.interface.flags_notifications.get(raw)
                    if translated:
                        self.notifications.add(translated)
                    if raw in self.interface.active.interface.flags_set_warning:
                        self.interface.warnings.add(self.source_key)
            except TypeError:
                pass

    class _ActiveInterface:
        def __init__(self, source: str, active: AcquisitionTranslator.ActiveInterface):
            self.source = source
            self.active = active
            self.display_state: typing.Dict[str, typing.Any] = dict()
            self.notifications: typing.Dict[typing.Hashable, typing.Set[str]] = dict()
            self.warnings: typing.Set[typing.Hashable] = set()

        def update_instrument_state(self, state: typing.Optional[typing.Dict[str, typing.Any]]) -> None:
            self.active.update_instrument_state(state)
            self.display_state = self.active.interface.display_state(state)

        def update_interface_information(self, info: typing.Optional[typing.Dict[str, typing.Any]]) -> typing.Any:
            return self.active.display_information(info)

        @property
        def instrument_state(self) -> typing.Dict[str, typing.Any]:
            result = dict(self.display_state)

            notifications = set()
            for add in self.notifications.values():
                notifications.update(add)
            result['notifications'] = notifications

            if len(self.warnings) != 0:
                result['warning'] = True

            return result

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
                 translator: AcquisitionTranslator, acquisition: AcquisitionSocket):
        super().__init__(reader, writer)
        self.translator = translator
        self.acquisition = acquisition

        self._active_interfaces: typing.Dict[str, _AcquisitionTranslatorClient._ActiveInterface] = dict()

        self._data_dispatch: typing.Dict[RealtimeName, typing.Callable[[_AcquisitionTranslatorClient.IncomingDataResult, typing.Any], None]] = dict()

    async def incoming_bypass(self, bypassed: bool) -> None:
        if bypassed:
            await self.acquisition.bypass_set()
        else:
            await self.acquisition.bypass_clear_all()

    async def incoming_message_log(self, author: str, text: str, auxiliary: typing.Any) -> None:
        if not isinstance(auxiliary, dict):
            auxiliary = {}
        await self.acquisition.message_log({
            'Author': author,
            'Text': text,
            'Information': {
                'Origin': 'Forge',
                'UserID': auxiliary.get('user_id'),
                'UserName': auxiliary.get('user_name'),
                'NetworkOrigin': auxiliary.get('network_origin'),
            },
        })

    async def incoming_restart(self) -> None:
        await self.acquisition.restart_acquisition_system()

    async def incoming_command(self, target: str, command: str, data: typing.Any) -> None:
        if target:
            interface = self._active_interfaces.get(target)
            if interface:
                result = interface.active.translate_command(command, data)
                if result:
                    await self.acquisition.command(target=target, command=data)

    async def acquisition_event(self, event: typing.Dict[str, typing.Any]) -> None:
        data = {
            'message': event.get('Text'),
        }
        if event.get('ShowRealtime'):
            data['level'] = 'error'
        event_time = event.get('Time')
        if event_time:
            data['epoch_ms'] = round(float(event_time) * 1000)
        self.send_event_log(str(event.get('Source', '')), data)

    async def acquisition_interface_information(self, interface_name: str,
                                                info: typing.Optional[typing.Dict[str, typing.Any]]) -> None:
        if info is None:
            existing_interface = self._active_interfaces.pop(interface_name, None)
            if existing_interface is not None:
                self.send_instrument_remove(interface_name)
                self._data_dispatch.clear()
            return

        existing_interface = self._active_interfaces.get(interface_name)
        if existing_interface is not None:
            if not existing_interface.active.matches(info):
                del self._active_interfaces[interface_name]
                self.send_instrument_remove(interface_name)
            else:
                self.send_instrument_update(interface_name, existing_interface.update_interface_information(info))
                return

        self._data_dispatch.clear()
        for interface in self.translator.interfaces:
            if not interface.matches(interface_name, info):
                continue
            existing_interface = self._ActiveInterface(interface_name, interface.activate(interface_name, info))
            self._active_interfaces[interface_name] = existing_interface
            self.send_instrument_add(interface_name, existing_interface.active.display_information(info))
            break

    async def acquisition_interface_state(self, interface_name: str,
                                          state: typing.Optional[typing.Dict[str, typing.Any]]) -> None:
        interface = self._active_interfaces.get(interface_name)
        if interface:
            interface.update_instrument_state(state)
            self.send_instrument_state(interface_name, interface.instrument_state)

    def _assemble_dispatch(self, name: RealtimeName) -> typing.Callable[[IncomingDataResult, typing.Any], None]:
        def null_dispatch(result: _AcquisitionTranslatorClient.IncomingDataResult, value: typing.Any) -> None:
            pass

        if self.acquisition.has_instantaneous_data:
            if name.archive != 'rt_instant':
                return null_dispatch
        else:
            if name.archive != 'raw':
                return null_dispatch

        for interface in self._active_interfaces.values():
            override = interface.active.translator_override(name)
            if override:
                return self.translator.translator_shim(name, override)

        variable_split = name.variable.split('_', 2)
        if len(variable_split) < 2:
            return self._BaseDispatch()
        variable_source = variable_split[0]
        interface_source = variable_split[1]

        interface = self._active_interfaces.get(interface_source)
        if interface is None:
            return self.translator.translator_shim(name, null_dispatch)

        if variable_source == 'ZSTATE':
            return self.translator.translator_shim(name, self._ZStateDispatch(interface))
        elif variable_source == 'F1':
            return self.translator.translator_shim(name, self._FlagsDispatch(interface))

        dispatch = self._DataDispatch()

        variable = AcquisitionTranslator.Variable(variable_source, name.flavors)
        translated_field = interface.active.interface.variable_map.get(variable)
        if translated_field:
            dispatch.targets.append(self._DataDispatch.Target(interface, translated_field))

        variable = AcquisitionTranslator.Variable(variable_source)
        translated_field = interface.active.interface.variable_map.get(variable)
        if translated_field:
            dispatch.targets.append(self._DataDispatch.Target(interface, translated_field))

        translated_field, translation = interface.active.interface.value_translator(name)
        if translated_field and translation:
            dispatch.targets.append(self._DataDispatch.TranslatedTarget(interface, translated_field, translation))

        return self.translator.translator_shim(name, dispatch)

    async def acquisition_data(self, values: typing.Dict[RealtimeName, typing.Any]) -> None:
        result = self.IncomingDataResult()
        for name, value in values.items():
            dispatch = self._data_dispatch.get(name)
            if dispatch is None:
                dispatch = self._assemble_dispatch(name)
                self._data_dispatch[name] = dispatch
            dispatch(result, value)

        for source, data in result.source_data.items():
            self.send_data(source, data)
        for interface in result.updated_state:
            self.send_instrument_state(interface.source, interface.instrument_state)


class DownlinkSocket(AcquisitionSocket):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.station: str = None
        self.realtime_translator: typing.Optional[RealtimeTranslator] = None
        self.realtime_output: typing.Optional[RealtimeOutput] = None
        self.acquisition_translator: typing.Optional[AcquisitionTranslator] = None
        self.acquisition_client: typing.Optional[_AcquisitionTranslatorClient] = None
        self._acquisition_run: typing.Optional[asyncio.Task] = None

    async def handshake(self, websocket: WebSocket, data: bytes) -> bool:
        self.station = websocket.path_params['station'].lower()
        if self.station not in STATIONS:
            _LOGGER.debug(f"Rejecting invalid station {self.station} for {self.display_id}")
            return False

        self.display_id = f"{self.display_id} - {self.station.upper()}"
        if not await websocket.scope['processing'].acquisition_uplink_authorized(self.public_key, self.station):
            _LOGGER.debug(f"Rejected CPD3 acquisition uplink for {self.display_id}")
            return False

        translator = get_realtime_translator(self.station)
        if translator and isinstance(translator, RealtimeTranslator):
            realtime_socket_name = CONFIGURATION.get('REALTIME.SOCKET', '/run/forge-vis-realtime.socket')
            _LOGGER.debug(f"Connecting realtime translator for {self.display_id} to {realtime_socket_name}")
            try:
                _, writer = await asyncio.open_unix_connection(realtime_socket_name)
                realtime_output = RealtimeOutput(writer)
                await realtime_output.connect()

                self.realtime_translator = translator
                self.realtime_output = realtime_output
            except OSError:
                _LOGGER.warning(f"Failed to connect realtime data socket {realtime_socket_name}", exc_info=True)

        translator = get_acquisition_translator(self.station)
        if translator and isinstance(translator, AcquisitionTranslator):
            acquisition_socket_name = CONFIGURATION.get('ACQUISITION.SOCKET', '/run/forge-vis-acquisition.socket')
            _LOGGER.debug(f"Connecting acquisition translator for {self.display_id} to {acquisition_socket_name}")
            try:
                reader, writer = await asyncio.open_unix_connection(acquisition_socket_name)
                acquisition_client = _AcquisitionTranslatorClient(reader, writer, translator, self)
                await acquisition_client.connect(self.station, False)

                self.acquisition_translator = translator
                self.acquisition_client = acquisition_client
                self._acquisition_run = asyncio.ensure_future(self.acquisition_client.run())
            except OSError:
                _LOGGER.warning(f"Failed to connect acquisition controller socket {acquisition_socket_name}", exc_info=True)

        _LOGGER.debug(f"Acquisition uplink connected for {self.display_id}")
        return await super().handshake(websocket, data)

    async def on_disconnect(self, websocket: WebSocket, close_code):
        if self._acquisition_run:
            t = self._acquisition_run
            self._acquisition_run = None
            try:
                t.cancel()
            except:
                pass
            try:
                await t
            except:
                pass

        await super().on_disconnect(websocket, close_code)

        if self.realtime_output:
            try:
                self.realtime_output.writer.close()
            except OSError:
                pass
        if self.acquisition_client:
            try:
                self.acquisition_client.writer.close()
            except OSError:
                pass

    async def _send_realtime_data(self, values: typing.Dict[RealtimeName, typing.Any]) -> None:
        if not self.realtime_translator:
            return
        if not self.realtime_output:
            return

        data: typing.Dict[str, typing.Dict[str, typing.Optional[typing.Union[float, typing.List[float]]]]] = dict()
        for name, value in values.items():
            if name.archive != 'raw':
                continue
            targets = self.realtime_translator.realtime_targets(RealtimeTranslator.Key(name.variable, name.flavors))
            for target in targets:
                data_target = data.get(target.data_name)
                if data_target is None:
                    data_target: typing.Dict[str, typing.Optional[typing.Union[float, typing.List[float]]]] = dict()
                    data[target.data_name] = data_target
                data_target[target.field] = value

        for data_name, record in data.items():
            await self.realtime_output.send_data(self.station, data_name, record)

    async def incoming_data(self, values: typing.Dict[RealtimeName, typing.Any]) -> None:
        await self._send_realtime_data(values)

        if self.acquisition_client:
            await self.acquisition_client.acquisition_data(values)

    async def incoming_event(self, event: typing.Dict[str, typing.Any]) -> None:
        if self.acquisition_client:
            await self.acquisition_client.acquisition_event(event)

    async def autoprobe_state_updated(self, state: typing.Dict[str, typing.Any]) -> None:
        pass

    async def interface_information_updated(self, interface_name: str,
                                            information: typing.Optional[typing.Dict[str, typing.Any]]) -> None:
        if self.acquisition_client:
            await self.acquisition_client.acquisition_interface_information(interface_name, information)

    async def interface_state_updated(self, interface_name: str,
                                      state: typing.Optional[typing.Dict[str, typing.Any]]) -> None:
        if self.acquisition_client:
            await self.acquisition_client.acquisition_interface_state(interface_name, state)
