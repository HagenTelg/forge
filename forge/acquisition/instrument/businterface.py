import typing
import asyncio
from .base import BaseBusInterface
from ..bus.client import AcquisitionBusClient
from ..cutsize import CutSize


_EMPTY_SOURCES = dict()


class BusInterface(BaseBusInterface):
    class _Client(AcquisitionBusClient):
        def __init__(self, interface: "BusInterface",
                     source: str, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
            super().__init__(source, reader, writer)
            self.interface = interface

        async def incoming_message(self, source: str, record: str, message: typing.Any) -> None:
            await self.interface._client_message(source, record, message)

    def __init__(self, source: str, bus_socket: str):
        super().__init__(source)
        self.client: "BusInterface._Client" = None
        self._bus_socket_name = bus_socket

        self._data_source_dispatch: typing.Dict[str, typing.Dict[str, typing.List[typing.Callable[[typing.Any], None]]]] = dict()
        self._data_any_dispatch: typing.Dict[str, typing.List[typing.Callable[[typing.Any], None]]] = dict()

        self._bypass_held: typing.Set[str] = set()
        self.bypass_ignore_source: typing.Set[str] = set()
        self.bypass_only_source: typing.Optional[typing.Set[str]] = None

        self._command_dispatch: typing.Dict[str, typing.List[typing.Callable[[typing.Any], None]]] = dict()

    def _dispatch_data_record(self, source: str, message: typing.Dict[str, typing.Any]) -> None:
        if not isinstance(message, dict):
            return
        if not self._data_any_dispatch and not self._data_source_dispatch:
            return

        source_filtered = self._data_source_dispatch.get(source)
        if not source_filtered:
            source_filtered = _EMPTY_SOURCES
        for field, value in message.items():
            for target in self._data_any_dispatch.get(field):
                target(value)
            for target in source_filtered.get(field):
                target(value)

    def _handle_bypass(self, source: str, state: bool) -> None:
        if source in self.bypass_ignore_source:
            return
        elif self.bypass_only_source is not None and source not in self.bypass_only_source:
            return

        was_bypassed = bool(self._bypass_held)
        if state:
            self._bypass_held.add(source)
        else:
            self._bypass_held.discard(source)
        is_bypassed = bool(self._bypass_held)

        if was_bypassed != is_bypassed:
            self.bypass_updated()

    def _dispatch_command(self, message: typing.Dict[str, typing.Any]) -> None:
        if not isinstance(message, dict):
            return
        if not self._command_dispatch:
            return

        target = message.get('target')
        if target and target != self.source:
            return

        command = message.get('command')
        handlers = self._command_dispatch.get(command)
        if not handlers:
            return

        data = message.get('data')
        for h in handlers:
            h(data)

    @property
    def bypassed(self) -> bool:
        return bool(self._bypass_held)

    async def _client_message(self, source: str, record: str, message: typing.Any) -> None:
        if record == 'data':
            self._dispatch_data_record(source, message)
        elif record == 'bypass_held':
            self._handle_bypass(source, message)
        elif record == 'bypass_user':
            self._handle_bypass('', message)
        elif record == 'command':
            self._dispatch_command(message)

    async def start(self) -> None:
        reader, writer = await asyncio.open_unix_connection(self._bus_socket_name)
        self.client = self._Client(self, self.source, reader, writer)
        await self.client.start()

    async def shutdown(self) -> None:
        await self.client.shutdown()
        self.client = None

    async def set_instrument_info(self, contents: typing.Dict[str, typing.Any]) -> None:
        self.client.set_source_information('instrument', contents)

    async def set_instrument_state(self, contents: typing.Dict[str, typing.Any]) -> None:
        self.client.set_state('state', contents)

    async def emit_data_record(self, contents: typing.Dict[str, typing.Union[float, typing.List[float]]]) -> None:
        self.client.send_data('data', contents)

    async def emit_average_record(self, contents: typing.Dict[str, typing.Union[float, typing.List[float]]],
                                  cutsize: CutSize.Size = CutSize.Size.WHOLE) -> None:
        record = 'avg'
        if cutsize != CutSize.Size.WHOLE:
            record = record + '.' + str(cutsize).lower()
        self.client.send_data(record, contents)

    async def emit_averaged_extra(self, contents: typing.Dict[str, typing.Union[float, typing.List[float]]]) -> None:
        self.client.send_data('noavg', contents)

    async def set_state_value(self, name: str, contents: typing.Any) -> None:
        self.client.set_state(name, contents)

    async def set_bypass_held(self, held: bool) -> None:
        self.client.set_state('bypass_held', held)

    def log(self, message: str, auxiliary: typing.Dict[str, typing.Any] = None,
            type: "BaseBusInterface.LogType" = None) -> None:
        message: typing.Dict[str, typing.Any] = {
            'message': message,
            'type': (type.value if type else BaseBusInterface.LogType.INFO.value),
        }
        if auxiliary:
            message['auxiliary'] = auxiliary
        self.client.send_data('event_log', message)

    def connect_data(self, source: typing.Optional[str], field: str,
                     target: typing.Callable[[typing.Any], None]) -> None:
        if source:
            field_dispatch = self._data_source_dispatch.get(source)
            if not field_dispatch:
                field_dispatch = dict()
                self._data_source_dispatch[source] = field_dispatch
        else:
            field_dispatch = self._data_any_dispatch

        targets = field_dispatch.get(field)
        if not targets:
            targets = []
            field_dispatch[field] = targets

        targets.append(target)

    def connect_command(self, command: str, handler: typing.Callable[[typing.Any], None]) -> None:
        targets = self._command_dispatch.get(command)
        if not targets:
            targets = list()
            self._command_dispatch[command] = targets

        targets.append(handler)
