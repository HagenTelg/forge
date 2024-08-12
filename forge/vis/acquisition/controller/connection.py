import typing
import asyncio
import struct
import logging
from .protocol import PacketType, deserialize_value, serialize_value

_LOGGER = logging.getLogger(__name__)


class Station:
    class Instrument:
        def __init__(self, source: str):
            self.source = source
            self.information: typing.Optional[typing.Dict[str, typing.Any]] = None
            self.state: typing.Optional[typing.Dict[str, typing.Any]] = None

        def update_information(self, info: typing.Dict[str, typing.Any]) -> bool:
            update = info != self.information
            self.information = info
            return update

        def update_state(self, state: typing.Dict[str, typing.Any]) -> bool:
            update = state != self.state
            self.state = state
            return update

    class DataSource:
        def __init__(self, source: str):
            self.source = source
            self.values: typing.Dict[str, typing.Any] = dict()

        def update(self, values: typing.Dict[str, typing.Any]) -> None:
            for name, value in values.items():
                if value is None:
                    self.values.pop(name, None)
                    continue
                self.values[name] = value

    class Connection:
        def __init__(self, station: "Station", reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
            self.station = station
            self.reader = reader
            self.writer = writer
            self.instruments: typing.Dict[str, Station.Instrument] = dict()
            self.data: typing.Dict[str, Station.DataSource] = dict()

            self._read_task: typing.Optional[asyncio.Task] = None

        async def _read_string(self) -> str:
            s_len = struct.unpack('<I', await self.reader.readexactly(4))[0]
            return (await self.reader.readexactly(s_len)).decode('utf-8')

        def write_string(self, s: str) -> None:
            raw = s.encode('utf-8')
            self.writer.write(struct.pack('<I', len(raw)))
            self.writer.write(raw)

        async def _read_incoming(self) -> typing.NoReturn:
            while True:
                packet_type = PacketType(struct.unpack('<B', await self.reader.readexactly(1))[0])

                if packet_type == PacketType.DATA:
                    source = await self._read_string()
                    count = struct.unpack('<I', await self.reader.readexactly(4))[0]
                    
                    update: typing.Dict[str, typing.Any] = dict()
                    for i in range(count):
                        name = await self._read_string()                    
                        value = await deserialize_value(self.reader)
                        update[name] = value

                    data_source = self.data.get(source)
                    if not data_source:
                        data_source = Station.DataSource(source)
                        self.data[source] = data_source
                    data_source.update(update)

                    for c in self.station.connections:
                        if c == self:
                            continue
                        try:
                            c.update_data(data_source, update)
                        except OSError:
                            pass
                elif packet_type == PacketType.INSTRUMENT_ADD:
                    source = await self._read_string()
                    info = await deserialize_value(self.reader)
                    
                    existing = self.instruments.get(source)
                    if existing:
                        for c in self.station.connections:
                            if c == self:
                                continue
                            try:
                                c.remove_instrument(existing)
                            except OSError:
                                pass
                            
                    add = Station.Instrument(source)
                    add.update_information(info)
                    self.instruments[source] = add
                    for c in self.station.connections:
                        if c == self:
                            continue
                        try:
                            c.add_instrument(add)
                        except OSError:
                            pass
                elif packet_type == PacketType.INSTRUMENT_UPDATE:
                    source = await self._read_string()
                    info = await deserialize_value(self.reader)

                    existing = self.instruments.get(source)
                    if not existing:
                        _LOGGER.warning(f"No instrument defined for {source}")
                        continue

                    if not existing.update_information(info):
                        continue

                    for c in self.station.connections:
                        if c == self:
                            continue
                        try:
                            c.update_instrument(existing)
                        except OSError:
                            pass
                elif packet_type == PacketType.INSTRUMENT_REMOVE:
                    source = await self._read_string()

                    self.data.pop(source, None)
                    existing = self.instruments.pop(source, None)
                    if not existing:
                        _LOGGER.warning(f"No instrument defined for {source}")
                        continue

                    for c in self.station.connections:
                        if c == self:
                            continue
                        try:
                            c.remove_instrument(existing)
                        except OSError:
                            pass
                elif packet_type == PacketType.INSTRUMENT_STATE:
                    source = await self._read_string()
                    state = await deserialize_value(self.reader)

                    existing = self.instruments.get(source)
                    if not existing:
                        _LOGGER.warning(f"No instrument defined for {source}")
                        continue

                    if not existing.update_state(state):
                        continue

                    for c in self.station.connections:
                        if c == self:
                            continue
                        try:
                            c.send_instrument_state(existing)
                        except OSError:
                            pass
                elif packet_type == PacketType.EVENT_LOG:
                    source = await self._read_string()
                    event = await deserialize_value(self.reader)

                    for c in self.station.connections:
                        if c == self:
                            continue
                        try:
                            c.send_event_log(source, event)
                        except OSError:
                            pass
                elif packet_type == PacketType.COMMAND:
                    target = await self._read_string()
                    command = await self._read_string()
                    data = await deserialize_value(self.reader)

                    for c in self.station.connections:
                        if c == self:
                            continue
                        try:
                            c.send_command(target, command, data)
                        except OSError:
                            pass
                elif packet_type == PacketType.BYPASS:
                    bypassed = (struct.unpack('<B', await self.reader.readexactly(1))[0] != 0)

                    for c in self.station.connections:
                        if c == self:
                            continue
                        try:
                            c.send_bypass(bypassed)
                        except OSError:
                            pass
                elif packet_type == PacketType.WRITE_MESSAGE_LOG:
                    author = await self._read_string()
                    text = await self._read_string()
                    auxiliary = await deserialize_value(self.reader)
                    for c in self.station.connections:
                        if c == self:
                            continue
                        try:
                            c.write_message_log(author, text, auxiliary)
                        except OSError:
                            pass
                elif packet_type == PacketType.RESTART_SYSTEM:
                    for c in self.station.connections:
                        if c == self:
                            continue
                        try:
                            c.send_restart()
                        except OSError:
                            pass
                elif packet_type == PacketType.CHAT:
                    epoch_ms = struct.unpack('<Q', await self.reader.readexactly(8))[0]
                    name = await self._read_string()
                    text = await self._read_string()

                    for c in self.station.connections:
                        if c == self:
                            continue
                        try:
                            c.send_chat(epoch_ms, name, text)
                        except OSError:
                            pass
                else:
                    raise ValueError("Invalid packet type")

        async def run(self) -> None:
            self._read_task = asyncio.ensure_future(self._read_incoming())
            try:
                await self._read_task
            except (asyncio.CancelledError, EOFError):
                pass

        def close(self):
            t = self._read_task
            self._read_task = None
            if t:
                try:
                    t.cancel()
                except:
                    pass

        def send_existing_instruments(self, target: "Station.Connection") -> None:
            pass

        def send_cached_data(self, target: "Station.Connection") -> None:
            pass

        def update_data(self, source: "Station.DataSource", update: typing.Dict[str, typing.Any]) -> None:
            pass

        def add_instrument(self, instrument: "Station.Instrument") -> None:
            pass

        def update_instrument(self, instrument: "Station.Instrument") -> None:
            pass

        def remove_instrument(self, instrument: "Station.Instrument") -> None:
            pass

        def send_instrument_state(self, instrument: "Station.Instrument") -> None:
            pass

        def send_event_log(self, source: str, event: typing.Dict[str, typing.Any]) -> None:
            pass

        def send_command(self, target: str, command: str, data: typing.Any) -> None:
            pass

        def send_bypass(self, bypassed: bool) -> None:
            pass

        def write_message_log(self, author: str, text: str, auxiliary: typing.Any) -> None:
            pass

        def send_restart(self) -> None:
            pass

        def send_chat(self, epoch_ms: int, name: str, text: str) -> None:
            self.writer.write(struct.pack('<BQ', PacketType.CHAT.value, epoch_ms))
            self.write_string(name)
            self.write_string(text)

    class _AcquisitionConnection(Connection):
        def __init__(self, station: "Station", reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
            super().__init__(station, reader, writer)
            
        def send_existing_instruments(self, target: "Station.Connection") -> None:
            for inst in self.instruments.values():
                target.add_instrument(inst)

                if inst.state:
                    target.send_instrument_state(inst)

        def send_cached_data(self, target: "Station.Connection") -> None:
            for data in self.data.values():
                if len(data.values) == 0:
                    continue
                target.update_data(data, data.values)

        def send_command(self, target: str, command: str, data: typing.Any) -> None:
            self.writer.write(struct.pack('<B', PacketType.COMMAND.value))
            self.write_string(target)
            self.write_string(command)
            serialize_value(self.writer, data)

        def send_bypass(self, bypassed: bool) -> None:
            self.writer.write(struct.pack('<BB', PacketType.BYPASS.value, 1 if bypassed else 0))

        def write_message_log(self, author: str, text: str, auxiliary: typing.Any) -> None:
            self.writer.write(struct.pack('<B', PacketType.WRITE_MESSAGE_LOG.value))
            self.write_string(author)
            self.write_string(text)
            serialize_value(self.writer, auxiliary)

        def send_restart(self) -> None:
            self.writer.write(struct.pack('<B', PacketType.RESTART_SYSTEM.value))

    class _DisplayConnection(Connection):
        def __init__(self, station: "Station", reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
            super().__init__(station, reader, writer)

        def update_data(self, source: "Station.DataSource", update: typing.Dict[str, typing.Any]) -> None:
            self.writer.write(struct.pack('<B', PacketType.DATA.value))
            self.write_string(source.source)
            self.writer.write(struct.pack('<I', len(update)))
            for name, value in update.items():
                self.write_string(name)
                serialize_value(self.writer, value)

        def add_instrument(self, instrument: "Station.Instrument") -> None:
            self.writer.write(struct.pack('<B', PacketType.INSTRUMENT_ADD.value))
            self.write_string(instrument.source)
            serialize_value(self.writer, instrument.information)

        def update_instrument(self, instrument: "Station.Instrument") -> None:
            self.writer.write(struct.pack('<B', PacketType.INSTRUMENT_UPDATE.value))
            self.write_string(instrument.source)
            serialize_value(self.writer, instrument.information)

        def remove_instrument(self, instrument: "Station.Instrument") -> None:
            self.writer.write(struct.pack('<B', PacketType.INSTRUMENT_REMOVE.value))
            self.write_string(instrument.source)

        def send_instrument_state(self, instrument: "Station.Instrument") -> None:
            self.writer.write(struct.pack('<B', PacketType.INSTRUMENT_STATE.value))
            self.write_string(instrument.source)
            serialize_value(self.writer, instrument.state)

        def send_event_log(self, source: str, event: typing.Dict[str, typing.Any]) -> None:
            self.writer.write(struct.pack('<B', PacketType.EVENT_LOG.value))
            self.write_string(source)
            serialize_value(self.writer, event)

    def __init__(self, station: str):
        self.station = station
        self.connections: typing.Set[Station.Connection] = set()

    def attach_acquisition(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> "Station.Connection":
        connection = self._AcquisitionConnection(self, reader, writer)
        self.connections.add(connection)
        return connection

    def attach_display(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> "Station.Connection":
        connection = self._DisplayConnection(self, reader, writer)

        for c in self.connections:
            c.send_existing_instruments(connection)
            c.send_cached_data(connection)

        self.connections.add(connection)
        return connection

    def detach(self, connection: "Station.Connection") -> bool:
        self.connections.discard(connection)
        connection.station = None

        have_acquisition = False
        for c in self.connections:
            for i in connection.instruments.values():
                try:
                    c.remove_instrument(i)
                except OSError:
                    pass
            if isinstance(c, self._AcquisitionConnection):
                have_acquisition = True

        if not have_acquisition:
            for c in self.connections:
                c.close()

        return len(self.connections) == 0
