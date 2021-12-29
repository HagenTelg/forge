import typing
import asyncio
import logging
import struct
from .protocol import ToServerPacketType, FromServerPacketType
from forge.cpd3.variant import serialize as variant_serialize, deserialize as variant_deserialize
from forge.cpd3.identity import Name as RealtimeName
from forge.cpd3.timeinterval import TimeUnit, TimeInterval


_LOGGER = logging.getLogger(__name__)


class Client:
    CONNECTION_TIMEOUT = 30.0

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self.reader = reader
        self.writer = writer

    async def _connect(self) -> None:
        _LOGGER.debug("Starting connection to CPD3 acquisition")
        self.writer.write(struct.pack('<B', ToServerPacketType.HELLO.value))
        await self.writer.drain()
        response = FromServerPacketType(struct.unpack('<B', await self.reader.readexactly(1))[0])
        if response != FromServerPacketType.HELLO:
            raise ValueError(f"Invalid response from server {response}")
        _LOGGER.debug("Connection established to CPD3 acquisition")

    async def _ping(self) -> None:
        while True:
            await asyncio.sleep(10)
            self.writer.write(struct.pack('<B', ToServerPacketType.PING.value))
            await self.writer.drain()

    async def _read_string(self) -> str:
        size = struct.unpack('<H', await self.reader.readexactly(2))[0]
        raw = await self.reader.readexactly(size)
        return raw.decode('utf-8')

    async def _read_value(self) -> typing.Any:
        size = struct.unpack('<I', await self.reader.readexactly(4))[0]
        return variant_deserialize(await self.reader.readexactly(size))

    def _write_string(self, s: str) -> None:
        raw = s.encode('utf-8')
        self.writer.write(struct.pack('<H', len(raw)))
        self.writer.write(raw)

    def _write_value(self, value: typing.Any) -> None:
        raw = variant_serialize(value)
        self.writer.write(struct.pack('<I', len(raw)))
        self.writer.write(raw)

    async def _process_incoming(self) -> None:
        name_lookup: typing.List[RealtimeName] = list()
        name_lookup_replace: int = 0
        while True:
            packet_type = FromServerPacketType(struct.unpack('<B', await self.reader.readexactly(1))[0])
            if packet_type == FromServerPacketType.REALTIME_VALUE:
                name_index = struct.unpack('<H', await self.reader.readexactly(2))[0]
                value = await self._read_value()
                if name_index < len(name_lookup):
                    await self.incoming_value(name_lookup[name_index], value)
                else:
                    _LOGGER.debug(f"Realtime name index {name_index} out of range")
            elif packet_type == FromServerPacketType.REALTIME_NAME:
                size = struct.unpack('<I', await self.reader.readexactly(4))[0]
                name = RealtimeName.deserialize(await self.reader.readexactly(size))
                if len(name_lookup) < 0xFFFF:
                    name_lookup.append(name)
                else:
                    name_lookup[name_lookup_replace] = name
                    name_lookup_replace = (name_lookup_replace + 1) % 0xFFFF
            elif packet_type == FromServerPacketType.EVENT:
                event = await self._read_value()
                await self.incoming_event(event)
            elif packet_type == FromServerPacketType.AUTOPROBE_STATE:
                state = await self._read_value()
                await self.incoming_autoprobe_state(state)
            elif packet_type == FromServerPacketType.INTERFACE_INFORMATION:
                name = await self._read_string()
                info = await self._read_value()
                await self.incoming_interface_information(name, info)
            elif packet_type == FromServerPacketType.INTERFACE_STATE:
                name = await self._read_string()
                info = await self._read_value()
                await self.incoming_interface_state(name, info)
            elif packet_type == FromServerPacketType.PONG:
                pass
            elif packet_type == FromServerPacketType.ARCHIVE_VALUE:
                size = struct.unpack('<I', await self.reader.readexactly(4))[0]
                await self.reader.readexactly(size)
                _LOGGER.warning("Unsolicited CPD3 archive value packet")
            elif packet_type == FromServerPacketType.ARCHIVE_END:
                _LOGGER.warning("Unsolicited CPD3 archive end packet")
            else:
                raise ValueError(f"Invalid response from server {packet_type}")

    async def run(self) -> typing.NoReturn:
        await asyncio.wait_for(self._connect(), timeout=self.CONNECTION_TIMEOUT)
        await self.connection_ready()

        self.writer.write(struct.pack('<B', ToServerPacketType.REALTIME_RESEND.value))
        await self.writer.drain()

        tasks = [
            asyncio.ensure_future(self._ping()),
            asyncio.ensure_future(self._process_incoming()),
        ]
        try:
            await asyncio.wait(tasks)
        finally:
            for t in tasks:
                try:
                    t.cancel()
                except:
                    pass
                try:
                    await t
                except:
                    pass

    async def message_log(self, log_event: typing.Dict[str, typing.Any]) -> None:
        self.writer.write(struct.pack('<B', ToServerPacketType.MESSAGE_LOG_EVENT.value))
        self._write_value(log_event)

    async def command(self, command: typing.Any = None, target: typing.Optional[str] = None) -> None:
        self.writer.write(struct.pack('<B', ToServerPacketType.COMMAND.value))
        self._write_string(target or '')
        self._write_value(command)
        await self.writer.drain()

    async def system_flush(self, duration: typing.Optional[float] = None) -> None:
        if duration is None:
            duration = -1.0
        self.writer.write(struct.pack('<Bd', ToServerPacketType.SYSTEM_FLUSH.value, duration))
        await self.writer.drain()

    async def set_averaging_time(self, interval: TimeInterval) -> None:
        self.writer.write(struct.pack('<BBiB', ToServerPacketType.SYSTEM_FLUSH.value,
                                      interval.unit and interval.unit.value or TimeUnit.Minute.value,
                                      interval.count is not None and interval.count or 1,
                                      interval.align is not None and int(interval.align) or 1))
        await self.writer.drain()

    async def data_flush(self) -> None:
        self.writer.write(struct.pack('<B', ToServerPacketType.DATA_FLUSH.value))

    async def bypass(self, flag: str = 'Bypass') -> None:
        self.writer.write(struct.pack('<B', ToServerPacketType.BYPASS_FLAG_SET.value))
        self._write_string(flag)
        await self.writer.drain()

    async def unbypass(self, flag: str = 'Bypass') -> None:
        self.writer.write(struct.pack('<B', ToServerPacketType.BYPASS_FLAG_CLEAR.value))
        self._write_string(flag)
        await self.writer.drain()

    async def unbypass_override(self) -> None:
        self.writer.write(struct.pack('<B', ToServerPacketType.BYPASS_FLAGS_CLEAR_ALL.value))
        await self.writer.drain()

    async def flag(self, flag: str = 'Contaminated') -> None:
        self.writer.write(struct.pack('<B', ToServerPacketType.SYSTEM_FLAG_SET.value))
        self._write_string(flag)
        await self.writer.drain()

    async def unflag(self, flag: str = 'Contaminated') -> None:
        self.writer.write(struct.pack('<B', ToServerPacketType.SYSTEM_FLAG_CLEAR.value))
        self._write_string(flag)
        await self.writer.drain()

    async def unflag_override(self) -> None:
        self.writer.write(struct.pack('<B', ToServerPacketType.SYSTEM_FLAGS_CLEAR_ALL.value))
        await self.writer.drain()

    async def restart_acquisition_system(self) -> None:
        self.writer.write(struct.pack('<B', ToServerPacketType.RESTART_REQUEST.value))
        await self.writer.drain()

    async def connection_ready(self) -> None:
        pass

    async def incoming_event(self, event: typing.Any) -> None:
        pass

    async def incoming_autoprobe_state(self, state: typing.Any) -> None:
        pass

    async def incoming_interface_information(self, interface: str, info: typing.Any) -> None:
        pass

    async def incoming_interface_state(self, interface: str, state: typing.Any) -> None:
        pass

    async def incoming_value(self, name: RealtimeName, value: typing.Any) -> None:
        pass
