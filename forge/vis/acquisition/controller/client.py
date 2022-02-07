import typing
import asyncio
import struct
import logging
from forge.vis.acquisition.controller.protocol import ConnectionType, PacketType, deserialize_value, serialize_value

_LOGGER = logging.getLogger(__name__)


class Client:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self.reader = reader
        self.writer = writer

    async def _read_string(self) -> str:
        s_len = struct.unpack('<I', await self.reader.readexactly(4))[0]
        return (await self.reader.readexactly(s_len)).decode('utf-8')

    def _write_string(self, s: str) -> None:
        raw = s.encode('utf-8')
        self.writer.write(struct.pack('<I', len(raw)))
        self.writer.write(raw)

    async def connect(self, station: str, as_display: bool = False) -> None:
        if as_display:
            self.writer.write(struct.pack('<B', ConnectionType.DISPLAY.value))
        else:
            self.writer.write(struct.pack('<B', ConnectionType.ACQUISITION.value))
        self._write_string(station)
        await self.writer.drain()

        response = (struct.unpack('<B', await self.reader.readexactly(1)))[0]
        if response != 1:
            raise ConnectionError("Connection handshake refused")

    async def run(self) -> typing.NoReturn:
        while True:
            packet_type = PacketType(struct.unpack('<B', await self.reader.readexactly(1))[0])

            if packet_type == PacketType.DATA:
                source = await self._read_string()
                count = struct.unpack('<I', await self.reader.readexactly(4))[0]
                values: typing.Dict[str, typing.Any] = dict()
                for i in range(count):
                    name = await self._read_string()
                    value = await deserialize_value(self.reader)
                    values[name] = value
                await self.incoming_data(source, values)
            elif packet_type == PacketType.INSTRUMENT_ADD:
                source = await self._read_string()
                info = await deserialize_value(self.reader)
                await self.incoming_instrument_add(source, info)
            elif packet_type == PacketType.INSTRUMENT_UPDATE:
                source = await self._read_string()
                info = await deserialize_value(self.reader)
                await self.incoming_instrument_update(source, info)
            elif packet_type == PacketType.INSTRUMENT_REMOVE:
                source = await self._read_string()
                await self.incoming_instrument_remove(source)
            elif packet_type == PacketType.INSTRUMENT_STATE:
                source = await self._read_string()
                state = await deserialize_value(self.reader)
                await self.incoming_instrument_state(source, state)
            elif packet_type == PacketType.EVENT_LOG:
                source = await self._read_string()
                event = await deserialize_value(self.reader)
                await self.incoming_event_log(source, event)
            elif packet_type == PacketType.COMMAND:
                target = await self._read_string()
                command = await self._read_string()
                data = await deserialize_value(self.reader)
                await self.incoming_command(target, command, data)
            elif packet_type == PacketType.BYPASS:
                bypassed = (struct.unpack('<B', await self.reader.readexactly(1))[0] != 0)
                await self.incoming_bypass(bypassed)
            elif packet_type == PacketType.WRITE_MESSAGE_LOG:
                author = await self._read_string()
                text = await self._read_string()
                auxiliary = await deserialize_value(self.reader)
                await self.incoming_message_log(author, text, auxiliary)
            elif packet_type == PacketType.RESTART_SYSTEM:
                await self.incoming_restart()
            elif packet_type == PacketType.CHAT:
                epoch_ms = struct.unpack('<Q', await self.reader.readexactly(8))[0]
                name = await self._read_string()
                text = await self._read_string()
                await self.incoming_chat(epoch_ms, name, text)
            else:
                raise ValueError("Invalid packet type")
            
    def send_data(self, source: str, values: typing.Dict[str, typing.Any]) -> None:
        self.writer.write(struct.pack('<B', PacketType.DATA.value))
        self._write_string(source)
        self.writer.write(struct.pack('<I', len(values)))
        for name, value in values.items():
            self._write_string(name)
            serialize_value(self.writer, value)

    def send_instrument_add(self, source: str, information: typing.Optional[typing.Dict[str, typing.Any]]) -> None:
        self.writer.write(struct.pack('<B', PacketType.INSTRUMENT_ADD.value))
        self._write_string(source)
        serialize_value(self.writer, information)

    def send_instrument_update(self, source: str, information: typing.Dict[str, typing.Any]) -> None:
        self.writer.write(struct.pack('<B', PacketType.INSTRUMENT_UPDATE.value))
        self._write_string(source)
        serialize_value(self.writer, information)

    def send_instrument_remove(self, source: str) -> None:
        self.writer.write(struct.pack('<B', PacketType.INSTRUMENT_REMOVE.value))
        self._write_string(source)

    def send_instrument_state(self, source: str, state: typing.Dict[str, typing.Any]) -> None:
        self.writer.write(struct.pack('<B', PacketType.INSTRUMENT_STATE.value))
        self._write_string(source)
        serialize_value(self.writer, state)

    def send_event_log(self, source: str, event: typing.Dict[str, typing.Any]) -> None:
        self.writer.write(struct.pack('<B', PacketType.EVENT_LOG.value))
        self._write_string(source)
        serialize_value(self.writer, event)

    def send_command(self, target: str, command: str, data: typing.Any = None) -> None:
        self.writer.write(struct.pack('<B', PacketType.COMMAND.value))
        self._write_string(target)
        self._write_string(command)
        serialize_value(self.writer, data)

    def send_bypass(self, bypassed: bool) -> None:
        self.writer.write(struct.pack('<BB', PacketType.BYPASS.value, 1 if bypassed else 0))

    def send_message_log(self, author: str, text: str, auxiliary: typing.Any = None) -> None:
        self.writer.write(struct.pack('<B', PacketType.WRITE_MESSAGE_LOG.value))
        self._write_string(author)
        self._write_string(text)
        serialize_value(self.writer, auxiliary)

    def send_restart(self) -> None:
        self.writer.write(struct.pack('<B', PacketType.RESTART_SYSTEM.value))

    def send_chat(self, epoch_ms: int, name: str, text: str) -> None:
        self.writer.write(struct.pack('<BQ', PacketType.CHAT.value, epoch_ms))
        self._write_string(name)
        self._write_string(text)

    async def incoming_data(self, source: str, values: typing.Dict[str, typing.Any]) -> None:
        pass

    async def incoming_instrument_add(self, source: str, 
                                      information: typing.Optional[typing.Dict[str, typing.Any]]) -> None:
        pass

    async def incoming_instrument_update(self, source: str, information: typing.Dict[str, typing.Any]) -> None:
        pass

    async def incoming_instrument_remove(self, source: str) -> None:
        pass

    async def incoming_instrument_state(self, source: str, state: typing.Dict[str, typing.Any]) -> None:
        pass

    async def incoming_event_log(self, source: str, event: typing.Dict[str, typing.Any]) -> None:
        pass

    async def incoming_command(self, target: str, command: str, data: typing.Any) -> None:
        pass

    async def incoming_bypass(self, bypassed: bool) -> None:
        pass

    async def incoming_message_log(self, author: str, text: str, auxiliary: typing.Any) -> None:
        pass

    async def incoming_restart(self) -> None:
        pass

    async def incoming_chat(self, epoch_ms: int, name: str, text: str) -> None:
        pass


if __name__ == '__main__':
    import argparse
    import time
    from json import dumps as to_json
    from forge.vis import CONFIGURATION

    parser = argparse.ArgumentParser(description="Acquisition test client.")
    parser.add_argument('station',
                        help="station to connect")
    parser.add_argument('--socket',
                        dest='socket', type=str,
                        default=CONFIGURATION.get('ACQUISITION.SOCKET', '/run/forge-vis-acquisition.socket'),
                        help="server socket")
    args = parser.parse_args()

    async def run():
        reader, writer = await asyncio.open_unix_connection(args.socket)

        class DebugClient(Client):
            async def incoming_data(self, source: str, values: typing.Dict[str, typing.Any]) -> None:
                if len(values) == 0:
                    return
                print(f"{source} data: " + to_json(values), flush=True)

            async def incoming_instrument_add(self, source: str,
                                              information: typing.Optional[typing.Dict[str, typing.Any]]) -> None:
                print(f"{source} add: " + to_json(information), flush=True)

            async def incoming_instrument_update(self, source: str, information: typing.Dict[str, typing.Any]) -> None:
                print(f"{source} update: " + to_json(information), flush=True)

            async def incoming_instrument_remove(self, source: str) -> None:
                print(f"{source} remove")

            async def incoming_instrument_state(self, source: str, state: typing.Dict[str, typing.Any]) -> None:
                print(f"{source} state: " + to_json(state), flush=True)

            async def incoming_event_log(self, source: str, event: typing.Dict[str, typing.Any]) -> None:
                print(f"{source} event log: " + to_json(event), flush=True)

            async def incoming_chat(self, epoch_ms: int, name: str, text: str) -> None:
                print(f"Chat {name}: {text}", flush=True)

        acquisition_client = DebugClient(reader, writer)
        await acquisition_client.connect(args.station, True)
        await acquisition_client.run()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(run())
    loop.close()
