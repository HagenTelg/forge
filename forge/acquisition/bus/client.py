import typing
import asyncio
import struct
from forge.acquisition.bus.protocol import PersistenceLevel, serialize_string, deserialize_string, serialize_value, deserialize_value


class AcquisitionBusClient:
    def __init__(self, source: str, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self.source = source
        self.reader = reader
        self.writer = writer
        self._reader: typing.Optional[asyncio.Task] = None

    async def incoming_message(self, source: str, record: str, message: typing.Any) -> None:
        pass

    async def _run(self):
        while True:
            try:
                source = await deserialize_string(self.reader)
                record = await deserialize_string(self.reader)
                value = await deserialize_value(self.reader)
            except (OSError, UnicodeDecodeError, EOFError):
                if self.writer:
                    try:
                        self.writer.close()
                    except OSError:
                        pass
                    self.writer = None
                return
            await self.incoming_message(source, record, value)

    async def start(self) -> None:
        serialize_string(self.writer, self.source)
        self._reader = asyncio.ensure_future(self._run())

    async def shutdown(self) -> None:
        if self._reader:
            t = self._reader
            self._reader = None
            try:
                t.cancel()
            except:
                pass
            try:
                await t
            except:
                pass
        if self.writer:
            try:
                self.writer.close()
            except OSError:
                pass
            self.writer = None

    def send_message(self, level: PersistenceLevel, record: str, message: typing.Any) -> None:
        if not self.writer:
            return
        self.writer.write(struct.pack('<B', level.value))
        serialize_string(self.writer, record)
        serialize_value(self.writer, message)

    def send_data(self, record: str, message: typing.Any) -> None:
        self.send_message(PersistenceLevel.DATA, record, message)

    def set_state(self, record: str, message: typing.Any) -> None:
        self.send_message(PersistenceLevel.STATE, record, message)

    def set_source_information(self, record: str, message: typing.Any) -> None:
        self.send_message(PersistenceLevel.SOURCE, record, message)

    def set_system_information(self, record: str, message: typing.Any) -> None:
        self.send_message(PersistenceLevel.SYSTEM, record, message)


if __name__ == '__main__':
    import argparse
    import os
    from json import dumps as to_json
    from forge.acquisition import CONFIGURATION

    parser = argparse.ArgumentParser(description="Acquisition bus test client.")
    parser.add_argument('--socket',
                        dest='socket', type=str,
                        default=CONFIGURATION.get('ACQUISITION.BUS', '/run/forge-acquisition-bus.socket'),
                        help="server socket")
    args = parser.parse_args()

    client: typing.Optional[AcquisitionBusClient] = None

    async def start():
        reader, writer = await asyncio.open_unix_connection(args.socket)

        class DebugClient(AcquisitionBusClient):
            async def incoming_message(self, source: str, record: str, message: typing.Any) -> None:
                message = to_json(message)
                print(f"{source}:{record} = {message}", flush=True)

        global client
        client = DebugClient(f'__debug{os.getpid()}', reader, writer)
        await client.start()


    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(start())
    loop.run_forever()
    loop.run_until_complete(client.shutdown())
    loop.close()
