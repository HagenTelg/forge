import sys
import os
import asyncio
import typing
import struct
from dynaconf import Dynaconf
from dynaconf.constants import DEFAULT_SETTINGS_FILES

CONFIGURATION = Dynaconf(
    environments=False,
    lowercase_read=False,
    merge_enabled=True,
    default_settings_paths=DEFAULT_SETTINGS_FILES,
)


async def connect(args: typing.List[str]) -> asyncio.StreamReader:
    reader, writer = await asyncio.open_unix_connection(
        CONFIGURATION.get('CPD3.CACHE.SOCKET', '/run/forge-cpd3-cache.socket'))
    header = struct.pack('<I', len(args))
    for a in args:
        raw = a.encode('utf-8')
        header += struct.pack('<I', len(raw))
        header += raw
    writer.write(header)
    await writer.drain()
    return reader


def main():
    args = sys.argv[1:]

    async def run():
        operation = args[0]

        reader = await connect(args)
        if operation == "archive_read" or operation == "edited_read":
            sys.stdin.close()
            while True:
                data = await reader.read(65536)
                if not data:
                    break
                sys.stdout.buffer.write(data)
            sys.stdout.close()
        else:
            os.execvp(CONFIGURATION.get('CPD3.CACHE.INTERFACE', 'cpd3_forge_interface'), args)
            exit(1)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(run())
    loop.close()


if __name__ == '__main__':
    main()
