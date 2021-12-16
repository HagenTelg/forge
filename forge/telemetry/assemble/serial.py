import typing
import asyncio
from os import readlink
from pathlib import Path


async def add_serial_ports(telemetry: typing.Dict[str, typing.Any]) -> None:
    telemetry['serial_ports'] = {}

    def add_all_ports(source: Path):
        if not source.is_dir():
            return
        for port in source.iterdir():
            if not port.is_char_device():
                continue
            target = ""
            if port.is_symlink():
                try:
                    target = Path(readlink(str(port)))
                    target = target.name
                except OSError:
                    pass
            telemetry['serial_ports'][str(port)] = target

    add_all_ports(Path('/dev/serial/by-id'))
    add_all_ports(Path('/dev/serial/by-path'))
