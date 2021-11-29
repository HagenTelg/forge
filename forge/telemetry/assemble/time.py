import typing
import time


async def add_time(telemetry: typing.Dict[str, typing.Any]) -> None:
    telemetry['time'] = round(time.time())
