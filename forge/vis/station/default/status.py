import typing
from forge.vis.data.stream import DataStream
from forge.vis.status.archive import read_latest_passed, read_passed, read_instruments


def latest_passed(station: str, mode_name: str) -> typing.Awaitable[typing.Optional[int]]:
    from forge.vis.station.cpd3 import use_cpd3, latest_passed as cpd3_latest_passed
    if use_cpd3(station):
        return cpd3_latest_passed(station, mode_name)
    return read_latest_passed(station, mode_name)


def passed(station: str, mode_name: str,
           send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
    return read_passed(station, mode_name, send)


def instruments(station: str, mode_name: str,
                send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
    return read_instruments(station, mode_name, send)
