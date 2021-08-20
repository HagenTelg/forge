import typing
from forge.vis.data.stream import DataStream


def get(station: str, mode_name: str, start_epoch_ms: int, end_epoch_ms: int,
        send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
    from forge.vis.station.cpd3 import eventlog_get
    return eventlog_get(station, mode_name, start_epoch_ms, end_epoch_ms, send)
