import typing
from forge.vis.data.stream import DataStream
from forge.vis.eventlog.archive import read_eventlog


def get(station: str, mode_name: str, start_epoch_ms: int, end_epoch_ms: int,
        send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
    from forge.vis.station.cpd3 import use_cpd3, eventlog_get as cpd3_get
    if use_cpd3(station):
        return cpd3_get(station, mode_name, start_epoch_ms, end_epoch_ms, send)
    return read_eventlog(station, mode_name, start_epoch_ms, end_epoch_ms, send)
