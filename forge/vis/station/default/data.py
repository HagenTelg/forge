import typing
from forge.vis.data.stream import DataStream


def get(station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
        send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
    from forge.vis.station.cpd3 import data_get
    return data_get(station, data_name, start_epoch_ms, end_epoch_ms, send)


def modes(station: str, data_name: str) -> typing.List[str]:
    # Just assume the same naming hierarchy
    return ['-'.join(data_name.split('-')[0:2])]
