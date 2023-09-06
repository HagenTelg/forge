import typing
from forge.vis.access import AccessUser
from forge.vis.data.stream import DataStream


def get(station: str, mode_name: str, start_epoch_ms: int, end_epoch_ms: int,
        send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
    from forge.vis.station.cpd3 import editing_get
    return editing_get(station, mode_name, start_epoch_ms, end_epoch_ms, send)


def available(station: str, mode_name: str, start_epoch_ms: int, end_epoch_ms: int,
              send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
    from forge.vis.station.cpd3 import editing_available
    return editing_available(station, mode_name, start_epoch_ms, end_epoch_ms, send)


def writable(user: AccessUser, station: str, mode_name: str, directive: typing.Dict[str, typing.Any]) -> bool:
    from forge.vis.station.cpd3 import editing_writable
    return editing_writable(user, station, mode_name, directive)


def save(user: AccessUser, station: str, mode_name: str,
         directive: typing.Dict[str, typing.Any]) -> typing.Optional[typing.Awaitable[typing.Optional[typing.Dict[str, typing.Any]]]]:
    from forge.vis.station.cpd3 import editing_save
    return editing_save(user, station, mode_name, directive)


def pass_data(station: str, mode_name: str, start_epoch_ms: int,
              end_epoch_ms: int, comment: typing.Optional[str] = None) -> typing.Awaitable[None]:
    from forge.vis.station.cpd3 import editing_pass
    return editing_pass(station, mode_name, start_epoch_ms, end_epoch_ms, comment)
