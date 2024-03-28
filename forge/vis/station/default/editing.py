import typing
from starlette.requests import Request
from forge.vis.access import AccessUser
from forge.vis.data.stream import DataStream
from forge.vis.editing.archive import read_edits, available_selections, edit_writable, edit_save, apply_pass


def get(station: str, mode_name: str, start_epoch_ms: int, end_epoch_ms: int,
        send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
    from forge.vis.station.cpd3 import use_cpd3, editing_get as cpd3_get
    if use_cpd3(station):
        return cpd3_get(station, mode_name, start_epoch_ms, end_epoch_ms, send)
    return read_edits(station, mode_name, start_epoch_ms, end_epoch_ms, send)


def available(station: str, mode_name: str, start_epoch_ms: int, end_epoch_ms: int,
              send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
    from forge.vis.station.cpd3 import use_cpd3, editing_available as cpd3_available
    if use_cpd3(station):
        return cpd3_available(station, mode_name, start_epoch_ms, end_epoch_ms, send)
    return available_selections(station, mode_name,  start_epoch_ms, end_epoch_ms, send)


def writable(user: AccessUser, station: str, mode_name: str, directive: typing.Dict[str, typing.Any]) -> bool:
    from forge.vis.station.cpd3 import use_cpd3, editing_writable as cpd3_writable
    if use_cpd3(station):
        return cpd3_writable(user, station, mode_name, directive)
    return edit_writable(user, station, mode_name, directive)


def save(request: Request, station: str, mode_name: str,
         directive: typing.Dict[str, typing.Any]) -> typing.Optional[typing.Awaitable[typing.Optional[typing.Dict[str, typing.Any]]]]:
    from forge.vis.station.cpd3 import use_cpd3, editing_save as cpd3_save
    if use_cpd3(station):
        return cpd3_save(request.user, station, mode_name, directive)
    return edit_save(request.user, station, mode_name, directive)


def pass_data(request: Request, station: str, mode_name: str, start_epoch_ms: int,
              end_epoch_ms: int, comment: typing.Optional[str] = None) -> typing.Awaitable[None]:
    from forge.vis.station.cpd3 import use_cpd3, editing_pass as cpd3_pass
    if use_cpd3(station):
        return cpd3_pass(station, mode_name, start_epoch_ms, end_epoch_ms, comment)
    return apply_pass(request, station, mode_name, start_epoch_ms, end_epoch_ms, comment)
