import asyncio
import typing
from forge.vis.access import BaseAccessUser
from forge.vis.station.lookup import station_data
from .stream import DataStream
from .permissions import is_available


def begin_stream(user: BaseAccessUser, station: str, data_name: str, start_epoch_ms: int, end_epoch_ms: int,
                 send: typing.Callable[[typing.Dict], typing.Awaitable[None]]) -> typing.Optional[DataStream]:
    if not is_available(user, station, data_name):
        return None

    if data_name.startswith("example-"):
        if data_name.startswith("example-timeseries"):
            from .example import ExampleTimeSeries
            return ExampleTimeSeries(start_epoch_ms, send)
        elif data_name.startswith("example-editing-directives"):
            from .example import ExampleEditDirectives
            return ExampleEditDirectives(start_epoch_ms, send)
        elif data_name.startswith("example-editing-available"):
            from .example import ExampleEditAvailable
            return ExampleEditAvailable(send)
        return None

    if data_name.endswith('-editing-directives'):
        components = data_name.split('-', 2)
        if len(components) == 3 and components[2] == 'directives':
            return station_data(station, 'editing', 'get')(station, '-'.join(components[:2]),
                                                           start_epoch_ms, end_epoch_ms, send)
    elif data_name.endswith('-editing-available'):
        components = data_name.split('-', 2)
        if len(components) == 3 and components[2] == 'available':
            return station_data(station, 'editing', 'available')(station, '-'.join(components[:2]),
                                                                 start_epoch_ms, end_epoch_ms, send)

    return station_data(station, 'data', 'get')(station, data_name, start_epoch_ms, end_epoch_ms, send)
