import typing
import starlette.status
from starlette.requests import Request
from starlette.exceptions import HTTPException
from forge.vis.station.lookup import station_data
from . import Mode, ModeGroup, VisibleModes


def lookup_mode(request: Request, station: str, mode_name: str) -> typing.Optional[Mode]:
    if mode_name.startswith("example-"):
        if not request.user.allow_station(station):
            raise HTTPException(starlette.status.HTTP_403_FORBIDDEN, detail="Station not available")

        if mode_name == "example-basic":
            from . example import example_view_list
            return example_view_list
        elif mode_name == "example-basic2":
            from . example import example_view_list2
            return example_view_list2
        elif mode_name == "example-basic3":
            from . example import example_view_list3
            return example_view_list3
        elif mode_name == "example-realtime":
            from . example import example_realtime
            return example_realtime
        elif mode_name == "example-editing":
            from . example import example_editing
            return example_editing
        elif mode_name == "example-solar":
            from . example import example_solar
            return example_solar
        elif mode_name == "example-acquisition":
            from . example import example_acquisition
            return example_acquisition
        return None

    return station_data(station, 'mode', 'get')(station, mode_name)


def visible_modes(request: Request, station: str, mode_name: typing.Optional[str] = None) -> VisibleModes:
    if mode_name is not None and mode_name.startswith("example-"):
        from .example import example_view_list, example_view_list2, example_view_list3, example_editing
        return VisibleModes([
            ModeGroup("Mode Group", [example_view_list, example_view_list2, example_editing]),
            ModeGroup("Second Group", [example_view_list3])
        ])

    return station_data(station, 'mode', 'visible')(station, mode_name)


def mode_exists(request: Request, station: str, mode_name: str) -> bool:
    return lookup_mode(request, station, mode_name) is not None


def default_mode(request: Request, station: str, mode_name: typing.Optional[str] = None) -> typing.Optional[Mode]:
    return visible_modes(request, station, mode_name).default_mode(request, station)
