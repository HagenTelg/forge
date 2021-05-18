import typing
import starlette.status
from starlette.authentication import requires
from starlette.responses import Response
from starlette.requests import Request
from starlette.exceptions import HTTPException
from forge.const import STATIONS
from forge.vis.station.lookup import station_data
from .permissions import is_available
from . import View


def _lookup_view(request: Request, station: str, view_name: str) -> typing.Optional[View]:
    if view_name.startswith("example-"):
        if not request.user.allow_station(station):
            raise HTTPException(starlette.status.HTTP_403_FORBIDDEN, detail="Station not available")

        if view_name.startswith("example-timeseries"):
            from . example import example_timeseries
            return example_timeseries
        return None

    return station_data(station, 'view', 'get')(station, view_name)


@requires('authenticated')
async def interior(request: Request) -> Response:
    station = request.path_params['station'].lower()
    if station not in STATIONS:
        raise HTTPException(starlette.status.HTTP_404_NOT_FOUND, detail="Invalid station")

    view_name = request.path_params['view_name'].lower()
    if not is_available(request, station, view_name):
        raise HTTPException(starlette.status.HTTP_403_FORBIDDEN, detail="View not available")

    view = _lookup_view(request, station, view_name)
    if view is None:
        raise HTTPException(starlette.status.HTTP_404_NOT_FOUND, detail="View not found")
    return await view(request, station=station)
