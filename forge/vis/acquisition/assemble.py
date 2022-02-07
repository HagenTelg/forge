import typing
import starlette.status
from starlette.authentication import requires
from starlette.responses import Response
from starlette.requests import Request
from starlette.exceptions import HTTPException
from forge.const import STATIONS
from forge.vis.station.lookup import station_data
from . import SummaryItem, Display
from .permissions import is_display_available, is_summary_available, is_writable


def _lookup_display(request: Request, station: str, display_type: str, source: typing.Optional[str]) -> typing.Optional[Display]:
    if display_type.startswith("example-"):
        if not request.user.allow_station(station):
            raise HTTPException(starlette.status.HTTP_403_FORBIDDEN, detail="Station not available")
        if display_type == 'example-instrument':
            from .example import example_display_instrument
            return example_display_instrument
        return None

    return station_data(station, 'acquisition', 'display')(station, display_type, source)


def _lookup_summary(request: Request, station: str, summary_type: str, source: typing.Optional[str]) -> typing.Optional[SummaryItem]:
    if summary_type.startswith("example-"):
        if not request.user.allow_station(station):
            raise HTTPException(starlette.status.HTTP_403_FORBIDDEN, detail="Station not available")
        if summary_type == 'example-static':
            from .example import example_summary_static
            return example_summary_static
        elif summary_type == 'example-instrument':
            from .example import example_summary_instrument
            return example_summary_instrument
        return None

    return station_data(station, 'acquisition', 'summary')(station, summary_type, source)


@requires('authenticated')
async def display(request: Request) -> Response:
    station = request.path_params['station'].lower()
    if station not in STATIONS:
        raise HTTPException(starlette.status.HTTP_404_NOT_FOUND, detail="Invalid station")
    display_type = request.path_params['type'].lower()

    source = request.query_params.get('source', None)
    if not is_display_available(request, station, display_type, source):
        raise HTTPException(starlette.status.HTTP_403_FORBIDDEN, detail="Display not available")

    display = _lookup_display(request, station, display_type, source)
    if display is None:
        raise HTTPException(starlette.status.HTTP_404_NOT_FOUND, detail="Instrument not found")

    return await display(request, station=station, display_type=display_type, source=source,
                         uid=request.query_params.get('uid', '_'),
                         writable=is_writable(request, request.path_params['station'].lower()))


@requires('authenticated')
async def summary(request: Request) -> Response:
    station = request.path_params['station'].lower()
    if station not in STATIONS:
        raise HTTPException(starlette.status.HTTP_404_NOT_FOUND, detail="Invalid station")
    summary_type = request.path_params['type'].lower()

    source = request.query_params.get('source', None)
    if not is_summary_available(request, station, summary_type, source):
        raise HTTPException(starlette.status.HTTP_403_FORBIDDEN, detail="Summary not available")

    summary = _lookup_summary(request, station, summary_type, source)
    if summary is None:
        raise HTTPException(starlette.status.HTTP_404_NOT_FOUND, detail="Instrument not found")
    return await summary(request, station=station, summary_type=summary_type, source=source,
                         uid=request.query_params.get('uid', '_'),
                         writable=is_writable(request, request.path_params['station'].lower()))
