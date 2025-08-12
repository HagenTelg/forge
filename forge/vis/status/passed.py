import typing
import starlette.status
from starlette.authentication import requires
from starlette.responses import Response, JSONResponse, HTMLResponse
from starlette.requests import Request
from starlette.exceptions import HTTPException
from forge.const import STATIONS
from forge.vis.util import package_template
from forge.vis.station.lookup import station_data
from .permissions import is_available


async def _lookup_latest_passed(station: str, mode_name: str) -> int:
    if mode_name.startswith("example-"):
        return 1622505600000

    return await station_data(station, 'status', 'latest_passed')(station, mode_name)


@requires('authenticated')
async def latest_passed(request: Request) -> Response:
    station = request.path_params['station'].lower()
    if station not in STATIONS:
        raise HTTPException(starlette.status.HTTP_404_NOT_FOUND, detail="Invalid station")
    mode_name = request.path_params['mode_name'].lower()
    if not is_available(request, station, mode_name):
        raise HTTPException(starlette.status.HTTP_403_FORBIDDEN, detail="Mode not available")

    return JSONResponse({
        "latest_epoch_ms": await _lookup_latest_passed(station, mode_name)
    })


@requires('authenticated')
async def passed_modal(request: Request) -> Response:
    station = request.path_params['station'].lower()
    if station not in STATIONS:
        raise HTTPException(starlette.status.HTTP_404_NOT_FOUND, detail="Invalid station")
    mode_name = request.path_params['mode_name'].lower()
    if not is_available(request, station, mode_name):
        raise HTTPException(starlette.status.HTTP_403_FORBIDDEN, detail="Mode not available")

    return HTMLResponse(await package_template('status', 'passed.html').render_async(
        request=request,
        station=station,
        mode_name=mode_name,
    ))

