import typing
import starlette.status
import time
from starlette.authentication import requires
from starlette.responses import Response, JSONResponse
from starlette.requests import Request
from starlette.exceptions import HTTPException
from forge.const import STATIONS
from forge.vis.station.lookup import station_data
from .permissions import is_available, is_writable


@requires('authenticated')
async def save_edit(request: Request) -> Response:
    station = request.path_params['station'].lower()
    if station not in STATIONS:
        raise HTTPException(starlette.status.HTTP_404_NOT_FOUND, detail="Invalid station")
    mode_name = request.path_params['mode_name'].lower()
    if not is_available(request, station, mode_name):
        raise HTTPException(starlette.status.HTTP_403_FORBIDDEN, detail="Mode not available")
    if not is_writable(request, station, mode_name):
        raise HTTPException(starlette.status.HTTP_403_FORBIDDEN, detail="Modification not permitted")

    if mode_name.startswith("example-"):
        result = await request.json()
        result['_id'] = 999
        result['modified_epoch_ms'] = round(time.time() * 1000)
        return JSONResponse(result)

    contents = await request.json()
    if not isinstance(contents, dict):
        raise HTTPException(starlette.status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Invalid directive")
    if not station_data(station, 'editing', 'writable')(request.user, station, mode_name, contents):
        raise HTTPException(starlette.status.HTTP_403_FORBIDDEN, detail="Modification not permitted")
    result = await station_data(station, 'editing', 'save')(request.user, station, mode_name, contents)
    if result is None:
        raise HTTPException(starlette.status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Modification failed")
    return JSONResponse(result)
