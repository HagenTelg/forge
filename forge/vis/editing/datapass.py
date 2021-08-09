import asyncio
import typing
import logging
import starlette.status
from starlette.authentication import requires
from starlette.responses import Response, HTMLResponse, JSONResponse
from starlette.requests import Request
from starlette.exceptions import HTTPException
from forge.const import STATIONS
from forge.vis.util import package_template
from forge.vis.station.lookup import station_data
from .permissions import is_available, is_writable


_LOGGER = logging.getLogger(__name__)


@requires('authenticated')
async def pass_modal(request: Request) -> Response:
    station = request.path_params['station'].lower()
    if station not in STATIONS:
        raise HTTPException(starlette.status.HTTP_404_NOT_FOUND, detail="Invalid station")
    mode_name = request.path_params['mode_name'].lower()
    if not is_available(request, station, mode_name):
        raise HTTPException(starlette.status.HTTP_403_FORBIDDEN, detail="Mode not available")
    if not is_writable(request, station, mode_name):
        raise HTTPException(starlette.status.HTTP_403_FORBIDDEN, detail="Modification not permitted")

    return HTMLResponse(await package_template('editing', 'pass.html').render_async(
        request=request,
        station=station,
        mode_name=mode_name,
    ))


@requires('authenticated')
async def pass_data(request: Request) -> Response:
    station = request.path_params['station'].lower()
    if station not in STATIONS:
        raise HTTPException(starlette.status.HTTP_404_NOT_FOUND, detail="Invalid station")
    mode_name = request.path_params['mode_name'].lower()
    if not is_available(request, station, mode_name):
        raise HTTPException(starlette.status.HTTP_403_FORBIDDEN, detail="Mode not available")
    if not is_writable(request, station, mode_name):
        raise HTTPException(starlette.status.HTTP_403_FORBIDDEN, detail="Modification not permitted")

    if mode_name.startswith("example-"):
        return JSONResponse({'status': 'ok'})

    try:
        data = await request.json()
    except:
        data = {}
    try:
        start_epoch_ms = request.query_params.get('start')
        if not start_epoch_ms:
            start_epoch_ms = data.get('start')
        start_epoch_ms = int(start_epoch_ms)
    except (ValueError, TypeError):
        raise HTTPException(starlette.status.HTTP_400_BAD_REQUEST, detail="Invalid start time")
    try:
        end_epoch_ms = request.query_params.get('end')
        if not end_epoch_ms:
            end_epoch_ms = data.get('end')
        end_epoch_ms = int(end_epoch_ms)
    except (ValueError, TypeError):
        raise HTTPException(starlette.status.HTTP_400_BAD_REQUEST, detail="Invalid end time")
    if start_epoch_ms <= 0 or end_epoch_ms <= 0 or end_epoch_ms <= start_epoch_ms:
        raise HTTPException(starlette.status.HTTP_400_BAD_REQUEST, detail="Invalid time bounds")
    if (end_epoch_ms - start_epoch_ms) > 366 * 24 * 60 * 60 * 1000:
        raise HTTPException(starlette.status.HTTP_400_BAD_REQUEST, detail="Too much data selected")

    comment = request.query_params.get('comment')
    if not comment and data:
        comment = data.get('comment')

    _LOGGER.info(f"Passing data for {station} {mode_name} on {start_epoch_ms} to {end_epoch_ms}")

    await station_data(station, 'editing', 'pass_data')(station, mode_name, start_epoch_ms, end_epoch_ms, comment)

    return JSONResponse({'status': 'ok'})

