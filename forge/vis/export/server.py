import typing
import starlette.status
from starlette.routing import Route
from starlette.authentication import requires
from starlette.responses import Response, HTMLResponse, FileResponse
from starlette.requests import Request
from starlette.exceptions import HTTPException
from forge.const import STATIONS
from forge.vis.util import package_template, package_data
from .permissions import is_available
from .assemble import export_data, visible_exports


@requires('authenticated')
async def _export_data(request: Request) -> Response:
    station = request.path_params['station'].lower()
    if station not in STATIONS:
        raise HTTPException(starlette.status.HTTP_404_NOT_FOUND, detail="Invalid station")
    mode_name = request.path_params['mode_name'].lower()
    if not is_available(request, station, mode_name):
        raise HTTPException(starlette.status.HTTP_403_FORBIDDEN, detail="Data not available")

    try:
        data = await request.json()
    except:
        data = {}

    export_key = request.query_params.get('key')
    if not export_key:
        export_key = data.get('key')
    if not export_key:
        raise HTTPException(starlette.status.HTTP_400_BAD_REQUEST, detail="Invalid export key")

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

    export = export_data(station, mode_name, export_key, start_epoch_ms, end_epoch_ms)
    if not export:
        raise HTTPException(starlette.status.HTTP_400_BAD_REQUEST, detail="Export not available")

    return await export()


@requires('authenticated')
async def _export_modal(request: Request) -> Response:
    station = request.path_params['station'].lower()
    if station not in STATIONS:
        raise HTTPException(starlette.status.HTTP_404_NOT_FOUND, detail="Invalid station")
    mode_name = request.path_params['mode_name'].lower()
    if not is_available(request, station, mode_name):
        raise HTTPException(starlette.status.HTTP_403_FORBIDDEN, detail="Data not available")

    exports = await visible_exports(station, mode_name)
    if not exports:
        raise HTTPException(starlette.status.HTTP_400_BAD_REQUEST, detail="No exports available")

    return HTMLResponse(await package_template('export', 'modal.html').render_async(
        request=request,
        station=station,
        mode_name=mode_name,
        available=exports,
    ))


routes: typing.List[Route] = [
    Route('/{station}/{mode_name}/modal', endpoint=_export_modal, name='export_modal'),
    Route('/{station}/{mode_name}', endpoint=_export_data, methods=['GET', 'POST'], name='export_data'),
]
