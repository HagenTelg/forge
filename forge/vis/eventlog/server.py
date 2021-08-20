import typing
import starlette.status
from starlette.routing import Route
from starlette.authentication import requires
from starlette.responses import Response, HTMLResponse
from starlette.requests import Request
from starlette.exceptions import HTTPException
from forge.const import STATIONS
from forge.vis.util import package_template
from .permissions import is_available
from .export import export_csv, export_json


@requires('authenticated')
async def _root(request: Request) -> Response:
    station = request.path_params['station'].lower()
    if station not in STATIONS:
        raise HTTPException(starlette.status.HTTP_404_NOT_FOUND, detail="Invalid station")
    mode_name = request.path_params['mode_name'].lower()
    if not is_available(request, station, mode_name):
        raise HTTPException(starlette.status.HTTP_403_FORBIDDEN, detail="Mode not available")

    return HTMLResponse(await package_template('eventlog', 'index.html').render_async(
        request=request,
        station=station,
        mode_name=mode_name,
    ))


routes: typing.List[Route] = [
    Route('/{station}/{mode_name}/csv', endpoint=export_csv, name='eventlog_csv'),
    Route('/{station}/{mode_name}/json', endpoint=export_json, name='eventlog_json'),
    Route('/{station}/{mode_name}', endpoint=_root, name='eventlog'),
]
