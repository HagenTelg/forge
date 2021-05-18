import typing
import starlette.status
from starlette.routing import Route
from starlette.authentication import requires
from starlette.responses import Response, HTMLResponse
from starlette.requests import Request
from starlette.exceptions import HTTPException
from jinja2 import TemplateNotFound
from forge.const import STATIONS
from forge.vis.util import package_template
from .permissions import is_available, is_writable
from .export import export_csv, export_json
from .save import save_edit
from .actions import action_codes, action_editor


@requires('authenticated')
async def _root(request: Request) -> Response:
    station = request.path_params['station'].lower()
    if station not in STATIONS:
        raise HTTPException(starlette.status.HTTP_404_NOT_FOUND, detail="Invalid station")
    mode_name = request.path_params['mode_name'].lower()
    if not is_available(request, station, mode_name):
        raise HTTPException(starlette.status.HTTP_403_FORBIDDEN, detail="Mode not available")

    writable = is_writable(request, station, mode_name)

    return HTMLResponse(await package_template('editing', 'index.html').render_async(
        request=request,
        station=station,
        mode_name=mode_name,
        writable=writable,
        actions=action_codes,
    ))


@requires('authenticated')
async def _details(request: Request) -> Response:
    station = request.path_params['station'].lower()
    if station not in STATIONS:
        raise HTTPException(starlette.status.HTTP_404_NOT_FOUND, detail="Invalid station")
    if not request.user.allow_station(station):
        raise HTTPException(starlette.status.HTTP_403_FORBIDDEN, detail="Station not available")

    return HTMLResponse(await package_template('editing', 'details.html').render_async(
        request=request,
        station=station,
        actions=action_codes,
    ))


routes: typing.List[Route] = [
    Route('/details/{station}', endpoint=_details, name='edit_details'),
    Route('/action/{station}/{action}', endpoint=action_editor, name='edit_action'),
    Route('/{station}/{mode_name}/csv', endpoint=export_csv, name='edits_csv'),
    Route('/{station}/{mode_name}/json', endpoint=export_json, name='edits_json'),
    Route('/{station}/{mode_name}/save', endpoint=save_edit, methods=['POST'], name='edit_save'),
    Route('/{station}/{mode_name}', endpoint=_root, name='editing'),
]
