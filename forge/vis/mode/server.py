import typing
import starlette.status
from collections import OrderedDict
from starlette.routing import Route, NoMatchFound
from starlette.authentication import requires
from starlette.responses import Response, HTMLResponse, RedirectResponse, JSONResponse
from starlette.requests import Request
from starlette.exceptions import HTTPException
from forge.const import STATIONS
from forge.vis.util import package_template
from forge.vis.access.database import AccessLayer as DatabaseLayer
from .assemble import lookup_mode, visible_modes, mode_exists, default_mode
from .permissions import is_available
from .viewlist import ViewList


@requires('authenticated')
async def _mode_request(request: Request) -> Response:
    station = request.path_params['station'].lower()
    if station not in STATIONS:
        raise HTTPException(starlette.status.HTTP_404_NOT_FOUND, detail="Invalid station")

    mode_name = request.path_params['mode_name'].lower()
    if not is_available(request, station, mode_name):
        mode = default_mode(request, station, mode_name=mode_name)
        if mode is None:
            if not request.user.can_request_access:
                try:
                    return RedirectResponse(request.url_for('login'))
                except NoMatchFound:
                    raise HTTPException(starlette.status.HTTP_403_FORBIDDEN, detail="Invalid authentication")
            return RedirectResponse(str(request.url_for('request_access')) + f"?station={station}")
        return RedirectResponse(request.url_for('mode', station=station, mode_name=mode.mode_name))

    mode = lookup_mode(request, station, mode_name)
    if mode is None:
        raise HTTPException(starlette.status.HTTP_404_NOT_FOUND, detail="Mode not found")
    available_modes = visible_modes(request, station, mode_name=mode_name)
    return await mode(
        request,
        station=station,
        available_modes=available_modes,
        mode_exists=mode_exists,
        enable_user_actions=request.user.layer_type(DatabaseLayer) is not None,
    )


@requires('authenticated')
async def _default_mode(request: Request) -> Response:
    station = request.path_params['station'].lower()
    if station not in STATIONS:
        raise HTTPException(starlette.status.HTTP_404_NOT_FOUND, detail="Invalid station")

    mode_name = request.query_params.get('mode')
    if mode_name is not None and is_available(request, station, mode_name):
        mode = lookup_mode(request, station, mode_name)
        if mode is not None:
            return RedirectResponse(request.url_for('mode', station=station, mode_name=mode.mode_name))
    mode = default_mode(request, station, mode_name=mode_name)
    if mode is None:
        if not request.user.can_request_access:
            try:
                return RedirectResponse(request.url_for('login'))
            except NoMatchFound:
                raise HTTPException(starlette.status.HTTP_403_FORBIDDEN, detail="Invalid authentication")
        return RedirectResponse(str(request.url_for('request_access')) + f"?station={station}")
    available_modes = visible_modes(request, station, mode_name=mode_name)
    return HTMLResponse(await package_template('mode', 'index.html').render_async(
        request=request,
        station=station,
        default_mode=mode,
        available_modes=available_modes,
    ))


async def local_settings(request: Request) -> Response:
    return HTMLResponse(await package_template('mode', 'settings.html').render_async(
        request=request,
    ))


@requires('authenticated')
async def query_modes(request: Request) -> Response:
    station = request.path_params['station'].lower()
    if station not in STATIONS:
        raise HTTPException(starlette.status.HTTP_404_NOT_FOUND, detail="Invalid station")
    contents = await request.json()
    if not isinstance(contents, dict):
        raise HTTPException(starlette.status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Invalid mode query")
    modes = contents.get('modes')
    if not modes:
        mode_name = request.query_params.get("mode")
        if not mode_name:
            raise HTTPException(starlette.status.HTTP_404_NOT_FOUND, detail="No mode specified")
        modes = [mode_name]

    result: typing.Dict[str, str] = OrderedDict()
    for mode in modes:
        mode = mode.lower()
        if not is_available(request, station, mode):
            continue
        mode = lookup_mode(request, station, mode)
        if mode is None:
            continue
        result[mode.mode_name] = mode.display_name

    return JSONResponse(result)


@requires('authenticated')
async def list_views(request: Request) -> Response:
    station = request.path_params['station'].lower()
    if station not in STATIONS:
        raise HTTPException(starlette.status.HTTP_404_NOT_FOUND, detail="Invalid station")

    result: typing.Dict[str, str] = OrderedDict()

    mode_name = request.query_params.get("mode")
    if not mode_name:
        available_modes = visible_modes(request, station)
        for group in available_modes.groups:
            for mode in group.modes:
                if not isinstance(mode, ViewList):
                    continue
                if not is_available(request, station, mode.mode_name):
                    continue
                for view in mode.views:
                    result[view.view_name] = view.display_name
    else:
        if not is_available(request, station, mode_name):
            raise HTTPException(starlette.status.HTTP_403_FORBIDDEN, detail="Invalid mode")
        mode = lookup_mode(request, station, mode_name)
        if mode is None:
            raise HTTPException(starlette.status.HTTP_404_NOT_FOUND, detail="Mode not found")
        if isinstance(mode, ViewList):
            for view in mode.views:
                result[view.view_name] = view.display_name

    return JSONResponse(result)


routes: typing.List[Route] = [
    Route('/{station}/{mode_name}', endpoint=_mode_request, name='mode'),
    Route('/{station}', endpoint=_default_mode, name='default_mode'),
]
