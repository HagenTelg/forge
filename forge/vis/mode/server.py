import typing
import starlette.status
from starlette.routing import Route, NoMatchFound
from starlette.authentication import requires
from starlette.responses import Response, HTMLResponse, RedirectResponse
from starlette.requests import Request
from starlette.exceptions import HTTPException
from forge.const import STATIONS
from forge.vis.util import package_template
from forge.vis.access.database import AccessUser as DatabaseUser
from .assemble import lookup_mode, visible_modes, default_mode
from .permissions import is_available


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
            return RedirectResponse(request.url_for('request_access') + f"?station={station}")
        return RedirectResponse(request.url_for('mode', station=station, mode_name=mode.mode_name))

    mode = lookup_mode(request, station, mode_name)
    if mode is None:
        raise HTTPException(starlette.status.HTTP_404_NOT_FOUND, detail="Mode not found")
    available_modes = visible_modes(request, station, mode_name=mode_name)
    enable_user_actions = isinstance(request.user, DatabaseUser)
    return await mode(request,
                      station=station,
                      available_modes=available_modes,
                      enable_user_actions=enable_user_actions
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
    mode = visible_modes(request, station).default_mode()
    if mode is None:
        if not request.user.can_request_access:
            try:
                return RedirectResponse(request.url_for('login'))
            except NoMatchFound:
                raise HTTPException(starlette.status.HTTP_403_FORBIDDEN, detail="Invalid authentication")
        return RedirectResponse(request.url_for('request_access') + f"?station={station}")
    return HTMLResponse(await package_template('mode', 'index.html').render_async(
        request=request,
        station=station,
        default_mode=mode,
    ))


async def local_settings(request: Request) -> Response:
    return HTMLResponse(await package_template('mode', 'settings.html').render_async(
        request=request,
    ))


routes: typing.List[Route] = [
    Route('/{station}/{mode_name}', endpoint=_mode_request, name='mode'),
    Route('/{station}', endpoint=_default_mode, name='default_mode'),
]
