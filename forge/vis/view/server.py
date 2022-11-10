import typing
import starlette.status
from starlette.routing import Route
from starlette.authentication import requires
from starlette.responses import Response, HTMLResponse
from starlette.requests import Request
from starlette.exceptions import HTTPException
from forge.const import STATIONS
from forge.vis.util import package_template
from .assemble import interior as assemble_interior
from .permissions import is_available


@requires('authenticated')
async def _standalone(request: Request) -> Response:
    station = request.path_params['station'].lower()
    if station not in STATIONS:
        raise HTTPException(starlette.status.HTTP_404_NOT_FOUND, detail="Invalid station")

    view_name = request.path_params['view_name'].lower()
    if not is_available(request, station, view_name):
        raise HTTPException(starlette.status.HTTP_403_FORBIDDEN, detail="View not available")

    return HTMLResponse(await package_template('view', 'standalone.html').render_async(
        request=request,
        station=station,
        view_url=request.url_for('view', station=station, view_name=view_name),
        view_name=view_name,
    ))


routes: typing.List[Route] = [
    Route('/{station}/{view_name}/interior', endpoint=assemble_interior, name='view'),
    Route('/{station}/{view_name}', endpoint=_standalone, name='view_standalone'),
]
