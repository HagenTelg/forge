import typing
import starlette.status
from collections import OrderedDict
from starlette.authentication import requires
from starlette.responses import Response, HTMLResponse
from starlette.requests import Request
from starlette.exceptions import HTTPException
from forge.const import STATIONS
from forge.vis.util import package_template
from .selection import selections, instrument_selections


class Condition:
    def __init__(self, display: str, description: typing.Optional[str] = None):
        self.display = display
        self.description = description


condition_codes: typing.Dict[str, Condition] = OrderedDict()
condition_codes['none'] = Condition("None", """
The edit is unconditionally applied.
""")


@requires('authenticated')
async def condition_editor(request: Request) -> Response:
    station = request.path_params['station'].lower()
    if station not in STATIONS:
        raise HTTPException(starlette.status.HTTP_404_NOT_FOUND, detail="Invalid station")
    if not request.user.allow_station(station):
        raise HTTPException(starlette.status.HTTP_403_FORBIDDEN, detail="Station not available")

    condition = request.path_params['condition'].lower()
    if condition not in condition_codes:
        raise HTTPException(starlette.status.HTTP_404_NOT_FOUND, detail="Invalid condition type")

    return HTMLResponse(await package_template('editing', 'condition', condition + '.html').render_async(
        request=request,
        station=station,
        condition=condition,
        selections=selections,
        instrument_selections=instrument_selections,
    ))
