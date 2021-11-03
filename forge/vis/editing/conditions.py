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
condition_codes['threshold'] = Condition("Threshold", """
Compare a value to threshold limits and apply the edit only when those limits are met.
The edit is applied when a value is inside a range (e.g. transmittance less than 0.5).
""")
condition_codes['periodic'] = Condition("Periodic", """
Divide time into periodic (UTC) intervals and apply the edit only for certain points in the period.
The edit is applied when the current time is within one of the selected moments in the total period.
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
