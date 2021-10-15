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


class Action:
    def __init__(self, display: str, description: typing.Optional[str] = None):
        self.display = display
        self.description = description


action_codes: typing.Dict[str, Action] = OrderedDict()
action_codes['invalidate'] = Action("Invalidate", """
Choose specific data to mark as missing in the final output.
This should be used on any instrument anomalies or other data that does not represent a valid measurement.
""")
action_codes['contaminate'] = Action("Contaminate", """
Mark the whole data stream as not representative of ambient conditions (e.g. a nearby truck emitting aerosol).
The data are considered valid in high resolution (1-minute) but are not included in the final averages for analysis.
""")
action_codes['calibration'] = Action("Calibration", """
Apply a calibration polynomial to data values.
This is used to adjust values based by applying a polynomial to the original data (e.g. using a slope and offset).
""")
action_codes['recalibrate'] = Action("Recalibrate", """
Reverse a calibration polynomial and apply a new one.
This is used to reverse an old calibration polynomial applied to values and then apply an updated one. 
""")


@requires('authenticated')
async def action_editor(request: Request) -> Response:
    station = request.path_params['station'].lower()
    if station not in STATIONS:
        raise HTTPException(starlette.status.HTTP_404_NOT_FOUND, detail="Invalid station")
    if not request.user.allow_station(station):
        raise HTTPException(starlette.status.HTTP_403_FORBIDDEN, detail="Station not available")

    action = request.path_params['action'].lower()
    if action not in action_codes:
        raise HTTPException(starlette.status.HTTP_404_NOT_FOUND, detail="Invalid action type")

    return HTMLResponse(await package_template('editing', 'action', action + '.html').render_async(
        request=request,
        station=station,
        action=action,
        selections=selections,
        instrument_selections=instrument_selections,
    ))
