from starlette.responses import HTMLResponse
from forge.vis.util import package_template
from . import View, Request, Response
from .timeseries import TimeSeries


class SolarTimeSeries(TimeSeries):
    def __init__(self, latitude: float, longitude: float):
        super().__init__()
        self.latitude = latitude
        self.longitude = longitude

    async def __call__(self, request: Request, **kwargs) -> Response:
        return HTMLResponse(await package_template('view', 'solartimeseries.html').render_async(
            request=request,
            view=self,
            **kwargs
        ))


class SolarPosition(View):
    def __init__(self, latitude: float, longitude: float):
        super().__init__()
        self.latitude = latitude
        self.longitude = longitude

    async def __call__(self, request: Request, **kwargs) -> Response:
        return HTMLResponse(await package_template('view', 'solarposition.html').render_async(
            request=request,
            view=self,
            **kwargs
        ))
