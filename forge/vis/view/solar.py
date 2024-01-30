import typing
from forge.processing.station.lookup import station_data
from starlette.responses import HTMLResponse
from forge.vis.util import package_template
from . import View, Request, Response
from .timeseries import TimeSeries


class SolarTimeSeries(TimeSeries):
    def __init__(self, latitude: typing.Optional[float] = None, longitude: typing.Optional[float] = None, **kwargs):
        super().__init__(**kwargs)
        self.latitude = latitude
        self.longitude = longitude

    async def __call__(self, request: Request, **kwargs) -> Response:
        latitude = self.latitude
        if latitude is None:
            station = kwargs.get('station')
            if station is not None:
                latitude = station_data(station, 'site', 'latitude')(station)
        longitude = self.longitude
        if longitude is None:
            station = kwargs.get('station')
            if station is not None:
                longitude = station_data(station, 'site', 'longitude')(station)

        return HTMLResponse(await package_template('view', 'solartimeseries.html').render_async(
            request=request,
            view=self.RequestContext(self, request),
            realtime=self.realtime,
            latitude=latitude,
            longitude=longitude,
            **kwargs
        ))


class SolarPosition(View):
    def __init__(self, latitude: typing.Optional[float] = None, longitude: typing.Optional[float] = None,
                 realtime: bool = False):
        super().__init__()
        self.latitude = latitude
        self.longitude = longitude
        self.realtime = realtime

    async def __call__(self, request: Request, **kwargs) -> Response:
        latitude = self.latitude
        if latitude is None:
            station = kwargs.get('station')
            if station is not None:
                latitude = station_data(station, 'site', 'latitude')(station)
        longitude = self.longitude
        if longitude is None:
            station = kwargs.get('station')
            if station is not None:
                longitude = station_data(station, 'site', 'longitude')(station)

        return HTMLResponse(await package_template('view', 'solarposition.html').render_async(
            request=request,
            view=self,
            realtime=self.realtime,
            latitude=latitude,
            longitude=longitude,
            **kwargs
        ))


class BSRNQC(View):
    def __init__(self, mode: str, latitude: typing.Optional[float] = None, longitude: typing.Optional[float] = None):
        super().__init__()
        self.latitude = latitude
        self.longitude = longitude
        self.record = f"{mode}-bsrnqc"
        self.contamination = f"{mode}-contamination"

    async def __call__(self, request: Request, **kwargs) -> Response:
        latitude = self.latitude
        if latitude is None:
            station = kwargs.get('station')
            if station is not None:
                latitude = station_data(station, 'site', 'latitude')(station)
        longitude = self.longitude
        if longitude is None:
            station = kwargs.get('station')
            if station is not None:
                longitude = station_data(station, 'site', 'longitude')(station)

        return HTMLResponse(await package_template('view', 'bsrnqc.html').render_async(
            request=request,
            view=self,
            contamination=self.contamination,
            record=self.record,
            latitude=latitude,
            longitude=longitude,
            **kwargs
        ))
