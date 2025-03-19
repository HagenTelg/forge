import typing
from starlette.responses import HTMLResponse
from forge.vis.util import package_template
from . import View, Request, Response


class Statistics(View):
    def __init__(self, record: str, profile: str = 'aerosolstats'):
        super().__init__()

        self.title: typing.Optional[str] = None
        self.units: typing.Optional[str] = None
        self.logarithmic: bool = False
        self.range: typing.Optional[typing.Union[int, typing.Tuple[float, float]]] = None

        self.bins_record: str = f"public-{profile}-{record}-bins"
        self.timeseries_record: str = f"public-{profile}-{record}-series"
        self.sub_um_fraction: bool = False

    async def __call__(self, request: Request, **kwargs) -> Response:
        return HTMLResponse(await package_template('view', 'statistics.html').render_async(
            request=request,
            view=self,
            **kwargs
        ))

