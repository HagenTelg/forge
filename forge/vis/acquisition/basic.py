import typing
from starlette.requests import Request
from starlette.responses import Response, HTMLResponse
from forge.vis.util import package_template
from . import Display, SummaryItem


class BasicDisplay(Display):
    async def __call__(self, request: Request, display_type: str = None, **kwargs) -> Response:
        return HTMLResponse(await package_template('acquisition', 'display', display_type + '.html').render_async(
            request=request,
            display=self,
            **kwargs
        ))


class InstrumentDisplay(Display):
    def __init__(self, base_type: str, instrument: str):
        self.base_type = base_type
        self.instrument = instrument

    async def __call__(self, request: Request, summary_type: str = None, **kwargs) -> Response:
        return HTMLResponse(await package_template('acquisition', 'display', self.base_type + '.html').render_async(
            request=request,
            display=self,
            instrument=self.instrument,
            **kwargs
        ))


class BasicSummary(SummaryItem):
    async def __call__(self, request: Request, summary_type: str = None, **kwargs) -> Response:
        return HTMLResponse(await package_template('acquisition', 'summary', summary_type + '.html').render_async(
            request=request,
            summary=self,
            **kwargs
        ))


class InstrumentSummary(SummaryItem):
    def __init__(self, base_type: str, instrument: str):
        self.base_type = base_type
        self.instrument = instrument

    async def __call__(self, request: Request, summary_type: str = None, **kwargs) -> Response:
        return HTMLResponse(await package_template('acquisition', 'summary', self.base_type + '.html').render_async(
            request=request,
            summary=self,
            instrument=self.instrument,
            **kwargs
        ))
