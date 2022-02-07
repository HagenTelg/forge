import typing
import asyncio
from starlette.responses import HTMLResponse
from forge.vis.util import package_template
from . import Request, Response, Display, SummaryItem


class _ExampleSummaryStatic(SummaryItem):
    async def __call__(self, request: Request, **kwargs) -> Response:
        return HTMLResponse(await package_template('acquisition', 'summary', 'example_static.html').render_async(
            request=request,
            summary=self,
            **kwargs
        ))


example_summary_static = _ExampleSummaryStatic()


class _ExampleSummaryInstrument(SummaryItem):
    async def __call__(self, request: Request, **kwargs) -> Response:
        return HTMLResponse(await package_template('acquisition', 'summary', 'example_instrument.html').render_async(
            request=request,
            summary=self,
            **kwargs
        ))


example_summary_instrument = _ExampleSummaryInstrument()



class _ExampleDisplayInstrument(Display):
    async def __call__(self, request: Request, **kwargs) -> Response:
        return HTMLResponse(await package_template('acquisition', 'display', 'example_instrument.html').render_async(
            request=request,
            display=self,
            **kwargs
        ))


example_display_instrument = _ExampleDisplayInstrument()
