import typing
import logging
from copy import deepcopy
from starlette.responses import HTMLResponse
from forge.vis.util import package_template
from forge.vis.acquisition.permissions import is_writable
from . import Mode, Request, Response


_LOGGER = logging.getLogger(__name__)


class Acquisition(Mode):
    class _SummaryItem:
        def __init__(self, summary_type: str, priority: typing.Optional[int] = 0):
            self.summary_type = summary_type
            self.priority: typing.Optional[int] = priority

        def __deepcopy__(self, memo):
            y = type(self)(self.summary_type)
            y.priority = self.priority
            memo[id(self)] = y
            return y

    class SummaryStatic(_SummaryItem):
        pass

    class SummaryInstrument(_SummaryItem):
        def __init__(self, summary_type: str, match_type: typing.Optional[str] = None,
                     match_source: typing.Optional[str] = None,
                     priority: typing.Optional[int] = 0):
            super().__init__(summary_type, priority=priority)
            self.match_type = match_type
            self.match_source = match_source

        def __deepcopy__(self, memo):
            y = super().__deepcopy__(memo)
            y.match_type = self.match_type
            y.match_source = self.match_source
            return y
        
    class _DisplayItem:
        def __init__(self, display_type: str):
            self.display_type = display_type

        def __deepcopy__(self, memo):
            y = type(self)(self.display_type)
            memo[id(self)] = y
            return y

    class DisplayStatic(_DisplayItem):
        def __init__(self, display_type: str):
            super().__init__(display_type)
            self.restore_key: typing.Optional[str] = None

    class DisplayInstrument(_DisplayItem):
        def __init__(self, display_type: str, match_type: typing.Optional[str] = None,
                     match_source: typing.Optional[str] = None):
            super().__init__(display_type)
            self.match_type = match_type
            self.match_source = match_source

        def __deepcopy__(self, memo):
            y = super().__deepcopy__(memo)
            y.match_type = self.match_type
            y.match_source = self.match_source
            return y

    def __init__(self, mode_name: str = 'acquisition', display_name: str = "Acquisition",
                 summary_instrument: typing.List["Acquisition.SummaryInstrument"] = None,
                 display_instrument: typing.List["Acquisition.DisplayInstrument"] = None,
                 summary_static: typing.List["Acquisition.SummaryStatic"] = None,
                 display_static: typing.List["Acquisition.DisplayStatic"] = None):
        super().__init__(mode_name, display_name)

        if summary_instrument is None:
            summary_instrument = list()
        self.summary_instrument: typing.List[Acquisition.SummaryInstrument] = summary_instrument

        if display_instrument is None:
            display_instrument = list()
        self.display_instrument: typing.List[Acquisition.DisplayInstrument] = display_instrument

        if summary_static is None:
            summary_static = list()
        self.summary_static: typing.List[Acquisition.SummaryStatic] = summary_static

        if display_static is None:
            display_static = list()
        self.display_static: typing.List[Acquisition.DisplayStatic] = display_static

    def __deepcopy__(self, memo):
        y = type(self)(self.mode_name, self.display_name)
        memo[id(self)] = y
        y.summary_instrument = list(self.summary_instrument)
        y.display_instrument = list(self.display_instrument)
        y.summary_static = list(self.summary_static)
        y.display_static = list(self.display_static)
        return y

    async def __call__(self, request: Request, **kwargs) -> Response:
        return HTMLResponse(await package_template('mode', 'acquisition.html').render_async(
            mode=self,
            request=request,
            writable=is_writable(request, request.path_params['station'].lower()),
            **kwargs
        ))
