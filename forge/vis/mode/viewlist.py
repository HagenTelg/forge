import typing
import logging
from copy import deepcopy
from starlette.responses import HTMLResponse
from forge.vis.util import package_template
from . import Mode, Request, Response


_LOGGER = logging.getLogger(__name__)


class ViewList(Mode):
    class Entry:
        def __init__(self, view_name: str, display_name: str):
            self.view_name = view_name
            self.display_name = display_name

        def __deepcopy__(self, memo):
            y = type(self)(self.view_name, self.display_name)
            memo[id(self)] = y
            return y

    def __init__(self, mode_name: str, display_name: str, views: typing.Optional[typing.List["ViewList.Entry"]] = None):
        super().__init__(mode_name, display_name)
        self.views: typing.List[ViewList.Entry] = views if views else list()

    def insert(self, entry: "ViewList.Entry", view_name: typing.Optional[str] = None, after=True):
        if not view_name:
            if after:
                self.views.append(entry)
            else:
                self.views.insert(0, entry)
            return
        for i in range(len(self.views)):
            if self.views[i].view_name == view_name:
                if after:
                    self.views.insert(i + 1, entry)
                else:
                    self.views.insert(i, entry)
                return
        _LOGGER.warning(f"View {view_name} does not exist in the mode")
        self.views.append(entry)

    def remove(self, view_name: str):
        for i in range(len(self.views)):
            if self.views[i].view_name == view_name:
                del self.views[i]
                return

    def __deepcopy__(self, memo):
        y = type(self)(self.mode_name, self.display_name, deepcopy(self.views, memo))
        memo[id(self)] = y
        return y

    async def __call__(self, request: Request, **kwargs) -> Response:
        return HTMLResponse(await package_template('mode', 'viewlist.html').render_async(
            mode=self,
            request=request,
            **kwargs
        ))


class Editing(ViewList):
    async def __call__(self, request: Request, **kwargs) -> Response:
        return HTMLResponse(await package_template('mode', 'editing.html').render_async(
            mode=self,
            request=request,
            **kwargs
        ))
