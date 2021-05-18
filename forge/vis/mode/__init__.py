import typing
from abc import ABC, abstractmethod
from starlette.requests import Request
from starlette.responses import Response


class Mode(ABC):
    def __init__(self, mode_name: str, display_name: str):
        self.mode_name = mode_name
        self.display_name = display_name

    @abstractmethod
    async def __call__(self, request: Request, **kwargs) -> Response:
        pass

    @property
    def header_name(self):
        return self.display_name


class ModeGroup:
    def __init__(self, display_name: str, modes: typing.Optional[typing.List[Mode]] = None):
        self.display_name = display_name
        self.modes: typing.List[Mode] = modes if modes else list()

    def default_mode(self) -> typing.Optional[Mode]:
        if len(self.modes) > 0:
            return self.modes[0]
        return None

    def contains_mode(self, mode_name: str) -> bool:
        for mode in self.modes:
            if mode.mode_name == mode_name:
                return True
        return False

    def is_displayed(self, request: Request, station: str) -> bool:
        for mode in self.modes:
            if request.user.allow_mode(station, mode.mode_name):
                return True
        return False


class VisibleModes:
    def __init__(self, groups: typing.Optional[typing.List[ModeGroup]] = None):
        self.groups: typing.List[ModeGroup] = groups if groups else list()

    def default_mode(self) -> typing.Optional[Mode]:
        for group in self.groups:
            mode = group.default_mode()
            if mode is not None:
                return mode
        return None

    @property
    def enable_selection(self) -> bool:
        for group in self.groups:
            if len(group.modes) > 0:
                return True
        return False

    def active_group(self, mode_name: str) -> typing.Optional[ModeGroup]:
        for group in self.groups:
            if group.contains_mode(mode_name):
                return group
        return None
