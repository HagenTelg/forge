import typing
import logging
from abc import ABC, abstractmethod
from copy import deepcopy
from starlette.responses import Response


_LOGGER = logging.getLogger(__name__)


class Export(ABC):
    @abstractmethod
    async def __call__(self) -> Response:
        pass


class ExportList:
    class Entry:
        def __init__(self, key: str, display: str):
            self.key = key
            self.display = display

        def __deepcopy__(self, memo):
            y = type(self)(self.key, self.display)
            memo[id(self)] = y
            return y

    def __init__(self, exports: typing.Optional[typing.List["ExportList.Entry"]] = None):
        self.exports: typing.List[ExportList.Entry] = exports if exports else list()

    def __deepcopy__(self, memo):
        y = type(self)(deepcopy(self.exports, memo))
        memo[id(self)] = y
        return y

    def __getitem__(self, key: str) -> "ExportList.Entry":
        for entry in self.exports:
            if entry.key == key:
                return entry
        raise KeyError

    def insert(self, entry: "ExportList.Entry", key: typing.Optional[str] = None, after=True):
        if not key:
            if after:
                self.exports.append(entry)
            else:
                self.exports.insert(0, entry)
            return
        for i in range(len(self.exports)):
            if self.exports[i].key == key:
                if after:
                    self.exports.insert(i + 1, entry)
                else:
                    self.exports.insert(i, entry)
                return
        _LOGGER.warning(f"Export {key} does not exist")
        self.exports.append(entry)

    def remove(self, key: str):
        for i in range(len(self.exports)):
            if self.exports[i].key == key:
                del self.exports[i]
                return
