import typing
from dynaconf import Dynaconf
from dynaconf.constants import DEFAULT_SETTINGS_FILES
from dynaconf.utils.boxing import Box

CONFIGURATION = Dynaconf(
    environments=False,
    lowercase_read=False,
    merge_enabled=True,
    default_settings_paths=DEFAULT_SETTINGS_FILES,
)


class LayeredConfiguration:
    def __init__(self, *roots: dict):
        self._roots = roots

    def get(self, *path: str, default=None):
        actual_path = list()
        for p in path:
            actual_path.extend(p.split('.'))

        for layer in self._roots:
            origin = layer
            for p in actual_path[:-1]:
                origin = origin.get(p, default=None)
                if isinstance(origin, dict):
                    continue
                if origin is not None:
                    # The path contains a non-dict element, so abort
                    return default
            if origin is None:
                continue
            value = origin.get(actual_path[-1], None)
            if value is not None:
                return value
        return default

    def comment(self, *path: str) -> typing.Optional[str]:
        return None

    def section_or_constant(self, *path: str) -> typing.Any:
        actual_path = []
        for p in path:
            actual_path.extend(p.split('.'))

        subroots: typing.List[dict] = list()
        for layer in self._roots:
            origin = layer
            for p in actual_path[:-1]:
                origin = origin.get(p, None)
                if not isinstance(origin, dict):
                    break
            else:
                # Path is valid up to the last component
                origin = origin.get(actual_path[-1], None)
                if origin is None:
                    continue
                if not isinstance(origin, dict):
                    if not subroots:
                        return origin
                    continue
                subroots.append(origin)
                continue
            if origin is not None:
                # A non-dict part of the path was encountered, so mask any other layers off
                break

        return LayeredConfiguration(*subroots)

    def section(self, *path: str) -> "LayeredConfiguration":
        s = self.section_or_constant(*path)
        if not isinstance(s, LayeredConfiguration):
            return LayeredConfiguration()
        return s

    def __getitem__(self, item):
        v = self.get(item)
        if not v:
            raise ValueError
        return v

    def __bool__(self) -> bool:
        if len(self._roots) == 0:
            return False
        if self._roots[0] == False:
            return False
        return True
