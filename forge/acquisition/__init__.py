import typing
from dynaconf import Dynaconf
from dynaconf.constants import DEFAULT_SETTINGS_FILES
from tomlkit.container import Container as TOMLContainer
from tomlkit.items import Item as TOMLItem, Table as TOMLTable, Comment as TOMLComment, Key as TOMLKey
from tomlkit.exceptions import NonExistentKey as TOMLNonExistentKey

CONFIGURATION = Dynaconf(
    environments=False,
    lowercase_read=False,
    merge_enabled=True,
    default_settings_paths=DEFAULT_SETTINGS_FILES,
)

_CONFIGURATION_TOML_LOADED: bool = False
_CONFIGURATION_TOML_ROOT: typing.Optional[TOMLContainer] = None


class LayeredConfiguration:
    def __init__(self, *roots: dict, toml: typing.Optional[TOMLContainer] = None):
        self._roots = roots
        self._toml = toml

    @staticmethod
    def _get_toml_key(toml: TOMLContainer, key: str) -> typing.Optional[TOMLItem]:
        try:
            return toml[key]
        except TOMLNonExistentKey:
            pass
        key = key.casefold()
        for check, value in toml.body:
            if not isinstance(check, TOMLKey):
                continue
            if check.key.casefold() == key:
                return value
        return None

    @staticmethod
    def _lookup_toml_path(toml: TOMLContainer, path: typing.Iterable[str]) -> typing.Optional[TOMLContainer]:
        for p in path:
            toml = LayeredConfiguration._get_toml_key(toml, p)
            if not toml:
                return None
            if not isinstance(toml, TOMLTable):
                return None
            toml = toml.value
        return toml

    @staticmethod
    def toml_path(toml: typing.Optional[TOMLContainer], *path: str) -> typing.Optional[TOMLContainer]:
        if toml is None:
            return None

        actual_path = list()
        for p in path:
            actual_path.extend(p.split('.'))

        return LayeredConfiguration._lookup_toml_path(toml, actual_path)

    @staticmethod
    def configuration_toml(*path: str) -> typing.Optional[TOMLContainer]:
        global _CONFIGURATION_TOML_LOADED
        global _CONFIGURATION_TOML_ROOT
        if not _CONFIGURATION_TOML_LOADED:
            _CONFIGURATION_TOML_LOADED = True

            def merge_toml(filename: typing.Optional[str]) -> None:
                global _CONFIGURATION_TOML_ROOT
                if not filename:
                    return

                from tomlkit.exceptions import (ParseError, TOMLKitError)
                from tomlkit import load
                try:
                    with open(filename, "rt") as f:
                        root = load(f)
                except (FileNotFoundError, ParseError):
                    return

                if _CONFIGURATION_TOML_ROOT is None:
                    _CONFIGURATION_TOML_ROOT = root
                    return

                for key, value in root.body:
                    try:
                        _CONFIGURATION_TOML_ROOT.append(key, value)
                    except TOMLKitError:
                        pass

            merge_toml(CONFIGURATION.find_file("settings.local.toml"))
            merge_toml(CONFIGURATION.find_file("settings.toml"))

        return LayeredConfiguration.toml_path(_CONFIGURATION_TOML_ROOT, *path)

    def get(self, *path: str, default=None):
        actual_path = list()
        for p in path:
            actual_path.extend(p.split('.'))

        for layer in self._roots:
            origin = layer
            if not isinstance(origin, dict):
                # Top level is non-dict, so can't have children
                return default
            for p in actual_path[:-1]:
                origin = origin.get(p, None)
                if isinstance(origin, dict):
                    continue
                if origin is None:
                    break
                # The path contains a non-dict element, so abort
                return default
            if origin is None:
                continue
            value = origin.get(actual_path[-1], None)
            if value is not None:
                return value
        return default

    def comment(self, *path: str) -> typing.Optional[str]:
        if not self._toml:
            return None

        actual_path = []
        for p in path:
            actual_path.extend(p.split('.'))
        toml = self._lookup_toml_path(self._toml, actual_path[:-1])
        if toml is None:
            return None

        value = self._get_toml_key(toml, actual_path[-1])
        if value is not None:
            comment = value.trivia.comment.strip()
            if comment.startswith("#"):
                comment = comment[1:].strip()
            if comment:
                return comment

        last_comment: typing.Optional[TOMLComment] = None
        key = actual_path[-1].casefold()
        for check, value in toml.body:
            if isinstance(value, TOMLComment):
                last_comment = value
            if not isinstance(check, TOMLKey):
                continue
            if check.key.casefold() != key:
                last_comment = None
                continue

            if last_comment is not None:
                comment = last_comment.trivia.comment.strip()
                if comment.startswith("#"):
                    comment = comment[1:].strip()
                if comment:
                    return comment

            break

        return None

    def section_or_constant(self, *path: str) -> typing.Any:
        actual_path = []
        for p in path:
            actual_path.extend(p.split('.'))

        subroots: typing.List[dict] = list()
        for layer in self._roots:
            origin = layer
            if not isinstance(origin, dict):
                # Top level is non-dict, so can't have children
                break
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

        if self._toml:
            toml = self._lookup_toml_path(self._toml, actual_path)
        else:
            toml = None
        return LayeredConfiguration(*subroots, toml=toml)

    def section(self, *path: str) -> "LayeredConfiguration":
        s = self.section_or_constant(*path)
        if not isinstance(s, LayeredConfiguration):
            return LayeredConfiguration()
        return s

    def keys(self) -> typing.Set[str]:
        result: typing.Set[str] = set()
        for layer in self._roots:
            result.update(layer.keys())
        return result

    def constant(self, default=None) -> typing.Any:
        for layer in self._roots:
            if isinstance(layer, dict):
                break
            return layer
        return default

    def __getitem__(self, item):
        v = self.get(item)
        if not v:
            raise ValueError
        return v

    def __bool__(self) -> bool:
        if len(self._roots) == 0:
            return False
        if isinstance(self._roots[0], bool):
            return bool(self._roots[0])
        return True

    def __repr__(self) -> str:
        return f"LayeredConfiguration({', '.join([repr(r) for r in self._roots])})"
