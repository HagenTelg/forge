import typing
import logging
import netCDF4
import re

_LOGGER = logging.getLogger(__name__)


class InstrumentSelection:
    def __init__(self, selection: typing.Dict[str, typing.Any] = None):
        self.instrument_id: typing.Optional["re.Pattern"] = None
        self.instrument: typing.Optional[str] = None
        self.require_tags: typing.Set[str] = set()
        self.exclude_tags: typing.Set[str] = set()

        if selection:
            m = selection.get("instrument_id", None)
            if m:
                self.instrument_id = re.compile(m)

            self.instrument = selection.get("instrument", None)

            tags = selection.get("tags", None)
            if tags:
                if isinstance(tags, str):
                    tags = tags.split()
                for t in tags:
                    if t.startswith('+'):
                        self.require_tags.add(t[1:])
                    elif t.startswith('-'):
                        self.exclude_tags.add(t[1:])
                    else:
                        self.require_tags.add(t)
            tags = selection.get("require_tags", None)
            if tags:
                if isinstance(tags, str):
                    tags = tags.split()
                self.require_tags.update(tags)
            tags = selection.get("exclude_tags", None)
            if tags:
                if isinstance(tags, str):
                    tags = tags.split()
                self.exclude_tags.update(tags)

            if self.matches_everything:
                _LOGGER.warning("Always matching instrument selector created from %s", repr(selection))

    @classmethod
    def to_selections(cls, selections) -> typing.List["InstrumentSelection"]:
        if isinstance(selections, InstrumentSelection):
            return [selections]
        if isinstance(selections, dict):
            return [cls(selections)]
        result = list()
        for sel in selections:
            if isinstance(sel, InstrumentSelection):
                result.append(sel)
                continue
            result.append(cls(sel))
        return result

    @property
    def matches_everything(self) -> bool:
        if self.instrument_id is not None:
            return False
        if self.instrument is not None:
            return False
        if self.require_tags or self.exclude_tags:
            return False
        return True

    def matches_file(self, root: netCDF4.Dataset) -> bool:
        if self.instrument_id is not None:
            check = getattr(root, 'instrument_id', None)
            if check is None or not self.instrument_id.fullmatch(check):
                return False

        if self.instrument is not None:
            check = getattr(root, 'instrument', None)
            if check is None or check != self.instrument:
                # Don't consider history, since we generally want a specific instrument type,
                # and we'd end up duplicating corrections if we accepted multiple
                return False

        if self.require_tags or self.exclude_tags:
            check = getattr(root, 'forge_tags', None)
            if not check:
                if self.require_tags:
                    return False
            else:
                check = set(check.split())
                if not self.require_tags.issubset(check):
                    return False
                if not self.exclude_tags.isdisjoint(check):
                    return False

        return True

    @classmethod
    def matcher(cls, selections) -> typing.Callable[[netCDF4.Dataset], bool]:
        selections = cls.to_selections(selections)

        def m(root: netCDF4.Dataset) -> bool:
            for s in selections:
                if s.matches_file(root):
                    return True
            return False

        return m


class VariableSelection:
    def __init__(self, selection: typing.Union[typing.Dict[str, typing.Any], str] = None):
        self.variable_id: typing.Optional["re.Pattern"] = None
        self.variable_name: typing.Optional[str] = None
        self.standard_name: typing.Optional[str] = None
        self.units: typing.Optional[str] = None

        if selection:
            if isinstance(selection, str):
                self.variable_id = re.compile(selection)
                return

            m = selection.get("variable_id", None)
            if m:
                self.variable_id = re.compile(m)

            self.variable_name = selection.get("variable_name", None)
            self.standard_name = selection.get("standard_name", None)
            self.units = selection.get("units", None)

            if self.matches_everything:
                _LOGGER.warning("Always matching variable selector created from %s", repr(selection))

    @classmethod
    def to_selections(cls, selections) -> typing.List["VariableSelection"]:
        if isinstance(selections, VariableSelection):
            return [selections]
        if isinstance(selections, dict) or isinstance(selections, str):
            return [cls(selections)]
        result = list()
        for sel in selections:
            if isinstance(sel, VariableSelection):
                result.append(sel)
                continue
            result.append(cls(sel))
        return result

    @property
    def matches_everything(self) -> bool:
        if self.variable_id is not None:
            return False
        if self.variable_name is not None:
            return False
        if self.standard_name is not None:
            return False
        if self.units is not None:
            return False
        return True

    def matches_variable(self, var: netCDF4.Variable) -> bool:
        if self.variable_name is not None:
            if var.name != self.variable_name:
                return False

        if self.variable_id is not None:
            check = getattr(var, 'variable_id', None)
            if check is None or not self.variable_id.fullmatch(check):
                return False

        if self.standard_name is not None:
            check = getattr(var, 'standard_name', None)
            if check is None or check != self.standard_name:
                return False

        if self.units is not None:
            check = getattr(var, 'units', None)
            if check is None or check != self.units:
                return False

        return True

    @classmethod
    def matcher(cls, selections) -> typing.Callable[[netCDF4.Variable], bool]:
        selections = cls.to_selections(selections)

        def m(var: netCDF4.Variable) -> bool:
            for s in selections:
                if s.matches_variable(var):
                    return True
            return False

        return m
