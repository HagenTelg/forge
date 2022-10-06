import typing
import struct
import time
from math import nan, isfinite
from forge.formattime import format_iso8601_time
from .variant import serialize_short_string, deserialize_short_string


class Name:
    def __init__(self,
                 station: typing.Optional[str] = None,
                 archive: typing.Optional[str] = None,
                 variable: typing.Optional[str] = None,
                 flavors: typing.Optional[typing.Set[str]] = None,
                 ):
        self.station: str = station.lower() if station else ''
        self.archive: str = archive.lower() if archive else ''
        self.variable: str = variable if variable else ''
        self.flavors: typing.Set[str] = {flavor.lower() for flavor in flavors} if flavors else set()

    def __eq__(self, other):
        if not isinstance(other, Name):
            return NotImplemented
        return (self.variable == other.variable and self.flavors == other.flavors and
                self.archive == other.archive and self.station == other.station)

    def __hash__(self):
        flavors = None
        if len(self.flavors) == 1:
            flavors = next(x for x in self.flavors)
        return hash((self.station, self.archive, self.variable, flavors))

    def __repr__(self):
        return f"Name({self.station}, {self.archive}, {self.variable}, {self.flavors})"

    @property
    def metadata(self) -> bool:
        return self.archive.endswith('_meta')

    def to_metadata(self) -> "Name":
        return Name(self.station, self.archive + '_meta', self.variable, self.flavors)

    @property
    def default_station(self) -> bool:
        return self.station == '_'

    def serialize(self) -> bytes:
        result = bytearray()
        result += serialize_short_string(self.station)
        result += serialize_short_string(self.archive)
        result += serialize_short_string(self.variable)
        result += serialize_short_string(' '.join(self.flavors))
        return bytes(result)

    @classmethod
    def deserialize(cls, data: typing.Union[bytearray, bytes]) -> "Name":
        if isinstance(data, bytes):
            data = bytearray(data)
        station = deserialize_short_string(data)
        archive = deserialize_short_string(data)
        variable = deserialize_short_string(data)
        flavors = deserialize_short_string(data)
        if len(flavors) == 0:
            flavors = set()
        else:
            flavors = set(flavors.split(' '))
        return Name(station, archive, variable, flavors)


class Identity:
    def __init__(self,
                 station: typing.Optional[str] = None,
                 archive: typing.Optional[str] = None,
                 variable: typing.Optional[str] = None,
                 flavors: typing.Optional[typing.Set[str]] = None,
                 start: typing.Optional[float] = None,
                 end: typing.Optional[float] = None,
                 priority: typing.Optional[int] = None,
                 name: typing.Optional[Name] = None,
                 ):
        if name:
            if station or archive or variable or flavors:
                self.name = Name(
                    station if station else name.station,
                    archive if archive else name.archive,
                    variable if variable else name.variable,
                    flavors if flavors else name.flavors
                )
            else:
                self.name = name
        else:
            self.name = Name(station, archive, variable, flavors)
        if not start or not isfinite(start):
            start = None
        else:
            start = float(start)
        self.start: typing.Optional[float] = start
        if not end or not isfinite(end):
            end = None
        else:
            end = float(end)
        self.end: typing.Optional[float] = end
        self.priority: int = int(priority) if priority else 0

    def __eq__(self, other):
        if not isinstance(other, Identity):
            return NotImplemented
        return (self.name == other.name and self.start == other.start and
                self.end == other.end and self.priority == other.priority)

    def __hash__(self):
        return hash((self.name, self.start, self.end, self.priority))

    def __repr__(self):
        def format_time(ts: float) -> str:
            if not ts:
                return "∞"
            return format_iso8601_time(ts)

        return f"Identity({self.name}, {format_time(self.start)}, {format_time(self.end)}, {self.priority})"

    @property
    def station(self):
        return self.name.station

    @property
    def archive(self):
        return self.name.archive

    @property
    def variable(self):
        return self.name.variable

    @property
    def flavors(self):
        return self.name.flavors

    @property
    def metadata(self):
        return self.archive.endswith('_meta')

    @property
    def default_station(self):
        return self.station == '_'

    def serialize(self) -> bytes:
        return self.name.serialize() + struct.pack('<ddi',
                                                   self.start if self.start else nan,
                                                   self.end if self.end else nan,
                                                   self.priority)

    @classmethod
    def deserialize(cls, data: typing.Union[bytearray, bytes]) -> "Identity":
        if isinstance(data, bytes):
            data = bytearray(data)
        name = Name.deserialize(data)
        start, end, priority = struct.unpack('<ddi', data[:20])
        del data[:20]
        return Identity(name=name, start=start, end=end, priority=priority)
