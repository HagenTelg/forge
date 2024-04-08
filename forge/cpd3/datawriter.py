import typing
import struct
import base64
from abc import ABC, abstractmethod
from math import nan
from .variant import serialize
from .identity import Name, Identity


class StandardDataOutput(ABC):
    def __init__(self):
        self._name_to_index: typing.Dict[Name, int] = dict()
        self._index_to_name: typing.List[Name] = list()
        self._next_index = 0

        self._packet_start: typing.Optional[float] = None
        self._packet_end: typing.Optional[float] = None
        self._packet_priority: typing.Optional[int] = None
        self._packet_contents: typing.List[typing.Tuple[int, typing.Any]] = list()

    def _flush_packet(self) -> None:
        if not self._packet_contents:
            return

        packet = bytearray()
        packet += struct.pack('<Bddi', len(self._packet_contents),
                              self._packet_start if self._packet_start is not None else nan,
                              self._packet_end if self._packet_end is not None else nan,
                              self._packet_priority)
        for index, value in self._packet_contents:
            packet += struct.pack('<H', index)
            packet += serialize(value)
        self.output_ready(bytes(packet))

        self._packet_start = None
        self._packet_end = None
        self._packet_priority = None
        self._packet_contents.clear()

    def _index_for_name(self, name: Name) -> int:
        existing = self._name_to_index.get(name, None)
        if existing is not None:
            return existing

        result = self._next_index
        self._next_index = (self._next_index + 1) & 0xFFFF
        if result >= len(self._index_to_name):
            self._index_to_name.append(name)
        else:
            prior = self._index_to_name[result]
            self._index_to_name[result] = name
            del self._name_to_index[prior]
        self._name_to_index[name] = result

        self._flush_packet()
        self.output_ready(struct.pack('<B', 0x80) + name.serialize())

        return result

    def _should_end_packet(self, identity: Identity) -> bool:
        if len(self._packet_contents) >= 0x7F:
            return True
        if self._packet_priority is None:
            return False

        if self._packet_priority != identity.priority:
            return True

        if self._packet_start is None:
            if identity.start is not None:
                return True
        else:
            if identity.start is None:
                return True
            if identity.start != self._packet_start:
                return True

        if self._packet_end is None:
            if identity.end is not None:
                return True
        else:
            if identity.end is None:
                return True
            if identity.end != self._packet_end:
                return True

        return False

    def incoming_value(self, identity: Identity, value: typing.Any) -> None:
        if self._should_end_packet(identity):
            self._flush_packet()
        index = self._index_for_name(identity.name)
        self._packet_contents.append((index, value))
        if self._packet_priority is None:
            self._packet_priority = identity.priority
            self._packet_start = identity.start
            self._packet_end = identity.end

    def finish(self) -> None:
        self._flush_packet()

    RAW_HEADER = b'\xC4\xD3\x02'
    DIRECT_HEADER = b'CPD3DATA-2.0\n'

    @staticmethod
    def direct_encode(packet: bytes) -> bytes:
        return base64.b64encode(packet) + b'\n'

    @staticmethod
    def raw_encode(packet: bytes) -> bytes:
        if len(packet) < 0xFFFF:
            return struct.pack('<H', len(packet)) + packet
        return struct.pack('<HI', 0xFFFF, len(packet)) + packet

    @abstractmethod
    def output_ready(self, packet: bytes) -> None:
        pass


def serialize_archive_value(identity: Identity, value: typing.Any, modified: float,
                            remote_referenced: bool = False) -> bytes:
    return (struct.pack('<dd', identity.start if identity.start else nan, identity.end if identity.end else nan) +
            identity.name.serialize() +
            struct.pack('<i', identity.priority) +
            serialize(value) +
            struct.pack('<dB', modified, 1 if remote_referenced else 0))
