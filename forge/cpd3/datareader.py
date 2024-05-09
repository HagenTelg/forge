import typing
import struct
import base64
from abc import ABC, abstractmethod
from .variant import deserialize
from .identity import Name, Identity


class StandardDataInput(ABC):
    def __init__(self):
        self._raw_buffer = bytearray()
        self._names: typing.List[Name] = list()
        self._name_index = 0

    def _process_packet(self, packet: bytearray) -> None:
        n = packet[0]
        del packet[0]
        if n == 0x80:
            name = Name.deserialize(packet)
            if self._name_index >= len(self._names):
                self._names.append(name)
            else:
                self._names[self._name_index] = name
            self._name_index = (self._name_index + 1) & 0xFFFF
            return

        start, end, priority = struct.unpack('<ddI', packet[:20])
        del packet[:20]
        for i in range(n):
            name = struct.unpack('<H', packet[:2])[0]
            del packet[:2]
            name = self._names[name]
            value = deserialize(packet)
            self.value_ready(Identity(name=name, start=start, end=end, priority=priority), value)

    def incoming_raw(self, data: typing.Union[bytes, bytearray]) -> None:
        self._raw_buffer += data
        while True:
            if len(self._raw_buffer) < 2:
                return

            n = struct.unpack('<H', self._raw_buffer[:2])[0]
            if n != 0xFFFF:
                if len(self._raw_buffer) < 2+n:
                    return
                self._process_packet(self._raw_buffer[2:2+n])
                del self._raw_buffer[:2+n]
                continue

            if len(self._raw_buffer) < 6:
                return

            n = struct.unpack('<I', self._raw_buffer[2:6])[0]
            if len(self._raw_buffer) < 6+n:
                return
            self._process_packet(self._raw_buffer[6:6+n])
            del self._raw_buffer[:6+n]

    def incoming_base64(self, data: typing.Union[str, bytes, bytearray]) -> None:
        return self._process_packet(bytearray(base64.b64decode(data)))

    @abstractmethod
    def value_ready(self, identity: Identity, value: typing.Any) -> None:
        pass


# noinspection PyAbstractClass
class RecordInput(StandardDataInput):
    TIME_SLACK = 10.0

    class _Active:
        def __init__(self, identity: Identity, value: typing.Any):
            self.identity = identity
            self.value = value

    def __init__(self):
        super().__init__()
        self._current_start: typing.Optional[float] = None
        self._values: typing.Dict[Name, RecordInput._Active] = dict()

    def _is_advance(self, start: float) -> bool:
        if self._current_start is None:
            return start is not None
        return start > self._current_start + self.TIME_SLACK

    def _assemble(self) -> typing.Tuple[float, typing.Dict[Name, typing.Any]]:
        record: typing.Dict[Name, typing.Any] = dict()
        last_end = None
        have_end = False
        for name, active in self._values.items():
            record[name] = active.value
            if not have_end:
                last_end = active.identity.end
                have_end = True
            elif not active.identity.end or active.identity.end > last_end:
                last_end = active.identity.end
        return last_end, record

    def flush(self) -> None:
        last_end, record = self._assemble()
        self.record_ready(self._current_start, last_end, record)
        self._values.clear()
        self._current_start = None

    def _advance(self, until: float):
        if len(self._values) == 0:
            return
        last_end, record = self._assemble()
        if last_end and last_end + self.TIME_SLACK < until:
            self.record_ready(self._current_start, last_end, record)
            self.record_break(last_end, until)
            self._values.clear()
        else:
            self.record_ready(self._current_start, until, record)
            if last_end and last_end <= until:
                self._values.clear()
            else:
                for name in list(self._values.keys()):
                    active = self._values[name]
                    if active.identity.end and active.identity.end <= until:
                        del self._values[name]

    def value_ready(self, identity: Identity, value: typing.Any) -> None:
        if self._is_advance(identity.start):
            self._advance(identity.start)
            self._current_start = identity.start
        self._values[identity.name] = self._Active(identity, value)

    @abstractmethod
    def record_ready(self, start: typing.Optional[float], end: typing.Optional[float],
                     record: typing.Dict[Name, typing.Any]) -> None:
        pass

    def record_break(self, start: float, end: float) -> None:
        pass


def deserialize_archive_value(data: typing.Union[bytearray, bytes]) -> typing.Tuple[Identity, typing.Any, float, bool]:
    if isinstance(data, bytes):
        data = bytearray(data)
    start, end = struct.unpack('<dd', data[:16])
    del data[:16]
    name = Name.deserialize(data)
    priority = struct.unpack('<i', data[:4])[0]
    del data[:4]
    value = deserialize(data)
    modified, remote_referenced = struct.unpack('<dB', data[:9])
    del data[:9]
    remote_referenced = (remote_referenced != 0)
    return Identity(name=name, start=start, end=end, priority=priority), value, modified, remote_referenced
