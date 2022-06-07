import typing
import asyncio
import struct
import logging
from io import BytesIO
from ..protocol import PersistenceLevel, deserialize_string, deserialize_value, serialize_string, serialize_value

_LOGGER = logging.getLogger(__name__)


class Message:
    def __init__(self, persistence: PersistenceLevel, record: str, value: typing.Any):
        self.persistence = persistence
        self.record = record
        self.value = value

    @staticmethod
    async def read(reader: asyncio.StreamReader) -> "Message":
        persistence = PersistenceLevel(struct.unpack('<B', await reader.readexactly(1))[0])
        record = await deserialize_string(reader)
        value = await deserialize_value(reader)
        return Message(persistence, record, value)

    def __repr__(self):
        return f"Message({repr(self.persistence)}, '{self.record}', {repr(self.value)})"


class _PersistenceKey:
    def __init__(self, source: str, record: str):
        self.source = source
        self.record = record

    def __eq__(self, other):
        if not isinstance(other, _PersistenceKey):
            return NotImplemented
        return self.source == other.source and self.record == other.record

    def __hash__(self):
        return hash((self.source, self.record))

    def __repr__(self):
        return f"({self.source}, {self.record})"


class Dispatch:
    class _Connection:
        def __init__(self, source: str, writer: asyncio.StreamWriter):
            self.source = source
            self.writer = writer
            self.owned_persistence: typing.Set[_PersistenceKey] = set()

        def send_message(self, contents: bytes):
            if not self.writer:
                return
            try:
                self.writer.write(contents)
            except IOError:
                # The read part is what actually handles a failed connection
                self.writer.close()
                self.writer = None

        def __repr__(self):
            return self.source or "_"

    class _PersistentRecord:
        def __init__(self, level: PersistenceLevel, value: typing.Any,
                     owner: typing.Optional["Dispatch._Connection"] = None):
            self.level = level
            self.value: typing.Any = value
            self.owner: typing.Optional["Dispatch._Connection"] = owner

    def __init__(self):
        self._connections: typing.Set["Dispatch._Connection"] = set()
        self._persistence: typing.Dict[_PersistenceKey, "Dispatch._PersistentRecord"] = dict()

    def _send_persistent(self, target: "Dispatch._Connection") -> None:
        messages: typing.List[typing.Tuple[PersistenceLevel, _PersistenceKey, typing.Any]] = list()
        for key, record in self._persistence.items():
            messages.append((record.level, key, record.value))
        messages.sort(key=lambda v: v[0])
        messages.reverse()
        for _, key, value in messages:
            raw = BytesIO()
            serialize_string(raw, key.source)
            serialize_string(raw, key.record)
            serialize_value(raw, value)
            target.send_message(raw.getvalue())

    def _dispatch_message(self, origin: "Dispatch._Connection", message: Message) -> None:
        if message.persistence != PersistenceLevel.DATA:
            key = _PersistenceKey(origin.source, message.record)
            record = self._persistence.get(key)
            if record is not None:
                if record.owner is None:
                    if message.persistence != PersistenceLevel.SYSTEM:
                        _LOGGER.warning(f"Demoting persistent data {key} from global ownership to {origin}")
                elif record.owner != origin:
                    _LOGGER.warning(f"Source {origin} taking ownership of persistence {key} from {record.owner}")
                    record.owner.owned_persistence.discard(key)
                elif message.persistence == PersistenceLevel.SYSTEM:
                    _LOGGER.warning(f"Promoting {origin} persistent data {key} to global")
                    origin.owned_persistence.discard(key)

                record.level = message.persistence
                record.value = message.value

                if message.value is None:
                    # Apply the erasure to future values
                    del self._persistence[key]
            else:
                # If it's an erasure, don't do anything if it doesn't already exist
                if message.value is None:
                    return
                record = self._PersistentRecord(message.persistence, message.value)
                self._persistence[key] = record

            if message.value is None:
                # Drop the erasure from ownership
                origin.owned_persistence.discard(key)
            elif message.persistence == PersistenceLevel.SYSTEM:
                record.owner = None
            else:
                record.owner = origin
                origin.owned_persistence.add(key)

        raw = BytesIO()
        serialize_string(raw, origin.source)
        serialize_string(raw, message.record)
        serialize_value(raw, message.value)
        raw = raw.getvalue()
        for c in self._connections:
            c.send_message(raw)

    def _detach_persistence(self, origin: "Dispatch._Connection") -> None:
        for key in origin.owned_persistence:
            removed = self._persistence.pop(key, None)
            if removed is None:
                continue
            raw = BytesIO()
            serialize_string(raw, key.source)
            serialize_string(raw, key.record)
            serialize_value(raw, None)
            raw = raw.getvalue()
            for c in self._connections:
                c.send_message(raw)

    async def connection(self, source: str, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        connection = self._Connection(source, writer)
        self._send_persistent(connection)
        self._connections.add(connection)
        try:
            while True:
                message = await Message.read(reader)
                self._dispatch_message(connection, message)
        except EOFError:
            pass
        finally:
            self._connections.discard(connection)
            self._detach_persistence(connection)
