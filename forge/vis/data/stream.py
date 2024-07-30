import typing
import asyncio
import logging
import struct
from abc import ABC, abstractmethod
from math import isfinite, nan
from base64 import b64encode
from forge.tasks import wait_cancelable
from forge.vis.util import sanitize_for_json
from forge.archive.client.connection import Connection, LockDenied, LockBackoff

_LOGGER = logging.getLogger(__name__)


class DataStream(ABC):
    def __init__(self, send: typing.Callable[[typing.Dict], typing.Awaitable[None]]):
        self.send = send

    async def begin(self, stall: typing.Callable[[typing.Optional[str]], typing.Awaitable[None]]) -> None:
        pass

    @abstractmethod
    async def run(self) -> None:
        pass


# noinspection PyAbstractClass
class RecordStream(DataStream):
    BUFFER_RECORDS = 256

    _MVC_FLOAT = struct.pack('<f', nan)

    def __init__(self, send: typing.Callable[[typing.Dict], typing.Awaitable[None]], fields: typing.List[str]):
        super().__init__(send)
        self.fields = fields
        self.epoch_ms: typing.List[int] = list()
        self.values: typing.Dict[str, typing.List[typing.Any]] = dict()
        for field in self.fields:
            self.values[field] = list()

    async def flush(self) -> None:
        if len(self.epoch_ms) == 0:
            return

        origin_epoch_ms = self.epoch_ms[0]
        maximum_delta = 0
        for i in range(len(self.epoch_ms)):
            delta = int(self.epoch_ms[i] - origin_epoch_ms)
            maximum_delta = max(maximum_delta, abs(delta))
            self.epoch_ms[i] = delta
        if maximum_delta < (1 << 31):
            raw = struct.pack(f'<{len(self.epoch_ms)}i', *self.epoch_ms)
        else:
            raw = struct.pack(f'<{len(self.epoch_ms)}q', *self.epoch_ms)

        content = {
            'time': {
                'origin': origin_epoch_ms,
                'count': len(self.epoch_ms),
                'offset': b64encode(raw).decode('ascii'),
            },
            'data': {}
        }

        def is_all_float(check: typing.List) -> bool:
            if len(check) == 0:
                return False
            for v in check:
                if v is None:
                    continue
                if isinstance(v, float):
                    continue
                return False
            return True

        def is_all_float_array(check: typing.List) -> bool:
            if len(check) == 0:
                return False
            any_valid = False
            for v in check:
                if v is None:
                    continue
                if not isinstance(v, list):
                    return False
                if not is_all_float(v):
                    return False
                any_valid = True
            return any_valid

        for field, values in self.values.items():
            if is_all_float(values):
                raw = bytearray()
                for v in values:
                    if v is None or not isfinite(v):
                        raw += self._MVC_FLOAT
                        continue
                    try:
                        raw += struct.pack('<f', v)
                    except OverflowError:
                        raw += self._MVC_FLOAT
                content['data'][field] = b64encode(raw).decode('ascii')
                continue

            if is_all_float_array(values):
                content['data'][field] = {
                    'type': 'array',
                    'values': [],
                }
                for i in range(len(values)):
                    if values[i] is None:
                        content['data'][field]['values'].append("")
                        continue
                    raw = bytearray()
                    for v in values[i]:
                        if v is None or not isfinite(v):
                            raw += self._MVC_FLOAT
                            continue
                        try:
                            raw += struct.pack('<f', v)
                        except OverflowError:
                            raw += self._MVC_FLOAT
                    content['data'][field]['values'].append(b64encode(raw).decode('ascii'))
                continue

            content['data'][field] = [sanitize_for_json(value) for value in values]

        await self.send(content)
        self.epoch_ms.clear()
        for values in self.values.values():
            values.clear()

    async def send_record(self, epoch_ms: int, fields: typing.Dict[str, typing.Any]) -> None:
        self.epoch_ms.append(epoch_ms)
        for field, values in self.values.items():
            values.append(fields.get(field))

        if len(self.epoch_ms) < self.BUFFER_RECORDS:
            return

        await self.flush()


class _BaseArchiveReadStream(ABC):
    MAXIMUM_LOCK_HOLD_TIME: typing.Optional[float] = 30 * 60

    def __init__(self):
        self.connection: typing.Optional[Connection] = None

    @property
    def connection_name(self) -> str:
        raise NotImplementedError

    @abstractmethod
    async def acquire_locks(self) -> None:
        pass

    @abstractmethod
    async def with_locks_held(self) -> None:
        pass

    async def _begin(self, stall: typing.Callable[[typing.Optional[str]], typing.Awaitable[None]]) -> None:
        assert self.connection is None

        self.connection = await Connection.default_connection(self.connection_name, use_environ=False)
        await self.connection.startup()

        backoff = LockBackoff()
        while True:
            await self.connection.transaction_begin(False)
            try:
                await self.acquire_locks()
                break
            except LockDenied as ld:
                await self.connection.transaction_abort()
                _LOGGER.debug("Archive busy: %s", ld.status)
                await stall(ld.status)
                await backoff()
                continue

    async def _stream_run(self) -> None:
        assert self.connection is not None
        try:
            if self.MAXIMUM_LOCK_HOLD_TIME:
                await wait_cancelable(self.with_locks_held(), self.MAXIMUM_LOCK_HOLD_TIME)
            else:
                await self.with_locks_held()

            await self.connection.transaction_commit()
            self._lock_held = False
        finally:
            await self.connection.shutdown()
            self.connection = None


class ArchiveReadStream(DataStream, _BaseArchiveReadStream):
    def __init__(self, send: typing.Callable[[typing.Dict], typing.Awaitable[None]]):
        DataStream.__init__(self, send)
        _BaseArchiveReadStream.__init__(self)

    async def begin(self, stall: typing.Callable[[typing.Optional[str]], typing.Awaitable[None]]) -> None:
        await self._begin(stall)

    async def run(self) -> None:
        await self._stream_run()


class ArchiveRecordStream(RecordStream, _BaseArchiveReadStream):
    def __init__(self, send: typing.Callable[[typing.Dict], typing.Awaitable[None]], fields: typing.List[str]):
        RecordStream.__init__(self, send, fields)
        _BaseArchiveReadStream.__init__(self)

    async def begin(self, stall: typing.Callable[[typing.Optional[str]], typing.Awaitable[None]]) -> None:
        await self._begin(stall)

    async def run(self) -> None:
        await self._stream_run()
        await RecordStream.flush(self)
