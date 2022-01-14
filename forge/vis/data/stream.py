import typing
import struct
from abc import ABC, abstractmethod
from math import isfinite, nan
from base64 import b64encode


class DataStream(ABC):
    class Stall(ABC):
        def __init__(self, reason: typing.Optional[str] = None):
            self.reason = reason

        @abstractmethod
        async def block(self):
            pass

    def __init__(self, send: typing.Callable[[typing.Dict], typing.Awaitable[None]]):
        self.send = send

    async def stall(self) -> typing.Optional["DataStream.Stall"]:
        return None

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
            for v in check:
                if not isinstance(v, list):
                    return False
                if is_all_float(v):
                    continue
                return False
            return True

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

            content['data'][field] = [
                (value if value is None or not isinstance(value, float) or isfinite(value) else None)
                for value in values
            ]

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
