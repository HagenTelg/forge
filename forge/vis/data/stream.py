import typing
from abc import ABC, abstractmethod
from math import isfinite
from statistics import mode


class DataStream(ABC):
    def __init__(self, send: typing.Callable[[typing.Dict], typing.Awaitable[None]]):
        self.send = send

    @abstractmethod
    async def run(self) -> None:
        pass


# noinspection PyAbstractClass
class RecordStream(DataStream):
    BUFFER_RECORDS = 256

    def __init__(self, send: typing.Callable[[typing.Dict], typing.Awaitable[None]], fields: typing.List[str],
                 precision: typing.Optional[typing.Dict[str, int]] = None):
        super().__init__(send)
        self.fields = fields
        self.epoch_ms: typing.List[int] = list()
        self.values: typing.Dict[str, typing.List[typing.Any]] = dict()
        for field in self.fields:
            self.values[field] = list()
        if precision is None:
            precision = dict()
        self.precision: typing.Dict[str, int] = precision

    def _calculate_epoch_delta(self) -> int:
        return mode(self.epoch_ms)

    async def flush(self) -> None:
        if len(self.epoch_ms) == 0:
            return

        delta_epoch_ms = self._calculate_epoch_delta()
        origin_epoch_ms = self.epoch_ms[0]
        for i in range(len(self.epoch_ms)):
            point_origin = i * delta_epoch_ms
            self.epoch_ms[i] = self.epoch_ms[i] - point_origin - origin_epoch_ms

        content = {
            'time': {
                'origin': origin_epoch_ms,
                'delta': delta_epoch_ms,
                'offset': self.epoch_ms,
            },
            'data': {}
        }

        for field, values in self.values.items():
            origin = None
            is_all_float = False
            for check in values:
                if check is None:
                    continue
                if not isinstance(check, float):
                    is_all_float = False
                    break
                is_all_float = True
                if origin is None and isfinite(check):
                    origin = check

            if not is_all_float:
                content['data'][field] = [
                    (value if value is None or not isinstance(value, float) or isfinite(value) else None)
                    for value in values
                ]
                continue

            precision = self.precision.get(field)

            if origin is None:
                if precision == 0:
                    values = [(round(value) if value is not None and isfinite(value) else None)
                              for value in values]
                elif precision is not None:
                    values = [(round(value, precision) if value is not None and isfinite(value) else None)
                              for value in values]
                content['data'][field] = values
                continue

            if precision == 0:
                origin = round(origin)
                values = [(round(value - origin) if value is not None and isfinite(value) else None)
                          for value in values]
            elif precision is not None:
                origin = round(origin, precision)
                values = [(round(value - origin, precision) if value is not None and isfinite(value) else None)
                          for value in values]
            else:
                values = [((value - origin) if value is not None and isfinite(value) else None)
                          for value in values]
            content['data'][field] = {
                'origin': origin,
                'offset': values,
            }

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
