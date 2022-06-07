import typing
import time
from math import isfinite, floor, atan2, sin, cos, sqrt, degrees, radians, nan
from forge.formattime import format_time_of_day, format_iso8601_time
from forge.acquisition import LayeredConfiguration
from forge.acquisition.util import parse_interval


class AverageRecord:
    def __init__(self, config: LayeredConfiguration):
        self.config = config

        self.interval: typing.Optional[float] = parse_interval(config.get("AVERAGE"), 60.0)
        if self.interval == 0:
            self.interval = None
        elif self.interval < 0.0:
            raise ValueError(f"invalid averaging interval {self.interval}")

        self._entries: typing.List[AverageRecord.Entry] = list()

        self._next_record_start: typing.Optional[float] = None
        self._accumulated_seconds: float = 0.0
        self._accumulated_count: int = 0
        self._average_start: typing.Optional[float] = None
        self._average_end: typing.Optional[float] = None

        self._averaging_enabled: bool = True
        self._flush_end_time: typing.Optional[float] = None

    class Entry:
        def clear(self) -> None:
            pass

        def accumulate(self, seconds: float) -> None:
            pass

        def complete(self) -> None:
            pass

        def reset(self) -> None:
            pass

    def has_entry(self, entry: "AverageRecord.Entry") -> bool:
        return entry in self._entries

    class Variable(Entry):
        def __init__(self):
            super().__init__()
            self.value: float = nan

            self._pending_value: typing.Optional[float] = None
            self._sum_times_seconds: float = 0.0
            self._count_seconds: float = 0.0

        def __call__(self, value: float) -> None:
            if value is None or not isfinite(value):
                self._pending_value = None
            else:
                self._pending_value = value

        def __float__(self) -> float:
            return self.value

        def clear(self) -> None:
            self._pending_value = None

        def accumulate(self, seconds: float) -> None:
            if self._pending_value is None:
                return
            self._sum_times_seconds += self._pending_value * seconds
            self._count_seconds += seconds

        def complete(self) -> None:
            if self._count_seconds <= 0.0:
                self.value = nan
            else:
                self.value = self._sum_times_seconds / self._count_seconds
            self._sum_times_seconds: float = 0.0
            self._count_seconds: float = 0.0

        def reset(self) -> None:
            self._pending_value = None
            self._sum_times_seconds: float = 0.0
            self._count_seconds: float = 0.0

    def variable(self) -> "AverageRecord.Variable":
        v = self.Variable()
        self._entries.append(v)
        return v

    class Flag(Entry):
        def __init__(self):
            self.value: bool = False

            self._pending_value: bool = False
            self._average: bool = False

        def __call__(self, value: bool) -> None:
            self._pending_value = value

        def __bool__(self) -> bool:
            return self.value

        def clear(self) -> None:
            self._pending_value = False

        def accumulate(self, seconds: float) -> None:
            if self._pending_value:
                self._average = True

        def complete(self) -> None:
            self.value = self._average
            self._average = False

        def reset(self) -> None:
            self._pending_value = False
            self._average = False

    def flag(self) -> "AverageRecord.Flag":
        f = self.Flag()
        self._entries.append(f)
        return f

    class FirstValid(Entry):
        def __init__(self):
            super().__init__()
            self.value: typing.Any = None

            self._pending_value: typing.Any = None
            self._first_valid: typing.Any = None

        def __call__(self, value: typing.Any) -> None:
            if value is None:
                self._pending_value = None
                return
            if isinstance(value, float) and not isfinite(value):
                self._pending_value = None
                return
            self._pending_value = value

        def __float__(self) -> float:
            if not isinstance(self.value, float):
                return nan
            return self.value

        def clear(self) -> None:
            self._pending_value = None

        def accumulate(self, seconds: float) -> None:
            if self._pending_value is None:
                return
            if self._first_valid is not None:
                return
            self._first_valid = self._pending_value

        def complete(self) -> None:
            self.value = self._first_valid
            self._first_valid = None

        def reset(self) -> None:
            self._pending_value = None
            self._first_valid = None

    def first_valid(self) -> "AverageRecord.FirstValid":
        f = self.FirstValid()
        self._entries.append(f)
        return f

    class LastValid(Entry):
        def __init__(self):
            super().__init__()
            self.value: typing.Any = None

            self._pending_value: typing.Any = None
            self._last_valid: typing.Any = None

        def __call__(self, value: typing.Any) -> None:
            if value is None:
                self._pending_value = None
                return
            if isinstance(value, float) and not isfinite(value):
                self._pending_value = None
                return
            self._pending_value = value

        def __float__(self) -> float:
            if not isinstance(self.value, float):
                return nan
            return self.value

        def clear(self) -> None:
            self._pending_value = None

        def accumulate(self, seconds: float) -> None:
            if self._pending_value is None:
                return
            self._last_valid = self._pending_value

        def complete(self) -> None:
            self.value = self._last_valid
            self._last_valid = None

        def reset(self) -> None:
            self._pending_value = None
            self._last_valid = None

    def last_valid(self) -> "AverageRecord.LastValid":
        f = self.LastValid()
        self._entries.append(f)
        return f

    class Vector(Entry):
        def __init__(self):
            super().__init__()
            self._variable_X = AverageRecord.Variable()
            self._variable_Y = AverageRecord.Variable()

        def __call__(self, magnitude: float, direction: float) -> None:
            if magnitude is None or not isfinite(magnitude) or direction is None or not isfinite(direction):
                self._variable_X(nan)
                self._variable_Y(nan)
            else:
                r = radians(direction - 180)
                self._variable_X(cos(r) * magnitude)
                self._variable_Y(sin(r) * magnitude)

        @property
        def magnitude(self) -> float:
            y = float(self._variable_Y)
            if not isfinite(y):
                return nan
            x = float(self._variable_X)
            if not isfinite(x):
                return nan
            return sqrt(x*x + y*y)

        @property
        def direction(self) -> float:
            y = float(self._variable_Y)
            if not isfinite(y):
                return nan
            x = float(self._variable_X)
            if not isfinite(x):
                return nan
            d = degrees(atan2(y, x)) + 180.0
            if abs(d - 360.0) < 1e-10:
                d = 0.0
            return d

        def clear(self) -> None:
            self._variable_X.clear()
            self._variable_Y.clear()

        def accumulate(self, seconds: float) -> None:
            self._variable_X.accumulate(seconds)
            self._variable_Y.accumulate(seconds)

        def complete(self) -> None:
            self._variable_X.complete()
            self._variable_Y.complete()

        def reset(self) -> None:
            self._variable_X.reset()
            self._variable_Y.reset()

    def vector(self):
        v = self.Vector()
        self._entries.append(v)
        return v

    class Array(Entry):
        def __init__(self):
            super().__init__()
            self.value: typing.List[float] = list()
            self._contents: typing.List[AverageRecord.Variable] = list()
            self._pending_size: typing.Optional[int] = None
            self._largest_size: typing.Optional[int] = None

        def __call__(self, contents: typing.List[float]) -> None:
            while len(contents) > len(self._contents):
                self._contents.append(AverageRecord.Variable())
            for i in range(len(contents)):
                self._contents[i](contents[i])
            self._pending_size = len(contents)

        def __getitem__(self, item: int) -> float:
            return self.value[item]

        def __len__(self) -> int:
            return len(self.value)

        def clear(self) -> None:
            for v in self._contents:
                v.clear()
            self._pending_size = None

        def accumulate(self, seconds: float) -> None:
            if self._pending_size is None:
                return
            if not self._largest_size:
                self._largest_size = self._pending_size
            else:
                self._largest_size = max(self._largest_size, self._pending_size)
            for v in self._contents:
                v.accumulate(seconds)

        def complete(self) -> None:
            if not self._largest_size:
                self._contents.clear()
            elif len(self._contents) > self._largest_size:
                del self._contents[self._largest_size:]

            for v in self._contents:
                v.complete()

            self.value.clear()
            for v in self._contents:
                self.value.append(float(v))

            self._pending_size = None
            self._largest_size = None

        def reset(self) -> None:
            for v in self._contents:
                v.reset()
            self._pending_size = None
            self._largest_size = None

    def array(self):
        a = self.Array()
        self._entries.append(a)
        return a

    def _accumulate_values(self, next_value_start: float, next_value_end: float) -> None:
        if not self._averaging_enabled or self._flush_end_time:
            return
        effective_seconds = next_value_end - next_value_start

        self._accumulated_seconds += effective_seconds
        self._accumulated_count += 1
        for v in self._entries:
            v.accumulate(effective_seconds)

    def _clear_pending_values(self) -> None:
        for v in self._entries:
            v.clear()

    def _advance_average(self, now: float) -> None:
        self._accumulated_seconds = 0.0
        self._accumulated_count = 0

        if not self.interval:
            self._average_start = now
            self._average_end = None
            return

        self._average_start = floor(now / self.interval) * self.interval
        self._average_end = self._average_start + self.interval

    def _update_flush(self, now: float) -> None:
        if not self._flush_end_time:
            return
        if now >= self._flush_end_time:
            self._flush_end_time = None

    class Result:
        def __init__(self, start_time: float, end_time: float, total_seconds: float, total_samples: int):
            self.start_time = start_time
            self.end_time = end_time
            self.total_seconds = total_seconds
            self.total_samples = total_samples

        def __str__(self):
            return format_time_of_day(self.start_time)

        def __repr__(self):
            start = format_iso8601_time(self.start_time)
            if self.end_time:
                end = format_iso8601_time(self.end_time)
            else:
                end = ""
            return f"AverageResult({start},{end},{int(self.total_seconds)},{self.total_samples})"

    def _complete_average(self, now: float) -> "AverageRecord.Result":
        if self._average_end and self.interval:
            # Check for a skipped interval (record rate slower than average rate)
            next_average_start = floor(now / self.interval) * self.interval            
            if next_average_start > self._average_end:
                self._average_end = next_average_start

        if self._next_record_start:
            if not self._average_end or now <= self._average_end:
                self._accumulate_values(self._next_record_start, now)
                self._next_record_start = None
            else:
                self._accumulate_values(self._next_record_start, self._average_end)
                self._next_record_start = self._average_end

        result = self.Result(self._average_start, self._average_end or now,
                             self._accumulated_seconds, self._accumulated_count)

        for v in self._entries:
            v.complete()

        return result

    def __call__(self, now: float = None) -> typing.Optional["AverageRecord.Result"]:
        """
        Finish the average.  For inputs that are emitted at the end of their averaging time (most instruments, since
        they emit the data at the end of onboard averaging), set the variable before finishing the average.  This will
        make the variable take the start time from the prior average finish call and the end time as the current time.
        """
        if not now:
            now = time.time()

        if self._average_end and now < self._average_end:
            self._accumulate_values(self._next_record_start, now)
            self._clear_pending_values()
            self._update_flush(now)
            self._next_record_start = now
            return None

        if not self._average_start:
            self._advance_average(now)
            self._update_flush(now)
            self._next_record_start = now
            return None

        result = self._complete_average(now)
        self._advance_average(now)

        if self._next_record_start and now > self._next_record_start:
            self._accumulate_values(self._next_record_start, now)

        self._clear_pending_values()
        self._update_flush(now)
        self._next_record_start = now
        return result

    def complete(self, now: float = None) -> typing.Optional["AverageRecord.Result"]:
        if not now:
            now = time.time()

        if not self._average_start:
            return None

        result = self._complete_average(now)
        self.reset()
        return result

    def reset(self) -> None:
        for v in self._entries:
            v.reset()
        self._average_start = None
        self._average_end = None
        self._next_record_start = None
        self._accumulated_seconds = 0.0
        self._accumulated_count = 0

    def set_averaging(self, enabled: bool) -> None:
        self._averaging_enabled = enabled

    def start_flush(self, duration: float, now: float = None) -> None:
        if not now:
            now = time.time()
        self.reset()
        end_time = now + duration
        if not self._flush_end_time or end_time > self._flush_end_time:
            self._flush_end_time = end_time
