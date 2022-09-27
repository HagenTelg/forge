import typing
import asyncio
import time
from math import floor
from . import LayeredConfiguration
from .util import parse_interval


class Schedule:
    class Active:
        def __init__(self, config: LayeredConfiguration):
            self.config = config

            self.cycle_offset: float = 0
            self.last_time: typing.Optional[float] = None
            self.next_time: typing.Optional[float] = None
            self.scheduled_time: typing.Optional[float] = None

            self._has_activated: bool = False

        def activate(self, now: float = None) -> bool:
            if self._has_activated:
                return False
            if not now:
                now = time.time()
            self.last_time = now
            self._has_activated = True
            return True

        async def automatic_activation(self, now: float = None) -> bool:
            return self.activate(now)

        def describe_offset(self) -> str:
            if self.cycle_offset <= 0.0:
                return "0"
            offset = self.cycle_offset

            result = ""
            days = int(offset / (24 * 60 * 60))
            if days:
                result = result + str(days) + "D"
            offset -= days * (24 * 60 * 60)

            hours = int(offset / (60 * 60))
            if hours:
                result = result + str(hours) + "H"
            offset -= hours * (60 * 60)

            minutes = int(offset / (60))
            if minutes:
                result = result + str(minutes) + "M"
            offset -= minutes * (60)

            seconds = int(offset)
            if seconds or not result:
                result = result + str(seconds) + "S"

            return result

        def __repr__(self) -> str:
            return self.describe_offset()

        def __str__(self):
            return "Schedule.Active(" + self.describe_offset() + ")"

    def __init__(self, config: LayeredConfiguration, single_entry: bool = False):
        self.active: typing.List[Schedule.Active] = list()
        self._current_index: typing.Optional[int] = None

        if single_entry:
            self.config = LayeredConfiguration()
            self.active.append(self.Active(config))
        else:
            self.config = config
            entries = self.config.get('SCHEDULE')
            if not isinstance(entries, list):
                entries = [entries]
            for entry in entries:
                a = self.Active(entry)
                if isinstance(entry, dict):
                    offset = entry.get("TIME")
                else:
                    offset = None
                if offset is not None:
                    a.cycle_offset = parse_interval(offset)
                if a.cycle_offset < 0.0:
                    raise ValueError("invalid cycle offset")
                self.active.append(a)

        if len(self.active) == 0:
            raise ValueError("at least one cycle schedule is required")

        self.cycle_time = parse_interval(self.config.get('CYCLE_TIME', default=3600.0))
        if self.cycle_time <= 0.0:
            raise ValueError("invalid cycle time")

        for a in self.active:
            a.cycle_offset %= self.cycle_time
        self.active.sort(key=lambda a: a.cycle_offset)

        alternate = self.config.section('ALTERNATE')
        if alternate:
            interval = alternate.get("INTERVAL")
            if interval is None:
                interval = alternate.get("TIME")
            interval = parse_interval(interval)
            if interval <= 0.0:
                raise ValueError("invalid alternation interval")

            primary = self.active[-1].config
            offset = self.active[-1].cycle_offset
            stop_offset = self.active[0].cycle_offset
            have_wrapped = False
            while True:
                offset += interval
                if offset >= self.cycle_time:
                    have_wrapped = True
                offset %= self.cycle_time
                if have_wrapped and offset >= stop_offset:
                    break

                a = self.Active(alternate)
                a.cycle_offset = offset
                self.active.append(a)

                offset += interval
                if offset >= self.cycle_time:
                    have_wrapped = True
                offset %= self.cycle_time
                if have_wrapped and offset >= stop_offset:
                    break

                a = self.Active(primary)
                a.cycle_offset = offset
                self.active.append(a)

            self.active.sort(key=lambda a: a.cycle_offset)

    def _recalculate_current(self, now: float) -> None:
        current_cycle_time = now % self.cycle_time
        current_cycle_start = floor(now / self.cycle_time) * self.cycle_time

        least_time_before: float = 0
        least_time_index: typing.Optional[int] = None

        for i in range(len(self.active)):
            check = self.active[i]
            check._has_activated = False

            time_before_current = current_cycle_time - check.cycle_offset
            if time_before_current < 0.0:
                # On the prior cycle
                time_before_current += self.cycle_time

            if least_time_index is None or time_before_current < least_time_before:
                least_time_before = time_before_current
                least_time_index = i

        self._current_index = least_time_index

        for a in self.active:
            sched_time = current_cycle_start + a.cycle_offset
            if a != self.active[self._current_index]:
                if sched_time <= now:
                    sched_time += self.cycle_time
                a.next_time = sched_time
                a.scheduled_time = sched_time
            else:
                a.scheduled_time = sched_time
                if sched_time <= now:
                    sched_time += self.cycle_time
                a.next_time = sched_time

    def _update_current(self, now: float) -> None:
        if self._current_index is None:
            self._recalculate_current(now)
        else:
            next_active = self.active[(self._current_index + 1) % len(self.active)]
            if now >= next_active.next_time:
                self._recalculate_current(now)

    def advance(self, now: float = None) -> typing.Tuple["Schedule.Active", "Schedule.Active"]:
        if not now:
            now = time.time()
        self._update_current(now)
        return self.active[self._current_index], self.active[(self._current_index + 1) % len(self.active)]

    def current(self, now: float = None) -> "Schedule.Active":
        if not now:
            now = time.time()
        self._update_current(now)
        return self.active[self._current_index]

    def next(self, now: float = None) -> "Schedule.Active":
        if not now:
            now = time.time()
        self._update_current(now)
        return self.active[(self._current_index + 1) % len(self.active)]

    async def automatic_activation(self) -> typing.NoReturn:
        while True:
            now = time.time()
            current = self.current(now)
            await current.automatic_activation(now)

            next = self.next(now)
            delay = next.next_time - now
            if delay < 0.001:
                delay = 0.001
            await asyncio.sleep(delay)
