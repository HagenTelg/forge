import typing
import asyncio
import enum
from .base import BaseInstrument


class Persistent(BaseInstrument.Persistent):
    def __init__(self, instrument: BaseInstrument, name: str, send_to_bus: bool, save_value: bool):
        super().__init__(instrument, name)

        self.send_to_bus = send_to_bus
        self.save_value = save_value
        self.value: typing.Any = None
        self._deduplicate_value: typing.Any = None

        self._ignore_queue_drop: bool = False
        self._queued_for_bus: typing.Any = None
        self._queued_for_save: typing.Any = None

        self._loaded_time: typing.Optional[float] = None
        self._loaded_value: typing.Any = None

        self._update_queued: bool = False
        self.on_update: typing.List[typing.Callable[[], None]] = list()

    def __repr__(self) -> str:
        return f"Persistent({self.name}={self.value})"

    def to_bus_value(self, value: typing.Any) -> typing.Any:
        return value

    def to_save_value(self, value: typing.Any) -> typing.Any:
        return value

    def from_save_value(self, value: typing.Any) -> typing.Any:
        return value

    def __call__(self, value: typing.Any, deduplicate: bool = None,
                 oneshot: bool = False) -> typing.Any:
        if deduplicate is None:
            deduplicate = not oneshot
        if deduplicate and self._deduplicate_value == value:
            return self.value

        self.value = value

        if self.send_to_bus:
            self._queued_for_bus = self.to_bus_value(self.value)
        if self.save_value:
            self._queued_for_save = self.to_save_value(self.value)

        if oneshot:
            self._ignore_queue_drop = True

        self._update_queued = True
        return self.value

    def load_prior(self, value: typing.Any, effective_time: typing.Optional[float]) -> None:
        if value is None:
            return
        value = self.from_save_value(value)
        if value is None:
            return

        self._loaded_value = value
        self._loaded_time = effective_time

        if self.value is None:
            self.value = value
            self._deduplicate_value = value
            if self.send_to_bus:
                self._queued_for_bus = self.to_bus_value(value)

    def drop_queued(self) -> None:
        if self._ignore_queue_drop:
            return

        # Safe to drop the bus here because the initial one from loading will be sent before any drops happen
        self._queued_for_bus = self._loaded_value
        self._queued_for_save = None
        self._update_queued = False

    def prepare_prior(self) -> typing.Tuple[typing.Any, typing.Optional[float]]:
        if self._loaded_value is None:
            return None, None
        v = self._loaded_value
        self._loaded_value = None
        t = self._loaded_time
        self._loaded_time = None
        return v, t

    async def emit(self, now: float) -> None:
        self._ignore_queue_drop = False

        if self._update_queued:
            self._update_queued = False
            self._deduplicate_value = self.value
            for u in self.on_update:
                u()

        if self._queued_for_bus is not None:
            await self.instrument.context.bus.set_state_value(self.name, self._queued_for_bus)
            self._queued_for_bus = None
        if self._queued_for_save is not None:
            await self.instrument.context.persistent.save(self.name, self._queued_for_save, now)
            self._queued_for_save = None


class PersistentEnum(Persistent):
    def __init__(self, instrument: BaseInstrument, name: str, enum_type: typing.Type[enum.Enum],
                 send_to_bus: bool, save_value: bool):
        super().__init__(instrument, name, send_to_bus, save_value)
        self.enum_type = enum_type

    def to_bus_value(self, value: typing.Any) -> typing.Any:
        return value.name

    def to_save_value(self, value: typing.Any) -> typing.Any:
        return value.value

    def from_save_value(self, value: typing.Any) -> typing.Any:
        try:
            return self.enum_type(value)
        except ValueError:
            return None


class State(BaseInstrument.State):
    def __init__(self, instrument: BaseInstrument, source: Persistent,
                 name: str, code: typing.Optional[str], attributes: typing.Dict[str, typing.Any],
                 automatic: bool):
        super().__init__(instrument, name or source.name, code, attributes)
        self.data.state = self
        self.source = source
        self.automatic = automatic

    def __repr__(self) -> str:
        return f"State({self.name} {self.source.name}{' AUTO' if self.automatic else ''})"

    def apply_override(self, value) -> None:
        raise NotImplementedError


class ChangeEvent(BaseInstrument.ChangeEvent):
    def __init__(self, instrument: BaseInstrument, name: str, state: typing.Iterable[State]):
        super().__init__(instrument, name)

        self._first_emit: bool = True
        self._queued: bool = False

        self.data_record = self.instrument.context.data.state_record(name)

        self.state: typing.List[BaseInstrument.State] = list()

        field_names: typing.Set[str] = set()
        for s in state:
            if s is None:
                continue

            if s.data.name in field_names:
                raise ValueError(f"duplicate variable {repr(s)} in record {self.name}")
            field_names.add(s.data.name)

            self.state.append(s)
            if s.automatic:
                s.source.on_update.append(self)

            self.data_record.add_variable(s.data)

    def __repr__(self) -> str:
        return "ChangeEvent(" + repr(self.state) + ")"

    def __call__(self) -> None:
        self._queued = True

    def _emit_prior_historical(self, now: float) -> None:
        record_time: typing.Optional[float] = None
        for s in self.state:
            value, state_prior = s.source.prepare_prior()
            if value is None or state_prior is None:
                continue

            s.apply_override(value)

            if record_time is None or record_time < state_prior:
                record_time = state_prior

        if record_time is None:
            return

        if record_time > now:
            record_time = now

        self.data_record(record_time, historical=True)
        for s in self.state:
            s.apply_override(None)

    async def emit(self, now: float) -> None:
        if self._first_emit:
            self._first_emit = False
            self._emit_prior_historical(now)

        if not self._queued:
            return
        self._queued = False

        self.data_record(now)
