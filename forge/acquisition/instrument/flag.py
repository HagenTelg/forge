import typing
from forge.acquisition.average import AverageRecord
from .base import BaseInstrument


class Notification(BaseInstrument.Notification):
    def __init__(self, instrument: BaseInstrument, name: str, is_warning: bool):
        super().__init__(instrument, name, is_warning)
        self.value: bool = False

    def __call__(self, value: bool) -> bool:
        if self.value != value:
            self.instrument.notification_state_changed()
        self.value = value
        return self.value

    def __bool__(self) -> bool:
        return self.value


class Flag(BaseInstrument.Flag):
    class Output(BaseInstrument.Flag.Output):
        def __init__(self, name: str):
            super().__init__(name)
            self.flag: typing.Optional[Flag] = None

        @property
        def value(self) -> bool:
            return bool(self.flag)

    def __init__(self, instrument: BaseInstrument, source: BaseInstrument.Notification):
        super().__init__(instrument, source.name)
        self.source = source
        self.data.flag = self
        self.average: typing.Optional[AverageRecord.Flag] = None

    def __bool__(self) -> bool:
        if self.average is None:
            return False
        return bool(self.average)

    def __call__(self) -> None:
        self.average(bool(self.source))

    def clear(self) -> None:
        self.average.clear()

    def attach_to_record(self, record: BaseInstrument.Record) -> None:
        if self.average is None:
            self.average = record.average.flag()
        elif not record.average.has_entry(self.average):
            raise ValueError(f"flag {self.name} from {self.source.name} attached to multiple records")
