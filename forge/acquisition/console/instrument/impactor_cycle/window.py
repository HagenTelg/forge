import typing
import time
from forge.formattime import format_time_of_day
from forge.acquisition.console.ui import UserInterface
from ..default.window import InstrumentWindow as BaseWindow


class InstrumentWindow(BaseWindow):
    def __init__(self, ui: UserInterface, source: str, instrument_info: typing.Dict[str, typing.Any]):
        super().__init__(ui, source, instrument_info)
        self._active_size: typing.Optional[typing.Dict[str, typing.Any]] = None
        self._next_size: typing.Optional[typing.Dict[str, typing.Any]] = None

    def instrument_message(self, record: str, message: typing.Any) -> bool:
        if record == 'active':
            self._active_size = message
            return True
        elif record == 'next':
            self._next_size = message
            return True
        return super().instrument_message(record, message)

    @property
    def display_letter(self) -> typing.Optional[str]:
        return None

    @property
    def window_title(self) -> str:
        return "IMPACTOR CYCLE"

    @property
    def detailed_status(self) -> str:
        return ""

    @staticmethod
    def _countdown_time(t: float) -> str:
        timestamp = format_time_of_day(t)

        now = time.time()
        seconds = t - now
        if seconds <= 0.1:
            return timestamp + "        "
        seconds = round(seconds)

        if seconds < 1:
            seconds = 1

        minutes = int(seconds / 60)
        seconds = seconds % 60
        if minutes > 99:
            return f"(--:{seconds:02d}) {timestamp}"

        return f"({minutes:02d}:{seconds:02d}) {timestamp}"

    def data_lines(self) -> typing.List[typing.Tuple[str, str]]:
        result: typing.List[typing.Tuple[str, str]] = list()

        if self._active_size:
            size = self._active_size.get('size')
            t = self._active_size.get('epoch_ms')
            if t:
                t = format_time_of_day(t / 1000.0)
            else:
                t = ""
            result.append((f"Current size: {size}", t))

        if self._next_size:
            size = self._next_size.get('size')
            t = self._next_size.get('epoch_ms')
            if t:
                t = self._countdown_time(t / 1000.0)
            else:
                t = ""
            result.append((f"Next size: {size}", t))

        return result


def create(ui: UserInterface, source: str,
           instrument_info: typing.Dict[str, typing.Any]) -> typing.Optional[BaseWindow]:
    return InstrumentWindow(ui, source, instrument_info)