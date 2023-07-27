import typing
import time
from forge.formattime import format_iso8601_time
from forge.acquisition.console.ui import UserInterface
from ..default.window import InstrumentWindow as BaseWindow
from ..default.data import DataDisplay


class InstrumentWindow(BaseWindow):
    def __init__(self, ui: UserInterface, source: str, instrument_info: typing.Dict[str, typing.Any]):
        super().__init__(ui, source, instrument_info)
        self._persistent: typing.Dict[str, typing.Any] = {
            'Fn': None,
            'Fp': None,
        }
        for i in range(9):
            self._persistent[f"Qt{i}"] = None
        self._next_filter: typing.Optional[typing.Dict[str, typing.Any]] = None

    def instrument_message(self, record: str, message: typing.Any) -> bool:
        if record in self._persistent:
            self._persistent[record] = message
            return True
        elif record == 'next':
            self._next_filter = message
            return True
        return super().instrument_message(record, message)

    @staticmethod
    def _countdown_time(t: float) -> str:
        timestamp = format_iso8601_time(t)

        now = time.time()
        seconds = t - now
        if seconds <= 0.1:
            return timestamp + "        "
        seconds = round(seconds)

        if seconds < 1:
            seconds = 1

        minutes = int(seconds / 60)
        hours = int(minutes / 60)
        seconds = seconds % 60
        if hours > 99:
            return timestamp
        if minutes > 99:
            return f"({hours:04d}H) {timestamp}"

        return f"({minutes:02d}:{seconds:02d}) {timestamp}"

    def data_lines(self) -> typing.List[typing.Tuple[str, str]]:
        result: typing.List[typing.Tuple[str, str]] = list()

        for i in range(9):
            try:
                volume = float(self._persistent.get(f"Qt{i}"))
                result.append((f"Volume {i}", f"{volume:.5f}"))
            except (TypeError, ValueError):
                pass

        try:
            result.append(("Active", str(self._persistent.get('Fn'))))
        except (TypeError, ValueError):
            pass

        try:
            result.append(("Sampling", str(self._persistent.get('Fp'))))
        except (TypeError, ValueError):
            pass

        if self._next_filter:
            t = self._next_filter.get('epoch_ms')
            if t:
                t = self._countdown_time(t / 1000.0)
            else:
                t = ""
            result.append((f"Next", t))

        return result

    def _show_menu(self) -> None:
        menu = self.ui.show_menu()

        notifications = self.state.get('notifications')
        in_change = False
        on_last_filter = False
        if notifications:
            if 'carousel_change' in notifications:
                in_change = True
            if 'carousel_complete' in notifications:
                on_last_filter = True

        Fp = self._persistent.get('Fn')
        if Fp is not None and Fp >= 8:
            on_last_filter = True

        if in_change:
            menu.add_entry("End Carousel Change", lambda: self.send_command('end_change'))
        else:
            if not on_last_filter:
                menu.add_entry("Advance Filter", lambda: self.send_command('advance_filter'))
            menu.add_entry("Start Carousel Change", lambda: self.send_command('start_change'))

        if menu.is_empty:
            menu.hide()

    def handle_key(self, key: int) -> bool:
        if key == ord('m') or key == ord('M'):
            self._show_menu()
            return True
        return super().handle_key(key)


def create(ui: UserInterface, source: str,
           instrument_info: typing.Dict[str, typing.Any]) -> typing.Optional[BaseWindow]:
    return InstrumentWindow(ui, source, instrument_info)
