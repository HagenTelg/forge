import typing
import time
import enum
from forge.acquisition.console.ui import UserInterface
from ..default.window import InstrumentWindow as BaseWindow
from ..default.data import DataDisplay


class InstrumentWindow(BaseWindow):
    class _Page(enum.Enum):
        DATA = enum.auto(),
        COUNTS = enum.auto()
        SPANCHECK = enum.auto()

    def __init__(self, ui: UserInterface, source: str, instrument_info: typing.Dict[str, typing.Any]):
        super().__init__(ui, source, instrument_info)
        self._page = self._Page.DATA
        self._persistent: typing.Dict[str, typing.Any] = {
            'spancheck_result': None,
            'BswB': None, 'BswG': None, 'BswR': None,
            'BbswB': None, 'BbswG': None, 'BbswR': None,
            'BswdB': None, 'BswdG': None, 'BswdR': None,
            'BbswdB': None, 'BbswdG': None, 'BbswdR': None,
        }

    def instrument_message(self, record: str, message: typing.Any) -> bool:
        if record in self._persistent:
            self._persistent[record] = message
            return True
        return super().instrument_message(record, message)

    def _data_BGR(self, lines: typing.List[typing.Tuple[str, str]], root: str) -> None:
        for wl in ('B', 'G', 'R'):
            lines.append((root + wl, DataDisplay.apply_default_format(self.data.get(root + wl))))

    def _persistent_BGR(self, lines: typing.List[typing.Tuple[str, str]], root: str) -> None:
        for wl in ('B', 'G', 'R'):
            lines.append((root + wl, DataDisplay.apply_default_format(self._persistent.get(root + wl))))

    def _add_spancheck(self, lines: typing.List[typing.Tuple[str, str]], *path: str) -> None:
        value = self._persistent.get('spancheck_result')
        for p in path:
            if not isinstance(value, dict):
                lines.append((".".join(path), ""))
                return
            value = value.get(p)
        lines.append((".".join(path), DataDisplay.apply_default_format(value)))

    def _add_spancheck_BGR(self, lines: typing.List[typing.Tuple[str, str]], *path: str) -> None:
        for wl in ('B', 'G', 'R'):
            self._add_spancheck(lines, *path, wl)

    def data_lines(self) -> typing.List[typing.Tuple[str, str]]:
        result: typing.List[typing.Tuple[str, str]] = list()
        if self._page == self._Page.DATA:
            self._data_BGR(result, 'Bs')
            self._data_BGR(result, 'Bbs')
            self._persistent_BGR(result, 'Bsw')
            self._persistent_BGR(result, 'Bbsw')
            self._persistent_BGR(result, 'Bswd')
            self._persistent_BGR(result, 'Bbswd')
            for var in ('Psample', 'Tsample', 'Usample', 'Tcell', 'Cd'):
                result.append((var, DataDisplay.apply_default_format(self.data.get(var))))
        elif self._page == self._Page.COUNTS:
            self._data_BGR(result, 'Cs')
            self._data_BGR(result, 'Cbs')
            self._data_BGR(result, 'Cf')
            for var in ('Psample', 'Tsample', 'Usample', 'Tcell', 'Cd'):
                result.append((var, DataDisplay.apply_default_format(self.data.get(var))))
        else:
            self._add_spancheck_BGR(result, "percent_error", "total")
            self._add_spancheck_BGR(result, "percent_error", "back")
            self._add_spancheck_BGR(result, "sensitivity_factor", "total")
            self._add_spancheck_BGR(result, "sensitivity_factor", "back")
            self._add_spancheck_BGR(result, "calibration", "C")
            self._add_spancheck_BGR(result, "calibration", "M")
        return result

    def _show_menu(self) -> None:
        menu = self.ui.show_menu()

        notifications = self.state.get('notifications')
        in_run_mode = True
        in_spancheck = False
        if notifications:
            if 'zero' in notifications:
                in_run_mode = False
            elif 'blank' in notifications:
                in_run_mode = False
            elif 'spancheck' in notifications:
                in_run_mode = False

            if 'spancheck' in notifications:
                in_spancheck = False

        if in_run_mode:
            menu.add_entry("Start Zero", lambda: self.send_command('start_zero'))
            menu.add_entry("Start Spancheck", lambda: self.send_command('start_spancheck'))
        if in_spancheck:
            menu.add_entry("Stop Spancheck", lambda: self.send_command('stop_spancheck'))

        def have_spancheck_result() -> bool:
            value = self._persistent.get('spancheck_result')
            if not value:
                return False
            value = value.get('calibration')
            if not value:
                return False
            return value.get('M')

        if have_spancheck_result():
            menu.add_entry("Apply Spancheck Calibration", lambda: self.send_command('apply_spancheck_calibration'))

        menu.add_entry("Reboot Instrument", lambda: self.send_command('reboot'))

        if menu.is_empty:
            menu.hide()

    def handle_key(self, key: int) -> bool:
        if key == ord('d') or key == ord('D'):
            if self._page == self._Page.DATA:
                self._page = self._Page.COUNTS
            elif self._page == self._Page.COUNTS:
                self._page = self._Page.SPANCHECK
            else:
                self._page = self._Page.DATA
            return True
        elif key == ord('m') or key == ord('M'):
            self._show_menu()
            return True
        return super().handle_key(key)


def create(ui: UserInterface, source: str,
           instrument_info: typing.Dict[str, typing.Any]) -> typing.Optional[BaseWindow]:
    return InstrumentWindow(ui, source, instrument_info)
