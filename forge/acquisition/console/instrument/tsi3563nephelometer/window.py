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
            'modestring': None,
            'spancheck_result': None,
            'parameters': None,
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
            try:
                result.append(("MODE", str(self._persistent.get('modestring'))))
            except (TypeError, ValueError):
                pass
            try:
                result.append(("REMAINING", str(int(self.data.get('modetime')))))
            except (TypeError, ValueError):
                pass
            for var in ('Psample', 'Tsample', 'Usample', 'Tinlet', 'Uinlet', 'Vl', 'Al'):
                result.append((var, DataDisplay.apply_default_format(self.data.get(var))))
        elif self._page == self._Page.COUNTS:
            self._data_BGR(result, 'Cs')
            self._data_BGR(result, 'Cbs')
            self._data_BGR(result, 'Cd')
            self._data_BGR(result, 'Cbd')
            self._data_BGR(result, 'Cf')
            try:
                result.append(("MODE", str(self._persistent.get('modestring'))))
            except (TypeError, ValueError):
                pass
            try:
                result.append(("REMAINING", str(int(self.data.get('modetime')))))
            except (TypeError, ValueError):
                pass
            for var in ('Psample', 'Tsample', 'Usample', 'Tinlet', 'Uinlet', 'Vl', 'Al'):
                result.append((var, DataDisplay.apply_default_format(self.data.get(var))))
        else:
            self._add_spancheck_BGR(result, "percent_error", "total")
            self._add_spancheck_BGR(result, "percent_error", "back")
            self._add_spancheck_BGR(result, "sensitivity_factor", "total")
            self._add_spancheck_BGR(result, "sensitivity_factor", "back")
            self._add_spancheck_BGR(result, "calibration", "K2")
            self._add_spancheck_BGR(result, "calibration", "K4")
        return result

    def _show_parameter_set(self) -> None:
        dialog = self.ui.show_dialog()
        dialog.title = "Set Nephelometer Parameters"

        output_data: typing.Dict[str, typing.Callable[[], typing.Any]] = dict()
        parameter_values = self._persistent.get("parameters", dict())

        def integer_parameter(name: str, minimum: int = None, maximum: int = None):
            param = dialog.integer(name + ": ", minimum, maximum)
            value = parameter_values.get(name)
            if value is not None:
                param.text = str(value)
            output_data[name] = lambda: param.value

        integer_parameter("SMZ", 0, 24)
        integer_parameter("SP", 0, 150)
        integer_parameter("STA", 1, 9960)
        integer_parameter("STB", 15, 999)
        integer_parameter("STP", 10, 32767)
        integer_parameter("STZ", 1, 9999)
        integer_parameter("SVB", 800, 1200)
        integer_parameter("SVG", 800, 1200)
        integer_parameter("SVR", 800, 1200)
        integer_parameter("B", 0, 255)

        def boolean_parameter(name: str):
            param = dialog.checkbox(name)
            value = parameter_values.get(name)
            if value is not None:
                param.value = bool(value)
            output_data[name] = lambda: param.value

        boolean_parameter("SMB")
        boolean_parameter("H")

        def calibration(name: str):
            original = parameter_values.get(name, dict())

            K2 = dialog.float(name + " K2: ", 0.0)
            value = original.get("K2")
            if value:
                K2.text = "%.3e" % value

            K4 = dialog.float(name + " K4: ", 0.0, 1.0)
            value = original.get("K4")
            if value:
                K4.text = "%.3f" % value

            output_data[name] = lambda: {"K2": K2.value, "K4": K4.value}

        calibration("SKB")
        calibration("SKG")
        calibration("SKR")

        def send_set():
            data: typing.Dict[str, typing.Any] = dict()
            for name, fetch in output_data.items():
                value = fetch()
                if value is None:
                    continue
                data[name] = value

            if not data:
                return
            self.send_command('set_parameters', data)

        confirm = dialog.yes_no(on_yes=send_set)
        confirm.yes_text = "APPLY"
        confirm.no_text = "CANCEL"

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
        if in_run_mode:
            menu.add_entry("Set Parameters", self._show_parameter_set)

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
