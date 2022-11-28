import typing
from forge.acquisition.console.ui import UserInterface
from ..default.window import InstrumentWindow as BaseWindow
from ..default.data import DataDisplay


class InstrumentWindow(BaseWindow):
    def __init__(self, ui: UserInterface, source: str, instrument_info: typing.Dict[str, typing.Any]):
        super().__init__(ui, source, instrument_info)
        self._persistent: typing.Dict[str, typing.Any] = {
            'Fn': None,
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

    def data_lines(self) -> typing.List[typing.Tuple[str, str]]:
        result: typing.List[typing.Tuple[str, str]] = list()

        self._data_BGR(result, 'Ba')
        self._data_BGR(result, 'Ir')
        self._data_BGR(result, 'Ip')
        self._data_BGR(result, 'If')

        for var in ('Q', 'Vflow', 'Tsample', 'Tcase'):
            result.append((var, DataDisplay.apply_default_format(self.data.get(var))))

        try:
            result.append(("Fn", str(self._persistent.get('Fn'))))
        except (TypeError, ValueError):
            pass

        return result

    def _show_menu(self) -> None:
        menu = self.ui.show_menu()

        notifications = self.state.get('notifications')
        in_filter_change = False
        in_white_filter_change = False
        need_filter_change = False
        on_last_spot = False
        if notifications:
            if 'filter_change' in notifications:
                in_filter_change = True
            if 'white_filter_change' in notifications:
                in_white_filter_change = True
            if 'need_filter_change' in notifications:
                need_filter_change = True
            if 'need_white_filter_change' in notifications:
                need_filter_change = True

        Fn = self._persistent.get('Fn')
        if Fn is not None and Fn >= 8:
            on_last_spot = True

        if in_filter_change or in_white_filter_change:
            menu.add_entry("End Filter Change", lambda: self.send_command('filter_change_end'))
        else:
            if not on_last_spot and not need_filter_change:
                menu.add_entry("Advance Spot", lambda: self.send_command('spot_advance'))
            menu.add_entry("Start Filter Change", lambda: self.send_command('filter_change_start'))

        if not in_white_filter_change:
            menu.add_entry("Start White Filter Change", lambda: self.send_command('white_filter_change'))

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
