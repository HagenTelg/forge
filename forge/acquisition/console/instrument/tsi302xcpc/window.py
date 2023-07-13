import typing
from forge.acquisition.console.ui import UserInterface
from ..default.window import InstrumentWindow as BaseWindow


class InstrumentWindow(BaseWindow):
    def _show_menu(self) -> None:
        menu = self.ui.show_menu()
        menu.add_entry("Execute Fill", lambda: self.send_command('fill'))

    def handle_key(self, key: int) -> bool:
        if key == ord('m') or key == ord('M'):
            self._show_menu()
            return True
        return super().handle_key(key)


def create(ui: UserInterface, source: str,
           instrument_info: typing.Dict[str, typing.Any]) -> typing.Optional[BaseWindow]:
    return InstrumentWindow(ui, source, instrument_info)
