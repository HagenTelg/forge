import typing
import time
from math import nan, isfinite
from forge.acquisition.console.ui import UserInterface
from ..default.window import InstrumentWindow as BaseWindow
from ..default.data import DataDisplay


class InstrumentWindow(BaseWindow):

    def __init__(self, ui: UserInterface, source: str, instrument_info: typing.Dict[str, typing.Any]):
        super().__init__(ui, source, instrument_info)
        self._persistent: typing.Dict[str, typing.Any] = {
            'setpoint': None,
        }

    def instrument_message(self, record: str, message: typing.Any) -> bool:
        if record in self._persistent:
            self._persistent[record] = message
            return True
        return super().instrument_message(record, message)

    def data_lines(self) -> typing.List[typing.Tuple[str, str]]:
        result: typing.List[typing.Tuple[str, str]] = list()

        channel_address = self.instrument_info.get('address')
        if not isinstance(channel_address, list):
            channel_address = list()
        channel_names = self.instrument_info.get('variable')
        if not isinstance(channel_names, list):
            channel_names = list()
        channel_values = self.data.get('value')
        if not isinstance(channel_values, list):
            channel_values = list()
        channel_raw = self.data.get('raw')
        if not isinstance(channel_raw, list):
            channel_raw = list()
        channel_setpoint = self._persistent.get('setpoint')
        if not isinstance(channel_setpoint, list):
            channel_setpoint = list()

        total_channels = max(len(channel_names), len(channel_values), len(channel_raw))
        for i in range(total_channels):
            address = ""
            if i < len(channel_address):
                address = channel_address[i]
                address = int(address)
                address = "%X" % address
            if not address:
                address = "%d" % i

            name = ""
            if i < len(channel_names):
                name = channel_names[i]

            value = nan
            if i < len(channel_values):
                value = channel_values[i]
            raw = nan
            if i < len(channel_raw):
                raw = channel_raw[i]
            setpoint = nan
            if i < len(channel_setpoint):
                setpoint = channel_setpoint[i]

            result.append((f"{address} {name}", DataDisplay.apply_default_format(value) +
                           " " + DataDisplay.apply_default_format(setpoint) +
                           " " + DataDisplay.apply_default_format(raw)))

        return result

    def _show_change_setpoint(self, name: str, index: int) -> None:
        dialog = self.ui.show_dialog()
        dialog.title = "Change Setpoint"

        setpoint = dialog.float(name + ": ")
        value = self._persistent.get('setpoint')
        if not isinstance(value, list):
            value = list()
        if index < len(value) and isfinite(value[index]):
            setpoint.text = f"{value[index]:.3f}"

        channel_address = self.instrument_info.get('address')
        if not isinstance(channel_address, list):
            channel_address = list()
        if index < len(channel_address):
            index = int(channel_address[index])

        def send_set():
            value = setpoint.value
            if value is None or not isfinite(value):
                return
            self.send_command('set_analog_channel', {
                'channel': index,
                'value': value,
            })

        confirm = dialog.yes_no(on_yes=send_set)
        confirm.yes_text = "APPLY"
        confirm.no_text = "CANCEL"

    def _show_menu(self) -> None:
        menu = self.ui.show_menu()

        analog_output_names = self.instrument_info.get('variable')
        if not isinstance(analog_output_names, list):
            analog_output_names = list()
        addresses = self.instrument_info.get('address')
        if not isinstance(addresses, list):
            addresses = list()
        while len(analog_output_names) < len(addresses):
            analog_output_names.append("")
        for i in range(len(addresses)):
            if analog_output_names[i]:
                continue
            analog_output_names[i] = "%X" % (int(addresses[i]))

        def change_setpoint_action(index: int):
            if index >= len(analog_output_names):
                return None
            name = analog_output_names[index]
            if not name:
                return None
            def act():
                self._show_change_setpoint(name, index)
            return act

        for i in range(len(analog_output_names)):
            act = change_setpoint_action(i)
            if act is None:
                continue
            menu.add_entry("Set " + analog_output_names[i], act)

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
