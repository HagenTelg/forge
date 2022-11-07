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
            'output': None,
            'digital': None,
        }

    def instrument_message(self, record: str, message: typing.Any) -> bool:
        if record in self._persistent:
            self._persistent[record] = message
            return True
        return super().instrument_message(record, message)

    def data_lines(self) -> typing.List[typing.Tuple[str, str]]:
        result: typing.List[typing.Tuple[str, str]] = list()
        for var in ('T', 'V'):
            result.append((var, DataDisplay.apply_default_format(self.data.get(var))))

        digital = self._persistent.get('digital') or 0
        result.append(('Digital', f"{digital:04X}"))

        channel_names = self.instrument_info.get('variable')
        if not isinstance(channel_names, list):
            channel_names = list()
        channel_values = self.data.get('value')
        if not isinstance(channel_values, list):
            channel_values = list()
        channel_raw = self.data.get('raw')
        if not isinstance(channel_raw, list):
            channel_raw = list()

        total_channels = max(len(channel_names), len(channel_values), len(channel_raw))
        for i in range(total_channels):
            name = ""
            if i < len(channel_names):
                name = channel_names[i]
            value = nan
            if i < len(channel_values):
                value = channel_values[i]
            raw = nan
            if i < len(channel_raw):
                raw = channel_raw[i]

            result.append((f"{i:2d} {name}", DataDisplay.apply_default_format(value) +
                           " " + DataDisplay.apply_default_format(raw) + "V"))

        return result

    def _show_set_digital(self) -> None:
        dialog = self.ui.show_dialog()
        dialog.title = "Set Digital Output"

        digital_bits = dialog.integer("Bits: ", 0, 0xFFFF, base=16)
        value = self._persistent.get('digital')
        if value is None:
            value = 0
        digital_bits.text = f"{value:04X}"

        def send_set():
            value = digital_bits.value
            if value is None:
                return
            self.send_command('set_digital_output', value)

        confirm = dialog.yes_no(on_yes=send_set)
        confirm.yes_text = "APPLY"
        confirm.no_text = "CANCEL"

    def _show_analog_digital(self, name: str, index: int) -> None:
        dialog = self.ui.show_dialog()
        dialog.title = "Set Analog Output"

        analog_voltage = dialog.float(name + ": ", 0.0, 5.0)
        value = self._persistent.get('output')
        if not isinstance(value, list):
            value = list()
        if index < len(value) and isfinite(value[index]):
            analog_voltage.text = f"{value[index]:.3f}"

        def send_set():
            value = analog_voltage.value
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

        menu.add_entry("Set Digital Output", self._show_set_digital)

        analog_output_names = self.instrument_info.get('output')
        if not isinstance(analog_output_names, list):
            analog_output_names = list()

        def set_analog_action(index: int):
            if index >= len(analog_output_names):
                return None
            name = analog_output_names[index]
            if not name:
                return None
            def act():
                self._show_analog_digital(name, index)
            return act

        for i in range(len(analog_output_names)):
            act = set_analog_action(i)
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
