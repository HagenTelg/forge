import typing
from forge.acquisition.console.ui import UserInterface
from forge.acquisition.instrument.admagic250cpc.parameters import Parameters as InstrumentParameters
from ..default.window import InstrumentWindow as BaseWindow


class InstrumentWindow(BaseWindow):
    def __init__(self, ui: UserInterface, source: str, instrument_info: typing.Dict[str, typing.Any]):
        super().__init__(ui, source, instrument_info)
        self._persistent: typing.Dict[str, typing.Any] = {
            'parameters': None,
        }

    def instrument_message(self, record: str, message: typing.Any) -> bool:
        if record in self._persistent:
            self._persistent[record] = message
            return True
        return super().instrument_message(record, message)

    def _show_parameter_set(self) -> None:
        dialog = self.ui.show_dialog()
        dialog.title = "Set MAGIC 250 Parameters"

        output_data: typing.Dict[str, typing.Callable[[], typing.Any]] = dict()
        parameter_values = self._persistent.get("parameters", dict())

        def integer_parameter(name: str, minimum: int = None, maximum: int = None):
            param = dialog.integer(name + ": ", minimum, maximum)
            value = parameter_values.get(name)
            if value is not None:
                param.text = str(value)
            output_data[name] = lambda: param.value

        for name in InstrumentParameters.INTEGER_PARAMETERS:
            integer_parameter(name)

        def float_parameter(name: str, minimum: float = None, maximum: float = None):
            param = dialog.float(name + ": ", minimum, maximum)
            value = parameter_values.get(name)
            if value is not None:
                param.text = str(value)
            output_data[name] = lambda: param.value

        for name in InstrumentParameters.FLOAT_PARAMETERS:
            float_parameter(name)

        def temperature(name: str):
            original = parameter_values.get(name, dict())

            setpoint = dialog.float(name + ": ")
            value = original.get("setpoint")
            if value is not None:
                setpoint.text = "%.1f" % value

            absolute = dialog.checkbox(name + " Absolute")
            value = original.get("mode")
            if value and value == "A":
                absolute.value = True

            output_data[name] = lambda: {"setpoint": setpoint.value, "mode": "A" if absolute.value else "R"}

        for name in InstrumentParameters.TEMPERATURE_PARAMETERS:
            temperature(name)

        def send_set():
            data: typing.Dict[str, typing.Any] = dict()
            for name, fetch in output_data.items():
                value = fetch()
                if value is None:
                    continue
                if value == parameter_values.get(name):
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

        menu.add_entry("Set Parameters", self._show_parameter_set)
        menu.add_entry("Save Settings", lambda: self.send_command('save_settings'))

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
