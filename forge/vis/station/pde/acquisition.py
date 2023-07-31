import typing
from forge.vis.acquisition import Translator
from forge.vis.acquisition.basic import GenericDisplay, Display
from ..default.acquisition import Acquisition as BaseAcquisition
from ..default.acquisition import display as base_display
from ..cpd3 import AcquisitionTranslator, acquisition_translator


class Acquisition(BaseAcquisition):
    def __init__(self, mode_name: str = 'acquisition', display_name: str = "Acquisition", **kwargs):
        super().__init__(mode_name, display_name, **kwargs)

        self.display_instrument.append(self.DisplayInstrument(display_type='pwrgate', match_source='XPW'))
        self.display_instrument.append(self.DisplayInstrument(display_type='rpicpu', match_source='XPI'))


_type_display: typing.Dict[str, Display] = {
    'pwrgate': GenericDisplay('Epic PWRgate', [
        GenericDisplay.Row('Charger Current (A)', 'Acharger', decimals=2),
        GenericDisplay.Row('Supply Voltage (V)', 'Vsupply', decimals=2),
        GenericDisplay.Row('Battery Voltage (V)', 'Vbattery', decimals=2),
        GenericDisplay.Row('Solar Voltage (V)', 'Vsolar', decimals=2),
    ]),
    'rpicpu': GenericDisplay('Raspberry Pi CPU', [
        GenericDisplay.Row('Temperature (°C)', 'Tcpu', decimals=1),
    ]),
}


station_acquisition_translator = acquisition_translator.detach()


class PWRgateInterface(AcquisitionTranslator.Interface):
    def __init__(self):
        super().__init__('pwrgate', variable_map={
            AcquisitionTranslator.Variable('A'): 'Acharger',
            AcquisitionTranslator.Variable('V1'): 'Vsupply',
            AcquisitionTranslator.Variable('V2'): 'Vbattery',
            AcquisitionTranslator.Variable('V3'): 'Vsolar',
        })

    def matches(self, interface_name: str, interface_info: typing.Dict[str, typing.Any]) -> bool:
        return interface_name == 'XPW'


station_acquisition_translator.interfaces.append(PWRgateInterface())


class RPICPUInterface(AcquisitionTranslator.Interface):
    def __init__(self):
        super().__init__('rpicpu', variable_map={
            AcquisitionTranslator.Variable('T'): 'Tcpu',
        })

    def matches(self, interface_name: str, interface_info: typing.Dict[str, typing.Any]) -> bool:
        return interface_name == 'XPI'


station_acquisition_translator.interfaces.append(PWRgateInterface())


def visible(station: str, mode_name: typing.Optional[str] = None) -> bool:
    return True


def display(station: str, display_type: str, source: typing.Optional[str]) -> typing.Optional[Display]:
    d = _type_display.get(display_type)
    if d:
        return d
    return base_display(station, display_type, source)


def translator(station: str) -> typing.Optional[Translator]:
    return station_acquisition_translator
