import typing
from forge.vis.acquisition import Translator
from forge.vis.acquisition.basic import ParameterDisplay, ParameterSummary, Display, SummaryItem, GenericDisplay
from ..default.acquisition import Acquisition as BaseAcquisition
from ..default.acquisition import display as base_display
from ..default.acquisition import summary as base_summary
from ..cpd3 import Name, acquisition_translator
from ..cpd3 import AcquisitionTranslator as BaseAcquisitionTranslator


def visible(station: str, mode_name: typing.Optional[str] = None) -> bool:
    return True


class Acquisition(BaseAcquisition):
    def __init__(self, mode_name: str = 'acquisition', display_name: str = "Acquisition", **kwargs):
        super().__init__(mode_name, display_name, **kwargs)

        self.display_instrument.append(self.DisplayInstrument(display_type='filtercarousel', match_source='F21'))
        self.display_instrument.append(self.DisplayInstrument(display_type='filtercarousel', match_source='F31'))
        self.display_instrument.append(self.DisplayInstrument(display_type='realtimewind', match_source='XM2'))

        self.summary_instrument.append(self.SummaryInstrument(summary_type='realtimewind', match_source='XM2',
                                                              priority=1000))


_source_display: typing.Dict[str, Display] = {
    'F21': ParameterDisplay('filtercarousel', {'header': 'PMEL Filter Carousel'}),
    'F31': ParameterDisplay('filtercarousel', {'header': 'SCRIPPS Filter Carousel'}),
}

_type_display: typing.Dict[str, Display] = {
    'realtimewind': GenericDisplay('NOAA Windbird', [
        GenericDisplay.Row('Wind speed (m/s)', 'WS', decimals=1),
        GenericDisplay.Row('Wind direction (degrees)', 'WD', decimals=1),
    ]),
}

_type_summary: typing.Dict[str, SummaryItem] = {
    'realtimewind': ParameterSummary('wind', {'instrument': "Tower"}),
}


class AcquisitionTranslator(BaseAcquisitionTranslator):
    def __init__(self):
        super().__init__(interfaces=acquisition_translator.interfaces)

        self._filter_shims: typing.Dict[str, typing.Tuple[str, str]] = {
            'T_V21': ('F21', 'Tsample'),
            'U_V21': ('F21', 'Usample'),
            'T_V22': ('F21', 'Track'),
            'Fn_F21': ('F21', 'Fp'),

            'T_V31': ('F31', 'Tsample'),
            'U_V31': ('F31', 'Usample'),
            'T_V32': ('F31', 'Track'),
            'Fn_F31': ('F31', 'Fp'),
        }
        for i in range(9):
            self._filter_shims['Pd_P2' + str(i)] = ('F21', 'PD' + str(i))
        for i in range(9):
            self._filter_shims['Pd_P3' + str(i)] = ('F31', 'PD' + str(i))

    @staticmethod
    def _create_filter_shim(source: str, field: str, target: typing.Callable[[typing.Any, typing.Any], None]) -> typing.Callable[[typing.Any, typing.Any], None]:
        def shim(result, value):
            result[source][field] = value
            return target(result, value)

        return shim

    def translator_shim(self, name: Name, target: typing.Callable[[typing.Any, typing.Any], None]) -> typing.Callable[[typing.Any, typing.Any], None]:
        filter_shim = self._filter_shims.get(name.variable)
        if filter_shim:
            return self._create_filter_shim(filter_shim[0], filter_shim[1], target)
        return super().translator_shim(name, target)


station_acquisition_translator = AcquisitionTranslator()


class _FilterCarouselInterface(AcquisitionTranslator.Interface):
    def __init__(self):
        variable_map: typing.Dict[AcquisitionTranslator.Variable, str] = {}
        for i in range(9):
            var = 'Qt' + str(i)
            variable_map[AcquisitionTranslator.Variable(var)] = var

        super().__init__('filtercarousel', command_map={
            'start_change': 'StartAccumulateChange',
            'end_change': 'EndAccumulateChange',
            'advance_filter': 'AdvanceAccumulator',
        }, zstate_notifications={
            'Changing': 'carousel_change',
            'BypassedChanging': 'carousel_change',
            'Blank': 'initial_blank',
            'End': 'carousel_complete',
            'BypassedEnd': 'carousel_complete',
        }, variable_map=variable_map)

    def value_translator(self, name: Name) -> typing.Tuple[
            typing.Optional[str], typing.Optional[typing.Callable[[typing.Any], typing.Any]]]:
        if name.variable.startswith('ZNEXT_'):
            def translator(value: typing.Dict[str, typing.Any]) -> typing.Dict[str, typing.Any]:
                if value is None:
                    return {}

                return {
                    'epoch_ms': round(value.get('Time', 0) * 1000.0)
                }

            return 'next', translator
        elif name.variable.startswith('Fn_'):
            def translator(value: typing.Optional[int]) -> typing.Optional[int]:
                if value is None or value < 0:
                    return None
                return value

            return 'Fn', translator
        return super().value_translator(name)


class PMELFilterCarousel(_FilterCarouselInterface):
    def matches(self, interface_name: str, interface_info: typing.Dict[str, typing.Any]) -> bool:
        return interface_name == 'F21'

    def display_information(self, interface_info: typing.Dict[str, typing.Any]) -> typing.Any:
        info = super().display_information(interface_info)
        info['display_letter'] = 'F'
        return info


class SCRIPPSFilterCarousel(_FilterCarouselInterface):
    def matches(self, interface_name: str, interface_info: typing.Dict[str, typing.Any]) -> bool:
        return interface_name == 'F31'

    def display_information(self, interface_info: typing.Dict[str, typing.Any]) -> typing.Any:
        info = super().display_information(interface_info)
        info['display_letter'] = 'S'
        return info


station_acquisition_translator.interfaces.append(PMELFilterCarousel())
station_acquisition_translator.interfaces.append(SCRIPPSFilterCarousel())


class RealtimeWindInterface(AcquisitionTranslator.Interface):
    def __init__(self):
        super().__init__('realtimewind', variable_map={
            AcquisitionTranslator.Variable('WS'): 'WS',
            AcquisitionTranslator.Variable('WD'): 'WD',
        })

    def matches(self, interface_name: str, interface_info: typing.Dict[str, typing.Any]) -> bool:
        return interface_name == 'XM2'


station_acquisition_translator.interfaces.append(RealtimeWindInterface())


def display(station: str, display_type: str, source: typing.Optional[str]) -> typing.Optional[Display]:
    d = _source_display.get(source)
    if d:
        return d
    d = _type_display.get(display_type)
    if d:
        return d
    return base_display(station, display_type, source)


def summary(station: str, summary_type: str, source: typing.Optional[str]) -> typing.Optional[SummaryItem]:
    s = _type_summary.get(summary_type)
    if s:
        return s
    return base_summary(station, summary_type, source)


def translator(station: str) -> typing.Optional[Translator]:
    return station_acquisition_translator
