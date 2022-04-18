import typing
from forge.vis.acquisition import Translator
from forge.vis import CONFIGURATION
from forge.vis.acquisition import SummaryItem, Display
from forge.vis.acquisition.basic import BasicDisplay, BasicSummary, ParameterSummary, ParameterDisplay
from forge.vis.mode.acquisition import Acquisition as BaseAcquisition


class Acquisition(BaseAcquisition):
    class SummaryInstrumentSimple(BaseAcquisition.SummaryInstrument):
        def __init__(self, summary_type: str, priority: typing.Optional[int] = 0):
            super().__init__(summary_type, match_type=summary_type, priority=priority)

    class DisplayInstrumentSimple(BaseAcquisition.DisplayInstrument):
        def __init__(self, display_type: str):
            super().__init__(display_type, match_type=display_type)

    def __init__(self, mode_name: str = 'acquisition', display_name: str = "Acquisition", **kwargs):
        super().__init__(mode_name, display_name, **kwargs)

        self.display_instrument.append(self.DisplayInstrumentSimple('tsi3760cpc'))
        self.display_instrument.append(self.DisplayInstrumentSimple('tsi377xcpc'))
        self.display_instrument.append(self.DisplayInstrumentSimple('tsi3010cpc'))
        self.display_instrument.append(self.DisplayInstrumentSimple('admagiccpc'))
        self.display_instrument.append(self.DisplayInstrumentSimple('bmi1710cpc'))
        self.display_instrument.append(self.DisplayInstrumentSimple('bmi1720cpc'))
        self.display_instrument.append(self.DisplayInstrumentSimple('clap'))
        self.display_instrument.append(self.DisplayInstrumentSimple('tap'))
        self.display_instrument.append(self.DisplayInstrumentSimple('mageeae33'))
        self.display_instrument.append(self.DisplayInstrumentSimple('teledynet640'))
        self.display_instrument.append(self.DisplayInstrumentSimple('tsi3563nephelometer'))
        self.display_instrument.append(self.DisplayInstrumentSimple('ecotechnephelometer'))
        self.display_instrument.append(self.DisplayInstrumentSimple('thermo49'))
        self.display_instrument.append(self.DisplayInstrumentSimple('csdpops'))
        self.display_instrument.append(self.DisplayInstrumentSimple('lovepid'))
        self.display_instrument.append(self.DisplayInstrumentSimple('purpleair'))
        self.display_instrument.append(self.DisplayInstrumentSimple('tsimfm'))
        self.display_instrument.append(self.DisplayInstrumentSimple('vaisalapwdx2'))
        self.display_instrument.append(self.DisplayInstrumentSimple('vaisalawmt700'))
        self.display_instrument.append(self.DisplayInstrumentSimple('vaisalawxt5xx'))
        self.display_instrument.append(self.DisplayInstrumentSimple('azonixumac1050'))
        self.display_instrument.append(self.DisplayInstrumentSimple('campbellcr1000gmd'))

        self.display_static.append(BaseAcquisition.DisplayStatic('spancheck'))


        self.summary_instrument.append(self.SummaryInstrumentSimple('tsi3760cpc', priority=4000))
        self.summary_instrument.append(self.SummaryInstrumentSimple('tsi377xcpc', priority=4000))
        self.summary_instrument.append(self.SummaryInstrumentSimple('tsi3010cpc', priority=4000))
        self.summary_instrument.append(self.SummaryInstrumentSimple('admagiccpc', priority=4000))

        self.summary_instrument.append(self.SummaryInstrumentSimple('clap', priority=3000))
        self.summary_instrument.append(self.SummaryInstrumentSimple('tap', priority=3000))

        self.summary_instrument.append(self.SummaryInstrumentSimple('tsi3563nephelometer', priority=2000))

        self.summary_instrument.append(self.SummaryInstrumentSimple('vaisalawmt700', priority=1000))
        self.summary_instrument.append(self.SummaryInstrumentSimple('vaisalawxt5xx', priority=1000))

        self.summary_static.append(BaseAcquisition.SummaryStatic(summary_type='pitot_flow', priority=-2000))

        self.summary_instrument.append(self.SummaryInstrumentSimple('impactor_cycle', priority=-1000))


def visible(station: str, mode_name: typing.Optional[str] = None) -> bool:
    enable = CONFIGURATION.get('ACQUISITION.VISIBLE', False)
    if isinstance(enable, bool):
        return enable
    return station in enable


_default_display = BasicDisplay()
_type_display: typing.Dict[str, Display] = {
    'tap': ParameterDisplay('clap', {'instrument': "BMI TAP"}),
    'bmi1710cpc': ParameterDisplay('bmi17x0cpc', {'instrument': "BMI 1710"}),
    'bmi1720cpc': ParameterDisplay('bmi17x0cpc', {'instrument': "BMI 1720"}),
}

_default_summary = BasicSummary()
_type_summary: typing.Dict[str, SummaryItem] = {
    'tsi3760cpc': ParameterSummary('cpc', {'instrument': "TSI 3760"}),
    'tsi377xcpc': ParameterSummary('cpc', {'instrument': "TSI 377x"}),
    'tsi3010cpc': ParameterSummary('cpc', {'instrument': "TSI 3010"}),
    'admagiccpc': ParameterSummary('cpc', {'instrument': "Magic"}),
    'tap': ParameterSummary('clap', {'instrument': "BMI TAP"}),
    'tsi3563nephelometer': ParameterSummary('nephelometer', {'instrument': "TSI 3563"}),
    'ecotechnephelometer': ParameterSummary('nephelometer', {
        'instrument': "Ecotech Aurora",
        'blue': 450,
        'green': 525,
        'red': 635,
    }),
    'vaisalawmt700': ParameterSummary('wind', {'instrument': "Vaisala WMT 700"}),
    'vaisalawxt5xx': ParameterSummary('wind', {'instrument': "Vaisala WXT"}),
}


def display(station: str, display_type: str, source: typing.Optional[str]) -> typing.Optional[Display]:
    return _type_display.get(display_type, _default_display)


def summary(station: str, summary_type: str, source: typing.Optional[str]) -> typing.Optional[SummaryItem]:
    return _type_summary.get(summary_type, _default_summary)


def translator(station: str) -> typing.Optional[Translator]:
    from forge.vis.station.cpd3 import acquisition_translator
    return acquisition_translator
