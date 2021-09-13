import typing
from collections import OrderedDict
from forge.vis.view.timeseries import TimeSeries
from ..default.aerosol.green import Green as GreenBase
from ..default.aerosol.green import AethalometerOptical


class Green(TimeSeries):
    AdjustWavelength = GreenBase.AdjustWavelength

    def __init__(self, mode: str):
        super().__init__()
        self.title = "Optical Properties Adjusted to 550nm"

        scattering = TimeSeries.Graph()
        scattering.title = "Light Scattering"
        scattering.contamination = f'{mode}-contamination'
        self.graphs.append(scattering)

        Mm_1 = TimeSeries.Axis()
        Mm_1.title = "Mm⁻¹"
        Mm_1.format_code = '.2f'
        scattering.axes.append(Mm_1)

        G0 = TimeSeries.Trace(Mm_1)
        G0.legend = "Dry Scattering (Coarse)"
        G0.data_record = f'{mode}-scattering-coarse'
        G0.data_field = 'G'
        G0.color = '#0f0'
        scattering.traces.append(G0)
        self.processing[G0.data_record] = self.AdjustWavelength(OrderedDict([
            ('BsB', 450), ('BsG', 550), ('BsR', 700),
        ]))

        G1 = TimeSeries.Trace(Mm_1)
        G1.legend = "Dry Scattering (Fine)"
        G1.data_record = f'{mode}-scattering-fine'
        G1.data_field = 'G'
        G1.color = '#070'
        scattering.traces.append(G1)
        self.processing[G1.data_record] = self.AdjustWavelength(OrderedDict([
            ('BsB', 450), ('BsG', 550), ('BsR', 700),
        ]))

        G0 = TimeSeries.Trace(Mm_1)
        G0.legend = "Wet Scattering (Coarse)"
        G0.data_record = f'{mode}-scattering2-coarse'
        G0.data_field = 'G'
        G0.color = '#f00'
        scattering.traces.append(G0)
        self.processing[G0.data_record] = self.AdjustWavelength(OrderedDict([
            ('BsB', 450), ('BsG', 550), ('BsR', 700),
        ]))

        G1 = TimeSeries.Trace(Mm_1)
        G1.legend = "Wet Scattering (Fine)"
        G1.data_record = f'{mode}-scattering2-fine'
        G1.data_field = 'G'
        G1.color = '#700'
        scattering.traces.append(G1)
        self.processing[G1.data_record] = self.AdjustWavelength(OrderedDict([
            ('BsB', 450), ('BsG', 550), ('BsR', 700),
        ]))

        absorption = TimeSeries.Graph()
        absorption.title = "Light Absorption"
        absorption.contamination = f'{mode}-contamination'
        self.graphs.append(absorption)

        Mm_1 = TimeSeries.Axis()
        Mm_1.title = "Mm⁻¹"
        Mm_1.format_code = '.2f'
        absorption.axes.append(Mm_1)

        G0 = TimeSeries.Trace(Mm_1)
        G0.legend = "CLAP (Coarse)"
        G0.data_record = f'{mode}-absorption-coarse'
        G0.data_field = 'G'
        G0.color = '#0f0'
        absorption.traces.append(G0)
        self.processing[G0.data_record] = self.AdjustWavelength(OrderedDict([
            ('BaB', 467), ('BaG', 528), ('BaR', 652),
        ]))

        G1 = TimeSeries.Trace(Mm_1)
        G1.legend = "CLAP (Fine)"
        G1.data_record = f'{mode}-absorption-fine'
        G1.data_field = 'G'
        G1.color = '#070'
        absorption.traces.append(G1)
        self.processing[G1.data_record] = self.AdjustWavelength(OrderedDict([
            ('BaB', 467), ('BaG', 528), ('BaR', 652),
        ]))
