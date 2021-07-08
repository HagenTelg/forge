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
        G0.legend = "Scattering (Coarse)"
        G0.data_record = f'{mode}-scattering-coarse'
        G0.data_field = 'G'
        G0.color = '#0f0'
        scattering.traces.append(G0)
        self.processing[G0.data_record] = self.AdjustWavelength(OrderedDict([
            ('BsB', 450), ('BsG', 550), ('BsR', 700),
        ]))

        G1 = TimeSeries.Trace(Mm_1)
        G1.legend = "Scattering (Fine)"
        G1.data_record = f'{mode}-scattering-fine'
        G1.data_field = 'G'
        G1.color = '#070'
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

        G0 = TimeSeries.Trace(Mm_1)
        G0.legend = "Second CLAP (Coarse)"
        G0.data_record = f'{mode}-clap2-coarse'
        G0.data_field = 'G'
        absorption.traces.append(G0)
        self.processing[G0.data_record] = self.AdjustWavelength(OrderedDict([
            ('BaB', 467), ('BaG', 528), ('BaR', 652),
        ]))

        G1 = TimeSeries.Trace(Mm_1)
        G1.legend = "Second CLAP (Fine)"
        G1.data_record = f'{mode}-clap2-fine'
        G1.data_field = 'G'
        absorption.traces.append(G1)
        self.processing[G1.data_record] = self.AdjustWavelength(OrderedDict([
            ('BaB', 467), ('BaG', 528), ('BaR', 652),
        ]))

        G0 = TimeSeries.Trace(Mm_1)
        G0.legend = "PSAP (Coarse)"
        G0.data_record = f'{mode}-psap-coarse'
        G0.data_field = 'G'
        absorption.traces.append(G0)
        self.processing[G0.data_record] = self.AdjustWavelength(OrderedDict([
            ('BaB', 467), ('BaG', 530), ('BaR', 660),
        ]))

        G1 = TimeSeries.Trace(Mm_1)
        G1.legend = "PSAP (Fine)"
        G1.data_record = f'{mode}-psap-fine'
        G1.data_field = 'G'
        absorption.traces.append(G1)
        self.processing[G1.data_record] = self.AdjustWavelength(OrderedDict([
            ('BaB', 467), ('BaG', 530), ('BaR', 660),
        ]))

        aethalometer = TimeSeries.Trace(Mm_1)
        aethalometer.legend = "AE31"
        aethalometer.data_record = f'{mode}-ae31'
        aethalometer.data_field = 'G'
        absorption.traces.append(aethalometer)
        input_fields = OrderedDict()
        for index in range(len(AethalometerOptical.SevenWavelength.WAVELENGTH_NM)):
            input_fields[f'Ba{index + 1}'] = AethalometerOptical.SevenWavelength.WAVELENGTH_NM[index]
        self.processing[aethalometer.data_record] = self.AdjustWavelength(input_fields)

        aethalometer = TimeSeries.Trace(Mm_1)
        aethalometer.legend = "AE33"
        aethalometer.data_record = f'{mode}-ae33'
        aethalometer.data_field = 'G'
        absorption.traces.append(aethalometer)
        input_fields = OrderedDict()
        for index in range(len(AethalometerOptical.SevenWavelength.WAVELENGTH_NM)):
            input_fields[f'Ba{index + 1}'] = AethalometerOptical.SevenWavelength.WAVELENGTH_NM[index]
        self.processing[aethalometer.data_record] = self.AdjustWavelength(input_fields)
