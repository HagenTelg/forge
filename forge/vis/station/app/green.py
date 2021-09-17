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

        for size in [("Whole", 'whole', '#0f0'), ("PM10", 'pm10', '#0f0'),
                     ("PM2.5", 'pm25', '#070'), ("PM1", 'pm1', '#070')]:
            trace = TimeSeries.Trace(Mm_1)
            trace.legend = f"Dry Scattering ({size[0]})"
            trace.data_record = f'{mode}-scattering-{size[1]}'
            trace.data_field = 'G'
            trace.color = size[2]
            scattering.traces.append(trace)
            self.processing[trace.data_record] = self.AdjustWavelength(OrderedDict([
                ('BsB', 450), ('BsG', 550), ('BsR', 700),
            ]))

        for size in [("Whole", 'whole', '#0f0'), ("PM10", 'pm10', '#0f0'),
                     ("PM2.5", 'pm25', '#070'), ("PM1", 'pm1', '#070')]:
            trace = TimeSeries.Trace(Mm_1)
            trace.legend = f"Wet Scattering ({size[0]})"
            trace.data_record = f'{mode}-scattering2-{size[1]}'
            trace.data_field = 'G'
            trace.color = size[2]
            scattering.traces.append(trace)
            self.processing[trace.data_record] = self.AdjustWavelength(OrderedDict([
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

        for size in [("Whole", 'whole', '#0f0'), ("PM10", 'pm10', '#0f0'),
                     ("PM2.5", 'pm25', '#070'), ("PM1", 'pm1', '#070')]:
            trace = TimeSeries.Trace(Mm_1)
            trace.legend = f"Absorption ({size[0]})"
            trace.data_record = f'{mode}-absorption-{size[1]}'
            trace.data_field = 'G'
            trace.color = size[2]
            absorption.traces.append(trace)
            self.processing[trace.data_record] = self.AdjustWavelength(OrderedDict([
                ('BaB', 467), ('BaG', 528), ('BaR', 652),
            ]))
