import typing
from forge.vis.view.timeseries import TimeSeries


class Green(TimeSeries):
    def __init__(self, mode: str):
        super().__init__()
        self.title = "Optical Properties Adjusted to 550nm (NOT YET ADJUSTED)"

        scattering = TimeSeries.Graph()
        scattering.title = "Light Scattering"
        self.graphs.append(scattering)

        Mm_1 = TimeSeries.Axis()
        Mm_1.title = "Mm⁻¹"
        scattering.axes.append(Mm_1)

        G0 = TimeSeries.Trace(Mm_1)
        G0.legend = "Scattering (PM10)"
        G0.data_record = f'{mode}-scattering-pm10'
        G0.data_field = 'BsG'
        G0.color = '#0f0'
        scattering.traces.append(G0)

        G1 = TimeSeries.Trace(Mm_1)
        G1.legend = "Scattering (PM1)"
        G1.data_record = f'{mode}-scattering-pm1'
        G1.data_field = 'BsG'
        G1.color = '#070'
        scattering.traces.append(G1)


        absorption = TimeSeries.Graph()
        absorption.title = "Light Absorption"
        self.graphs.append(absorption)

        Mm_1 = TimeSeries.Axis()
        Mm_1.title = "Mm⁻¹"
        absorption.axes.append(Mm_1)

        G0 = TimeSeries.Trace(Mm_1)
        G0.legend = "Absorption (PM10)"
        G0.data_record = f'{mode}-absorption-pm10'
        G0.data_field = 'BaG'
        G0.color = '#0f0'
        absorption.traces.append(G0)

        G1 = TimeSeries.Trace(Mm_1)
        G1.legend = "Absorption (PM1)"
        G1.data_record = f'{mode}-absorption-pm1'
        G1.data_field = 'BaG'
        G1.color = '#070'
        absorption.traces.append(G1)

        aethalometer = TimeSeries.Trace(Mm_1)
        aethalometer.legend = "Aethalometer"
        aethalometer.data_record = f'{mode}-aethalometer'
        aethalometer.data_field = 'BaG'
        absorption.traces.append(aethalometer)
