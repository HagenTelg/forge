import typing
from forge.vis.view.timeseries import TimeSeries


class Optical(TimeSeries):
    class ThreeWavelength(TimeSeries.Graph):
        def __init__(self, record: str, field: str, name: typing.Optional[str] = None):
            super().__init__()
            
            if not name:
                name = field + '{code} ({size})'

            Mm_1 = TimeSeries.Axis()
            Mm_1.title = "Mm⁻¹"
            Mm_1.format_code = '.2f'
            self.axes.append(Mm_1)

            B0 = TimeSeries.Trace(Mm_1)
            B0.legend = name.format(code='B', size='PM10')
            B0.data_record = f'{record}-pm10'
            B0.data_field = f'{field}B'
            B0.color = '#00f'
            self.traces.append(B0)

            G0 = TimeSeries.Trace(Mm_1)
            G0.legend = name.format(code='G', size='PM10')
            G0.data_record = f'{record}-pm10'
            G0.data_field = f'{field}G'
            G0.color = '#0f0'
            self.traces.append(G0)

            R0 = TimeSeries.Trace(Mm_1)
            R0.legend = name.format(code='R', size='PM10')
            R0.data_record = f'{record}-pm10'
            R0.data_field = f'{field}R'
            R0.color = '#f00'
            self.traces.append(R0)

            B1 = TimeSeries.Trace(Mm_1)
            B1.legend = name.format(code='B', size='PM1')
            B1.data_record = f'{record}-pm1'
            B1.data_field = f'{field}B'
            B1.color = '#007'
            self.traces.append(B1)

            G1 = TimeSeries.Trace(Mm_1)
            G1.legend = name.format(code='G', size='PM1')
            G1.data_record = f'{record}-pm1'
            G1.data_field = f'{field}G'
            G1.color = '#070'
            self.traces.append(G1)

            R1 = TimeSeries.Trace(Mm_1)
            R1.legend = name.format(code='R', size='PM1')
            R1.data_record = f'{record}-pm1'
            R1.data_field = f'{field}R'
            R1.color = '#700'
            self.traces.append(R1)

    def __init__(self, mode: str):
        super().__init__()
        self.title = "Optical Properties"

        total_scattering = self.ThreeWavelength(f'{mode}-scattering', 'Bs')
        total_scattering.title = "Total Light Scattering"
        total_scattering.contamination = f'{mode}-contamination'
        self.graphs.append(total_scattering)

        back_scattering = self.ThreeWavelength(f'{mode}-scattering', 'Bbs')
        back_scattering.title = "Backwards-hemispheric Light Scattering"
        back_scattering.contamination = f'{mode}-contamination'
        self.graphs.append(back_scattering)

        absorption = self.ThreeWavelength(f'{mode}-absorption', 'Ba')
        absorption.title = "Light Absorption"
        absorption.contamination = f'{mode}-contamination'
        self.graphs.append(absorption)
