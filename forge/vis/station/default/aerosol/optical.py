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

            for size in [("Whole", 'whole'), ("PM10", 'pm10')]:
                for color in [("B", '#00f'), ("G", '#0f0'), ("R", '#f00')]:
                    trace = TimeSeries.Trace(Mm_1)
                    trace.legend = name.format(code=color[0], size=size[0])
                    trace.data_record = f'{record}-{size[1]}'
                    trace.data_field = f'{field}{color[0]}'
                    trace.color = color[1]
                    self.traces.append(trace)

            for size in [("PM2.5", 'pm25'), ("PM1", 'pm1')]:
                for color in [("B", '#007'), ("G", '#070'), ("R", '#700')]:
                    trace = TimeSeries.Trace(Mm_1)
                    trace.legend = name.format(code=color[0], size=size[0])
                    trace.data_record = f'{record}-{size[1]}'
                    trace.data_field = f'{field}{color[0]}'
                    trace.color = color[1]
                    self.traces.append(trace)

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
