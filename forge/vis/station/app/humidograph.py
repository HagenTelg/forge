import typing
from forge.vis.view.timeseries import TimeSeries


class WetDryRatio(TimeSeries):
    class CalculateRatio(TimeSeries.Processing):
        def __init__(self):
            super().__init__()
            self.components.append('generic_operations')
            self.script = r"""(function(dataName) {
        return new GenericOperations.SingleOutput(dataName, GenericOperations.divide, 'ratio', 'wet', 'dry');
    })"""

    def __init__(self, mode: str, **kwargs):
        super().__init__(**kwargs)
        self.title = "Wet/Dry Scattering Ratio at 550nm"

        humidograph = TimeSeries.Graph()
        humidograph.contamination = f'{mode}-contamination'
        self.graphs.append(humidograph)

        ratio = TimeSeries.Axis()
        ratio.title = "Wet/Dry"
        ratio.format_code = '.2f'
        humidograph.axes.append(ratio)

        R0 = TimeSeries.Trace(ratio)
        R0.legend = "PM10"
        R0.data_record = f'{mode}-humidograph-pm10'
        R0.data_field = 'ratio'
        humidograph.traces.append(R0)
        self.processing[R0.data_record] = self.CalculateRatio()

        R1 = TimeSeries.Trace(ratio)
        R1.legend = "PM1"
        R1.data_record = f'{mode}-humidograph-pm1'
        R1.data_field = 'ratio'
        humidograph.traces.append(R1)
        self.processing[R1.data_record] = self.CalculateRatio()
