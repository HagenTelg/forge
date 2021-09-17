import typing
from forge.vis.view.timeseries import TimeSeries
from forge.vis.view.sizedistribution import SizeDistribution


class GrimmStatus(TimeSeries):
    def __init__(self, mode: str):
        super().__init__()
        self.title = "Grimm OPC Status"

        flow = TimeSeries.Graph()
        flow.title = "Flow"
        self.graphs.append(flow)

        lpm = TimeSeries.Axis()
        lpm.title = "lpm"
        lpm.format_code = '.2f'
        flow.axes.append(lpm)

        sample = TimeSeries.Trace(lpm)
        sample.legend = "Sample"
        sample.data_record = f'{mode}-grimmstatus'
        sample.data_field = 'Qsample'
        flow.traces.append(sample)


class GrimmDistribution(SizeDistribution):
    def __init__(self, mode: str):
        super().__init__()
        self.title = "Grimm OPC Size Distribution"

        self.contamination = f'{mode}-contamination'
        self.size_record = f'{mode}-grimm'
        self.measured_record = f'{mode}-scattering-pm10'
