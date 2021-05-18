import typing
from forge.vis.view.timeseries import TimeSeries


class SystemCPCFlow(TimeSeries):
    def __init__(self, mode: str):
        super().__init__()
        self.title = "CPC Flow"

        cpc_flow = TimeSeries.Graph()
        self.graphs.append(cpc_flow)

        lpm = TimeSeries.Axis()
        lpm.title = "lpm"
        cpc_flow.axes.append(lpm)

        sample = TimeSeries.Trace(lpm)
        sample.legend = "Sample"
        sample.data_record = f'{mode}-cpcstatus'
        sample.data_field = 'Qsample'
        cpc_flow.traces.append(sample)

        drier = TimeSeries.Trace(lpm)
        drier.legend = "Drier"
        drier.data_record = f'{mode}-cpcstatus'
        drier.data_field = 'Qdrier'
        cpc_flow.traces.append(drier)
