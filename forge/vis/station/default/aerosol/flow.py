import typing
from forge.vis.view.timeseries import TimeSeries


class Flow(TimeSeries):
    def __init__(self, mode: str):
        super().__init__()
        self.title = "System Flow"

        system_flow = TimeSeries.Graph()
        self.graphs.append(system_flow)

        lpm = TimeSeries.Axis()
        lpm.title = "Analyzer Flow (lpm)"
        lpm.range = [0, 50]
        system_flow.axes.append(lpm)

        sample_flow = TimeSeries.Trace(lpm)
        sample_flow.legend = "Q_Q11 (sample)"
        sample_flow.data_record = f'{mode}-flow'
        sample_flow.data_field = 'sample'
        system_flow.traces.append(sample_flow)

        stack_lpm = TimeSeries.Axis()
        stack_lpm.title = "Stack Flow (lpm) - NOT CONVERTED YET"
        stack_lpm.range = [0, 50]
        system_flow.axes.append(stack_lpm)

        stack_flow = TimeSeries.Trace(stack_lpm)
        stack_flow.legend = "Q_P01 (stack pitot)"
        stack_flow.data_record = f'{mode}-flow'
        stack_flow.data_field = 'pitot'
        system_flow.traces.append(stack_flow)
