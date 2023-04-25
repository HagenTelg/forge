import typing
from forge.vis.view.timeseries import TimeSeries
from ..default.aerosol.flow import Flow as BaseFlow


class Flow(TimeSeries):
    def __init__(self, mode: str, **kwargs):
        super().__init__(**kwargs)
        self.title = "System Flow"

        system_flow = TimeSeries.Graph()
        self.graphs.append(system_flow)

        lpm = TimeSeries.Axis()
        lpm.title = "Analyzer Flow (lpm)"
        lpm.range = [0, 50]
        lpm.format_code = '.2f'
        system_flow.axes.append(lpm)

        sample_flow = TimeSeries.Trace(lpm)
        sample_flow.legend = "Q_Q11 (sample)"
        sample_flow.data_record = f'{mode}-flow'
        sample_flow.data_field = 'sample'
        system_flow.traces.append(sample_flow)

        filter_flow = TimeSeries.Trace(lpm)
        filter_flow.legend = "Q_Q21 (PMEL)"
        filter_flow.data_record = f'{mode}-flow'
        filter_flow.data_field = 'filter'
        system_flow.traces.append(filter_flow)

        filter2_flow = TimeSeries.Trace(lpm)
        filter2_flow.legend = "Q_Q31 (SCRIPPS)"
        filter2_flow.data_record = f'{mode}-flow'
        filter2_flow.data_field = 'filter2'
        system_flow.traces.append(filter2_flow)

        stack_lpm = TimeSeries.Axis()
        stack_lpm.title = "Stack Flow (lpm)"
        stack_lpm.format_code = '.1f'
        system_flow.axes.append(stack_lpm)

        stack_flow = TimeSeries.Trace(stack_lpm)
        stack_flow.legend = "Q_P01 (stack pitot)"
        stack_flow.data_record = f'{mode}-flow'
        stack_flow.data_field = 'pitot'
        system_flow.traces.append(stack_flow)
        self.processing[stack_flow.data_record] = BaseFlow.CalculatePitotFlow()
