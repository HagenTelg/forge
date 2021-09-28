import typing
from forge.vis.view.timeseries import TimeSeries


class DilutionFlow(TimeSeries):
    def __init__(self, mode: str):
        super().__init__()
        self.title = "System Flow"

        system_flow = TimeSeries.Graph()
        self.graphs.append(system_flow)

        lpm = TimeSeries.Axis()
        lpm.title = "lpm"
        lpm.range = [0, 50]
        lpm.format_code = '.2f'
        system_flow.axes.append(lpm)

        sample_flow = TimeSeries.Trace(lpm)
        sample_flow.legend = "Q_Q11 (sample)"
        sample_flow.data_record = f'{mode}-flow'
        sample_flow.data_field = 'sample'
        system_flow.traces.append(sample_flow)

        dilution_flow = TimeSeries.Trace(lpm)
        dilution_flow.legend = "Q_Q12 (dilution)"
        dilution_flow.data_record = f'{mode}-flow'
        dilution_flow.data_field = 'dilution'
        system_flow.traces.append(dilution_flow)
