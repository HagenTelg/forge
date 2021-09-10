import typing
from forge.vis.view.timeseries import TimeSeries


class NephelometerStatus(TimeSeries):
    def __init__(self, mode: str):
        super().__init__()
        self.title = "Nephelometer Status"

        reference = TimeSeries.Graph()
        reference.title = "Reference Count Rate"
        self.graphs.append(reference)

        Hz = TimeSeries.Axis()
        Hz.title = "Hz"
        Hz.format_code = '.0f'
        reference.axes.append(Hz)

        CfG = TimeSeries.Trace(Hz)
        CfG.legend = "Green Reference"
        CfG.data_record = f'{mode}-nephstatus'
        CfG.data_field = 'CfG'
        reference.traces.append(CfG)

