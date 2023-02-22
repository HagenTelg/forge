import typing
from forge.vis.view.timeseries import TimeSeries
from .tsi3563nephelometer import NephelometerZero


class NephelometerStatus(TimeSeries):
    def __init__(self, mode: str, **kwargs):
        super().__init__(**kwargs)
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

