import typing
from forge.vis.view.timeseries import TimeSeries
from ..default.aerosol.bmi17XXcpc import BMI1720CPCStatus as BaseBMI1720CPCStatus


class BMI1720CPCStatus(BaseBMI1720CPCStatus):
    def __init__(self, mode: str):
        super().__init__(mode)

        drier_flow = TimeSeries.Graph()
        drier_flow.title = "Drier Flow"
        self.graphs.append(drier_flow)

        lpm = TimeSeries.Axis()
        lpm.title = "lpm"
        lpm.format_code = '.3f'
        drier_flow.axes.append(lpm)

        sample = TimeSeries.Trace(lpm)
        sample.legend = "CPC Flow"
        sample.data_record = f'{mode}-cpcstatus'
        sample.data_field = 'Qcpc'
        drier_flow.traces.append(sample)

        drier = TimeSeries.Trace(lpm)
        drier.legend = "Drier Flow"
        drier.data_record = f'{mode}-cpcstatus'
        drier.data_field = 'Qdrier'
        drier_flow.traces.append(drier)
