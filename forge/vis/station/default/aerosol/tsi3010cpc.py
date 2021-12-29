import typing
from forge.vis.view.timeseries import TimeSeries


class TSI3010CPCStatus(TimeSeries):
    def __init__(self, mode: str, **kwargs):
        super().__init__(**kwargs)
        self.title = "CPC Status"

        temperatures = TimeSeries.Graph()
        temperatures.title = "Temperature"
        self.graphs.append(temperatures)

        degrees = TimeSeries.Axis()
        degrees.title = "°C"
        degrees.format_code = '.1f'
        temperatures.axes.append(degrees)

        saturator = TimeSeries.Trace(degrees)
        saturator.legend = "Saturator"
        saturator.data_record = f'{mode}-cpcstatus'
        saturator.data_field = 'Tsaturator'
        temperatures.traces.append(saturator)

        condenser = TimeSeries.Trace(degrees)
        condenser.legend = "Condenser"
        condenser.data_record = f'{mode}-cpcstatus'
        condenser.data_field = 'Tcondenser'
        temperatures.traces.append(condenser)

        cpc_flow = TimeSeries.Graph()
        cpc_flow.title = "Flow"
        self.graphs.append(cpc_flow)

        lpm = TimeSeries.Axis()
        lpm.title = "lpm"
        lpm.format_code = '.3f'
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

