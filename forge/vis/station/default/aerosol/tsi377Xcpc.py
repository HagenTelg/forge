import typing
from forge.vis.view.timeseries import TimeSeries


class TSI377XCPCStatus(TimeSeries):
    def __init__(self, mode: str):
        super().__init__()
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

        optics = TimeSeries.Trace(degrees)
        optics.legend = "Optics"
        optics.data_record = f'{mode}-cpcstatus'
        optics.data_field = 'Toptics'
        temperatures.traces.append(optics)

        cabinet = TimeSeries.Trace(degrees)
        cabinet.legend = "Cabinet"
        cabinet.data_record = f'{mode}-cpcstatus'
        cabinet.data_field = 'Tcabinet'
        temperatures.traces.append(cabinet)

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

        inlet = TimeSeries.Trace(lpm)
        inlet.legend = "Inlet"
        inlet.data_record = f'{mode}-cpcstatus'
        inlet.data_field = 'Qinlet'
        cpc_flow.traces.append(inlet)
