import typing
from forge.vis.view.timeseries import TimeSeries


class BMI1710CPCStatus(TimeSeries):
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

        inlet = TimeSeries.Trace(degrees)
        inlet.legend = "Inlet"
        inlet.data_record = f'{mode}-cpcstatus'
        inlet.data_field = 'Tinlet'
        temperatures.traces.append(inlet)

        saturator_bottom = TimeSeries.Trace(degrees)
        saturator_bottom.legend = "Saturator Bottom"
        saturator_bottom.data_record = f'{mode}-cpcstatus'
        saturator_bottom.data_field = 'Tsaturatorbottom'
        temperatures.traces.append(saturator_bottom)

        saturator_top = TimeSeries.Trace(degrees)
        saturator_top.legend = "Saturator Top"
        saturator_top.data_record = f'{mode}-cpcstatus'
        saturator_top.data_field = 'Tsaturatortop'
        temperatures.traces.append(saturator_top)

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

        saturator_flow = TimeSeries.Trace(lpm)
        saturator_flow.legend = "Saturator"
        saturator_flow.data_record = f'{mode}-cpcstatus'
        saturator_flow.data_field = 'Qsaturator'
        cpc_flow.traces.append(saturator_flow)


class BMI1720CPCStatus(BMI1710CPCStatus):
    def __init__(self, mode: str):
        super().__init__(mode)

        pressure = TimeSeries.Graph()
        pressure.title = "Pressure"
        self.graphs.append(pressure)

        hPa = TimeSeries.Axis()
        hPa.title = "hPa"
        hPa.format_code = '.1f'
        pressure.axes.append(hPa)

        sample = TimeSeries.Trace(hPa)
        sample.legend = "Sample Pressure"
        sample.data_record = f'{mode}-cpcstatus'
        sample.data_field = 'Psample'
        pressure.traces.append(sample)
