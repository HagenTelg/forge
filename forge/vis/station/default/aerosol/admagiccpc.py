import typing
from forge.vis.view.timeseries import TimeSeries


class ADMagicCPC200Status(TimeSeries):
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

        td_inlet = TimeSeries.Trace(degrees)
        td_inlet.legend = "Inlet Dewpoint"
        td_inlet.data_record = f'{mode}-cpcstatus'
        td_inlet.data_field = 'TDinlet'
        temperatures.traces.append(td_inlet)

        conditioner = TimeSeries.Trace(degrees)
        conditioner.legend = "Conditioner"
        conditioner.data_record = f'{mode}-cpcstatus'
        conditioner.data_field = 'Tconditioner'
        temperatures.traces.append(conditioner)

        initiator = TimeSeries.Trace(degrees)
        initiator.legend = "Initiator"
        initiator.data_record = f'{mode}-cpcstatus'
        initiator.data_field = 'Tinitiator'
        temperatures.traces.append(initiator)

        moderator = TimeSeries.Trace(degrees)
        moderator.legend = "Moderator"
        moderator.data_record = f'{mode}-cpcstatus'
        moderator.data_field = 'Tmoderator'
        temperatures.traces.append(moderator)

        optics = TimeSeries.Trace(degrees)
        optics.legend = "Optics"
        optics.data_record = f'{mode}-cpcstatus'
        optics.data_field = 'Toptics'
        temperatures.traces.append(optics)

        heat_sink = TimeSeries.Trace(degrees)
        heat_sink.legend = "Heat Sink"
        heat_sink.data_record = f'{mode}-cpcstatus'
        heat_sink.data_field = 'Theatsink'
        temperatures.traces.append(heat_sink)

        pcb = TimeSeries.Trace(degrees)
        pcb.legend = "PCB"
        pcb.data_record = f'{mode}-cpcstatus'
        pcb.data_field = 'Tpcb'
        temperatures.traces.append(pcb)

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
