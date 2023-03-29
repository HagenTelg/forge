import typing
from forge.vis.view.timeseries import TimeSeries


class ADMagicCPC200Status(TimeSeries):
    class CalculateMissing(TimeSeries.Processing):
        def __init__(self):
            super().__init__()
            self.components.append('numeric_solve')
            self.components.append('dewpoint')
            self.script = r"""(function(dataName) { return new Dewpoint.CalculateDispatch(dataName); })"""

    def __init__(self, mode: str, **kwargs):
        super().__init__(**kwargs)
        self.title = "CPC Status"

        self.processing[f'{mode}-cpcstatus'] = self.CalculateMissing()

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


        rh = TimeSeries.Graph()
        rh.title = "Relative Humidity"
        self.graphs.append(rh)

        rh_percent = TimeSeries.Axis()
        rh_percent.title = "%"
        rh_percent.format_code = '.1f'
        rh.axes.append(rh_percent)

        inlet = TimeSeries.Trace(rh_percent)
        inlet.legend = "Inlet Humidity"
        inlet.data_record = f'{mode}-cpcstatus'
        inlet.data_field = 'Uinlet'
        rh.traces.append(inlet)


        dewpoint = TimeSeries.Graph()
        dewpoint.title = "Dewpoint"
        self.graphs.append(dewpoint)

        degrees = TimeSeries.Axis()
        degrees.title = "°C"
        degrees.format_code = '.1f'
        dewpoint.axes.append(degrees)

        inlet = TimeSeries.Trace(degrees)
        inlet.legend = "Inlet Dewpoint"
        inlet.data_record = f'{mode}-cpcstatus'
        inlet.data_field = 'TDinlet'
        dewpoint.traces.append(inlet)


        pressure = TimeSeries.Graph()
        pressure.title = "Pressure"
        self.graphs.append(pressure)

        absolute = TimeSeries.Axis()
        absolute.title = "Absolute (hPa)"
        absolute.format_code = '.1f'
        pressure.axes.append(absolute)

        delta = TimeSeries.Axis()
        delta.title = "Delta (hPa)"
        delta.format_code = '.1f'
        pressure.axes.append(delta)

        sample = TimeSeries.Trace(absolute)
        sample.legend = "Sample Pressure"
        sample.data_record = f'{mode}-cpcstatus'
        sample.data_field = 'Psample'
        pressure.traces.append(sample)

        orifice = TimeSeries.Trace(delta)
        orifice.legend = "Orifice Pressure Drop"
        orifice.data_record = f'{mode}-cpcstatus'
        orifice.data_field = 'PDorifice'
        pressure.traces.append(orifice)


class ADMagicCPC250Status(TimeSeries):
    class CalculateMissing(TimeSeries.Processing):
        def __init__(self):
            super().__init__()
            self.components.append('numeric_solve')
            self.components.append('dewpoint')
            self.script = r"""(function(dataName) { return new Dewpoint.CalculateDispatch(dataName); })"""

    def __init__(self, mode: str, **kwargs):
        super().__init__(**kwargs)
        self.title = "CPC Status"

        self.processing[f'{mode}-cpcstatus'] = self.CalculateMissing()


        wick_laser = TimeSeries.Graph()
        wick_laser.title = "Wick and Laser"
        self.graphs.append(wick_laser)

        wick_percent = TimeSeries.Axis()
        wick_percent.title = "Wick Saturation (%)"
        wick_percent.format_code = '.0f'
        wick_laser.axes.append(wick_percent)

        pulse_mV = TimeSeries.Axis()
        pulse_mV.title = "Pulse Height (mV)"
        pulse_mV.format_code = '.0f'
        wick_laser.axes.append(pulse_mV)

        wick = TimeSeries.Trace(wick_percent)
        wick.legend = "Wick Saturation"
        wick.data_record = f'{mode}-cpcstatus'
        wick.data_field = 'PCTwick'
        wick_laser.traces.append(wick)

        pulse = TimeSeries.Trace(pulse_mV)
        pulse.legend = "Pulse Height"
        pulse.data_record = f'{mode}-cpcstatus'
        pulse.data_field = 'Vpulse'
        wick_laser.traces.append(pulse)


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

        case = TimeSeries.Trace(degrees)
        case.legend = "Case"
        case.data_record = f'{mode}-cpcstatus'
        case.data_field = 'Tcase'
        temperatures.traces.append(case)


        rh = TimeSeries.Graph()
        rh.title = "Relative Humidity"
        self.graphs.append(rh)

        rh_percent = TimeSeries.Axis()
        rh_percent.title = "%"
        rh_percent.format_code = '.1f'
        rh.axes.append(rh_percent)

        inlet = TimeSeries.Trace(rh_percent)
        inlet.legend = "Inlet Humidity"
        inlet.data_record = f'{mode}-cpcstatus'
        inlet.data_field = 'Uinlet'
        rh.traces.append(inlet)


        dewpoint = TimeSeries.Graph()
        dewpoint.title = "Dewpoint"
        self.graphs.append(dewpoint)

        degrees = TimeSeries.Axis()
        degrees.title = "°C"
        degrees.format_code = '.1f'
        dewpoint.axes.append(degrees)

        inlet = TimeSeries.Trace(degrees)
        inlet.legend = "Inlet Dewpoint"
        inlet.data_record = f'{mode}-cpcstatus'
        inlet.data_field = 'TDinlet'
        dewpoint.traces.append(inlet)

        inlet = TimeSeries.Trace(degrees)
        inlet.legend = "Growth Tube Dewpoint"
        inlet.data_record = f'{mode}-cpcstatus'
        inlet.data_field = 'TDgrowth'
        dewpoint.traces.append(inlet)


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

