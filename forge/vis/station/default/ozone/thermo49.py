import typing
from forge.vis.view.timeseries import TimeSeries


class Thermo49Status(TimeSeries):
    def __init__(self, mode: str, **kwargs):
        super().__init__(**kwargs)
        self.title = "Instrument Status"

        temperatures = TimeSeries.Graph()
        temperatures.title = "Temperature"
        self.graphs.append(temperatures)

        degrees = TimeSeries.Axis()
        degrees.title = "°C"
        degrees.format_code = '.1f'
        temperatures.axes.append(degrees)

        sample = TimeSeries.Trace(degrees)
        sample.legend = "Sample Temperature"
        sample.data_record = f'{mode}-status'
        sample.data_field = 'Tsample'
        temperatures.traces.append(sample)

        case_temperature = TimeSeries.Trace(degrees)
        case_temperature.legend = "Lamp Temperature"
        case_temperature.data_record = f'{mode}-status'
        case_temperature.data_field = 'Tlamp'
        temperatures.traces.append(case_temperature)


        pressure = TimeSeries.Graph()
        pressure.title = "Pressure"
        self.graphs.append(pressure)

        hpa = TimeSeries.Axis()
        hpa.title = "hPa"
        hpa.format_code = '.1f'
        pressure.axes.append(hpa)

        sample = TimeSeries.Trace(hpa)
        sample.legend = "Sample Pressure"
        sample.data_record = f'{mode}-status'
        sample.data_field = 'Psample'
        pressure.traces.append(sample)


class Thermo49Cells(TimeSeries):
    def __init__(self, mode: str, **kwargs):
        super().__init__(**kwargs)
        self.title = "Measurement Cell Status"

        flows = TimeSeries.Graph()
        flows.title = "Flow"
        self.graphs.append(flows)

        lpm = TimeSeries.Axis()
        lpm.title = "lpm"
        lpm.format_code = '.3f'
        flows.axes.append(lpm)

        a = TimeSeries.Trace(lpm)
        a.legend = "Cell A Flow"
        a.data_record = f'{mode}-cells'
        a.data_field = 'Qa'
        flows.traces.append(a)

        b = TimeSeries.Trace(lpm)
        b.legend = "Cell B Flow"
        b.data_record = f'{mode}-cells'
        b.data_field = 'Qb'
        flows.traces.append(b)


        counts = TimeSeries.Graph()
        counts.title = "Count Rate"
        self.graphs.append(counts)

        Hz = TimeSeries.Axis()
        Hz.title = "Hz"
        Hz.format_code = '.0f'
        counts.axes.append(Hz)

        a = TimeSeries.Trace(Hz)
        a.legend = "Cell A Counts"
        a.data_record = f'{mode}-cells'
        a.data_field = 'Ca'
        counts.traces.append(a)

        b = TimeSeries.Trace(Hz)
        b.legend = "Cell B Counts"
        b.data_record = f'{mode}-cells'
        b.data_field = 'Cb'
        counts.traces.append(b)

