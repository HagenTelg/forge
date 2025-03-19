import typing
from forge.vis.view.timeseries import PublicTimeSeries
from ...met.wind import Wind as BaseWind


class PublicHousekeepingShort(PublicTimeSeries):
    WIND_DIRECTION_BREAK_SCRIPT = BaseWind.DIRECTION_BREAK_SCRIPT

    def __init__(self, mode: str = 'public-ozoneweb', **kwargs):
        super().__init__(**kwargs)

        temperatures = PublicTimeSeries.Graph()
        temperatures.title = "Temperature"
        self.graphs.append(temperatures)

        degrees = PublicTimeSeries.Axis()
        degrees.title = "°C"
        degrees.format_code = '.1f'
        temperatures.axes.append(degrees)

        sample = PublicTimeSeries.Trace(degrees)
        sample.legend = "Sample Temperature"
        sample.data_record = f'{mode}-ozonestatus'
        sample.data_field = 'Tsample'
        temperatures.traces.append(sample)

        lamp_temperature = PublicTimeSeries.Trace(degrees)
        lamp_temperature.legend = "Lamp Temperature"
        lamp_temperature.data_record = f'{mode}-ozonestatus'
        lamp_temperature.data_field = 'Tlamp'
        temperatures.traces.append(lamp_temperature)


        pressure = PublicTimeSeries.Graph()
        pressure.title = "Pressure"
        self.graphs.append(pressure)

        hpa = PublicTimeSeries.Axis()
        hpa.title = "hPa"
        hpa.format_code = '.1f'
        pressure.axes.append(hpa)

        sample = PublicTimeSeries.Trace(hpa)
        sample.legend = "Sample Pressure"
        sample.data_record = f'{mode}-ozonestatus'
        sample.data_field = 'Psample'
        pressure.traces.append(sample)


        flows = PublicTimeSeries.Graph()
        flows.title = "Flow"
        self.graphs.append(flows)

        lpm = PublicTimeSeries.Axis()
        lpm.title = "lpm"
        lpm.format_code = '.3f'
        flows.axes.append(lpm)

        a = PublicTimeSeries.Trace(lpm)
        a.legend = "Flow"
        a.data_record = f'{mode}-ozonestatus'
        a.data_field = 'Q'
        flows.traces.append(a)


        counts = PublicTimeSeries.Graph()
        counts.title = "Count Rate"
        self.graphs.append(counts)

        Hz = PublicTimeSeries.Axis()
        Hz.title = "Hz"
        Hz.format_code = '.0f'
        counts.axes.append(Hz)

        a = PublicTimeSeries.Trace(Hz)
        a.legend = "Cell A Counts"
        a.data_record = f'{mode}-ozonestatus'
        a.data_field = 'Ca'
        counts.traces.append(a)

        b = PublicTimeSeries.Trace(Hz)
        b.legend = "Cell B Counts"
        b.data_record = f'{mode}-ozonestatus'
        b.data_field = 'Cb'
        counts.traces.append(b)


        speed = PublicTimeSeries.Graph()
        speed.title = "Wind Speed"
        self.graphs.append(speed)
        mps = PublicTimeSeries.Axis()
        mps.title = "m/s"
        mps.range = 0
        mps.format_code = '.1f'
        speed.axes.append(mps)

        ws = PublicTimeSeries.Trace(mps)
        ws.legend = "Speed"
        ws.data_record = f'{mode}-wind'
        ws.data_field = 'WS'
        speed.traces.append(ws)

        direction = PublicTimeSeries.Graph()
        direction.title = "Wind Direction"
        self.graphs.append(direction)

        degrees = PublicTimeSeries.Axis()
        degrees.title = "degrees"
        degrees.range = [0, 360]
        degrees.ticks = [0, 90, 180, 270, 360]
        degrees.format_code = '.0f'
        direction.axes.append(degrees)

        wd = PublicTimeSeries.Trace(degrees)
        wd.legend = "Direction"
        wd.data_record = f'{mode}-wind'
        wd.data_field = 'WD'
        wd.script_incoming_data = self.WIND_DIRECTION_BREAK_SCRIPT
        direction.traces.append(wd)

    @property
    def required_components(self) -> typing.List[str]:
        return super().required_components + ['winds']



class PublicHousekeepingLong(PublicHousekeepingShort):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.average = self.Averaging.HOUR

