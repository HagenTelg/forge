import typing
from forge.vis.view.timeseries import TimeSeries


class NOxConcentration(TimeSeries):
    def __init__(self, mode: str, **kwargs):
        super().__init__(**kwargs)
        self.title = "NOₓ Concentration"

        concentration = TimeSeries.Graph()
        concentration.contamination = f'{mode}-contamination'
        self.graphs.append(concentration)

        ppb = TimeSeries.Axis()
        ppb.title = "ppb"
        ppb.format_code = '.2f'
        concentration.axes.append(ppb)

        no = TimeSeries.Trace(ppb)
        no.legend = "NO"
        no.data_record = f'{mode}-nox'
        no.data_field = 'no'
        concentration.traces.append(no)

        no2 = TimeSeries.Trace(ppb)
        no2.legend = "NO₂"
        no2.data_record = f'{mode}-nox'
        no2.data_field = 'no2'
        concentration.traces.append(no2)

        nox = TimeSeries.Trace(ppb)
        nox.legend = "NOₓ"
        nox.data_record = f'{mode}-nox'
        nox.data_field = 'nox'
        concentration.traces.append(nox)


class TeledyneN500Status(TimeSeries):
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

        manifold = TimeSeries.Trace(degrees)
        manifold.legend = "Manifold Temperature"
        manifold.data_record = f'{mode}-noxstatus'
        manifold.data_field = 'Tmanifold'
        temperatures.traces.append(manifold)

        oven = TimeSeries.Trace(degrees)
        oven.legend = "Oven Temperature"
        oven.data_record = f'{mode}-noxstatus'
        oven.data_field = 'Toven'
        temperatures.traces.append(oven)

        box = TimeSeries.Trace(degrees)
        box.legend = "Box Temperature"
        box.data_record = f'{mode}-noxstatus'
        box.data_field = 'Tbox'
        temperatures.traces.append(box)


        pressure = TimeSeries.Graph()
        pressure.title = "Pressure"
        self.graphs.append(pressure)

        hpa = TimeSeries.Axis()
        hpa.title = "hPa"
        hpa.format_code = '.1f'
        pressure.axes.append(hpa)

        sample = TimeSeries.Trace(hpa)
        sample.legend = "Sample Pressure"
        sample.data_record = f'{mode}-noxstatus'
        sample.data_field = 'Psample'
        pressure.traces.append(sample)
