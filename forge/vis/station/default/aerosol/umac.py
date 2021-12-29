import typing
from forge.vis.view.timeseries import TimeSeries


class UMACStatus(TimeSeries):
    def __init__(self, mode: str, **kwargs):
        super().__init__(**kwargs)

        self.title = "μMAC Status"

        temperatures = TimeSeries.Graph()
        temperatures.title = "Internal Temperature"
        self.graphs.append(temperatures)

        degrees = TimeSeries.Axis()
        degrees.title = "°C"
        degrees.format_code = '.1f'
        temperatures.axes.append(degrees)

        internal = TimeSeries.Trace(degrees)
        internal.legend = "Internal"
        internal.data_record = f'{mode}-umacstatus'
        internal.data_field = 'T'
        temperatures.traces.append(internal)


        voltage = TimeSeries.Graph()
        voltage.title = "Supply Voltage"
        self.graphs.append(voltage)

        V = TimeSeries.Axis()
        V.title = "V"
        V.format_code = '.3f'
        voltage.axes.append(V)

        supply = TimeSeries.Trace(V)
        supply.legend = "Supply"
        supply.data_record = f'{mode}-umacstatus'
        supply.data_field = 'V'
        voltage.traces.append(supply)

