import typing
from forge.vis.view.timeseries import TimeSeries


class Pressure(TimeSeries):
    def __init__(self, mode: str):
        super().__init__()

        pressure = TimeSeries.Graph()
        pressure.title = "Pressure"
        self.graphs.append(pressure)

        hpa = TimeSeries.Axis()
        hpa.title = "hPa"
        hpa.format_code = '.1f'
        pressure.axes.append(hpa)

        ambient = TimeSeries.Trace(hpa)
        ambient.legend = "Ambient"
        ambient.data_record = f'{mode}-pressure'
        ambient.data_field = 'ambient'
        pressure.traces.append(ambient)
