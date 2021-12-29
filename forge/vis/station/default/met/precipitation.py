import typing
from forge.vis.view.timeseries import TimeSeries


class Precipitation(TimeSeries):
    def __init__(self, mode: str, **kwargs):
        super().__init__(**kwargs)

        precipitation = TimeSeries.Graph()
        precipitation.title = "Precipitation"
        self.graphs.append(precipitation)

        mmh = TimeSeries.Axis()
        mmh.title = "mm/h"
        mmh.format_code = '.2f'
        precipitation.axes.append(mmh)

        rate = TimeSeries.Trace(mmh)
        rate.data_record = f'{mode}-precipitation'
        rate.data_field = 'precipitation'
        precipitation.traces.append(rate)
