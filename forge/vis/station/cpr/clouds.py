import typing
from forge.vis.view.timeseries import TimeSeries


class Clouds(TimeSeries):
    def __init__(self, mode: str):
        super().__init__()
        self.title = "Cloud Status"

        precipitation = TimeSeries.Graph()
        precipitation.title = "Precipitation"
        precipitation.contamination = f'{mode}-contamination'
        self.graphs.append(precipitation)

        mm_h = TimeSeries.Axis()
        mm_h.title = "mm/h"
        mm_h.format_code = '.2f'
        mm_h.range = 0
        precipitation.axes.append(mm_h)

        precip = TimeSeries.Trace(mm_h)
        precip.legend = "Precipitation"
        precip.data_record = f'{mode}-clouds'
        precip.data_field = 'precipitation'
        precipitation.traces.append(precip)


        visibility = TimeSeries.Graph()
        visibility.title = "Visibility"
        visibility.contamination = f'{mode}-contamination'
        self.graphs.append(visibility)

        km = TimeSeries.Axis()
        km.title = "km"
        km.format_code = '.3f'
        km.range = 0
        visibility.axes.append(km)

        vis = TimeSeries.Trace(km)
        vis.legend = "Visibility"
        vis.data_record = f'{mode}-clouds'
        vis.data_field = 'visibility'
        visibility.traces.append(vis)


        radiation = TimeSeries.Graph()
        radiation.title = "Solar Radiation"
        radiation.contamination = f'{mode}-contamination'
        self.graphs.append(radiation)

        w_m2 = TimeSeries.Axis()
        w_m2.title = "W/m²"
        w_m2.format_code = '.1f'
        radiation.axes.append(w_m2)

        intensity = TimeSeries.Trace(w_m2)
        intensity.legend = "Solar Radiation"
        intensity.data_record = f'{mode}-clouds'
        intensity.data_field = 'radiation'
        radiation.traces.append(intensity)
