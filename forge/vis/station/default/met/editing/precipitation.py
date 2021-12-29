import typing
from forge.vis.view.timeseries import TimeSeries


class EditingPrecipitation(TimeSeries):
    def __init__(self, profile: str = 'met', **kwargs):
        super().__init__(**kwargs)
        self.title = "Precipitation"

        raw = TimeSeries.Graph()
        raw.title = "Raw"
        self.graphs.append(raw)

        mmh = TimeSeries.Axis()
        mmh.title = "mm/h"
        mmh.format_code = '.2f'
        raw.axes.append(mmh)

        rate = TimeSeries.Trace(mmh)
        rate.legend = "Raw"
        rate.data_record = f'{profile}-raw-precipitation'
        rate.data_field = 'precipitation'
        raw.traces.append(rate)


        edited = TimeSeries.Graph()
        edited.title = "Edited"
        self.graphs.append(edited)

        mmh = TimeSeries.Axis()
        mmh.title = "mm/h"
        mmh.format_code = '.2f'
        edited.axes.append(mmh)

        rate = TimeSeries.Trace(mmh)
        rate.legend = "Edited"
        rate.data_record = f'{profile}-editing-precipitation'
        rate.data_field = 'precipitation'
        edited.traces.append(rate)
