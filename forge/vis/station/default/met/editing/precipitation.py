import typing
from forge.vis.view.timeseries import TimeSeries
from ..precipitation import Precipitation


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
        mmh.range = 0
        raw.axes.append(mmh)

        mm = TimeSeries.Axis()
        mm.title = "mm"
        mm.format_code = '.2f'
        mm.range = 0
        raw.axes.append(mm)

        rate = TimeSeries.Trace(mmh)
        rate.legend = "Raw Rate"
        rate.data_record = f'{profile}-raw-precipitation'
        rate.data_field = 'precipitation'
        raw.traces.append(rate)

        total = TimeSeries.Trace(mm)
        total.legend = "Raw Total"
        total.data_record = f'{profile}-raw-precipitation'
        total.data_field = 'precipitation'
        total.script_incoming_data = Precipitation.ACCUMULATE_INCOMING
        raw.traces.append(total)


        edited = TimeSeries.Graph()
        edited.title = "Edited"
        self.graphs.append(edited)

        mmh = TimeSeries.Axis()
        mmh.title = "mm/h"
        mmh.format_code = '.2f'
        mmh.range = 0
        edited.axes.append(mmh)

        mm = TimeSeries.Axis()
        mm.title = "mm"
        mm.format_code = '.2f'
        mm.range = 0
        edited.axes.append(mm)

        rate = TimeSeries.Trace(mmh)
        rate.legend = "Edited Rate"
        rate.data_record = f'{profile}-editing-precipitation'
        rate.data_field = 'precipitation'
        edited.traces.append(rate)

        total = TimeSeries.Trace(mm)
        total.legend = "Edited Total"
        total.data_record = f'{profile}-editing-precipitation'
        total.data_field = 'precipitation'
        total.script_incoming_data = Precipitation.ACCUMULATE_INCOMING
        edited.traces.append(total)
